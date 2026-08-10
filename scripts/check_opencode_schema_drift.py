#!/usr/bin/env python3
"""
Diff our snapshotted opencode OpenAPI spec against the LIVE spec served by the
installed `opencode` binary.

Unlike the codex drift check (which fetches a schema from a GitHub URL) and the
claude one (which extracts zod definitions from a compiled ELF), opencode's wire
contract lives behind its own HTTP server: `GET /doc` returns the full OpenAPI
3.1 document. So the source of truth here is a locally-running server. We either
spawn `opencode serve` on a free port ourselves, or (with --url) talk to a server
someone already started.

We reduce both the live document and our snapshot to a format-invariant
contract fingerprint: path and operation sets, complete request/response
operation shapes, and complete component schema shapes. Documentation-only
fields are removed before comparison. This catches changes to unions, enums,
required fields, request bodies, and responses while ignoring key ordering,
pretty-printing, descriptions, summaries, and examples.

Writes a structured Markdown report to stdout and exits:
  0 — no drift
  1 — drift detected; report on stdout describes what changed
  2 — could not obtain the live spec (binary missing, spawn/fetch failed); a
      transient/soft skip that CI treats separately so we don't open spurious
      issues on infra blips

Usage:
  python3 scripts/check_opencode_schema_drift.py            # spawn opencode serve
  python3 scripts/check_opencode_schema_drift.py --url http://127.0.0.1:41999
  python3 scripts/check_opencode_schema_drift.py --update   # rewrite the snapshot

The local snapshot lives at:
  opencode-codes/tests/schemas/opencode_openapi.json
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = ROOT / "opencode-codes" / "tests" / "schemas" / "opencode_openapi.json"

# Where the npm global install and the workspace-local install drop the binary,
# checked in order after $PATH. CI installs opencode globally, so `opencode` on
# $PATH is the common case.
BIN_CANDIDATES = [
    "~/.local/opencode-npm/node_modules/.bin/opencode",
]

DOC_PATH = "/doc"
READY_TIMEOUT_S = 30.0
READY_POLL_S = 0.2


# ──────────────────────────────────────────────────────────────────────────
# Obtaining the live spec
# ──────────────────────────────────────────────────────────────────────────


def _basic_auth_header() -> dict[str, str]:
    """HTTP Basic header when OPENCODE_SERVER_PASSWORD is set (username defaults
    to "opencode", matching the crate)."""
    password = os.environ.get("OPENCODE_SERVER_PASSWORD")
    if not password:
        return {}
    import base64

    user = os.environ.get("OPENCODE_SERVER_USERNAME", "opencode")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def fetch_doc(base_url: str, timeout: float = 10.0) -> dict[str, Any]:
    url = base_url.rstrip("/") + DOC_PATH
    headers = {"User-Agent": "opencode-codes-drift-check", **_basic_auth_header()}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


def resolve_binary(explicit: str | None) -> str | None:
    if explicit:
        return explicit if Path(explicit).expanduser().exists() else None
    found = shutil.which("opencode")
    if found:
        return found
    for cand in BIN_CANDIDATES:
        p = Path(cand).expanduser()
        if p.exists():
            return str(p)
    return None


def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
    finally:
        s.close()


def spawn_and_fetch(binary: str) -> dict[str, Any]:
    """Start `opencode serve` on a free port, wait for /doc, return the parsed
    document, and always tear the server down."""
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    proc = subprocess.Popen(
        [binary, "serve", "--port", str(port), "--hostname", "127.0.0.1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.monotonic() + READY_TIMEOUT_S
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError(
                    f"opencode serve exited early with code {proc.returncode}"
                )
            try:
                return fetch_doc(base_url, timeout=2.0)
            except (urllib.error.URLError, ConnectionError, OSError) as e:
                last_err = e
                time.sleep(READY_POLL_S)
        raise TimeoutError(
            f"opencode server did not answer {base_url}{DOC_PATH} within "
            f"{READY_TIMEOUT_S:.0f}s (last error: {last_err})"
        )
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def obtain_live_spec(args: argparse.Namespace) -> dict[str, Any]:
    """Return the live OpenAPI document or raise. Raising is mapped to the exit-2
    soft skip by the caller."""
    if args.url:
        return fetch_doc(args.url)
    binary = resolve_binary(args.opencode_bin)
    if not binary:
        raise RuntimeError(
            "no `opencode` binary found on $PATH or known install locations; "
            "pass --opencode-bin or --url"
        )
    return spawn_and_fetch(binary)


# ──────────────────────────────────────────────────────────────────────────
# Fingerprinting + diffing
# ──────────────────────────────────────────────────────────────────────────


HTTP_METHODS = {"delete", "get", "head", "options", "patch", "post", "put", "trace"}
DOCUMENTATION_KEYS = {"description", "example", "examples", "externalDocs", "summary"}


def _contract_shape(node: Any) -> Any:
    """Return a deterministic, documentation-free OpenAPI contract subtree."""
    if isinstance(node, dict):
        return {
            key: _contract_shape(value)
            for key, value in sorted(node.items())
            if key not in DOCUMENTATION_KEYS
        }
    if isinstance(node, list):
        return [_contract_shape(item) for item in node]
    return node


def fingerprint(doc: dict[str, Any]) -> dict[str, Any]:
    path_items = doc.get("paths") or {}
    paths = sorted(path_items)
    operations: dict[str, Any] = {}
    for path, path_item in path_items.items():
        if not isinstance(path_item, dict):
            continue
        inherited_parameters = path_item.get("parameters")
        for method, operation in path_item.items():
            if method.lower() not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            shape: dict[str, Any] = {"operation": operation}
            if inherited_parameters is not None:
                shape["path_parameters"] = inherited_parameters
            operations[f"{method.upper()} {path}"] = _contract_shape(shape)

    schemas = (doc.get("components") or {}).get("schemas") or {}
    return {
        "paths": paths,
        "operations": dict(sorted(operations.items())),
        "schemas": {
            name: _contract_shape(schema) for name, schema in sorted(schemas.items())
        },
    }


def summarize_diff(snapshot_fp: dict[str, Any], live_fp: dict[str, Any]) -> dict[str, Any]:
    snap_paths = set(snapshot_fp["paths"])
    live_paths = set(live_fp["paths"])

    snap_operations = snapshot_fp["operations"]
    live_operations = live_fp["operations"]
    snap_operation_names = set(snap_operations)
    live_operation_names = set(live_operations)

    snap_schemas = snapshot_fp["schemas"]
    live_schemas = live_fp["schemas"]
    snap_names = set(snap_schemas)
    live_names = set(live_schemas)

    return {
        "paths_added": sorted(live_paths - snap_paths),
        "paths_removed": sorted(snap_paths - live_paths),
        "operations_added": sorted(live_operation_names - snap_operation_names),
        "operations_removed": sorted(snap_operation_names - live_operation_names),
        "operations_changed": sorted(
            name
            for name in snap_operation_names & live_operation_names
            if snap_operations[name] != live_operations[name]
        ),
        "schemas_added": sorted(live_names - snap_names),
        "schemas_removed": sorted(snap_names - live_names),
        "schemas_changed": sorted(
            name
            for name in snap_names & live_names
            if snap_schemas[name] != live_schemas[name]
        ),
    }


def has_drift(diff: dict[str, Any]) -> bool:
    return any(
        diff[k]
        for k in (
            "paths_added",
            "paths_removed",
            "operations_added",
            "operations_removed",
            "operations_changed",
            "schemas_added",
            "schemas_removed",
            "schemas_changed",
        )
    )


# ──────────────────────────────────────────────────────────────────────────
# Rendering
# ──────────────────────────────────────────────────────────────────────────


def _bullet_section(header: str, items: list[str]) -> list[str]:
    if not items:
        return []
    lines = [f"**{header}** ({len(items)}):", ""]
    for it in items[:80]:
        lines.append(f"- `{it}`")
    if len(items) > 80:
        lines.append(f"- ... and {len(items) - 80} more")
    lines.append("")
    return lines


def render_markdown(diff: dict[str, Any], source: str) -> str:
    lines = [
        "# opencode OpenAPI schema drift report",
        "",
        "Comparing `opencode-codes/tests/schemas/opencode_openapi.json` "
        f"against the live spec from {source}.",
        "",
    ]
    lines += _bullet_section("Paths added upstream", diff["paths_added"])
    lines += _bullet_section("Paths removed upstream", diff["paths_removed"])
    lines += _bullet_section("Operations added upstream", diff["operations_added"])
    lines += _bullet_section("Operations removed upstream", diff["operations_removed"])
    lines += _bullet_section("Operations whose contract changed", diff["operations_changed"])
    lines += _bullet_section("Component schemas added upstream", diff["schemas_added"])
    lines += _bullet_section("Component schemas removed upstream", diff["schemas_removed"])
    lines += _bullet_section("Component schemas whose contract changed", diff["schemas_changed"])

    lines.append("---")
    lines.append("")
    lines.append("Regenerate the snapshot + typed bindings with:")
    lines.append("")
    lines.append("```bash")
    lines.append("python3 scripts/check_opencode_schema_drift.py --update")
    lines.append("python3 scripts/codegen_opencode.py  # regenerate typed structs + samples")
    lines.append("```")
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────────
# Snapshot IO
# ──────────────────────────────────────────────────────────────────────────


def canonical_json(doc: dict[str, Any]) -> str:
    # Preserve the server's schema/property order because code generation uses
    # it to assign stable names to structurally deduplicated inline types. The
    # drift fingerprint itself normalizes dictionaries, so comparison remains
    # independent of wire ordering and whitespace.
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False) + "\n"


def write_snapshot(doc: dict[str, Any]) -> None:
    SNAPSHOT.write_text(canonical_json(doc), encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--url",
        default=None,
        help="Base URL of an already-running opencode server (e.g. http://127.0.0.1:41999). "
        "If omitted, `opencode serve` is spawned on a free port.",
    )
    ap.add_argument(
        "--opencode-bin",
        default=None,
        help="Path to the opencode binary (default: $PATH, then known install locations).",
    )
    ap.add_argument(
        "--update",
        action="store_true",
        help="Rewrite the snapshot from the live spec in canonical form and exit 0.",
    )
    args = ap.parse_args()

    try:
        live_doc = obtain_live_spec(args)
    except (
        RuntimeError,
        TimeoutError,
        urllib.error.URLError,
        urllib.error.HTTPError,
        json.JSONDecodeError,
        OSError,
    ) as e:
        print(f"error: could not obtain live opencode spec: {e}", file=sys.stderr)
        return 2

    if args.update:
        write_snapshot(live_doc)
        # Re-save in canonical form (idempotent) so any older byte layout is
        # normalized and future diffs stay clean.
        write_snapshot(json.loads(SNAPSHOT.read_text(encoding="utf-8")))
        print(f"Updated snapshot at {SNAPSHOT} ({len(live_doc.get('paths', {}))} paths, "
              f"{len((live_doc.get('components') or {}).get('schemas') or {})} schemas).")
        return 0

    try:
        snapshot_doc = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"error: local snapshot missing at {SNAPSHOT}", file=sys.stderr)
        return 2

    diff = summarize_diff(fingerprint(snapshot_doc), fingerprint(live_doc))
    source = args.url if args.url else "a spawned `opencode serve`"

    if not has_drift(diff):
        print(
            "# opencode OpenAPI schema drift report\n\n"
            f"No drift: {len(fingerprint(live_doc)['paths'])} paths and "
            f"{len(fingerprint(live_doc)['schemas'])} component schemas match the snapshot "
            f"(source: {source})."
        )
        return 0

    print(render_markdown(diff, source))
    return 1


if __name__ == "__main__":
    sys.exit(main())
