"""Regression tests for the opencode OpenAPI contract fingerprint."""

from __future__ import annotations

import copy
import unittest

from scripts.check_opencode_schema_drift import fingerprint, has_drift, summarize_diff


def _document() -> dict:
    return {
        "paths": {
            "/session": {
                "post": {
                    "description": "Create a session.",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Session"}
                            }
                        }
                    },
                    "responses": {"200": {"description": "OK"}},
                }
            }
        },
        "components": {
            "schemas": {
                "Session": {
                    "description": "A session.",
                    "type": "object",
                    "properties": {
                        "mode": {
                            "anyOf": [
                                {"type": "boolean"},
                                {"type": "string", "enum": ["reasoning"]},
                            ]
                        }
                    },
                }
            }
        },
    }


class ContractFingerprintTests(unittest.TestCase):
    def test_union_shape_change_is_drift(self) -> None:
        snapshot = _document()
        live = copy.deepcopy(snapshot)
        live["components"]["schemas"]["Session"]["properties"]["mode"]["anyOf"].append(
            {"type": "string"}
        )

        diff = summarize_diff(fingerprint(snapshot), fingerprint(live))

        self.assertTrue(has_drift(diff))
        self.assertEqual(diff["schemas_changed"], ["Session"])

    def test_request_contract_change_is_drift(self) -> None:
        snapshot = _document()
        live = copy.deepcopy(snapshot)
        live["paths"]["/session"]["post"]["requestBody"]["required"] = True

        diff = summarize_diff(fingerprint(snapshot), fingerprint(live))

        self.assertTrue(has_drift(diff))
        self.assertEqual(diff["operations_changed"], ["POST /session"])

    def test_documentation_and_key_order_are_ignored(self) -> None:
        snapshot = _document()
        live = copy.deepcopy(snapshot)
        live["paths"]["/session"]["post"]["description"] = "New wording."
        live["components"]["schemas"]["Session"]["description"] = "New wording."

        diff = summarize_diff(fingerprint(snapshot), fingerprint(live))

        self.assertFalse(has_drift(diff))


if __name__ == "__main__":
    unittest.main()
