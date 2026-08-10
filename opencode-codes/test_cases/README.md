# Captured wire fixtures

Real HTTP/SSE payloads captured live from opencode 1.18.5 and 1.18.15 servers.
They are the type-level drift tripwire consumed by
`tests/deserialization_tests.rs`: every file must deserialize losslessly into
the generated `opencode_codes::protocol_generated::types` and re-serialize to a
subset of its original wire JSON.

Do not hand-edit these files. To refresh them, re-run the captures below against
a live server and re-run the deserialization suite.

## Provenance

The original fixtures were captured 2026-07-26 against 1.18.5 by driving the
initial client endpoints with `curl`. The server had a working provider
(`providerID: "opencode"`, model `big-pickle`), so assistant turns carry real
reasoning/text/step parts rather than a provider error.

The `_1_18_15` fixtures were captured 2026-08-10 in one bounded authenticated
workflow using `opencode/big-pickle`. The prompt requested one `bash` tool call
behind an explicit permission and one `question` tool call. The harness replied
to both and waited for `session.idle`, then reconciled with the REST message
list. The event stream keeps every non-delta frame plus one representative
`message.part.delta` frame. Session/message/request ids, timestamps, and wire
payloads are retained. Only the local username and random temporary-directory
suffix were normalized; no API credentials or authentication headers were
captured.

## Contents

### `rest/` — REST request/response bodies

| File | Endpoint | Type it deserializes into |
|------|----------|---------------------------|
| `session_create.json` | `POST /session` (200) | `Session` |
| `session_status_empty.json` | `GET /session/status` (200, idle) | `BTreeMap<String, SessionStatus>` |
| `session_status_busy.json` | `GET /session/status` (200, mid-prompt) | `BTreeMap<String, SessionStatus>` |
| `messages_empty.json` | `GET /session/{id}/message` (200, fresh session) | `Vec<MessageWithParts>` |
| `messages_after_prompt.json` | `GET /session/{id}/message` (200, after a prompt) | `Vec<MessageWithParts>` |
| `abort_response.json` | `POST /session/{id}/abort` (200) | `bool` |
| `session_create_1_18_15.json` | `POST /session` (200) | `Session` |
| `messages_tool_question_1_18_15.json` | `GET /session/{id}/message` after permission + question workflow | `Vec<MessageWithParts>` |

`POST /session/{id}/prompt_async` is not represented by a response body: it
answers **HTTP 204 No Content**. Its effect is observed in
`messages_after_prompt.json` and the event stream.

### `events/` — `GET /event` Server-Sent Events

`event_stream.jsonl` is one captured SSE `data:` payload per line (the frames
observed while a prompt was submitted on a second session). Each line
deserializes into the internally-tagged `Event` enum. Per-type pretty-printed
samples (`type_*.json`) are the first occurrence of each distinct event type,
kept for readable inspection. Types exercised: `server.connected`,
`session.updated`, `message.updated`, `message.part.updated`,
`message.part.delta`, `session.status`, `session.diff`, `session.idle`.

`event_stream_tool_question_1_18_15.jsonl` adds `session.created`,
`permission.asked`, `permission.replied`, `question.asked`, and
`question.replied`, as well as the pending/running/completed transitions of
both tool calls. Suffixed per-type files are readable samples from that stream.

### `errors/` — error responses

| File | How produced | Type |
|------|--------------|------|
| `session_not_found.json` | any per-session call with an unknown session id (404) | `NotFoundError` |

## Recapture recipe

```bash
BASE=http://127.0.0.1:41999
SID=$(curl -s -X POST $BASE/session -H 'Content-Type: application/json' \
  -d '{"title":"fixture"}' | tee rest/session_create.json | jq -r .id)
curl -s "$BASE/session/status" > rest/session_status_empty.json
curl -s "$BASE/session/$SID/message" > rest/messages_empty.json
curl -s -X POST "$BASE/session/$SID/prompt_async" -H 'Content-Type: application/json' \
  -d '{"parts":[{"type":"text","text":"Say hello in one word."}]}'   # HTTP 204
sleep 4
curl -s "$BASE/session/$SID/message" > rest/messages_after_prompt.json
curl -s -X POST "$BASE/session/$SID/abort" > rest/abort_response.json   # -> true
curl -s -N --max-time 8 "$BASE/event" \
  | sed -n 's/^data: //p' > events/event_stream.jsonl
curl -s "$BASE/session/ses_doesnotexist000000000000/message" > errors/session_not_found.json
```
