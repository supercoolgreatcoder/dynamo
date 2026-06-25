---
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
title: Agent Simulation
subtitle: Replay agent request traces with DynoSim or AIPerf
---

Capture an agent workload once, then use the request trace in either of two ways:

- Convert it to Agentic Mooncake and simulate it offline with DynoSim.
- Convert it to an AIPerf dataset and replay it against a live endpoint.

A request trace stores token lengths, prompt hashes, timing, and session identity. It does not store prompts, responses, or tool arguments. Start with [Agent Tracing](agent-tracing.md) to collect request rows.

## Collect a Trace

Set `DYN_REQUEST_TRACE=1` while running the agent workload. This writes compressed JSONL to `/tmp/dynamo-request-trace.*.jsonl.gz` by default.

For tool timing fidelity, publish explicit tool events over the optional ZMQ ingress described in [Agent Tracing](agent-tracing.md#tool-call-observability). Without tool events, replay preserves the full gap between adjacent LLM requests but cannot separate tool time from agent overhead.

## Convert to Agentic Mooncake

**Experimental.** The converter uses Dynamo `request_end` rows for request timing, token lengths, worker placement, and replay hashes. It also uses terminal harness tool rows (`tool_end` / `tool_error`) to preserve tool-wait time between dependent LLM requests.

Replay ignores non-replay request fields such as `finish_reason_metadata`; use the Perfetto view in [Agent Tracing](agent-tracing.md#view-traces-in-perfetto) when you want to inspect final finish reasons, backend stop signals, or complete tool-call metadata inside the trace.

```bash
cargo run -p dynamo-bench --bin request_trace_to_mooncake -- \
  --agentic \
  --input-path /tmp/dynamo-request-trace.*.jsonl.gz \
  --output-file /tmp/dynamo-request-trace.agentic-mooncake.jsonl
```

## Replay Offline

The converter prints `trace_block_size`. Pass that value to `--trace-block-size` so hash segmentation matches the capture. The example also uses it as the mock engine block size for a simple smoke test; the two settings are otherwise independent.

```bash
TRACE_BLOCK_SIZE=128
python -m dynamo.replay /tmp/dynamo-request-trace.agentic-mooncake.jsonl \
  --trace-format agentic_mooncake \
  --trace-block-size "${TRACE_BLOCK_SIZE}" \
  --replay-mode offline \
  --router-mode kv_router \
  --num-workers 4 \
  --extra-engine-args "{\"block_size\":${TRACE_BLOCK_SIZE}}" \
  --report-json /tmp/dynamo-request-trace.replay-report.json
```

`kv_router` needs at least two mock workers. For a single-worker smoke test, use `--router-mode round_robin --num-workers 1`.

## Agentic Row Semantics

Agentic Mooncake rows preserve:

- `request_id`: the LLM request row identity.
- Mooncake `session_id`: derived from the Dynamo `session_id`.
- `wait_for`: request IDs that must complete before this row becomes eligible.
- `branches`: child request IDs spawned from this row.
- `prefix_reset`: first request in a session.
- `delay`: non-tool delay after dependencies finish.
- `tool_wait_ms`: tool time after dependencies finish, parallel-aware as the union of overlapping spans rather than their sum.
- `tool_events`: per-tool spans attributed to this LLM request, each carrying `tool_call_id`, `tool_class`, `status`, `started_at_unix_ms`, `ended_at_unix_ms`, `duration_ms`, and optional `output_bytes`, `output_tokens`, or `error_type`.
- `hash_ids`, `input_length`, and `output_length`: prompt-prefix and length data for mocker replay.

Rows with no `wait_for` use their `timestamp` as the replay start time. Rows with dependencies wait for all listed requests to complete, then wait `delay + tool_wait_ms` before dispatch. For more flags and engine settings, see [DynoSim Runs](../dynosim/runs.md).

## Replay Live with AIPerf

**Experimental.** This path currently requires [AIPerf PR #3](https://github.com/ajcasagrande/aiperf/pull/3). Its converter reads uncompressed JSONL:

```bash
gzip -cd /tmp/dynamo-request-trace.*.jsonl.gz > /tmp/dynamo-request-trace.jsonl
aiperf synthesize dynamo-trace /tmp/dynamo-request-trace.jsonl --output /tmp/dynamo-weka
```

Replay `/tmp/dynamo-weka` with AIPerf's fixed schedule and Dynamo header transport by following the [Weka replay guide](https://github.com/ishandhanani/aiperf/blob/idhanani/agentx-dynamo-trajectories/docs/tutorials/weka-trace.md#replay-a-dynamo-request-trace).

The conversion preserves request timing, token lengths, prompt hashes, and direct parent-child relationships. AIPerf synthesizes prompt content from the hashes and sends recorded output length as `max_tokens`; it does not replay the original model response or execute tools.
