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

See this [ToolOrchestra request trace](https://gist.github.com/ishandhanani/31ffa697ca9d068624f280f10c19d45d) for a complete example with 75 sessions and 149 requests.

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

## How Scheduling Works

Each `request_end` row becomes one replay request. `session_id` orders turns in a session, while `parent_session_id` identifies child sessions. Prompt hashes and token lengths reproduce workload shape without storing request content.

DynoSim starts root requests at their recorded timestamps. Dependent requests wait for their predecessors, then for the recorded agent and tool delay. See [DynoSim Runs](../dynosim/runs.md) for the row schema and other replay modes.

## Replay Live with AIPerf

**Experimental.** This path currently requires [AIPerf PR #3](https://github.com/ajcasagrande/aiperf/pull/3). Its converter reads uncompressed JSONL:

```bash
gzip -cd /tmp/dynamo-request-trace.*.jsonl.gz > /tmp/dynamo-request-trace.jsonl
aiperf synthesize dynamo-trace /tmp/dynamo-request-trace.jsonl --output /tmp/dynamo-weka
```

Replay `/tmp/dynamo-weka` with AIPerf's fixed schedule and Dynamo header transport by following the [Weka replay guide](https://github.com/ishandhanani/aiperf/blob/idhanani/agentx-dynamo-trajectories/docs/tutorials/weka-trace.md#replay-a-dynamo-request-trace).

The conversion preserves request timing, token lengths, prompt hashes, and direct parent-child relationships. Root sessions target their recorded timestamps; later turns also wait for the previous request to finish, so a slower endpoint produces positive schedule drift instead of overlapping a session's turns.

AIPerf synthesizes prompt content from the hashes and sends recorded output length as `max_tokens`; it does not replay the original model response or execute tools.
