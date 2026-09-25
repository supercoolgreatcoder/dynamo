# Envoy callout parity against the preserved Claude prototype

This report records the September 23, 2026 vCluster benchmark of the refactored
component pipeline. All deployments and benchmark Jobs ran in vCluster namespace
`dynamo-components-v2`; nothing was deployed directly into the host namespace.

## Result

The Envoy generic callout path reaches parity with the preserved Claude prototype.

| Workload | Refactored callout | Claude reference | Difference | Interpretation |
|---|---:|---:|---:|---|
| Short capacity | 11,272 RPS | 11,862 RPS | -5.0% | parity within Claude's documented ~6% single-cell spread |
| ISL4000 | 9,082 RPS | no callout reference | n/a | absolute characterization |
| Mooncake | 3,022 RPS | 3,018 RPS | +0.1% | offered-load parity |

The short and ISL4000 capacity runs each used six AIPerf clients at concurrency 128
(768 aggregate), 45 seconds, and a 3/3 client split across two nodes. Both completed
with zero AIPerf errors and zero cancellations. Envoy logged no pipeline failure,
consumer-lag, dropped-frame, panic, or error messages.

The frozen prompt files were synthesized once with AIPerf 0.12.0's own `sonnet`
generator and the Qwen2.5-0.5B tokenizer, then converted to AIPerf's native
`raw_payload` input so measured clients replay complete OpenAI request bodies without
tokenizing or formatting them on the hot path.

| Dataset | Rows | SHA256 | Shape |
|---|---:|---|---|
| `short-claude-sonnet-raw.jsonl` | 4,096 | `ab030551a31fb4ac8e7a864a940862a68bc0d386b152b4aba7bc249322833426` | ISL mean 128, stddev 16; OSL 50 |
| `isl4000-claude-sonnet-raw.jsonl` | 4,096 | `3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743` | ISL mean 4,000, stddev 100; OSL 50 |

The six per-client AIPerf JSON, CSV, and console exports for each run are
committed in `results/2026-09-23-callout-parity/raw_aiperf/`, grouped by the
Job names below. Summing `request_throughput.avg` across the six JSON exports
reproduces 11,272.38 short, 9,081.89 ISL4000, and 3,021.73 Mooncake RPS. All
18 exports have an empty `error_summary` and `was_cancelled: false`. These are
client summary exports, not per-request traces. The larger AIPerf diagnostic
logs remain on persistent vCluster storage:

- `/shared/aiperf/results/raw-short-envoy-callout-r43/{0..5}`
- `/shared/aiperf/results/raw-isl4000-envoy-callout-r44/{0..5}`
- `/shared/aiperf/results/moonparity-envoy-callouts-parity-r17/{0..5}`

Native `raw_payload` replay deliberately omits AIPerf's derived ISL/OSL metrics. The
same structured datasets were separately replayed through the equivalent request path
and produced exactly 50 output tokens per request. The mock worker is fixed at the
requested output length, and the final raw-payload runs had no gateway drops or errors.

## Why Mooncake stops near 3,000 RPS

This is not an Envoy or callout capacity ceiling. The rescaled trace has 22,699
requests over 45 seconds, or 504.4 RPS per replay client. Six clients therefore offer
about 3,026 RPS. The measured 3,022 RPS is 99.9% of that offered rate. The short
capacity run demonstrates more than 11,000 RPS through the same callout implementation.

At the Mooncake point, each request also returns about 171 output-token chunks, so the
gateway transports more than 500,000 streamed token frames per second while keeping up
with the entire trace.

## Implementation change

The generic gRPC transport now decodes the configured protobuf `bytes` response-body
field directly from protobuf wire bytes. It avoids materializing a reflective
`DynamicMessage`, converting the whole message to JSON, and base64-decoding it for
every streamed token. A descriptor-driven reflective fallback preserves generality for
all other response shapes.

The Envoy callout completion path now drains its bounded response backlog asynchronously
instead of dropping queued frames when the upstream stream completes in the same Envoy
callback. The bound is 8,192 frames, which covers a complete long generation delivered
in one callback while retaining bounded memory.

These are transport/facade changes. Canonical request rendering, tokenization, request
normalization, and worker behavior remain supplied by Dynamo crates, principally
`dynamo-llm::preprocessor::OpenAIPreprocessor`; the facade does not maintain a parallel
model implementation.

## Diagnostics excluded from the result

Several preserved runs diagnosed benchmark-harness effects and are not decision-grade:

- Jobs without topology spreading sometimes placed all six clients on one node and
  measured only 7,600-8,000 RPS. Enforcing `maxSkew: 1` restored 11,000+ RPS.
- AIPerf 0.12.0's structured-conversation mmap path occasionally produced one or two
  `MemoryMapSerializationError` records in more than 400,000 requests. Native
  `raw_payload` replay removed that decoder and completed with zero errors.
- Disabling mmap entirely made AIPerf the bottleneck and is not a gateway capacity test.

The Mooncake direct and non-callout generic parity points collected with the same binary
were 3,005 RPS for Envoy direct and 3,024 RPS for AGW generic, against Claude references
of 3,014 and 3,015 respectively. A full two-pass recreation of Claude's static,
Dynamo-frontend, and disaggregated matrix was not rerun in this campaign.
