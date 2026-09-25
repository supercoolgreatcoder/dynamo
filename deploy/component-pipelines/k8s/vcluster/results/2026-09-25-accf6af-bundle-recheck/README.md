<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Exact Nix-bundle mocker recheck

This campaign runs the complete Nix bundle
`/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699`
inside the vCluster at `https://gateway-poc.mkhadkevich-dev:443`, namespace
`dynamo-components-v2`. The bundle is built from
`dynamo-nix-envs` branch `feat/dynamo-component-pipeline-builds` commit
`7024cac`, pinning Dynamo graph/facade source `accf6af699`, Agentgateway
`b14ca87d0a`, Envoy `1f921705de`, and the unchanged Envoy ABI patch source
`98676215fa`. The 25 applied benchmark resources are captured in
[`benchmark_topology.json`](benchmark_topology.json); its server-side dry-run
passed after capture. Envoy callout worker addresses are dynamic Pod IPs and
must be refreshed after replaying that topology.

## Three-workload, three-run matrix

The four arms ran in three interleaved rotations `r28`–`r30`, one gateway
replica at a time. Each Job had six AIPerf 0.12.0 clients, three on each of
the two dedicated client nodes, concurrency 128 per client, a common
90-second start barrier, and a 45-second measured phase. The four Dynamo
`fastokens` preprocessors, one InferencePool selector, and 16 synthetic
facade workers were unchanged between arms. Clients replayed the frozen raw
short payloads without synthesizing or tokenizing prompts during measurement.

| Gateway | New short median RPS | Three-run range | Prior facade median RPS |
|---|---:|---:|---:|
| AGW static | 12,339.69 | 12,252.67–12,418.55 | 11,783.14 |
| AGW generic | 12,396.61 | 11,881.83–12,433.04 | 11,733.00 |
| Envoy generic/direct | 12,726.63 | 12,495.05–12,807.76 | 11,913.69 |
| Envoy generic/callouts | **12,970.74** | 12,709.86–13,011.78 | 12,321.92 |

All 12 Jobs had six complete exports, zero AIPerf request errors, no
cancellations, and six retained Pod placements. The raw JSON, CSV, and
console exports are under [`raw_aiperf/`](raw_aiperf/); the exact commands
and placements are in [`benchmark_short_execution.json`](benchmark_short_execution.json),
and per-Job metrics and medians in
[`benchmark_short_summary.json`](benchmark_short_summary.json). The prior
points are from the [three-run 846821 facade recheck](../2026-09-25-facade-846821-recheck/README.md)
on the same frozen policy. The 5–7% descriptive uplift is not a causal
binary A/B: the campaigns were not interleaved with each other, and
six-client closed-loop throughput does not establish a single-gateway
saturation ceiling.

The frozen short payload SHA256 is
`ab030551a31fb4ac8e7a864a940862a68bc0d386b152b4aba7bc249322833426`.
The client tokenizer identity is the pinned
`wjq1b3wfjpzak4yd4rmj9arwqk1gkiir-qwen-tokenizer` Nix output, but
AIPerf used server token counts and did no measured-phase tokenization.
The parent [vCluster guide](../../README.md) records the exact build,
staging, rollout, environment variables, Job names, and summary commands.

The identical Nix bundle also completed three rotations each for ISL4000 and
Mooncake. Every one of the 36 Jobs had six successful AIPerf clients, no
reported request errors or cancellations, and six retained Pod placements on
the same two client nodes. The exact Job commands and placements are in
[`benchmark_execution.json`](benchmark_execution.json); all 36 normalized
per-Job values and medians are in
[`benchmark_summary.json`](benchmark_summary.json).

| Gateway | ISL4000 median RPS | Three-run range | Prior facade median RPS |
|---|---:|---:|---:|
| AGW static | 8,930.00 | 8,791.37–9,463.95 | 8,902.99 |
| AGW generic | 10,707.87 | 10,627.69–10,725.11 | 10,592.59 |
| Envoy generic/direct | 10,550.35 | 10,483.96–10,730.96 | 10,518.55 |
| Envoy generic/callouts | **11,768.32** | 11,266.46–11,788.90 | 11,379.44 |

ISL4000 uses AIPerf's concurrency-driven phase, not a fixed request schedule.
AGW static's highest run is visibly above its other two; the range is retained
and the median is used rather than silently discarding it. The prior medians
are historical context, not a causal old/new A/B, because the two campaigns
were not interleaved with each other.

| Gateway | Mooncake median RPS | Three-run range | Prior facade median RPS |
|---|---:|---:|---:|
| AGW static | 3,023.55 | 3,023.15–3,023.80 | 3,023.76 |
| AGW generic | 3,023.34 | 3,023.31–3,023.40 | 3,023.52 |
| Envoy generic/direct | 3,023.62 | 3,021.72–3,024.03 | 3,023.40 |
| Envoy generic/callouts | 3,023.68 | 3,023.67–3,023.86 | 3,023.67 |

Mooncake replays a fixed schedule of 22,699 requests per client over 45 seconds:
six clients offer at most about 3,026.53 requests/second. All four gateways
reproduce the prior offered-rate result; this **does not establish a 3,000-RPS
single-gateway saturation ceiling**. A higher-offered-load campaign must be
a separate benchmark series. The retained AIPerf exports are summary-level,
not per-request JSONL, so request-ID completeness, exact per-request
ISL/OSL, and custom tail percentiles are not independently auditable here.
These values are descriptive achieved-throughput evidence, not a
promotion-grade latency or SLO verdict.

The real-vLLM P/D correctness smoke is
[recorded separately](../2026-09-25-real-vllm-pd/README.md) and is not a
mocker throughput number.
