<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Prepared Envoy module frozen-workload parity

This campaign reuses the six-client frozen short/ISL4000 raw-payload and
prepared Mooncake workloads from the Nix-built mocker matrix. The active
gateway is one Envoy-direct replica using the prepared-output module built
from Dynamo `98676215fa` by the envs flake `8a612b7`. Four preprocessors,
one selector, and 16 mock workers remain Ready. AIPerf jobs run only inside
the `dynamo-components-v2` vCluster namespace, sequentially, and export their
raw summaries plus Job/Pod placement evidence here.

The six-client jobs were `nixv2-short-envoy-generic-r7`,
`nixv2-isl4000-envoy-generic-r12`, and
`nixv2-mooncake-envoy-generic-r5`. Each ran for approximately 45 seconds
with concurrency 128 per client, AIPerf 0.12.0, streaming OpenAI chat,
server-side token counts, and a shared start barrier. All 18 client Pods
completed with zero reported request errors or cancellations. The retained
[`benchmark_execution.json`](benchmark_execution.json) has all 18 Pod
placements and the executed Job commands; the six per-client JSON/CSV/console
exports per workload are under [`raw_aiperf/`](raw_aiperf/).

| Workload | New module RPS | Previous optimized direct reference | Difference | Weighted mean request latency |
|---|---:|---:|---:|---:|
| Short | 12,542.67 | 11,970.06 median of 3 | +4.78% | 17.38 ms |
| ISL4000 | 10,336.42 | 9,680.97 median of 3 | +6.77% | 40.14 ms |
| Mooncake | 3,020.80 | 3,023.11 single valid run | -0.08% | 22.20 ms |

The short, ISL4000, and normalized Mooncake source SHA256 values verified
inside the vCluster store were respectively
`ab030551a31fb4ac8e7a864a940862a68bc0d386b152b4aba7bc249322833426`,
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`,
and `28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550`.
Short and ISL4000 used frozen raw OpenAI payloads; Mooncake used the prepared
content-addressed mmap. No prompt synthesis was done in the measured client
path. Live Deployment status after the jobs showed one Ready Envoy-direct
gateway using the prepared module, four Ready preprocessors, one Ready selector,
and 16 Ready synthetic workers. Envoy had six workers; its startup log
confirmed the three-step aggregate graph, four gRPC channels, and eight Tokio
threads. Each Job placed three clients on each of the two selected CPU nodes.

These are descriptive, single-run checks against an older series, not a
promotion-grade A/B: gateway and tokenizer caches were not reset between
workloads, the comparison was not interleaved, and the previous optimized
direct series had different worker placement. AIPerf was configured with
`--export-level summary`, so per-request records and actual output-length
distributions are unavailable for a full request-integrity/SLO audit. The
Mooncake rate again tracks its approximately 3,026 RPS offered trace; it is
not a gateway ceiling. The separate 18-client [old/new module A/B](../2026-09-25-envoy-direct-ceiling/README.md)
is stronger evidence for the decoder's throughput effect under saturation.
