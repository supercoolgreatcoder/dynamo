<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Envoy-direct runtime-thread parity

The [Nix-built four-arm mocker matrix](../2026-09-24-nix-build-mocker-parity/README.md)
initially showed Envoy direct well behind Envoy callouts. Both arms use the
same Envoy executable, Rust dynamic module, generic pipeline core, aggregate
graph, protobuf descriptor, and facade services; they differ in outbound
transport. A configuration audit also found unequal thread budgets: direct
had four Tokio runtime threads and six Envoy workers, while callouts had eight
of each. The direct module's `grpc_conns` value was four; changing the
Deployment environment does not override that module-config value.

This series changed **only** `GENERIC_PIPELINE_THREADS` on Envoy direct from
four to eight, leaving its six Envoy workers, four tonic channels per endpoint,
binary, graph, and facade topology unchanged. The module startup log verified
`4 grpc conns, 8 runtime threads`. Six AIPerf 0.12.0 clients, concurrency 128
each, ran for 45 seconds, 3/3 across the same dedicated CPU nodes, using the
same frozen raw short/ISL4000 bodies or cached Mooncake text as the baseline.
Everything ran within vCluster namespace `dynamo-components-v2`; no gateway
other than the one being measured had replicas. All seven Jobs had six
completed clients, zero request errors and cancellations, and complete
retained Pod-placement evidence in
[`benchmark_execution.json`](benchmark_execution.json).

| Workload | Four-thread direct baseline median | Eight-thread direct result | Callout baseline median |
|---|---:|---:|---:|
| Short | 9,751.02 RPS | **11,970.06** median of 3 (11,848.86–12,110.20) | 11,215.71 RPS |
| ISL4000 | 5,820.48 RPS | **9,680.97** median of 3 (9,644.89–9,708.79) | 9,255.85 RPS |
| Mooncake | 2,971.60 RPS | **3,023.11** from 1 valid run | 3,018.16 RPS |

The optimized direct arm now matches or exceeds the retained callout medians
on the same frozen/cached workloads. The short median also exceeds the older
Claude prototype's ~11,862 RPS point, though that point was not a paired
three-run series. Mooncake is offered at about 3,026 RPS and is therefore a
trace-rate parity check, not a gateway ceiling. The [normalized per-Job
metrics](benchmark_summary.json) and [six-client raw summaries](raw_aiperf/)
are retained. The execution ledger was reconstructed after the Jobs, not
pre-registered, so these are reproducible descriptive comparisons, not a
formal promotion-grade AIPerf experiment. The stock Dynamo frontend is also
not a strict same-backend A/B: it uses `dynamo.mocker` while these gateway arms
use the facade's synthetic benchmark engine.

The separate [eight-Envoy-worker probe](../2026-09-25-envoy-direct-envoy-workers/README.md)
reached 9,637.86 ISL4000 RPS with eight Tokio and eight Envoy workers, showing
no material gain over the six-Envoy-worker setting in one run. The
[direct](../2026-09-25-envoy-direct-profile/README.md) and
[callout](../2026-09-25-envoy-callout-profile/README.md) CPU diagnostics
attribute a modest direct-only share to tonic/HTTP2 client work, but the
large initial throughput difference was primarily the unequal Tokio budget.
