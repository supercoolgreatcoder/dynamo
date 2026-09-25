<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# One-replica Envoy-direct short-workload load sweep

The six-client parity matrix did not saturate the optimized Envoy-direct
gateway. This follow-up kept exactly one gateway replica, four preprocessors,
one selector, 16 synthetic facade workers, the frozen short raw-payload
dataset, AIPerf 0.12.0, concurrency 128 per client, and a 45-second measurement.
Only the number of load-generator clients changed. Jobs ran sequentially
inside vCluster namespace `dynamo-components-v2`; all other gateways had
zero replicas. The gateway retained eight Tokio threads, six Envoy workers,
and four configured gRPC channels per endpoint.

| Clients | Valid runs (requests/s) | Mean request latency (ms) | Effective concurrency | Errors |
|---:|---:|---:|---:|---:|
| 6 | 11,848.86–12,110.20 (three-run parity series) | — | — | 0 |
| 12 | 13,047.87 | 102.82 | 1,342.29 | 0 |
| 18 | 13,840.63 / 13,353.78 | 122.37 / 143.67 | 1,694.74 / 1,919.87 | 0 |
| 24 | 13,668.15 / 13,472.17 | 177.44 / 177.96 | 2,427.02 / 2,398.63 | 0 |

The higher-load results form a **13.35–13.84k requests/s plateau** under this
topology, while latency rises as concurrency increases. This is a measured
system plateau, **not yet an isolated gateway ceiling**: the selector had one
replica, preprocessor four, and one of the three client nodes also hosted
synthetic workers. AIPerf client CPU or these downstream components could
still constrain the result. The vCluster Metrics API was unavailable, so
there is no direct CPU-utilization evidence. Further selector/preprocessor
scaling and, ideally, non-colocated clients are needed for attribution.

All five valid Jobs completed with zero AIPerf errors and cancellations.
Each has retained [Job/Pod placement evidence](.) showing equal client
counts on the three selected CPU nodes, plus [raw AIPerf summaries](raw_aiperf/).
The initial 12-client `r1` Job was invalid because one selected node had
been cordoned by cluster autoscaler. It was deleted after two clients
completed and is excluded. The runner now checks node readiness and taints
before creating a Job. The retained valid 12-client run is therefore `r2`.
This sweep intentionally changes offered load and is not a same-load
comparison against the older Claude prototype or stock Dynamo frontend.

## Downstream-replica probes at 18 clients

Two follow-up probes changed one downstream replica count at a time. The
single gateway was restarted after each scaling change because its persistent
gRPC channels connect through Kubernetes ClusterIP Services. The same frozen
requests, client nodes, 18×128 concurrency, and 45-second duration were used.
Each Job completed all 18 clients with zero errors and retained its own Pod
placement record and raw AIPerf JSON/CSV summaries. These are diagnostic
measurements; they were not interleaved, the server-side replica snapshots
were observed live rather than persisted in the Job record, and the client
node that hosts synthetic workers remains an isolation limitation.

| Topology | Requests/s in two runs | Mean request latency (ms) |
|---|---:|---:|
| 4 preprocessors, 1 selector (baseline) | 13,840.63 / 13,353.78 | 122.37 / 143.67 |
| 4 preprocessors, 4 selectors | 12,615.29 / 12,290.65 | 160.12 / 163.75 |
| 8 preprocessors, 1 selector | 14,107.22 / 12,957.51 | 122.81 / 152.16 |

All four selector replicas shared one 32-core CPU node with the other pods
already there. The eight preprocessors were Ready, four on each of two CPU
nodes. Adding selector replicas did not improve throughput in this placement;
the preprocessor result overlaps the baseline's run-to-run range, so its
effect is inconclusive. During the first eight-preprocessor measurement, a
17-second read-only cgroup sample on the sole gateway Pod increased from
222,063,445 to 419,114,143 `usage_usec` (about 11.6 CPU cores in use), with
`cpu.max` showing no quota. This supports substantial gateway CPU work, but
does not isolate its maximum capacity or rule out client/downstream effects.

The second four-selector Job completed successfully, but an in-flight edit
caused its launching shell to fail before collection. The independent
[`collect-nix-mocker-ceiling.sh`](../../collect-nix-mocker-ceiling.sh) runner
recovered its unchanged Job, Pod placement, and AIPerf exports from the
vCluster store. No measurement was rerun or reconstructed.
