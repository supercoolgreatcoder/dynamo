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

## Gateway-thread and client-isolation probes

With the original worker placement, raising only the gateway's Tokio runtime
threads from 8 to 12 produced **13,475.21 requests/s**, 137.56 ms mean latency,
and zero errors at 18 clients. The live module log confirmed `4 grpc conns,
12 runtime threads`; Envoy remained at six workers. The result lies inside
the original 8-thread, 18-client range (13,353.78–13,840.63 requests/s), so
there is no detected benefit from this one run. The gateway was restored to
eight threads afterward.

The original third client node also hosted three synthetic workers. To test
that isolation concern, the worker Deployment's node affinity was temporarily
restricted to three other CPU nodes. After its rollout settled, exactly 16
Ready workers remained, with none on the third client node. All other serving
replicas and gateway settings stayed at baseline. Two valid 18-client runs
gave **12,995.96** and **13,102.46 requests/s** (151.11 and 150.05 ms mean
latency), both error-free. Removing worker/client co-location did not lift the
measured throughput; this is a separate placement series and should not be
presented as a same-topology gain/loss against the earlier runs.

On that isolated placement, changing only Envoy `--concurrency` from six to
12 workers gave **12,835.87 requests/s**, 153.91 ms mean latency, zero errors.
This is not evidence of a higher ceiling. It is one run and only 1.6% below
the two-run six-worker average, so the small difference is inconclusive.
Envoy workers and mock-worker node affinity were restored to their original
values afterward. Each probe has its own retained Job/Pod placement JSON and
raw AIPerf JSON/CSV exports; the five initial sweep Jobs remain unchanged.

## Baseline short-workload CPU profile

The unmodified eight-Tokio/six-Envoy-worker gateway also ran one diagnostic
18-client Job with its existing `GENERIC_PIPELINE_PROFILE_SECS=180` sampler at
199 Hz. That Job completed at 12,511.05 requests/s with zero errors, but its
throughput is **not** compared to unprofiled runs because sampling adds work.
The sampler printed 23,931 CPU samples in the gateway Pod log:

| Classified area | Share of samples |
|---|---:|
| Envoy | 35.05% |
| serde_json | 32.60% |
| prost_reflect | 15.76% |
| pipeline_grpc | 9.19% |
| tonic/HTTP2 client | 6.12% |
| Tokio | 1.21% |

The largest named leaf was `MessageDescriptor::get_field_by_name` at 5.43%.
The classifier assigns whole stacks to its first matching category and
samples a window extending beyond the 45-second AIPerf phase; these shares
are diagnostic, not precise per-request CPU cost. They motivated a separate
thin-facade experiment to resolve the response field once per stream rather
than once per chunk. No Dynamo core or worker implementation was changed for
that experiment.

## Prepared-output Envoy module A/B check

The revised module from Dynamo commit `98676215fa` was built by the independent
Nix flake at envs commit `8a612b7`, staged as a Nix-store closure, loaded by the
same one-replica `envoy-independent` Deployment, and passed a streaming OpenAI
smoke test. Its only Envoy-host/transport code delta relative to the old module
is preparing the protobuf response decoder once per request stream and avoiding
per-chunk graph-request/fallback clones. The Nix package also adds the
`libgeneric_pipeline.so` alias required by Envoy's dynamic-module loader.

The 18-client AIPerf workload and 45-second window remained fixed. All 16 mock
workers were Ready and off the third client node, with four preprocessors, one
selector, eight Tokio threads, six Envoy workers, and four gRPC channels per
endpoint. The new-module runs were followed by one old-module control on this
same isolated placement, after which the new module was restored. All 18
clients completed in each Job with zero reported errors or cancellations.

| Module | Run | Requests/s | Weighted mean latency (ms) | Requests |
|---|---|---:|---:|---:|
| New prepared decoder | r1 | 16,150.49 | 79.05 | 729,378 |
| New prepared decoder | r2 | 16,525.34 | 71.09 | 745,845 |
| Old decoder, same placement | r3 control | 13,773.52 | 133.08 | 622,109 |

The two new-module runs averaged **16,337.92 requests/s**, 18.6% above the
same-placement old-module control. The earlier old-module isolated runs were
12,995.96 and 13,102.46 requests/s; that earlier worker distribution was
5/6/5 across its three non-client nodes, versus 4/6/6 for this A/B check.
This evidence supports a material improvement, but the three runs were not
interleaved and the old-module control has only one repeat. It does not prove
the final gateway ceiling. The retained per-client AIPerf JSON/CSV summaries
and Job/Pod placement records are diagnostic campaign artifacts, not a
request-level SLO audit.
