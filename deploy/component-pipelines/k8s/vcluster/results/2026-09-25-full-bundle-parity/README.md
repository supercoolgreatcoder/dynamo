<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Complete Nix bundle frozen-workload parity

This campaign measures the complete bundle
`/nix/store/g7achknzv9ibixmfdaxgjy4a3pp33dp5-dynamo-component-pipelines-98676215fa`
built from envs commit `8a612b7` and Dynamo source `98676215fa`. The same
bundle supplies the Envoy executable and dynamic module, AGW binaries, and
the preprocessor, selector, and synthetic benchmark-worker facades. It was
staged into the existing vCluster NFS store and rolled out only inside
namespace `dynamo-components-v2`. All Deployments reached readiness, and a
streaming OpenAI smoke request returned two tokens and `[DONE]`.

The first four frozen six-client Jobs used AIPerf 0.12.0, concurrency 128 per
client, a 45-second measurement, raw short/ISL4000 payloads or the prepared
Mooncake mmap, and no prompt synthesis during measurement. The later
18-client short Job used the same frozen raw payload on three CPU client
nodes, with the 16 synthetic workers off the third client node. All Jobs
completed with zero reported errors and cancellations. The [six-client
execution ledger](benchmark_execution.json) retains every Job command and
all 24 client Pod placements; the [18-client execution record](execution-ceilv1-short-envoy-direct-c18-r3-prepared.json)
retains all 18 placements. Raw per-client AIPerf JSON/CSV/console summaries
are under [`raw_aiperf/`](raw_aiperf/).

| Frozen workload | Full-bundle Job | Requests/s | Weighted mean latency | Requests |
|---|---|---:|---:|---:|
| Short, six clients | `nixv2-short-envoy-generic-r8` | 11,413.82 | 18.25 ms | 515,101 |
| Short, six clients | `nixv2-short-envoy-generic-r9` | 10,272.88 | 18.61 ms | 465,486 |
| ISL4000, six clients | `nixv2-isl4000-envoy-generic-r13` | 9,350.45 | 31.75 ms | 422,431 |
| Mooncake, six clients | `nixv2-mooncake-envoy-generic-r6` | 3,022.64 | 19.84 ms | 136,075 |
| Short, 18 clients | `ceilv1-short-envoy-direct-c18-r3-prepared` | **16,263.81** | 74.77 ms | 734,360 |

The six-client short/ISL4000 points were lower than the earlier
[module-only series](../2026-09-25-prepared-module-parity/README.md), but
achieved concurrency and client pacing also changed. A one-binary
[preprocessor A–B–A probe](../2026-09-25-preprocessor-binary-ab/README.md)
did not reproduce a new-binary regression. The 18-client full-bundle result
falls inside the earlier prepared-module 16,150.49–16,525.34 RPS range at
the same configured load. The worker distribution across its three
non-client nodes changed from 4/6/6 to 4/7/5, so this is descriptive
parity rather than a perfectly paired experiment. Mooncake again tracked
its approximately 3,026 RPS offered trace, not a gateway ceiling.

The source SHA256 values verified in the vCluster store were
`ab030551a31fb4ac8e7a864a940862a68bc0d386b152b4aba7bc249322833426`
(short), `3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`
(ISL4000), and `28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550`
(Mooncake). The executed AIPerf policy was `--export-level summary`; no
per-request records or actual output-length distribution are available for
a promotion-grade SLO audit.
