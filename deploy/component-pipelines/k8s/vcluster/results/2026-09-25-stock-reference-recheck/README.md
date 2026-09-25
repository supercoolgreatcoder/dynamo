<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Stock Dynamo frontend reference recheck

This campaign reruns the existing stock Dynamo frontend plus 16 logical
`dynamo.mocker` workers in the same vCluster, against the same frozen short,
ISL4000, and prepared Mooncake datasets used by the
[Nix facade 846821 recheck](../2026-09-25-facade-846821-recheck/README.md).
The stock reference uses the previously staged immutable
`/nix/store/jx56mllhp02qj5y5hxk9lpj29wn8xlbd-dynamo-mocker-env` closure,
not the newer `8468212130` facade source. Its etcd discovery, TCP request and
response transport, and ZMQ event transport run without NATS. Each trial
restarts the frontend and four mocker Pods; this remains an architectural
comparison, not a same-worker or same-source A/B.

The runner requires the exact vCluster API server and refuses active benchmark
Jobs, gateway Pods, or real-worker fixtures. It uses the existing six-client
AIPerf 0.12.0 Job templates, concurrency 128 per client, a 45-second measured
interval, and a shared 90-second start barrier. Raw six-client exports are
accepted only if all are present, error-free, and uncancelled.

| Workload | Accepted RPS | Valid Jobs | Prior stock reference RPS | Facade callout median RPS |
|---|---:|---:|---:|---:|
| Short | 7,402.15 median (7,368.19–7,517.82) | 3/3 | 7,576.15 median | 12,321.92 |
| ISL4000 | 6,500.41 median (6,498.09–6,542.87) | 3/3 | 6,715.36 median | 11,379.44 |
| Mooncake | 3,022.14 (one point) | 1/3 | 3,021.44 (one point) | 3,023.67 |

The new stock short median is 2.3% below the prior stock median and ISL4000
3.2% below. The new callout facade medians are 66.5% and 75.1% above the
respective stock medians, but this is **not** a same-worker A/B: stock
`dynamo.mocker` performs KV bookkeeping while the facade benchmark worker
emits a fixed synthetic stream. Mooncake is trace-rate-limited near 3,026 RPS,
so its accepted points do not establish a frontend capacity ceiling.

All short and ISL4000 Jobs (`r25`–`r27`) have six exports, zero request
errors, and no cancellations. Mooncake `r25` likewise completed with zero
errors, but `r26` had four and `r27` had two
`InvalidInferenceResultError` requests: AIPerf received a stream with no
content, only metadata, empty data, or the terminal marker. Those two Jobs
are preserved under `raw_aiperf/` but excluded from a Mooncake median. The
same error type appeared in earlier stock-reference Mooncake diagnostics.
The normalized prepared trace contains 22,699 records, all with a positive
`output_length` (minimum one), so zero requested output length does not
explain these failures. AIPerf's summary export does not identify the
specific failing conversation; the causal origin of the empty stock streams
remains unproven. The facade gateways each had three zero-error Mooncake runs
on this trace.
