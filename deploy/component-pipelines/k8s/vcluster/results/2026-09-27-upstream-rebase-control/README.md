<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Same-day old-bundle controls, September 27, 2026

These three isolated vCluster Jobs used the previous `accf6af699` Nix bundle
`/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699`
in the same 16-worker, four-preprocessor, one-selector aggregate fixture as
the new `b140462e2c` campaign. Each used six AIPerf 0.12.0 clients,
concurrency 128, a 45-second measured interval, the same frozen workload,
and one gateway replica. All six-client exports passed start-skew and
zero-error checks. The applied Job commands and complete Pod placements are
in `benchmark_execution.json`; raw exports are under `raw_aiperf/`.

| Control Job | Aggregate requests/second |
| --- | ---: |
| `nixv2-short-envoy-generic-r104` | 11,937.1 |
| `nixv2-short-agw-generic-r104` | 11,732.6 |
| `nixv2-isl4000-envoy-callouts-r104` | 10,138.6 |

The corresponding new-bundle latest short passes were 12,032.7 and
11,760.4 RPS; its new-bundle ISL4000 callout median was 10,579.1 RPS.
These single-run controls indicate that the historical throughput gap is
not, by itself, evidence of a rebase regression. They do not replace the
new bundle's three-run medians or prove an absolute gateway ceiling.
