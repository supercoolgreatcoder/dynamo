<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Preprocessor binary isolation probe

The complete Nix bundle was live, but short/ISL4000 rates were below the
earlier module-only series while AIPerf achieved lower concurrency. This
probe changes only the `dynamo-preprocessor` Deployment executable back to
the previous facade build. Envoy executable/module, selector, benchmark
workers, replica counts, frozen short payloads, and AIPerf settings remain
unchanged. The old preprocessor binary is
`/nix/store/b5rgzxw0s62ncf65lf7qvy0174c12lsc-dynamo-component-facade-1.6.0-e007954067/bin/dynamo-component-facade`.

The frozen six-client short workload was run sequentially with the new bundle,
the old preprocessor, and the restored new preprocessor. Each Job used AIPerf
0.12.0, six clients at configured concurrency 128, a 45-second measurement,
and the same raw-payload dataset. Every Job completed with zero reported
errors or cancellations. [`benchmark_execution.json`](benchmark_execution.json)
captures both probe Jobs and all 12 client placements; their raw summaries
are under [`raw_aiperf/`](raw_aiperf/). The two earlier full-bundle runs are
retained in the [full-bundle campaign](../2026-09-25-full-bundle-parity/README.md).

| Sequence | Preprocessor binary | Short RPS | Achieved concurrency | Mean latency | Client credit-to-start |
|---|---|---:|---:|---:|---:|
| Earlier r8 | New bundle | 11,413.82 | 208.53 | 18.25 ms | 20.07 ms |
| Earlier r9 | New bundle | 10,272.88 | 191.35 | 18.61 ms | 21.06 ms |
| r10 | Previous facade | 11,908.10 | 208.93 | 17.53 ms | 20.19 ms |
| r11 | Restored new bundle | 12,186.33 | 201.93 | 16.55 ms | 18.30 ms |

The return leg with the new binary exceeded the old-binary probe by 2.34%,
so the measurements **do not support a preprocessor-binary regression**.
The same new bundle varied by 18.6% from r9 to r11; achieved concurrency
and client pacing changed too. Source review found no preprocessor
tokenization/rendering hot-path change between the two binary pins. This
is a diagnostic A–B–A, not a promotion-grade result: caches were reset by
Pod rollouts but load-generator and shared-node occupancy were not controlled,
and AIPerf exported summaries rather than per-request records. The new
preprocessor binary is left active.
