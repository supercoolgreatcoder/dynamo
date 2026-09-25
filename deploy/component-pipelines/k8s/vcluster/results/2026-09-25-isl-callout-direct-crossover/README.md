<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Full-bundle ISL4000 Envoy crossover

The [nine-cell full-bundle matrix](../2026-09-25-full-bundle-all-arms/README.md)
observed 11,162.98 RPS for Envoy callouts and 9,350.45 RPS for Envoy
direct on ISL4000, but those were single runs at different times. This
targeted crossover repeats the identical frozen ISL4000 payload, AIPerf
0.12.0 six-client policy, shared facade fleet, and complete Nix bundle
while activating only one Envoy arm at a time. It tests whether the large
apparent callout/direct gap survives a closer-in-time comparison.

The four new Jobs each completed six AIPerf 0.12.0 clients with zero reported
errors or cancellations. Their approximately 45-second measurements used the
same SHA256-verified frozen raw ISL4000 file
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.
The retained [`benchmark_execution.json`](benchmark_execution.json) contains
the executed commands and all 24 Pod placements; six raw JSON/CSV/console
summaries per Job are under [`raw_aiperf/`](raw_aiperf/).

| Sequence | Gateway arm | Envoy workers | Requests/s | Mean latency | Achieved concurrency | Client credit-to-start |
|---|---|---:|---:|---:|---:|---:|
| D1, `r14` | Direct | 6 | 10,364.76 | 36.97 ms | 383.48 | 17.53 ms |
| C, `r6` | Callouts | 8 | 11,088.63 | 26.08 ms | 289.62 | 21.11 ms |
| D2, `r15` | Direct | 6 | 10,289.44 | 38.79 ms | 399.43 | 17.79 ms |
| D8, `r16` | Direct | 8 | 10,416.15 | 38.06 ms | 396.69 | 18.94 ms |

The callout run exceeded the two bracketing six-worker direct runs by about
7.4% relative to their mean. A second callout run in the preceding matrix
gave 11,162.98 RPS. Raising only direct Envoy workers from six to eight did
not materially close the gap: its 10,416.15 RPS was inside 1.2% of the
six-worker direct points, and the six-worker setting was restored afterward.
The two arms used the same Nix Envoy executable/module bytes, four gRPC
channels, eight Tokio threads, facade fleet, dataset, and model. Their
HTTP request bytes sent and received per request matched in the AIPerf
summaries; the slightly different HTTP chunk counts reflect transport
framing and do not prove different model output lengths.

This closer-in-time D–C–D sequence supports an observed callout advantage
under the tested six-client ISL4000 load, but does not isolate the underlying
cause or prove a gateway maximum. The host paths differ, shared-node occupancy
was not frozen, and AIPerf summary-only export omits per-request output-token
records. In the earlier full-bundle matrix, direct's first ISL4000 point was
9,350.45 RPS, showing that same-arm temporal variation is material; do not
compare that single low point to the callout high point as an intrinsic 20%
architecture gap. Profile both arms at matched load before attributing the
remaining difference to decoding, Envoy SDK, or callout transport.
