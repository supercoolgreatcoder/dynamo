<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW static P/D ISL4000 channel and thread diagnostics

The current unified P/D matrix left AGW static well behind AGW generic on
ISL4000. The deployed static gateway used 32 independent tonic channels per
endpoint and 12 AGW worker threads; generic used four channels and 16 worker
threads. These runs isolate the static gateway knobs while retaining Dynamo
`cb970285af`, the same four preprocessors, four selectors, four synthetic
prefill workers, 16 synthetic decode workers, frozen raw-text dataset SHA
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`,
and six AIPerf 0.12.0 clients at concurrency 128 for 45 seconds. AIPerf did
not tokenize or synthesize prompts during measurement. Each Job ran alone
inside the explicit vCluster and passed the audit with zero errors and
cancellations; each gateway passed a streamed P/D handoff smoke test first.

| Job | Static channels/endpoint | AGW threads | Successful requests | Globally normalized RPS |
| --- | ---: | ---: | ---: | ---: |
| `nixpds-isl4000-pd-agw-static-r15` | 32 | 12 | 269,371 | 5,822.19 |
| `nixpds-isl4000-pd-agw-static-r18` | 4 | 12 | 291,246 | 6,233.44 |
| `nixpds-isl4000-pd-agw-static-r19` | 32 | 12 | 273,227 | 5,873.59 |
| `nixpds-isl4000-pd-agw-static-r20` | 4 | 16 | 300,610 | 6,523.09 |

The 4-channel point is 6.6% above the mean of the two bracketing 32-channel
points, consistent with reducing the much larger static connection pool
helping. Raising AGW worker threads from 12 to 16 at four channels adds a
further 4.6% versus the single 4-channel/12-thread run. Neither effect has
paired-repeat confidence intervals; time-varying node contention remains a
possible contributor. The best static point is still 30.4% below the
single-run unified generic result of 9,370.98 RPS, so static P/D ISL4000
parity is **not** established.

The original 32-channel plan, A run, and return leg are in
[AGW static unified](2026-09-26-mocker-pd-agw-static-unified/README.md).
Separate frozen plans, execution records, six raw JSON/CSV exports,
dataset checksums, occupancy snapshots, summaries, and valid audits for the
4-channel and 4-channel/16-thread trials are in
[channels4](2026-09-26-mocker-pd-static-channels4/benchmark_plan.json) and
[channels4-threads16](2026-09-26-mocker-pd-static-channels4-threads16/benchmark_plan.json).
The exact plan SHA-256 values are embedded in each execution record. The
return-leg plan did not encode channel count, but the vCluster Deployment was
explicitly restored to 32 before its rollout and smoke test. The static
gateway was scaled to zero after the experiments. Generated console tables
with padded trailing whitespace remain in the vCluster benchmark store; the
committed JSON exports are the auditor's raw input.

The next investigation should profile static gateway CPU, batching, and the
prefill/selector stage under the same four-channel/16-thread configuration,
then interleave a tuned static run with generic. These results do not justify
changing the production default yet.
