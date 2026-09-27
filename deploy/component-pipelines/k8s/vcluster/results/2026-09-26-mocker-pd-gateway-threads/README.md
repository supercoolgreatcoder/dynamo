<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW generic gateway worker-thread A/B, ISL4000

This vCluster-only diagnostic compares one AGW generic gateway with 16 and
32 worker threads. The gateway used the same pinned Nix binary, node, and
configuration except `workerThreads`; the mock P/D graph stayed at four
preprocessors, four selectors, four prefill workers, and 16 decode workers.
Six AIPerf 0.12.0 clients stayed on the same two CPU nodes at concurrency
128 each for 45 seconds, using the frozen raw ISL4000 dataset (SHA256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`).
The sequence crossed midnight UTC on September 26–27, 2026.

| Job | Gateway threads | Completed requests | Exported RPS | Start skew |
| --- | ---: | ---: | ---: | ---: |
| [r91](../2026-09-26-mocker-pd-decode-scale/README.md) | 16 | 440,843 | 9,779.18 | 2 s |
| r92 | 32 | 421,007 | 9,331.12 | 2 s |
| r93 | 16 | 434,868 | 9,635.48 | 2 s |

All three runs had zero errors and cancellations. The 32-thread result was
3.9% below the adjacent 16-thread controls' average (9,707.33 RPS).
This one interleaved sequence shows **no gain** from 32 threads under this
load; it does not yet establish a repeatable regression or an absolute
single-gateway ceiling. The earlier nine-client 16/32-thread comparison
was confounded by load-generator/decoder co-location; this six-client
sequence keeps AIPerf on the same two nodes throughout.

Reproduce by setting `PD_WORKER_THREADS=32` and then `16` with
`run-nix-mocker-pd-generic-stats.sh`, using the pinned
`/nix/store/ha9684gksqyiilhjd43y9ccyd65rxwi0-agentgateway-component-pipeline-0.0.0-b14ca87d0a/bin/agentgateway`.
Run `smoke-nix-mocker-pd.sh` after each rollout, then
`run-nix-mocker-trial.sh pd-agw-generic isl4000 rNEW` with the same
six-client vCluster nodes and dataset. This directory retains the raw
AIPerf exports and Job, Pod, Deployment, and ConfigMap snapshots for
r92–r93; r91 artifacts are in the linked decode-scaling series.

After r93, the AGW generic gateway was restored to `workerThreads: 16`
and scaled to zero. No deployment occurred outside the vCluster.
