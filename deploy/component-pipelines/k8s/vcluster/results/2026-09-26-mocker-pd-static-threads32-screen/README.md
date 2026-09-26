<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW static worker-thread screening, ISL4000

This is a two-run screening test of whether increasing the single static
gateway from 16 to 32 AGW worker threads materially closes its gap to AGW
generic. Both runs used the same uninstrumented Nix gateway binary
`/nix/store/4q3hi4jd1xvkq8ys498599a83m3zg1yh-agentgateway-component-pipeline-0.0.0-b14ca87d0a/bin/agentgateway`,
the same pinned gateway node, four gRPC channels per endpoint, preprocessing
batch cap 32 and linger 200 µs, and four preprocessor, four selector, four
prefill, and 16 decode mock-worker replicas. The frozen ISL4000 raw-text
dataset SHA256 was
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.
Six AIPerf 0.12.0 clients ran at concurrency 128 each for 45 seconds. Each
gateway passed the streamed P/D smoke test before measurement. All twelve
client Pods succeeded without an error or cancellation.

| Order | Job | AGW threads | Requests | Summed client RPS |
| --- | --- | ---: | ---: | ---: |
| First | `nixpds-isl4000-pd-agw-static-r79` | 32 | 301,912 | 6,696.93 |
| Second | `nixpds-isl4000-pd-agw-static-r80` | 16 | 288,693 | 6,404.95 |

The 32-thread arm was 4.56% above this one 16-thread control, but one
non-interleaved pair cannot distinguish the setting from run-to-run drift.
Even if that difference is repeatable, 6,697 RPS remains well below the
9,406.79-RPS generic diagnostic run `r78` on the same frozen workload. Do
not treat this screen as a validated improvement or as evidence that gateway
thread count explains the large static/generic RPC-wait gap. As with the
correlated RPC tests, this custom mocker runner lacks the recipe optimization
loop's execution contract, so it is diagnostic rather than candidate-promotion
evidence.

To repeat, set the explicit vCluster kubeconfig, expected server
`https://gateway-poc.mkhadkevich-dev:443`, namespace
`dynamo-components-v2`, stager Pod, and the exact baseline binary path. With
`PD_WORKER_THREADS=32` or `16`, use `run-nix-mocker-pd-static.sh` with
`PD_GRPC_CHANNELS_PER_ENDPOINT=4`, `PD_PREPROCESS_BATCH_MAX=32`,
`PD_PREPROCESS_BATCH_LINGER_US=200`, and `PD_RUST_LOG=warn`. The script
updates the gateway ConfigMap, but an unchanged Deployment does **not**
restart: call `kubectl rollout restart deployment/dynamo-pd-agw-static`
after each thread-count switch, wait for rollout, and smoke-test. Run
`run-nix-mocker-trial.sh pd-agw-static isl4000 rNEW` with the two pinned
AIPerf nodes, NFS export, tokenizer store basename, result directory, and
vCluster guard variables used by the runner. Use new Job IDs. For a gain
claim, run interleaved A/B repetitions and compare medians.

The raw six-client JSON/CSV/console exports, Job snapshots, and gateway
ConfigMap/Deployment snapshots are retained here. After `r80`, AGW static
was restored to its previous pinned Deployment at zero replicas with
`workerThreads: 16`; all four gateway variants were verified at zero, and
selector/prefill remained 4/4 Ready. Nothing outside the vCluster was
deployed or changed.
