<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW generic mock-decode replica A/B, ISL4000

This vCluster-only diagnostic tests whether 16 mock decode replicas limit
the one-replica AGW generic pipeline. The gateway stayed on the same
16-worker-thread Nix binary and Pod; preprocessor, selector, and prefill
stayed at four replicas each. Six AIPerf 0.12.0 clients ran on the same two
CPU nodes at 128 concurrency each for 45 seconds, using the frozen raw
ISL4000 dataset (SHA256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`).
The decode Deployment alone changed 16 → 32 → 16. All three runs had
zero errors/cancellations and exported measurement-window skew at most 2 s.

| Job | Ready decode replicas | Completed requests | Exported RPS | Start skew |
| --- | ---: | ---: | ---: | ---: |
| r89 | 16 | 422,108 | 9,363.75 | 1 s |
| r90 | 32 | 428,806 | 9,501.16 | 1 s |
| r91 | 16 | 440,843 | 9,779.18 | 2 s |

The 32-replica result is 0.73% **below** the midpoint of its two
16-replica controls (9,571.46 RPS). One interleaved sequence is not a
variance estimate, but it provides no evidence that adding mock decode
replicas closes the generic gateway's ~10k RPS plateau. The 32-replica
Deployment reached 32/32 Ready, and a streamed synthetic prefill/decode
smoke test passed after scaling up and again after restoring 16. Those
checks prove the pipeline remained functional; they do not prove traffic
was evenly spread across all 32 decode Pods.

The guarded runner at Dynamo commit `e7e05fb6b0` accepts
`PD_DECODE_REPLICAS=16` or `32` for P/D mocker trials; the default remains
16. Reproduce with the same vCluster kubeconfig, pinned Nix gateway,
NFS-backed AIPerf dataset/tokenizer, and A/B load-generator nodes used in
the [load-scaling series](../2026-09-26-mocker-pd-aiperf-scale/README.md).
Scale only `deployment/dynamo-pd-decode`, wait for the expected Ready count,
smoke via `smoke-nix-mocker-pd.sh`, and run
`run-nix-mocker-trial.sh pd-agw-generic isl4000 rNEW` with the matching
`PD_DECODE_REPLICAS`. The raw AIPerf JSON/CSV/console exports and Job,
Pod, gateway, and decode Deployment snapshots are retained here.

After r91, decode was restored to 16/16 and all four gateway Deployments
were scaled to zero. Nothing was deployed outside the vCluster.
