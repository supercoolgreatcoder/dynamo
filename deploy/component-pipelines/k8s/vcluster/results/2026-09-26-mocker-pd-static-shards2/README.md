<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW static parallel batch-collector diagnostic

This is an isolated, same-binary test of the opt-in
`DYN_PREPROCESS_BATCH_SHARDS` setting. Nix output
`/nix/store/ii9l6nl52zxxvxdzjzbaxh2pjlld5fn0-agentgateway-component-pipeline-0.0.0-b14ca87d0a`
pins Dynamo static-core revision `488059de3c` and is built by
`component-pipeline-agentgateway-static-shards` in the envs branch at
`9890e38`. The original one-shard behavior remains the default. Both arms
used a fresh single gateway rollout, successful synthetic P/D handoff smoke,
the same six-client frozen ISL4000 payload, batch cap 32, 200 µs configured
linger, 16 gateway worker threads, four gRPC channels/service, four
preprocessors/selectors/prefill workers, and 16 decode workers. Only the
batch-collector count changed.

| Arm | Job | Successful requests | Normalized RPS | Audit/errors |
| --- | --- | ---: | ---: | --- |
| One collector | `nixpds-isl4000-pd-agw-static-r48` | 304,751 | 6,568.70 | valid / 0 |
| Two collectors | `nixpds-isl4000-pd-agw-static-r49` | 297,365 | 6,452.30 | valid / 0 |

The two-collector result is 1.77% below the one-collector result. This is a
single pair, not a variance estimate, and does not justify promoting the
option. It also does not prove that parallel collection can never help.

Busy-window differences of the retained cumulative batch summaries show:

| Arm | Items/batch | First-item queue wait/batch | Collection time/batch | Batched preprocessing RPC |
| --- | ---: | ---: | ---: | ---: |
| One collector | 21.16 | 3.38 ms | 3.05 ms | 36.45 ms |
| Two collectors | 17.41 | 4.67 ms | 5.20 ms | 35.71 ms |

With input split across two bounded queues, each collector assembled smaller
batches and waited longer. Collection clocks include async scheduling waits
and run concurrently; they are not CPU time and must not be summed as
per-request latency. The gRPC RPC clock includes the preprocessor and
network. There were no RPC transport errors. This negative result narrows
the next investigation toward the batch wait policy and the static
pipeline's other stage latencies, rather than merely adding collector tasks.

The frozen plans, exact dataset hashes, raw AIPerf client summaries,
execution records, audits, and `batch-summary-*.log` files are retained in
this directory and `../2026-09-26-mocker-pd-static-shards1/`. After staging
the Nix closure in the vCluster, deploy/smoke each arm with
`run-nix-mocker-pd-static.sh` and `smoke-nix-mocker-pd.sh`, then run
`run-nix-mocker-pd-trial.sh isl4000 rN static-shards1` or
`static-shards2` from `deploy/component-pipelines/k8s/vcluster`. Audit
with `audit-nix-mocker-pd.py <arm-result-dir> isl4000 rN --static`.
