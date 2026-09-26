<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW static sleep-then-drain wait-policy diagnostic

This is a same-binary vCluster test of the opt-in
`DYN_PREPROCESS_BATCH_SLEEP_DRAIN=1` path. Nix output
`/nix/store/yrynlzm47d5x91lj9kzmjysl3cpyq5kp-agentgateway-component-pipeline-0.0.0-b14ca87d0a`
pins Dynamo static-core revision `b843bdb1ac` and is built by
`component-pipeline-agentgateway-static-sleep-drain` in the envs branch at
`d47d6c1`. The default timed-receive path is unchanged. Both arms used a
fresh single AGW gateway rollout, successful synthetic P/D handoff smoke,
the same six-client frozen ISL4000 payload, one batch collector, batch cap
32, configured linger 200 µs, 16 gateway worker threads, four gRPC
channels/service, four preprocessors/selectors/prefill workers, and 16
decode workers. Only the wait policy changed.

| Arm | Job | Successful requests | Normalized RPS | Audit/errors |
| --- | --- | ---: | ---: | --- |
| Timed receive | `nixpds-isl4000-pd-agw-static-r50` | 302,644 | 6,530.91 | valid / 0 |
| Sleep then drain | `nixpds-isl4000-pd-agw-static-r51` | 297,836 | 6,409.76 | valid / 0 |

The candidate is 1.85% lower in this single pair. This is a diagnostic,
not a variance estimate, and the option is not promoted.

Busy-window differences of the retained cumulative batch summaries show:

| Arm | Items/batch | First-item queue wait/batch | Collection time/batch | Queue depth at close | Batched preprocessing RPC |
| --- | ---: | ---: | ---: | ---: | ---: |
| Timed receive | 21.42 | 3.49 ms | 3.15 ms | 24.46 | 37.72 ms |
| Sleep then drain | 32.00 | 35.40 ms | 4.96 ms | 228.30 | 27.25 ms |

The single sleep fills nearly every batch but blocks the one collector for
roughly 5 ms under load. The large queue and first-item wait explain why
larger batches did not increase throughput. The collector clock includes
async scheduling wait rather than CPU time; RPC time includes the
preprocessor and network. There were zero RPC transport errors. The next
targeted experiment is to nonblockingly drain an existing queue before
waiting, so a full backlog is never held behind the timer.

The frozen plans, dataset hashes, raw six-client AIPerf summaries,
execution records, audits, and `batch-summary-*.log` files are retained
here and in `../2026-09-26-mocker-pd-static-sleep-drain-off/`. Reproduce
inside the vCluster with the pinned Nix output, deploying/smoking each arm
and running `run-nix-mocker-pd-trial.sh isl4000 rN
static-sleep-drain-{off|on}` from `deploy/component-pipelines/k8s/vcluster`.
Audit with `audit-nix-mocker-pd.py <arm-result-dir> isl4000 rN --static`.
