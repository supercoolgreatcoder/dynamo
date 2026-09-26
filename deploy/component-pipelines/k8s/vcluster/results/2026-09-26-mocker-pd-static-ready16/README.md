<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW static queue-ready batch bypass diagnostic

This is a same-binary vCluster test of the opt-in
`DYN_PREPROCESS_BATCH_READY_THRESHOLD=16` setting. Nix output
`/nix/store/fpms13kcraizqhbwv0vglj43yliqszz3-agentgateway-component-pipeline-0.0.0-b14ca87d0a`
pins Dynamo static-core revision `c99f255570` and is built by
`component-pipeline-agentgateway-static-ready-threshold` in the envs
branch at `3ec67fb`. The default timed-receive path is unchanged. Both
arms used a fresh single gateway rollout, successful synthetic P/D
handoff smoke, the same six-client frozen ISL4000 payload, one batch
collector, batch cap 32, configured linger 200 µs, 16 gateway worker
threads, four gRPC channels/service, four preprocessors/selectors/prefill
workers, and 16 decode workers. Only the queue-ready threshold changed.

| Arm | Job | Successful requests | Normalized RPS | Audit/errors |
| --- | --- | ---: | ---: | --- |
| Threshold off | `nixpds-isl4000-pd-agw-static-r52` | 290,059 | 6,398.77 | valid / 0 |
| Threshold 16 | `nixpds-isl4000-pd-agw-static-r53` | 289,027 | 6,262.79 | valid / 0 |

The candidate is 2.13% lower in this single pair. This is not a variance
estimate and the option is not promoted.

Busy-window differences of the retained cumulative batch summaries show:

| Arm | Items/batch | First-item queue wait/batch | Collection time/batch | Queue depth at close | Batched preprocessing RPC |
| --- | ---: | ---: | ---: | ---: | ---: |
| Threshold off | 21.27 | 3.52 ms | 3.18 ms | 24.65 | 39.21 ms |
| Threshold 16 | 21.06 | 3.64 ms | 1.82 ms | 16.01 | 40.34 ms |

The threshold cut collection time by about 43% without increasing
throughput. Collection clocks include async scheduling wait, not just
gateway CPU work; the RPC clock includes the preprocessor and network.
There were zero gRPC transport errors. Together with the earlier
batch-size, sharding, and sleep-drain diagnostics, this makes more
collector-wait tuning a weak path to closing the static-versus-generic
gap. The next investigation should measure the other static pipeline
stages and their transport behavior.

The frozen plans, dataset hashes, six raw AIPerf client summaries,
execution records, audits, and `batch-summary-*.log` files are retained
here and in `../2026-09-26-mocker-pd-static-ready-off/`. Reproduce in the
vCluster with the pinned Nix output, deploying/smoking each arm and
running `run-nix-mocker-pd-trial.sh isl4000 rN static-ready-off` or
`static-ready16` from `deploy/component-pipelines/k8s/vcluster`. Audit
with `audit-nix-mocker-pd.py <arm-result-dir> isl4000 rN --static`.
