<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW static batch-summary A/B, ISL4000

This is an interleaved, same-binary control of the opt-in static batch-summary
instrumentation. The off and on plans pin the same Nix AGW output
`/nix/store/x0bg70p06l4fckrijmc90zz12fxg8s23-agentgateway-component-pipeline-0.0.0-b14ca87d0a`
(Dynamo static core `9ecf9f1e74`), facade output, six AIPerf clients, frozen
ISL4000 payload SHA256, and vCluster-only prefill/decode topology. Only
`DYN_STATIC_BATCH_SUMMARY_SECS` changes from absent to `10`. The AGW binary is
the `component-pipeline-agentgateway-static-summary` output of the
`feat/dynamo-component-pipeline-static-pd` branch in `dynamo-nix-envs`.

| Arm | Trial | Normalized successful RPS | Audit | Errors |
| --- | --- | ---: | --- | ---: |
| Off | r41 | 6,312.21 | valid | 0 |
| On | r42 | 6,249.07 | valid | 0 |
| Off | r43 | 6,382.46 | valid | 0 |
| On | r44 | 6,275.40 | valid | 0 |
| Off | r45 | 6,210.53 | valid | 0 |
| On | r46 | 6,294.88 | valid | 0 |

Off median: **6,312.21 RPS**; on median: **6,275.40 RPS** (-0.58%). This
small difference sits within the observed run-to-run spread. These results
support using the summaries for diagnosis, but do not prove a speedup or a
specific gateway bottleneck. All six runs had a fresh gateway rollout, a
successful synthetic P/D handoff smoke, six-client execution evidence, and
zero AIPerf errors/cancellations. They measure mock-worker throughput, not
real-GPU throughput. The gateway was scaled to zero afterward.

In the stats-on runs, deltas between cumulative snapshots during the busy
window yielded approximately 21.1-21.2 items/batch, 3.5-3.6 ms first-item
queue wait/batch, 3.2-3.3 ms batch collection, 24-25 queued items at batch
close, and 38.9-39.9 ms/batched preprocessing RPC. There were zero gRPC
transport errors. These clocks overlap across concurrent batches; their
means must not be summed as per-request latency. In particular, the RPC
clock includes the preprocessor service and network, so it does not isolate
AGW CPU time. The next ceiling experiment should scale preprocessing and
load generation separately before attributing a bottleneck to the gateway.

Reproduce with the pinned plans in this directory and
`../2026-09-26-mocker-pd-static-summary-off/benchmark_plan.json`. Stage the
Nix output with `stage-nix-closure.sh`, deploy the AGW via
`run-nix-mocker-pd-static.sh` with the plan's `PD_*` settings, smoke via
`smoke-nix-mocker-pd.sh`, then from `deploy/component-pipelines/k8s/vcluster`
interleave:

```bash
bash run-nix-mocker-pd-trial.sh isl4000 r41 static-summary-off
bash run-nix-mocker-pd-trial.sh isl4000 r42 static-summary-on
```

Redeploy and smoke the gateway after changing arms; use fresh trial numbers
for repeats. The runner verifies the exact Nix binary, env, topology, plan
hash, dataset hash, and vCluster server before launching AIPerf. Audit each
raw export with `audit-nix-mocker-pd.py <arm-result-dir> isl4000 rN --static`.
The `audit-*.json`, `summary-*.json`, `execution-*.json`, dataset hashes,
raw AIPerf summaries, and stats-on `batch-summary-*.log` files are retained
alongside these plans.
