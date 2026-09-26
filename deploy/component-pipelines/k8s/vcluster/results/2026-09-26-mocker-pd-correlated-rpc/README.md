<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Request-correlated static gateway/facade RPC timing

One diagnostic ISL4000 run (`nixpds-isl4000-pd-agw-static-r77`) used the
frozen raw dataset with SHA256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.
It ran only in the `gateway-poc` vCluster namespace `dynamo-components-v2`
on 2026-09-26. The AGW static gateway was the Nix output
`/nix/store/4bf6wxscr3mf9c2cnwlmdq3m0x6x4p99-agentgateway-component-pipeline-0.0.0-b14ca87d0a`;
the selector and prefill used
`/nix/store/n6pnf4jdd2756w35y27lmhqpm7ksdw5i-dynamo-component-facade-1.6.0-4b10d580de`.
Both pin Dynamo source commit `4b10d580de` and are exported by the
`dynamo-nix-envs/gateway-pipeline` flake on branch
`feat/dynamo-component-pipeline-static-pd`.

The fixture retained one static gateway, 16 gateway worker threads, four
gRPC channels per endpoint, preprocessing batch cap 32 and linger 200 µs,
four preprocessors, four selectors, four prefill mock workers, and 16 decode
mock workers. The streamed P/D smoke passed. Six AIPerf 0.12.0 clients ran
at concurrency 128 each for 45 seconds. The six raw exports report 289,838
requests, **6,429.88 summed client RPS**, zero errors/cancellations, and
104.17–113.75 ms client mean-latency range. The Job reports six succeeded
Pods and no failed Pods. This is a diagnostic run, not a canonical
optimization-loop audit: the older mocker runner did not emit that loop's
`benchmark_execution.json` or frozen plan, so the result should not be used
as candidate-promotion evidence.

Both gateway and facades sampled UUID request IDs ending in `00`. The
`analyze-correlated-rpc.py` script joined **all 1,088 sampled IDs** in the
gateway, prefill-handler, prefill-terminal, and selector-handler logs. The
matched durations are:

| RPC | Gateway wait, mean / P95 | Facade handler, mean / P95 | Outside handler, mean / P95 |
| --- | ---: | ---: | ---: |
| Prefill response headers | 28.07 / 36.97 ms | 0.109 / 0.165 ms | 27.96 / 36.84 ms |
| Selector result | 28.02 / 36.16 ms | 0.415 / 1.105 ms | 27.61 / 35.70 ms |

The outside-handler durations subtract clocks measured within each process
and do **not** depend on cross-node synchronization. They show that the
facade's Dynamo-owned selector and prefill handlers do not account for the
long gateway waits. In this run AGW was on a different node from every
selector and prefill Pod. Comparing log wall timestamps across those nodes
*suggests* roughly 16.8–17.4 ms before facade entry and 10.2–11.2 ms from
handler completion to gateway return, but those two segments are
clock-skew-sensitive and are not yet a causal attribution. A few inferred
cross-clock segments were slightly negative; do not treat the split as an
exact network or scheduling breakdown. The prefill handoff stream itself
averaged 29 µs from response headers to terminal processing.

The prior static runs `r75` and `r76` reached 6,325.84 and 6,374.03 RPS
with the same workload and topology, so this probe did not show a large
throughput perturbation. The earlier generic reference reached about
9,402 RPS, but it is a different orchestration implementation and was not
rerun in this diagnostic series. Its result is context, not a direct gain
claim. Earlier 16-channel static testing distributed traffic to all four
preprocessors without closing the gap, so connection pinning alone is also
insufficient to explain it.

To reproduce, build the `component-pipeline-agentgateway-static-correlated`
and `component-pipeline-facade-correlated` outputs from the pinned envs
flake, stage both closures with `stage-nix-closure.sh`, and roll the two
facades with `run-nix-mocker-pd-rpc-stats.sh` using
`PD_RPC_SAMPLE_EVERY=1000` and `PD_RPC_CORRELATED_TIMING=1`. Roll static AGW
with `run-nix-mocker-pd-static.sh` using
`PD_STATIC_CORRELATED_TIMING=1`, `PD_STAGE_TIMING_EVERY=1000`, and
`PD_RUST_LOG=warn,dynamo_static_rpc_split=debug,dynamo_static_selector_rpc=debug`,
plus the fixture values above. Smoke, then run
`run-nix-mocker-pd-rpc-timing.sh pd-agw-static rNEW` with
`PD_RPC_CORRELATED_TIMING=1`, both exact binary paths, a new
`PD_RPC_RESULT_DIR`, the explicit vCluster kubeconfig/server/namespace,
two AIPerf nodes, NFS export, and stager Pod. Analyze with:

```bash
python3 analyze-correlated-rpc.py RESULTS_DIR nixpds-isl4000-pd-agw-static-rNEW
```

The runner preserves raw client exports, the dataset-hash record, Job and
Deployment snapshots, and all gateway/facade logs here. After `r77`, the
static gateway was restored to its previous pinned spec at zero replicas;
all four gateway variants were verified at zero, and selector/prefill were
restored to their default facade binaries at 4/4 Ready. No deployment
outside the vCluster was changed.
