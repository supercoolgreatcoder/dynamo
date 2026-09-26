# Static worker-channel lock: no material ISL4000 gain

This experiment changed only the AGW static facade's worker-channel map from
`tokio::sync::Mutex` to `std::sync::Mutex`. The critical section contains no
`.await`, and the generic gRPC transport already uses a synchronous lock.
The candidate is Dynamo commit `5d54df3a07`, built as the isolated Nix output
`component-pipeline-agentgateway-static-std-mutex` in the envs flake. The
production/default Nix bundle, Dynamo core crates, facade binary, and AGW host
patch were unchanged.

The frozen [candidate plan](benchmark_plan.json) has SHA-256
`9882a7ac40ae7ed5da4a43fdc3118e277d65d9f223eba3756f878102eaf566ac`.
The control uses the prior pinned async-mutex binary and its
[ready-off plan](../2026-09-26-mocker-pd-static-ready-off/benchmark_plan.json).
The arms were interleaved candidate/control/candidate/control/candidate/control,
with a fresh one-replica gateway rollout each time. Both used 16 gateway
threads, four gRPC channels per endpoint, 4/4/4/16 preprocessor/selector/
prefill/decode replicas, the same six pinned AIPerf 0.12.0 clients, the same
frozen ISL4000 raw-payload SHA-256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`,
and a 45-second measurement. Both enabled the same batch-summary diagnostics.
All six audits are valid with zero request errors and cancellations. The
gateway was scaled to zero afterward; all deployments and Jobs were confined
to `https://gateway-poc.mkhadkevich-dev:443/dynamo-components-v2`.

| Order | Arm | Trial | Globally normalized successful RPS |
| ---: | --- | --- | ---: |
| 1 | synchronous-lock candidate | `r54` | 6,410.26 |
| 2 | async-lock control | `r55` | 6,433.81 |
| 3 | synchronous-lock candidate | `r56` | 6,248.96 |
| 4 | async-lock control | `r57` | 6,326.53 |
| 5 | synchronous-lock candidate | `r58` | 6,427.06 |
| 6 | async-lock control | `r59` | 6,344.34 |

Candidate median: **6,410.26 RPS**. Control median: **6,344.34 RPS**
(+1.0% candidate/control). The per-pair differences are -23.55, -77.57, and
+82.72 RPS, so even their direction is inconsistent. This is not evidence of
a meaningful throughput improvement and does not close the roughly 6.4k
static-versus-9.5k generic ISL4000 gap observed in the earlier
[CPU-sampled comparison](../2026-09-26-mocker-pd-cpu-samples/README.md).
The source change should remain a reproducible diagnostic candidate, not an
unjustified default performance fix. These are summary-level AIPerf results,
not a statistical confidence interval or a GPU-worker measurement.

Reproduce by building the isolated Nix output, staging it through
`stage-nix-closure.sh`, deploying with `run-nix-mocker-pd-static.sh` and
`PD_AGW_BINARY` set to that output, smoke testing, then running
`run-nix-mocker-pd-trial.sh isl4000 rN static-std-mutex`. The control uses
`static-ready-off` and its plan-pinned binary. The result directories retain
per-client AIPerf summaries, Job identities, dataset hashes, gateway batch
summaries, and audit JSON for every run.
