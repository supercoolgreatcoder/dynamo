# AGW generic low-frequency step snapshots on ISL4000

The Nix-isolated AGW build from Dynamo patch commit `8ef25d175d` adds an
opt-in reporter that reads the generic core's existing cumulative per-step
wall-time counters every ten seconds. It adds no per-request logging or
clock reads. The same executable, graph, facade, four
preprocessor/selector/prefill replicas, 16 decode replicas, 16 AGW worker
threads, four gRPC channels per endpoint, six AIPerf 0.12.0 clients, and
frozen ISL4000 raw-text payloads were used in both arms. Both gateways
passed the streamed synthetic P/D smoke test before measurement.

| Job | Snapshot interval | Successful requests | Globally normalized RPS | Audit |
| --- | ---: | ---: | ---: | --- |
| `nixpd-isl4000-pd-agw-generic-r39` | Off | 436,745 | 9,505.46 | Valid |
| `nixpd-isl4000-pd-agw-generic-r40` | 10 s | 428,457 | 9,383.34 | Valid |

Both six-client audits passed with zero errors, cancellations, or
blockers. The stats-on result is 1.3% below the control, smaller than
the previously observed run-to-run variation; this single pair does not
establish zero perturbation or a statistically precise delta. The
control plan and raw exports are in
`../2026-09-26-mocker-pd-agw-generic-step-stats-off/`.

The reporter retained 17 structured snapshots in
`step-stats-nixpd-isl4000-pd-agw-generic-r40.log` (SHA-256
`0711f6d507d369cbc640810e38ea51d360dd14bb9b498f981a563f84e9e8f5a1`).
The first eleven precede the synchronized AIPerf start and contain only
the one smoke-test request. Near the end of measurement, at 391,884
completed requests, cumulative mean step times were prepare 13.38 ms,
prefill 5.11 ms, select/decode routing 4.88 ms, and decode 4.18 ms.
The final snapshot at 428,458 total requests (including the smoke test)
was 13.19, 5.01, 4.78, and 4.12 ms, respectively. These are step wall
times, not CPU time, and include downstream RPC waits; they are
cumulative means rather than interval means.

An earlier static diagnostic reported medians of 50.4 ms prepare,
29.0 ms prefill, and 28.6 ms selection at about 6.3k RPS. The generic
means here are much lower, but the different summary statistic and
separate run mean this is localization evidence, not an exact causal
decomposition. The remaining static investigation should measure its
batch queue and RPC wait without enabling the trace target, which
previously perturbed throughput severely.

To repeat, build `.#component-pipeline-agentgateway-generic-stats` from
the envs branch. Stage its Nix closure into the explicit vCluster and
deploy with `run-nix-mocker-pd-generic-stats.sh`, setting `PD_AGW_BINARY`
to its `bin/agentgateway`, `PD_GENERIC_STATS_INTERVAL_SECS=0` or `10`,
and `PD_RUST_LOG=warn,dynamo_generic_pipeline_stats=debug`. Smoke-test
the service, then run `run-nix-mocker-pd-trial.sh isl4000 rN
generic-step-stats-off` or `generic-step-stats-on`. The runner verifies
the frozen plan hash, dataset hash, deployment binary and reporter
setting, and vCluster API server before launching AIPerf. The gateway
was scaled to zero afterward. Raw JSON/CSV, execution, occupancy,
summary, audit, and gateway-log artifacts are retained here.
