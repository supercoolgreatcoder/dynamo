# AGW static zero-linger ISL4000 diagnostic

This same-binary trial asks whether the timed receive loop in the static
preprocessing batch collector explains the remaining gap to AGW generic.
Setting `DYN_PREPROCESS_BATCH_LINGER_US=0` switches the collector to a
single `yield_now()` followed by nonblocking queue draining, while leaving
the 32-item batch limit, 16 AGW threads, four gRPC channels per endpoint,
four preprocessor/selector/prefill replicas, 16 decode replicas, and six
AIPerf 0.12.0 clients unchanged. The frozen ISL4000 payload SHA-256 is
recorded in `benchmark_plan.json`; AIPerf did not synthesize or tokenize
prompts during measurement.

| Job | Linger | Successful requests | Globally normalized RPS | Audit |
| --- | ---: | ---: | ---: | --- |
| `r32` (earlier control) | 200 µs | 304,854 | 6,684.26 | Valid |
| `r36` (later control) | 200 µs | 298,623 | 6,447.90 | Valid |
| `r38` | 0 µs | 308,572 | 6,680.80 | Valid |

The zero-linger arm passed a streamed synthetic P/D handoff smoke test
and its six-client AIPerf audit with zero errors, cancellations, or audit
blockers. It is effectively level with the best same-binary 200 µs
control and does not explain or close the roughly 9.4k-RPS AGW-generic
comparison. One zero-linger run cannot establish a small positive or
negative effect; this result only rules out a large timer-driven gap.
The control audits and raw exports live in
`../2026-09-26-mocker-pd-static-batch-diagnostic-off/`.

To reproduce, deploy the Nix-pinned AGW binary in `benchmark_plan.json`
with `run-nix-mocker-pd-static.sh`, setting
`PD_PREPROCESS_BATCH_LINGER_US=0`, `PD_PREPROCESS_BATCH_MAX=32`,
`PD_WORKER_THREADS=16`, `PD_GRPC_CHANNELS_PER_ENDPOINT=4`,
`PD_BATCH_STATS_EVERY=0`, and `PD_RUST_LOG=warn`, plus the explicit
vCluster kubeconfig/server/namespace. Smoke-test, then run
`run-nix-mocker-pd-trial.sh isl4000 rN static-linger0`. The runner
verifies the plan and dataset hashes, exact gateway settings, and
vCluster API server. The gateway was scaled to zero afterward. Raw
AIPerf JSON/CSV, execution, occupancy, summary, and audit artifacts
are retained here.
