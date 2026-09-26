# AGW static preprocessing batch-size A/B on ISL4000

This trial tests whether raising the static gateway's preprocessing batch
limit from 32 to 64 improves the synthetic prefill/decode pipeline. The
gateway binary, four-channel gRPC pools, 16 AGW worker threads, 200 µs
batch linger, four preprocessor/selector/prefill replicas, 16 decode
replicas, six AIPerf 0.12.0 clients, and frozen Claude-reference ISL4000
request payloads are unchanged. The gateway runs with `RUST_LOG=warn` and
batch telemetry disabled; the trace-enabled measurements in the sibling
diagnostic directory are not a valid unperturbed baseline.

| Job | Gateway batch limit | Successful requests | Globally normalized RPS | Audit |
| --- | ---: | ---: | ---: | --- |
| `r32` (prior control) | 32 | 304,854 | 6,684.26 | Valid |
| `r35` | 64 | 291,799 | 6,335.27 | Valid |
| `r36` (interleaved control) | 32 | 298,623 | 6,447.90 | Valid |
| `r37` | 64 | 300,600 | 6,513.18 | Valid |

`r35` and `r37` are stored here; `r32` and `r36` are stored in
`../2026-09-26-mocker-pd-static-batch-diagnostic-off/`. All four audit
records report zero blockers, and the trial runners report zero request
errors and cancellations. Each run used a fresh gateway rollout. The
batch-64 result is lower than `r32` but higher than the immediately
preceding `r36`; the direction changes within the observed run-to-run
variation. Thus batch 64 has **no demonstrated throughput benefit** and
does not close the AGW-static ISL4000 gap. This is not a claim of
statistical equivalence: the two arms have only two valid runs each.

The Nix-pinned gateway executable is
`/nix/store/rw0xr8qw38yj4lcn90kzbsa2066g4f3k-agentgateway-component-pipeline-0.0.0-b14ca87d0a/bin/agentgateway`;
the facade output and remaining provenance are frozen in
`benchmark_plan.json`. To repeat a batch-64 run, set the explicit
vCluster kubeconfig/server/namespace and deploy with
`run-nix-mocker-pd-static.sh` using `PD_AGW_BINARY` above,
`PD_PREPROCESS_BATCH_MAX=64`, `PD_WORKER_THREADS=16`,
`PD_GRPC_CHANNELS_PER_ENDPOINT=4`,
`PD_PREPROCESS_BATCH_LINGER_US=200`, `PD_BATCH_STATS_EVERY=0`, and
`PD_RUST_LOG=warn`; smoke test, then run
`run-nix-mocker-pd-trial.sh isl4000 rN static-batch64`. The runner checks
the frozen plan hash, exact dataset hash, deployment parameters, and
vCluster API server before launching its AIPerf job. The sibling
`static-batch-diagnostic-off` variant reproduces the batch-32 control.

The gateway was scaled to zero after the trials. Raw per-client AIPerf
JSON/CSV, execution records, occupancy snapshots, summaries, and audits
are retained with the plans.
