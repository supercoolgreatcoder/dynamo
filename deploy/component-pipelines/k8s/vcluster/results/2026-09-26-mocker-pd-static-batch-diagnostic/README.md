# Static AGW batch telemetry perturbs ISL4000 throughput

The isolated `component-pipeline-agentgateway-batch-diagnostic` Nix output
from envs commit `83b8766` contains the static-core telemetry patch at
Dynamo `cc2b2d7eac`; the default AGW bundle and facades remain pinned to
`cb970285af`. A streamed synthetic P/D handoff smoke test passed before
benchmarking. All four six-client ISL4000 AIPerf 0.12.0 runs passed the
normal audit with zero errors or cancellations, the frozen dataset hash,
and the same vCluster fixture: one gateway, 16 AGW worker threads, four
gRPC channels per service, four preprocessors, four selectors, four prefill
workers, and 16 decode workers.

| Job | Batch timestamp sampling | Static-core trace enabled | Successful requests | Globally normalized RPS |
| --- | --- | --- | ---: | ---: |
| `nixpds-isl4000-pd-agw-static-r31` | 1 in 100 batches | Yes | 121,945 | 2,659.49 |
| `nixpds-isl4000-pd-agw-static-r32` | Off | No | 304,854 | 6,684.26 |
| `nixpds-isl4000-pd-agw-static-r33` | 1 in 100 batches | Yes | 128,474 | 2,757.80 |
| `nixpds-isl4000-pd-agw-static-r34` | 1 in 100 batches | No | 306,261 | 6,621.19 |

The two trace-enabled runs show a reproducible large slowdown. The
same-binary, timestamp-enabled but trace-filtered run stays near the
diagnostics-off control. Thus the trace-enabled configuration, not
timestamp collection by itself, perturbs this benchmark. We have not
isolated whether the cost is trace formatting/output or another effect
of enabling the static-core trace target. The 57 and 61 exact structured
batch samples from `r31` and `r33` are retained here, but their queue
and RPC timings describe only the perturbed ~2.7k-RPS state; they must
not be used as measurements of the ~6.6k-RPS baseline.

In those perturbed samples, mean batch size was 19.33 (`r31`) and 21.07
(`r33`), and sampled preprocessor RPC times averaged 104.22 and 96.17 ms.
All sampled RPCs succeeded. These values are diagnostic of the altered
state only; the next baseline-safe approach should export cumulative
counters without enabling this trace target on the gateway hot path.

To reproduce, deploy the isolated Nix AGW output with
`PD_WORKER_THREADS=16`, `PD_GRPC_CHANNELS_PER_ENDPOINT=4`,
`PD_PREPROCESS_BATCH_LINGER_US=200` via `run-nix-mocker-pd-static.sh`.
For the trace-on arms set `PD_BATCH_STATS_EVERY=100` and
`PD_RUST_LOG=warn,dynamo_static_pipeline=trace`, then use
`run-nix-mocker-pd-trial.sh isl4000 rN static-batch-diagnostic`.
The off and clock-only plans live in the sibling directories; their
runner variants are `static-batch-diagnostic-off` and
`static-batch-clock-only`. Plans, execution records, per-client raw
AIPerf JSON/CSV, summaries, audits, and trace-on batch samples are
retained in these directories.
