# Static P/D gateway log-level control

This vCluster-only ISL4000 control repeats the stage-timing run with
`PD_RUST_LOG=warn` and `PD_STAGE_TIMING_EVERY=500`. Run r10 passed the benchmark
audit: 5,928.20 globally normalized successful requests/s, zero request
errors, and 548 timing samples. Median stage durations were prepare 29.12 ms,
prefill 42.05 ms, selector 40.75 ms, decode setup 1.46 ms, and total 117.32 ms.

The change from r9 is small compared with the gap to the same-topology generic
AGW run (9,372.84 requests/s). This single control does not establish a
statistically significant log-level effect or a hardware throughput ceiling.
The plan, dataset hashes, execution record, normalized summary, and audit are
included; raw AIPerf exports and full gateway logs remain in the vCluster
benchmark store. Reproduce with `run-nix-mocker-pd-trial.sh` and the pinned
`benchmark_plan.json`.
