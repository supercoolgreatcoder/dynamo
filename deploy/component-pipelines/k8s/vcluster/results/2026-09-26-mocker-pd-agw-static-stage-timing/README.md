# Static P/D gateway stage-timing diagnostic

The pinned Nix gateway bundle uses Dynamo source `043f5d4c3e` with opt-in
`DYN_STATIC_STAGE_TIMING_EVERY=500`. The vCluster-only ISL4000 benchmark plan,
dataset hashes, execution records, AIPerf summaries and audit files are in this
directory. Raw AIPerf exports and full gateway logs remain local and on the
vCluster NFS benchmark store; only the small sampled timing log is committed.

Run r8 is **invalid**: Kubernetes rotated the high-volume gateway log before
the post-job fetch, leaving no timing samples. Run r9 used a live log follower
and passed the audit: 5,842.55 globally normalized successful requests/s,
zero request errors, and 538 timing samples. Median stage durations were
prepare 30.71 ms, prefill 43.85 ms, selector 41.49 ms, decode setup 1.47 ms,
and total 122.50 ms. These are sampled per-request stage durations, not an
additive decomposition of system-wide CPU.

Reproduce with `run-nix-mocker-pd-trial.sh` and this `benchmark_plan.json`,
using `PD_STAGE_TIMING_EVERY=500`. The runner now tails gateway logs during the
Job so log rotation cannot erase the timing evidence.
