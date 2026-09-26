# AGW static ISL4000 P/D stage-timing diagnostic

This vCluster-only run used the frozen ISL4000 dataset, six AIPerf clients,
one AGW static gateway with 16 worker threads and four gRPC channels per
service, four preprocessors, four selectors, four prefill mock workers, and
16 decode mock workers. The AIPerf audit passed with zero errors or
cancellations: job `nixpds-isl4000-pd-agw-static-r23` completed 292,310
requests at 6,321.68 globally normalized requests/s.

The static host emitted one timing record per 500 requests. Across 584
samples, median stage times were prepare 50.403 ms, prefill 29.015 ms,
select 28.553 ms, decode setup 1.515 ms, and total 111.254 ms. These
durations include time awaiting stage RPCs; they do not separate remote
service execution from network or queueing time. The raw gateway log and
its SHA256 are recorded in the summary.

The same tuned configuration without stage logging reached 6,523.09 RPS
in `nixpds-isl4000-pd-agw-static-r20`; the contemporaneous AGW generic
check reached 9,513.54 RPS in `nixpd-isl4000-pd-agw-generic-r24`.
These single runs establish a large throughput gap but not its cause.

Run `run-nix-mocker-pd-trial.sh isl4000 rN static-tuned-timing` against the
matching vCluster fixture to repeat this diagnostic. The benchmark plan,
execution record, per-client AIPerf JSON/CSV, summary, dataset hashes,
and audit are retained here. Console-format exports are omitted because
they are generated presentation artifacts.
