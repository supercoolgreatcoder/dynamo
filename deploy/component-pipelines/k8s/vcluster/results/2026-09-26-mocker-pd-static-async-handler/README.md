# AGW async-handler experiment: no ISL4000 P/D improvement

This isolated AGW-host experiment scheduled the static pipeline on a
Tokio task and returned the streaming response immediately. It used the
same upstream AGW revision, Dynamo core/facade source, frozen ISL4000
dataset, vCluster mock-worker topology, 16 worker threads, and four
gRPC channels per service as the tuned static baseline. Its separate Nix
output is `component-pipeline-agentgateway-async-experiment` in the envs
flake at commit `f78e8e1`; the default bundle retains the original
host patch. The experimental patch is preserved at Dynamo commit
`f884edc265` and was subsequently reverted from the default branch.

The six-client AIPerf audit passed with zero errors or cancellations:
`nixpds-isl4000-pd-agw-static-r25` completed 295,833 requests at
6,375.88 globally normalized requests/s. The tuned static baseline
`r20` reached 6,523.09 RPS, and the contemporaneous generic run `r24`
reached 9,513.54 RPS. A single unpaired run is not a variance estimate,
but this result gives no evidence that moving the pipeline off the HTTP
handler closes the gap. The patch also changes pre-stream errors from
HTTP 502 to errors within the SSE stream, so it was not promoted.

Run `run-nix-mocker-pd-trial.sh isl4000 rN static-async-handler` after
deploying the experimental Nix AGW output into the same vCluster fixture.
The plan, execution record, per-client AIPerf JSON/CSV, summary, dataset
hashes, and audit are retained here.
