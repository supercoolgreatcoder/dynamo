# AGW static P/D preprocessor linger diagnostic

Both runs used the same Nix AGW and facade bundle from Dynamo `cb970285af`,
the frozen ISL4000 raw-payload dataset, one gateway with 16 worker threads
and four gRPC channels per service, four preprocessors, four selectors,
four prefill mock workers, and 16 decode mock workers inside the vCluster.
Only preprocessor batch linger changed between 100 and 200 µs.

| Linger | Job | Successful requests | Globally normalized requests/s |
| ---: | --- | ---: | ---: |
| 100 µs | `nixpds-isl4000-pd-agw-static-r26` | 294,344 | 6,390.27 |
| 200 µs | `nixpds-isl4000-pd-agw-static-r27` | 292,820 | 6,339.19 |

Both six-client AIPerf 0.12.0 audits passed with zero errors or
cancellations and verified the exact dataset hash, vCluster API,
placement, and pinned plan identity. The 0.8% difference is too small
to establish a throughput benefit in these unpaired runs. An earlier
200 µs run, `r20`, reached 6,523.09 RPS; the generic AGW control
`r24` reached 9,513.54 RPS. Matching the generic graph's 100 µs linger
did not materially close the static/generic gap.

To repeat the 100 µs arm, deploy with
`PD_WORKER_THREADS=16 PD_GRPC_CHANNELS_PER_ENDPOINT=4
PD_PREPROCESS_BATCH_LINGER_US=100` using
`run-nix-mocker-pd-static.sh`, then run
`run-nix-mocker-pd-trial.sh isl4000 rN static-linger100` with the
vCluster and NFS variables described in the parent README. The raw
AIPerf JSON/CSV, execution record, normalized summary, and audit are
retained here; console-format exports are ignored.
