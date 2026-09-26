# AGW static P/D four-channel, 16-thread control

The frozen ISL4000 series uses one Nix-built AGW static gateway, 16
worker threads, four gRPC channels per endpoint, a 200 µs preprocessor
batch linger, and the same vCluster mock P/D fixture as the 100 µs
diagnostic. The six-client AIPerf audits passed with zero errors or
cancellations:

| Job | Successful requests | Globally normalized requests/s |
| --- | ---: | ---: |
| `nixpds-isl4000-pd-agw-static-r20` | 300,610 | 6,523.09 |
| `nixpds-isl4000-pd-agw-static-r27` | 292,820 | 6,339.19 |
| `nixpds-isl4000-pd-agw-static-r28` | 298,351 | 6,472.90 |
| `nixpds-isl4000-pd-agw-static-r30` | 297,066 | 6,419.39 |

Run `run-nix-mocker-pd-static.sh` with `PD_WORKER_THREADS=16`,
`PD_GRPC_CHANNELS_PER_ENDPOINT=4`, and
`PD_PREPROCESS_BATCH_LINGER_US=200`, then run
`run-nix-mocker-pd-trial.sh isl4000 rN static-channels4-threads16`.
See the 100 µs diagnostic for the near-time comparison and its limits.
Runs `r28` and `r30` also have read-only gateway CPU snapshots; see the
`cpu-samples/` report for the raw counters and interpretation.
