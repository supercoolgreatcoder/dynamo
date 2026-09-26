# Facade-side localization of the AGW static ISL4000 gap

This is a diagnostic series, not a parity claim. All five runs used one AGW
gateway, four preprocessors, four selectors, four synthetic Dynamo P/D
prefill workers, 16 synthetic decode workers, six AIPerf 0.12.0 clients,
45-second ISL4000 measurements, and the frozen raw-payload SHA-256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.
The exact vCluster was
`https://gateway-poc.mkhadkevich-dev:443/dynamo-components-v2`. No workload
was deployed outside it. Both gateway types passed the streamed prefill-handoff
and decode smoke test before measurement; all six-client exports in each run
had zero errors and cancellations.

The isolated facade binary was built from Dynamo commit `3c9c4e9d8f`, with
only `lib/component-facades/src/preprocess.rs` changed from the baseline
facade pin `cb970285af`. Its Nix output was
`/nix/store/s8ai17wscfq3flbq89xdpgaajbwlll9l-dynamo-component-facade-1.6.0-3c9c4e9d8f`.
`DYN_PREPROCESSOR_STATS_INTERVAL_SECS=10` enabled cumulative service-side
snapshots. The AGW static binary was the same isolated bulk-drain build in all
four static runs; bulk drain changed only via its runtime flag. Generic used
the earlier generic-step-statistics build. Each saved `gateway-*.json` and
`preprocessor-*.json` identifies the exact deployed binary and environment.

| Trial | Gateway/config | Exported six-client RPS | Requests | Result |
| --- | --- | ---: | ---: | --- |
| `r66` | Static, four gRPC channels, bulk off | 6,355.56 | 286,486 | Valid |
| `r67` | Generic, four gRPC channels | 9,501.48 | 428,983 | Valid |
| `r68` | Static, 16 gRPC channels, bulk off | 6,144.14 | 277,051 | Valid |
| `r69` | Static, 16 gRPC channels, bulk on | 6,236.15 | 281,147 | Valid |
| `r70` | Same as `r69`, sparse stage timing | 6,305.71 | 284,298 | Valid |

At four channels, all 286,487 static preprocessing items including smoke
traffic landed on a single preprocessor pod. Generic used three pods, roughly
107k, 107k, and 212k items during its run. Sixteen static channels distributed
traffic to all four pods, yet throughput did not rise. Thus Service-level
connection pinning is real but is **not sufficient** to explain the gap.
These are cumulative pod counters: `r67`–`r70` increments must be derived by
subtracting a pod's preceding snapshot, not by reading its final count alone.

With 16 channels and bulk drain off, the static gateway's final summary was
9,642 preprocessing RPCs for 277,052 items, 17.8 ms mean RPC wall time, and
43.4 seconds aggregate collection time across the run. Its queue depth
counter averaged 86.5 ready items per collection. Enabling bulk drain reduced
that queue-depth average to 0.11 while collecting roughly 143 items per pass
and making 9,780 concurrent RPCs; RPC wall time was 18.4 ms, but RPS was only
6,236. These gateway summaries were read from the vCluster gateway logs after
`r68` and `r69`; `r70` retains its full gateway log in this directory.

The `r70` one-in-1,000 static stage sample (285 requests) averaged 32.81 ms
prepare, 35.65 ms prefill, 39.18 ms selection, and 2.10 ms decode setup;
sampled total was 109.73 ms. These are gateway wall times, not CPU times and
not the full duration of the streamed response. The earlier generic
step-statistics experiment reported cumulative means of 13.19, 5.01, 4.78,
and 4.12 ms respectively, but it was a separate run, so the values localize
the next investigation rather than prove a causal delta. In particular, the
static prefill and selector waits now deserve service-side timing and payload
equivalence checks. Neither more connections nor faster batch collection alone
or together reaches the generic reference.

Reproduce the isolated facade with
`nix build .#component-pipeline-facade-stats` in the `gateway-pipeline/` flake
on branch `feat/dynamo-component-pipeline-static-pd` of the envs repository.
Stage its closure with `stage-nix-closure.sh`, roll it with
`run-nix-mocker-pd-preprocessor-stats.sh`, deploy each AGW variant using the
existing static or generic stats rollout, smoke test, and run
`run-nix-mocker-pd-facade-timing.sh`. The latter validates the exact vCluster,
the diagnostic facade binary and flag, one gateway replica, the frozen dataset
hash, and all six AIPerf clients; it retains deployment snapshots, Job JSON,
raw exports, and each preprocessor pod's log. The `r70` call set
`PD_CAPTURE_GATEWAY_LOGS=1`. Afterward the gateway replicas were scaled to
zero and the four preprocessors restored to the original `cb970285af` bundle,
verified 4/4 Ready with no diagnostic flag.
