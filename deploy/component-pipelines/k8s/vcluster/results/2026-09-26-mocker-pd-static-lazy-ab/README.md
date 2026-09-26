# Static gRPC connection-establishment A/B

On 2026-09-26, the same Nix-built AGW static binary from Dynamo commit
`b10ce94fa3` was run twice in the `gateway-poc` vCluster namespace
`dynamo-components-v2`. Its output was
`/nix/store/wq3ym25zwshqm7jw1h37s95jkg9bclr3-agentgateway-component-pipeline-0.0.0-b14ca87d0a`.
The only intended gateway change between runs was
`DYN_STATIC_LAZY_CHANNELS=1` in `r76`; `r75` retained eager connections.
Both arms passed the streamed prefill/decode smoke test.

The fixture used one static AGW gateway with 16 worker threads, four gRPC
channels per endpoint, a 32-item preprocess batch cap and 200 µs linger;
four preprocessor, four selector, four prefill, and 16 decode mock-worker
replicas. Six AIPerf 0.12.0 clients each used 128 concurrency for 45 seconds.
The frozen raw ISL4000 dataset SHA256 was
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.

| Connection mode | Run | Requests | RPS | Errors | Cancelled |
| --- | --- | ---: | ---: | ---: | --- |
| Eager, default | `r75` | 285,129 | 6,325.84 | 0 | No |
| Lazy, opt-in | `r76` | 287,284 | 6,374.03 | 0 | No |

Lazy mode was 0.76% higher in this single pair. That is not evidence of a
repeatable throughput win, and it does not close the roughly 9,402-versus-
6,326 RPS generic/static gap measured earlier on the same fixture. The
sampled `dynamo_static_rpc_split` logs, including pre-run smoke/warmup, show:

| Mode | Samples | Header wait mean / median / P95 | Handoff-stream mean / P95 |
| --- | ---: | ---: | ---: |
| Eager | 286 | 28.36 / 28.57 / 35.95 ms | 10.8 / 34 µs |
| Lazy | 288 | 28.72 / 29.37 / 37.39 ms | 14.4 / 39 µs |

The response-header wait is unchanged at the scale of the gap. Eager versus
lazy connection establishment is therefore not the main cause; this probe
does not distinguish gateway scheduling, gRPC client queueing, facade
admission, or response-header return. Lazy channels also forfeit the eager
startup connection check, so there is no reason to enable them by default.

Reproduce by staging the Nix output, rolling `run-nix-mocker-pd-static.sh`
with `PD_AGW_BINARY` set to its `bin/agentgateway`,
`PD_WORKER_THREADS=16`, `PD_GRPC_CHANNELS_PER_ENDPOINT=4`,
`PD_PREPROCESS_BATCH_MAX=32`, `PD_PREPROCESS_BATCH_LINGER_US=200`,
`PD_STAGE_TIMING_EVERY=1000`, and
`PD_RUST_LOG=warn,dynamo_static_rpc_split=debug`. Run the smoke script,
then `run-nix-mocker-pd-rpc-split.sh rNEW` with explicit vCluster, NFS,
AIPerf-node, stager, result-directory, and `PD_SPLIT_BINARY` inputs. Repeat
with `PD_LAZY_CHANNELS=1` for the lazy arm. The guarded runner checks the
dataset hash, binary, gateway state, and component counts, and captures the
deployment snapshots, gateway logs, Job objects, summaries, and raw client
exports here. After the pair, all four vCluster gateways were scaled to zero
and the static gateway's previous pinned binary/spec was restored. Nothing
was deployed outside the vCluster.
