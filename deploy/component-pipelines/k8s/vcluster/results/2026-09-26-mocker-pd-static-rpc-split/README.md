# Static prefill gRPC response-header timing

One diagnostic AGW static run used the frozen raw ISL4000 dataset, SHA256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`,
in the `gateway-poc` vCluster namespace `dynamo-components-v2` on 2026-09-26.
The fixture was one AGW static gateway with 16 worker threads, four gRPC
connections per endpoint, 32-item preprocessor batch cap and 200 µs linger;
four preprocessor, four selector, four prefill, and 16 decode mock-worker
replicas; and six AIPerf 0.12.0 clients at 128 concurrency each for 45 seconds.
The diagnostic binary was built from Dynamo commit `4c2012ce9a` as Nix output
`/nix/store/91s3dwdapry8jpp0yq4zxja1rv0dlm0i-agentgateway-component-pipeline-0.0.0-b14ca87d0a`.
The r74 facade services remained pinned to the default Nix bundle, whereas
the earlier r71/r72 diagnostic pair used opt-in sampled facades. Thus this
table is a stability check, not a strict binary-only A/B. The streamed P/D
handoff smoke passed before measurement.

| Run | Successful requests | Sum of client RPS | Errors/cancelled |
| --- | ---: | ---: | ---: |
| Static `r74` RPC split | 285,103 | 6,325.08 | 0 / no |
| Earlier static `r71` control | 285,837 | 6,341.12 | 0 / no |
| Earlier generic `r72` context | 423,983 | 9,401.88 | 0 / no |

The static stage sampler (`DYN_STATIC_STAGE_TIMING_EVERY=1000`) produced 286
records in the gateway log, including the pre-run smoke/warmup. Its mean
stages were prepare 51.80 ms, prefill 29.09 ms, selector 29.21 ms, decode
setup 1.82 ms, total 111.92 ms. These closely match static `r71` and do not
indicate a diagnostic-induced throughput change.

The new structured `dynamo_static_rpc_split` events divide the prefill gRPC
call into (1) the wait from invoking `GenerateRaw` until tonic returns the
stream response and (2) the wait from that point through the terminal
handoff. Filtering to client 0's AIPerf measurement window
`20:14:13.403327`–`20:14:58.450327` UTC yielded 279 samples:

| Sampled prefill segment | Mean | Median | P95 | Maximum |
| --- | ---: | ---: | ---: | ---: |
| Through response headers | 29.57 ms | 29.79 ms | 38.08 ms | 44.29 ms |
| Response stream to terminal handoff | 12.5 µs | — | — | 122 µs |

Thus the ~29 ms static prefill stage is overwhelmingly **before tonic returns
the response stream**. Prior paired facade sampling measured the prefill
handler at about 0.1 ms and its terminal output at about 0.4 ms. The
remaining time is upstream of the measured facade handler or in the
header-return path, but this probe cannot yet separate client scheduling,
HTTP/2 transport queues, server admission, or response-header dispatch.
The selector remains a separate ~29 ms unary wait. Do not infer that either
component's Dynamo core processing is slow or that a particular HTTP/2 knob
will close the gap without another targeted measurement.

The guarded reproduction command is
`run-nix-mocker-pd-rpc-split.sh rNEW` after staging the Nix output and
rolling AGW static with `PD_STAGE_TIMING_EVERY=1000`,
`PD_RUST_LOG=warn,dynamo_static_rpc_split=debug`, 16 worker threads, four
gRPC channels, 32-item batch cap, and 200 µs linger. Set the explicit
vCluster kubeconfig/server/namespace, two AIPerf node names, NFS store
server/export, `NIX_STAGER_POD`, and `PD_SPLIT_BINARY` to the built path.
The runner checks the dataset hash, active gateway, and binary, and retains
gateway/deployment/Job snapshots, the gateway log, summary, and six raw
client exports here. After the run, AGW static was scaled to zero and its
previous pinned binary/spec restored. No deployment outside the vCluster
was changed.
