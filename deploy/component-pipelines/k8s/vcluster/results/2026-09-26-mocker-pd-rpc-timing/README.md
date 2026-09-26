# Paired AGW static/generic selector and prefill RPC timing

This is a **diagnostic pair**, not a promotion-grade interleaved A/B. Both
jobs ran only in the `gateway-poc` vCluster in namespace
`dynamo-components-v2` on 2026-09-26. The workload was the same frozen raw
ISL4000 JSONL, SHA256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.
Each arm used six AIPerf 0.12.0 clients, 45 seconds, 128 concurrency per
client, one 16-thread AGW gateway with four gRPC channels per endpoint, four
preprocessors, four selectors, four prefill mock workers, and 16 decode mock
workers. The AGW static and generic gateway binaries differ by orchestration
implementation, but used the same preprocessor and diagnostic selector and
prefill facade binaries. Deployment and Job snapshots, six per-client
AIPerf exports, gateway logs, and component logs are retained here.

| Arm | Job | Completed requests | Exported RPS | Errors | Client mean latency range |
| --- | --- | ---: | ---: | ---: | ---: |
| AGW static | `nixpds-isl4000-pd-agw-static-r71` | 285,837 | 6,341.12 | 0 | 113.4–115.2 ms |
| AGW generic | `nixpd-isl4000-pd-agw-generic-r72` | 423,983 | 9,401.88 | 0 | 32.9–42.3 ms |

Both jobs passed the existing P/D streamed smoke and had no cancelled client
runs. All six static clients used about 120–122 of their 128 concurrency slots;
generic clients used about 45–68 slots. This matches the latency/throughput
relationship of the closed-loop load generator: static is waiting rather than
exhausting the available client load.

`DYN_COMPONENT_RPC_SAMPLE_EVERY=1000` selected sparse facade requests. The
following aggregates use log timestamps in the respective client measurement
windows (`19:15:37`–`19:16:23` UTC for static, `19:18:49`–`19:19:35` UTC for
generic) and exclude smoke requests by requiring `packed_token_bytes>10000`.
Generic logs contain earlier static samples because its Pods were not
restarted between arms; **filter by timestamp before comparing**.

| Facade sample | Static count / mean / max | Generic count / mean / max |
| --- | ---: | ---: |
| Selector `Select` handler | 282 / 410 µs / 14,384 µs | 412 / 131 µs / 1,963 µs |
| Prefill `GenerateRaw` handler | 282 / 108 µs / 317 µs | 412 / 111 µs / 806 µs |
| Prefill `GenerateRaw` terminal stream | 282 / 401 µs / 4,023 µs | 412 / 243 µs / 1,724 µs |

The mean packed token payload was 16.1 KiB in both arms; mean backend JSON
was 832 bytes. The static stage sampler, by contrast, measured approximately
29.2 ms for its prefill stage and 28.9 ms for selection (286 sparse samples).
Thus the large delay is **outside the measured facade handler and stream
generation**, likely in client-side scheduling, transport, or queueing; these
samples do not isolate which. It is not evidence for changing Dynamo selector
or worker core code. The previous [CPU samples](../2026-09-26-mocker-pd-cpu-samples/README.md)
also show the static gateway using only about five cores while the generic
gateway used about 15 without CPU throttling.

One contract difference deserves a separate correctness check: static
`GenerateRaw` carries `prompt_tokens≈4035`, while generic carries zero, even
though the packed token IDs and backend JSON payload sizes match. The mock
facade does not currently use that field to generate the handoff, so it does
not explain this throughput gap. It should not be assumed harmless for all
real-worker integrations.

To reproduce, first build and stage
`nix build .#component-pipeline-facade-rpc-stats` from the pinned
`dynamo-nix-envs/gateway-pipeline` flake, using the existing
`stage-nix-closure.sh` recipe. Roll only selector and prefill to the built
`.../bin/dynamo-component-facade` with
`run-nix-mocker-pd-rpc-stats.sh selector` and `prefill`, setting
`PD_RPC_SAMPLE_EVERY=1000`. Set the explicit vCluster kubeconfig and
`VCLUSTER_EXPECTED_SERVER=https://gateway-poc.mkhadkevich-dev:443`,
`VCLUSTER_NAMESPACE=dynamo-components-v2`, `PD_RPC_BINARY`, both AIPerf
nodes, the NFS server/export, and the existing store stager Pod. Scale only
the selected gateway to one replica (other gateways to zero), confirm a P/D
smoke response, then run
`run-nix-mocker-pd-rpc-timing.sh pd-agw-static rNEW` and
`run-nix-mocker-pd-rpc-timing.sh pd-agw-generic rNEW`, each with a fresh trial
ID. The runner verifies deployment counts, diagnostic binary, vCluster
identity, no active Jobs, and dataset hash; it records all artifacts under a
dated result directory. When finished, scale the gateway to zero and roll the
facades back to the pinned default binary with
`PD_RPC_SAMPLE_EVERY=0 run-nix-mocker-pd-rpc-stats.sh COMPONENT`.
