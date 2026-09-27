<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW generic single-gateway downstream-scale diagnostic, ISL4000

This vCluster-only A/B asks whether more non-gateway mock capacity raises
throughput through one AGW generic gateway. The gateway stayed on one Pod,
one pinned Nix binary
(`/nix/store/ha9684gksqyiilhjd43y9ccyd65rxwi0-agentgateway-component-pipeline-0.0.0-b14ca87d0a/bin/agentgateway`),
16 worker threads, and eight independent gRPC channels per endpoint for all
four runs. Eight channels give a Kubernetes Service multiple connections
over which to distribute traffic; they do not guarantee that every replica
receives requests. Six AIPerf 0.12.0 clients remained on the same two
CPU nodes at concurrency 128 each for 45 seconds. They replayed the frozen
raw ISL4000 payload with SHA256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.
The streamed P/D smoke passed at both replica counts.

| Job | Preprocessor / selector / prefill / decode replicas | Completed | Exported RPS | Start skew | Placement audit |
| --- | --- | ---: | ---: | ---: | --- |
| r94 | 4 / 4 / 4 / 16 | 414,550 | 9,188.53 | 1 s | baseline |
| r95 | 8 / 8 / 8 / 32 | 392,990 | 8,713.21 | 2 s | **confounded:** one selector Pod on gateway node |
| r96 | 8 / 8 / 8 / 32 | 406,139 | 8,988.71 | 2 s | zero downstream Pods on gateway/client nodes |
| r97 | 4 / 4 / 4 / 16 | 423,080 | 9,385.63 | 2 s | live pre-run check: zero downstream Pods on gateway/client nodes |

All four runs had zero errors and cancellations. The isolated scaled run
`r96` is 3.2% below the midpoint of the two baseline controls (9,287.08
RPS). This sequence provides **no evidence that doubling all four
downstream mock components lifts throughput**. It is not an absolute
single-gateway ceiling: this closed-loop load, shared non-client worker
nodes, and run-to-run drift can still limit or move observed RPS. The
eight-channel baseline should not be compared causally with earlier
four-channel series without an interleaved channel-count A/B.

`r95` is retained but excluded from that comparison. Its scaled selector
Pod co-located on the gateway's CPU node. We temporarily added selector
node affinity excluding the gateway and two AIPerf nodes, waited for the
old Pod to terminate, and verified the corrected 56-Pod fleet had zero
co-location before `r96`. That affinity stayed in force for the `r97`
return control. Job, AIPerf Pod, scaled component-Pod, and Deployment
snapshots make the `r95`/`r96` placement decision auditable. The `r97`
zero-co-location check was observed live before the run; the retained
restored-Pod snapshot was taken after selector affinity was removed, so
it is not a precise `r97` placement record.

To reproduce, use the explicit `gateway-poc` vCluster kubeconfig and
namespace `dynamo-components-v2`, the pinned Nix gateway and frozen dataset,
and the two AIPerf nodes named in `job-r94.json`. Set
`DYN_GRPC_CHANNELS_PER_ENDPOINT=8` on the AGW generic Deployment and keep
`workerThreads: 16`. Scale the four downstream Deployments together to
`8/8/8/32`, wait for Ready, and exclude the gateway and AIPerf nodes from
mock Pod placement. Run `run-nix-mocker-trial.sh pd-agw-generic isl4000 rNEW`
with `PD_PREPROCESSOR_REPLICAS=8`, `PD_SELECTOR_REPLICAS=8`,
`PD_PREFILL_REPLICAS=8`, and `PD_DECODE_REPLICAS=32`; return to `4/4/4/16`
and repeat without those overrides. The runner checks the requested Ready
counts and rejects unsynchronized or error-bearing AIPerf exports.

The fixture was restored afterward: downstream `4/4/4/16` Ready, selector
affinity removed, AGW gRPC channels back to four, and gateway scaled to
zero. No deployment occurred outside the vCluster.
