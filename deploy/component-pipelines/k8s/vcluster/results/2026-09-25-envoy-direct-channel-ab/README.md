<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Envoy-direct gRPC channel-count probe

This isolated, sequential A/B ran entirely inside the `dynamo-components-v2`
vCluster, using the same Nix-built Envoy direct gateway, aggregate graph, four
preprocessors, one selector, and 16 synthetic benchmark workers as the
[`2026-09-24` matrix](../2026-09-24-nix-build-mocker-parity/README.md).
The six AIPerf 0.12.0 clients reused its frozen ISL4000 raw-payload dataset,
45-second concurrency-128 policy, two-node 3/3 placement, and 90-second start
barrier. Only `DYN_GRPC_CHANNELS_PER_ENDPOINT` changed on the gateway. The
gateway was one replica for each trial and all other gateway arms were zero.

| Job | gRPC channels per endpoint | Aggregate RPS | Successful requests | Errors | Effective concurrency | Weighted mean latency |
|---|---:|---:|---:|---:|---:|---:|
| `nixv2-isl4000-envoy-generic-r4` | 4 | 5,879.23 | 265,143 | 0 | 742.79 | 126.31 ms |
| `nixv2-isl4000-envoy-generic-r5` | 32 | 5,677.72 | 256,039 | 0 | 745.04 | 131.18 ms |

Each value is calculated from the six retained AIPerf JSON summary exports
under [`raw_aiperf/`](raw_aiperf/). Increasing the channel count did not close
the roughly 5.8k versus 9.3k RPS Envoy-direct/callout ISL4000 gap; the single
32-channel trial was 3.4% slower than its adjacent four-channel baseline.
This is a diagnostic pair, not enough repeated randomized trials to quantify
a small change. It does rule out a large improvement from this knob in this
topology. Afterward, the vCluster Deployment was restored to four channels
and scaled to zero.

The gateway's `GrpcTransport` makes separate lazy tonic channels per endpoint,
so this test varied actual HTTP/2 connection fan-out without rebuilding Envoy
or changing the pipeline graph. The candidate bottleneck remains elsewhere in
the direct transport path or its interaction with Envoy's worker scheduling;
it is not established by this test alone.
