<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Envoy-callout CPU diagnostic (original eight-thread setting)

`nixv2-isl4000-envoy-callouts-r4` ran the same frozen ISL4000 workload inside
vCluster with the retained callout budget: eight Tokio threads and eight
Envoy workers. The module's existing `GENERIC_PIPELINE_PROFILE_SECS=210` hook
sampled CPU at 199 Hz during the 45-second AIPerf run. All six clients
completed with zero errors at 9,494.51 aggregate RPS. Their raw summaries
are under [`raw_aiperf/`](raw_aiperf/).

The module's own stack classifier reported 39,648 samples:

| Classified area | Share |
|---|---:|
| serde_json | 44.95% |
| Envoy | 37.73% |
| prost_reflect | 9.12% |
| pipeline_grpc | 7.80% |
| Tokio | 0.31% |
| tonic/h2/hyper client | 0.05% |

The tonic-client category nearly disappears because callout mode delegates
gRPC streams to Envoy-managed clusters. This shows a real transport-path
difference, but these stack percentages are not normalized CPU per request
and the [direct profile](../2026-09-25-envoy-direct-profile/README.md) used
only four Tokio threads. Raising direct mode to eight Tokio threads closed
its throughput gap without changing transport; see the
[thread-budget result](../2026-09-25-envoy-direct-threads/README.md).
