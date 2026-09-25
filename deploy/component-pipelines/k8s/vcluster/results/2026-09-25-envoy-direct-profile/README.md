<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Envoy-direct CPU diagnostic (original four-thread setting)

`nixv2-isl4000-envoy-generic-r7` ran the frozen ISL4000 workload inside
vCluster with the original direct-mode budget: four Tokio threads, six Envoy
workers, and four tonic channels per endpoint. The module's existing
`GENERIC_PIPELINE_PROFILE_SECS=210` hook sampled CPU at 199 Hz during the
45-second AIPerf run. All six clients completed with zero errors at 5,894.49
aggregate RPS. Their raw summaries are under [`raw_aiperf/`](raw_aiperf/).

The module's own stack classifier reported 35,489 samples:

| Classified area | Share |
|---|---:|
| Envoy | 49.38% |
| serde_json | 29.33% |
| pipeline_grpc | 7.89% |
| tonic/h2/hyper client | 7.57% |
| prost_reflect | 4.84% |
| Tokio | 0.89% |

The largest named leaves included base64 decode (5.48%), base64 encode
(2.61%), and JSON string escaping (3.02%). The classifier assigns a whole
stack to its first matching category, samples all process threads during a
window that includes idle time, and is not an exclusive component-level CPU
accounting system. This is diagnostic evidence only. The matched
[callout profile](../2026-09-25-envoy-callout-profile/README.md) had a
different eight-thread budget, so percentages alone do not establish the
cause of its higher throughput. The subsequent [thread-budget
experiment](../2026-09-25-envoy-direct-threads/README.md) provides the
stronger attribution.
