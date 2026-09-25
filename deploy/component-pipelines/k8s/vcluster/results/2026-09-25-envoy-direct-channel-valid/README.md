<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Verified Envoy-direct 32-channel probe

The [earlier channel probe](../2026-09-25-envoy-direct-channel-ab/README.md)
was invalid because the Envoy dynamic module read `grpc_conns` from its own
ConfigMap, overriding the Deployment environment setting. This corrected
trial changed `"grpc_conns":4` to `"grpc_conns":32` in the
`envoy-independent` ConfigMap **before** starting one gateway replica. The
startup log confirmed:

```text
generic_pipeline: loaded /etc/dynamo-pipeline/graphs/aggregate.yaml (3 steps), 32 grpc conns, 4 runtime threads
```

No binary, graph, facade, worker count, or AIPerf setting changed. The run
used the frozen ISL4000 raw-payload dataset, six AIPerf 0.12.0 clients at
concurrency 128 each for 45 seconds, a 90-second barrier, and the same two
dedicated load-generator nodes. All resources were confined to vCluster
namespace `dynamo-components-v2`.

| Actual channels per endpoint | Job | Aggregate RPS | Successful requests | Request errors |
|---:|---|---:|---:|---:|
| 4 | `nixv2-isl4000-envoy-generic-r4` | 5,879.23 | 265,143 | 0 |
| 4 | `nixv2-isl4000-envoy-generic-r5` | 5,677.72 | 256,039 | 0 |
| 32 | `nixv2-isl4000-envoy-generic-r6` | 5,466.62 | 246,611 | 0 |

The six JSON/CSV/console exports for the verified 32-channel run are retained
under [`raw_aiperf/`](raw_aiperf/); the four-channel exports are in the linked
earlier probe. One valid 32-channel run is insufficient for a fine-grained
regression claim, but it does not show a large gain capable of closing the
Envoy-direct versus Envoy-callout ISL4000 gap (earlier callout median
9,255.85 RPS). The direct path needs CPU/queueing attribution rather than
blind connection-pool scaling. The ConfigMap was restored to four channels
after this experiment.
