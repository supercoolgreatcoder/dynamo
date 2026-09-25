<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Envoy-direct Envoy-worker-count probe

After the successful [eight-Tokio-thread direct
series](../2026-09-25-envoy-direct-threads/README.md), this isolated vCluster
trial changed only Envoy's `--concurrency` value from six to eight, matching
the callout arm's Envoy worker count. The dynamic module still reported four
tonic connections per endpoint and eight Tokio runtime threads; the binary,
graph, facade services, frozen ISL4000 workload, and six-client AIPerf load
policy were unchanged.

`nixv2-isl4000-envoy-generic-r11` reached 9,637.86 RPS with 434,358
successful requests and zero errors. Its [six-client raw
exports](raw_aiperf/) and [execution snapshot](benchmark_execution.json)
are retained. The separate six-Envoy-worker series had a 9,680.97 RPS
three-run median. This one diagnostic trial shows no material benefit from
raising Envoy worker count after increasing Tokio threads; it does not prove
a small difference. The vCluster Deployment was returned to six Envoy
workers before the optimized short and Mooncake tests.
