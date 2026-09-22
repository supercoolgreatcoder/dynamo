<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Zero-delay gateway benchmark — 2026-09-22

The run was executed entirely in the `gateway-poc` vCluster, namespace
`dynamo-components-v2`. Each cell is the median of three interleaved trials with 200
warmup requests and 2,000 measured requests. Requests produce one streamed token.
All 90,000 measured requests completed without an error.

The four component-pipeline arms use the deterministic canonical worker facade. The
Dynamo reference uses four `dynamo.mocker` workers configured with prefill and decode
speedup ratios of 1,000,000. Its result includes the stock frontend, Dynamo runtime,
NATS, and mocker path, while the component arms include their gRPC component path; the
numbers therefore compare the end-to-end orchestration stacks, not just HTTP parsing.

| Arm | Concurrency | Median RPS | Median p50 (ms) | Median p95 (ms) | Median p99 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| AGW static | 1 | 509.54 | 1.895 | 2.325 | 3.411 |
| AGW static | 16 | 5,573.41 | 2.603 | 4.271 | 8.911 |
| AGW static | 64 | 9,888.64 | 6.036 | 9.691 | 11.899 |
| AGW generic | 1 | 291.57 | 3.262 | 4.437 | 5.858 |
| AGW generic | 16 | 3,918.84 | 3.998 | 5.541 | 7.049 |
| AGW generic | 64 | 8,200.26 | 7.298 | 11.892 | 15.056 |
| Envoy independent | 1 | 282.08 | 3.388 | 4.521 | 5.690 |
| Envoy independent | 16 | 6,968.07 | 2.151 | 3.675 | 4.496 |
| Envoy independent | 64 | 16,662.65 | 3.512 | 6.752 | 8.334 |
| Envoy callouts | 1 | 279.76 | 3.407 | 4.542 | 6.350 |
| Envoy callouts | 16 | 6,471.92 | 2.281 | 3.974 | 4.800 |
| Envoy callouts | 64 | 16,345.66 | 3.564 | 6.671 | 8.733 |
| Dynamo frontend reference | 1 | 23.16 | 43.005 | 44.015 | 45.221 |
| Dynamo frontend reference | 16 | 344.82 | 43.082 | 64.988 | 78.031 |
| Dynamo frontend reference | 64 | 1,416.44 | 43.026 | 47.102 | 67.119 |

The sibling JSONL file is the unmodified benchmark-pod log and is the source of truth.
The Envoy callout arm used a run-scoped authority-to-cluster map for the four stable
worker Pods. A production deployment requires CDS/xDS endpoint publication rather than
this static benchmark mapping.

Streaming correctness passed for every gateway arm with eight data chunks, terminal
`finish_reason: length`, and `[DONE]`. A stock Dynamo frontend plus Dynamo mocker also
passed the same OpenAI request. A real GPU vLLM/SGLang correctness run was not possible
inside this vCluster because every virtual node reports no `nvidia.com/gpu` capacity;
the test was not moved to the host cluster.
