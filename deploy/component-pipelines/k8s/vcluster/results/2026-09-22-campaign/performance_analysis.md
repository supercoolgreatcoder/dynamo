<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Component gateway campaign — 2026-09-22

All deployments and load generators ran in the `gateway-poc` vCluster namespace
`dynamo-components-v2`. Mock-worker throughput and real-GPU correctness were separate
experiments. AIPerf 0.12.0 ran six clients per cell; synthetic clients used concurrency
128 for 45 seconds. Mooncake replay used the 23,608-record trace, fixed scheduling,
80x time compression, and 128-token hash blocks.

## Audited throughput

| Workload | Arm | Successful RPS | Successful | Errors | Audit |
| --- | --- | ---: | ---: | ---: | --- |
| short | AGW static | 11,127.67 | 502,508 | 3 | valid |
| short | AGW generic | 5,379.16 | 243,576 | 0 | valid |
| short | Envoy generic | 5,789.67 | 261,148 | 0 | valid |
| short | Envoy callouts | 5,615.05 | 253,357 | 0 | valid |
| short | Dynamo reference | 7,238.75 | 326,397 | 1 | valid |
| ISL4000 | AGW static | 6,628.32 | 300,980 | 1 | valid |
| ISL4000 | AGW generic | 4,183.08 | 189,795 | 0 | valid |
| ISL4000 | Envoy generic | 4,738.48 | 213,786 | 0 | valid |
| ISL4000 | Envoy callouts | 4,963.60 | 223,866 | 0 | valid |
| ISL4000 | Dynamo reference | 5,303.30 | 240,219 | 2 | valid |
| Mooncake | AGW static | 634.40 | 38,278 | 0 | valid |
| Mooncake | AGW generic | 409.62 | 24,718 | 228 | invalid: OOM |
| Mooncake | Envoy generic | 565.42 | 34,205 | 0 | valid |
| Mooncake | Envoy callouts | 458.60 | 27,530 | 108,633 | invalid: gRPC resets |
| Mooncake | Dynamo reference | 0 | 0 | 90,626 | invalid: connection failures |

AGW static was 53.73% faster than the stock Dynamo reference on short requests and
24.99% faster on ISL4000. The generic implementations were slower than Dynamo in both
valid synthetic workloads. These are single-run results without an empirical noise
floor, so modest differences are characterization rather than promotion evidence.

The compressed Mooncake trace is a stability screen. AGW generic was OOM-killed
(exit 137), Envoy callouts remained alive but its prepare callout reported gRPC
`LocalReset`, and the stock Dynamo reference stopped accepting connections. Those
cells are retained as failed evidence and excluded from throughput comparisons. AGW
static and independent Envoy completed their Mooncake cells without request errors.

## Real engine correctness

The real-worker manifest uses a BusyBox root filesystem plus read-only Nix Python,
CUDA 13.3, compiler, and engine closures. Both `Qwen/Qwen3-0.6B` workers ran on B200
GPUs and ended 2/2 Ready with zero restarts. vLLM and SGLang each passed:

- model discovery through `/v1/models`;
- a unary OpenAI chat completion with HTTP 200 and non-empty generated tokens; and
- streaming chat chunks terminated by `data: [DONE]`.

The Nix CUDA compatibility view explicitly exports `/cuda/include` because Nix's
standalone `nvcc` cannot infer headers through the merged-toolkit symlink.

## Limitations and next work

Per-request AIPerf exports remain pod-local, CPU resources were not normalized across
hosts, and no n=3 noise-floor pilot was collected. Before choosing a generic host, fix
AGW memory growth and Envoy callout stream resets, rerun Mooncake unchanged, then run
the one-time three-repetition noise-floor pilot.
