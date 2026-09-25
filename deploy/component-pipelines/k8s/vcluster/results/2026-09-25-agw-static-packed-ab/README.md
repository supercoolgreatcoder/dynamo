<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW-static packed-token transport probe

This campaign changed only the AGW-static gateway's transport to request the
existing preprocessor facade's packed little-endian token bytes and forward
those bytes to the existing selector and worker facades. The canonical Dynamo
preprocessor, selector, and worker-side postprocessor implementations were not
changed. Source revision:
`f8a7d0f0b8be6e5eb639dc7e1c9192a2d4c4d5d1`. The pinned Nix flake is
`supercoolgreatcoder/dynamo-nix-envs` branch
`feat/dynamo-component-pipeline-builds`, commit `672d85e`; the successfully
built Agentgateway output is
`/nix/store/wm0g6l7sj1x4f63c936iyhy78igxsa5z-agentgateway-component-pipeline-0.0.0-b14ca87d0a`.

All runs were inside vCluster namespace `dynamo-components-v2`, with one
gateway replica, four `fastokens` preprocessors, one selector, 16 benchmark
workers, and six AIPerf 0.12.0 clients split 3/3 across the same two dedicated
CPU nodes as the [baseline matrix](../2026-09-24-nix-build-mocker-parity/README.md).
The frozen short and ISL4000 raw OpenAI payloads and prepared Mooncake mmap
were unchanged. Each client used concurrency 128 for 45 seconds; no AIPerf
tokenization or synthesis occurred during measurement. Every listed run had
six completed clients, zero request errors, and no cancellation.

| Workload | Jobs | Packed-token RPS | Previous AGW-static RPS | Interpretation |
|---|---|---:|---:|---|
| ISL4000 | `nixv2-isl4000-agw-static-r4/r5/r6` | 8,982.09 median (8,660.19–9,079.30) | 6,600.19 median | 36% higher achieved throughput; still 12% below AGW generic's 10,190.12 median |
| Short | `nixv2-short-agw-static-r4` | 12,844.10 | 12,575.44 median | One no-regression check, not a new median |
| Mooncake | `nixv2-mooncake-agw-static-r5` | 3,022.88 | 3,023.74 median | Trace-rate parity; not a gateway ceiling |

Six-client AIPerf JSON, CSV, and console exports for every Job are retained
under [`raw_aiperf/`](raw_aiperf/). The change is promising, but the original
and modified ISL4000 runs were sequential rather than interleaved, so the
36% figure is a descriptive comparison, not an independently controlled causal
estimate. The close three-run cluster and unchanged workload/topology support
the packed-wire-format hypothesis. A stricter A/B would alternate old and new
Nix executables under an immutable pre-registered plan.
