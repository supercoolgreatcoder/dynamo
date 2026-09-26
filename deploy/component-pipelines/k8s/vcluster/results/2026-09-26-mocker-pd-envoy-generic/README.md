<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Synthetic P/D Envoy generic replay, 2026-09-26

Three sequential six-client Jobs exercised a single Nix-built Envoy generic dynamic-module gateway against the same vCluster-only Dynamo-backed preprocessor, InferencePool selector, synthetic prefill workers, synthetic decode workers, gRPC descriptor/specs, and disaggregated graph used by the [AGW generic replay](../2026-09-26-mocker-pd-generic/README.md). The immutable [plan](benchmark_plan.json) has SHA-256 `9d0c3b7fef51ff82173d97623016f8b99869a58e78e41cfa12ddf3bc2aff3d68`. Envoy was 1/1 Ready and AGW 0/0 during these Jobs. Short and ISL4000 use 45-second phases; Mooncake uses the same trace-complete 46-second phase as the separate AGW grace series.

| Workload | Audit | Envoy requests | Envoy summed-client RPS | Matching AGW RPS | Observed Envoy difference | Errors / cancellations |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Short | Valid, summary-level | 502,985 | 11,140.53 | 11,253.51 | -1.00% | 0 / 0 |
| ISL4000 | Valid, summary-level | 407,417 | 9,036.72 | 9,502.47 | -4.90% | 0 / 0 |
| Mooncake, 46-second completion grace | Valid, full trace count | 136,194 / 136,194 scheduled | 3,022.67 | 3,024.60 | -0.06% | 0 / 0 |

The first table compares one trial per arm and workload; it is an observed difference, **not** a repeatability-qualified performance claim. ISL4000 was repeated under the unchanged plans:

| Arm / trial | Audit | Summed-client RPS | Global-window RPS | Client-start spread |
| --- | --- | ---: | ---: | ---: |
| AGW generic r1 | Valid | 9,502.47 | 8,933.44 | 2.896 s |
| AGW generic r2 | Valid | 9,607.01 | 9,296.84 | 1.664 s |
| Envoy generic r1 | Valid | 9,036.72 | 9,027.78 | 0.024 s |
| Envoy generic r2 | **Invalid: start synchronization** | 9,333.90 | 8,758.76 | 3.027 s |
| Envoy generic r3 | Valid | 9,278.17 | 9,271.77 | 0.034 s |

Envoy r2 exceeded the predeclared 3-second start-spread gate by 27 ms and was retained but excluded from valid comparisons. The two valid summed-client rates suggest Envoy may be lower on ISL4000, but AGW clients started 1.7–2.9 seconds apart while Envoy's valid clients started almost simultaneously. Global-window normalization then shows a much smaller difference. The fixture's wall-clock barrier occurs **before** `aiperf profile` initialization, so it does not force synchronized measurement starts; this is a plausible source of the observed skew, not proof of gateway behavior. The effective overlap of offered load differs; **the cause and size of any gateway-specific ISL4000 gap remain unresolved**. A tighter synchronized paired series or request-level load trace is needed before changing gateway code on this evidence.

The global-window rates, per-client metrics, exact raw-export SHA-256 digests, Job UIDs/specs, node occupancy, dataset hashes, and Mooncake cache-hit proof are in the adjacent summary/audit/execution files. AIPerf 0.12.0 omitted output-token throughput for short and ISL4000. Only six summary exports are available, not per-request records, so merged percentiles and request-ID uniqueness cannot be independently checked. The synthetic workers test gateway/component throughput, not vLLM/SGLang GPU serving or NIXL handoff.

To reproduce, first deploy the shared mock P/D fixture with `run-nix-mocker-pd.sh`, then the Envoy host with `run-nix-mocker-pd-envoy.sh`; both scripts require the explicit vCluster kubeconfig/server/namespace and support a server-side `PD_DRY_RUN=1`. Smoke `dynamo-pd-envoy-generic` with `PD_GATEWAY_SERVICE=dynamo-pd-envoy-generic smoke-nix-mocker-pd.sh`. Scale `dynamo-pd-agw-generic` to 0 and `dynamo-pd-envoy-generic` to 1 **inside the vCluster**, verify no active benchmark Job, and run `run-nix-mocker-pd-trial.sh {short|isl4000|mooncake} rN envoy` sequentially. The wrapper checks the frozen plan and dataset hashes, inactive competitor gateways, component replica counts, client-node occupancy, six raw AIPerf exports, and Mooncake mmap cache hits. Validate each run with `audit-nix-mocker-pd.py <this-directory> <workload> rN --envoy`.

Raw AIPerf exports are excluded from Git but remain in vCluster NFS `/shared/nix/aiperf/results/<job>/` and local ignored `raw_aiperf/`; committed summaries hash each raw export. All writes in this recipe target only `https://gateway-poc.mkhadkevich-dev:443` namespace `dynamo-components-v2`.
