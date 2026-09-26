<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Synthetic P/D generic AGW replay, 2026-09-26

This is a **vCluster-only**, six-client characterization of the generic AGW graph with a separate Dynamo-backed preprocessor, InferencePool-discovered selector, synthetic prefill workers, and synthetic decode workers. The workers use the real component facade and Dynamo `AsyncEngine` protocol but deliberately do **not** exercise GPU inference, NIXL, or model weights. This is gateway/component throughput evidence, not real-engine serving throughput.

The immutable [benchmark plan](benchmark_plan.json) has SHA-256 `36e98edcd8ae9007724870ae3072e46d45b0037a53e21485c1b0f695a2b6dccc`. Source facade commit: `995c04e74f785a498470f305d37875703fb72b86`; Nix package pin: `supercoolgreatcoder/dynamo-nix-envs`, branch `feat/dynamo-component-pipeline-builds`, commit `0cd7bb1`. Facade output: `/nix/store/w77ac5nlyc9543kgkwdxpf9a7yk5l7y6-dynamo-component-facade-1.6.0-995c04e74f`; AGW bundle: `/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699`.

| Workload | Audit | Successful requests | Summed-client RPS | Global-window RPS | Errors / cancellations |
| --- | --- | ---: | ---: | ---: | ---: |
| Short | Valid, summary-level | 508,227 | 11,253.51 | 10,856.92 | 0 / 0 |
| ISL4000 | Valid, summary-level | 428,621 | 9,502.47 | 8,933.44 | 0 / 0 |
| Mooncake, 45 seconds | **Invalid: incomplete trace** | 136,074 / 136,194 scheduled | 3,023.71 | 2,922.72 | 0 / 0 |

The Mooncake 45-second phase drops exactly 20 boundary-timestamp requests per client from AIPerf's completed count despite no reported errors. That result is retained as a diagnostic observation, **not** a trace-complete parity number. A distinct 46-second completion-grace series is recorded separately in `../2026-09-26-mocker-pd-mooncake-grace/`; it retains the same trace timestamps and serving topology but changes the measurement window and must not be presented as the same-series 45-second RPS.

An unchanged ISL4000 repeat, `nixpd-isl4000-pd-agw-generic-r2`, also passed audit: 434,012 successful requests, zero errors/cancellations, 9,607.01 summed-client RPS, and 9,296.84 global-window RPS. Its 1.664-second client-start spread differs from the much tighter Envoy generic P/D repeats; see the [paired Envoy comparison](../2026-09-26-mocker-pd-envoy-generic/README.md) before attributing an ISL4000 difference to either gateway.

`summed-client RPS` adds six AIPerf client-reported rates; `global-window RPS` divides all successful requests by the interval from the earliest client start to the latest client end. The latter is conservative when clients have skew. AIPerf 0.12.0 did not report output-token throughput for short or ISL4000; the audit records it as unavailable, not zero. Only six summary exports were captured, so merged latency percentiles and per-request ID completeness cannot be independently recomputed. The P/D topology and client-node placement also differ from the older aggregate/Claude replay, so these numbers are not a same-series improvement claim.

## Reproduce inside the existing vCluster

The scripts refuse a kubeconfig whose active server differs from `https://gateway-poc.mkhadkevich-dev:443`, and target only `dynamo-components-v2`. Do not use the host cluster's default service-account context. Set `VCLUSTER_KUBECONFIG`, `VCLUSTER_EXPECTED_SERVER`, `VCLUSTER_NAMESPACE`, `FACADE_STORE_PATH`, and `GATEWAY_BUNDLE_PATH` to the pinned values above, then run `run-nix-mocker-pd.sh` followed by `smoke-nix-mocker-pd.sh`. The fixture script first submits each generated resource to Kubernetes server-side dry-run. `PD_DRY_RUN=1` checks every resource without applying it.

For a trial, additionally set the existing vCluster Nix NFS server/path and `ENVSUBST_BIN`; invoke `run-nix-mocker-pd-trial.sh short rN`, `isl4000 rN`, or `mooncake rN` from the repository root. The wrapper checks the immutable plan SHA, source dataset SHA in the vCluster, gateway/components Ready states, inactive competing gateways, and six-client node occupancy. It refuses an already-existing Job name. Run `audit-nix-mocker-pd.py <result-directory> <workload> rN` on each result. The older AIPerf `0.12.0` image is intentional for reference-replay semantics; `0.13.0` single-manager Mooncake preparation failed before measurement in this campaign. Do not treat it as an untested default upgrade.

Raw AIPerf exports are deliberately not committed. The execution JSON records Job UID/spec/pods and each summary records the six raw-export SHA-256 digests. The raw files remain in the vCluster NFS `/shared/nix/aiperf/results/<job>/` and in local ignored `raw_aiperf/`; copy and hash them before using the results for a formal comparison. The benchmark-audit checks follow the AIPerf configuration, execution, and analysis skill quality gates, adapted to this custom gateway/mocker campaign rather than a DynamoGraphDeployment recipe optimization run.
