<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Disaggregated Envoy-callout mocker characterization

The Nix-built Envoy dynamic module ran the real generic P/D graph through Envoy HTTP/2 callouts inside the **vCluster only**. Four Dynamo-backed preprocessors, four selectors, four synthetic prefill workers, and 16 synthetic decode workers were Ready. The decode worker rejects any request missing the prefill handoff; [the streamed smoke test](../../smoke-nix-mocker-pd.sh) passed through the callout host. The callout Deployment used one gateway replica, Envoy concurrency 6, and eight generic-pipeline threads, matching the direct-Envoy host budget.

The [frozen plan](benchmark_plan.json), SHA-256 `6ffb6d54ad1ab37b8aee979b90e8f2e80a1c684fbc34b67567ee226c7a8ad001`, pins AIPerf 0.12.0, six clients on the same two nodes, 128 concurrency per client for short/ISL4000, the same prebuilt raw payloads, and Mooncake's 46-second fixed trace with prebuilt mmap cache. Each workload is a separate absolute-characterization series. All three runs audited valid with zero request errors or cancellations; Mooncake completed all 136,194 scheduled requests and every client proved a cache hit.

| Gateway host, P/D mockers | Short, RPS | ISL4000, RPS | Mooncake, RPS |
| --- | ---: | ---: | ---: |
| AGW generic, earlier series | 10,857 | 8,933 | 2,963¹ |
| Envoy generic direct, earlier series | 11,128 | 9,028 | 3,019 |
| **Envoy generic callouts, this series** | **11,770** | **10,024** | **2,958** |

The table uses successful requests divided by each six-client Job's global observed window. It is **contextual**, not a same-series gain/loss calculation: each earlier host had its own plan and client-start skew, and this summary-level export has no per-request common-window accounting. The AGW Mooncake value (¹) is from its valid [46-second grace series](../2026-09-26-mocker-pd-mooncake-grace/README.md), not its incomplete 45-second trace. The callout Mooncake clients started 1.02 seconds apart, making summed per-client RPS (3,024) materially different from globally normalized RPS (2,958). These synthetic workers measure gateway/component overhead, not GPU inference or NIXL.

## Reproduce

Use `VCLUSTER_KUBECONFIG=/tmp/dynamo-components-vcluster.kubeconfig`, `VCLUSTER_EXPECTED_SERVER=https://gateway-poc.mkhadkevich-dev:443`, and `VCLUSTER_NAMESPACE=dynamo-components-v2`; the fixture and wrapper reject a different server or namespace. Supply `NIX_STORE_NFS_SERVER`, `NIX_STORE_NFS_PATH`, and `ENVSUBST_BIN` for the existing vCluster Nix store, as in the [main fixture runbook](../2026-09-26-mocker-pd-generic/README.md). Do not deploy anything to the host cluster.

With the synthetic P/D components Ready, scale competing gateway Deployments to zero **in the vCluster**, then run:

```bash
PD_DRY_RUN=1 bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-callouts.sh
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-callouts.sh
PD_GATEWAY_SERVICE=dynamo-pd-envoy-callouts PD_LOCAL_PORT=18083 \
  bash deploy/component-pipelines/k8s/vcluster/smoke-nix-mocker-pd.sh
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-trial.sh short r1 callouts
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-trial.sh isl4000 r1 callouts
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-trial.sh mooncake r1 callouts
python3 deploy/component-pipelines/k8s/vcluster/audit-nix-mocker-pd.py \
  deploy/component-pipelines/k8s/vcluster/results/2026-09-26-mocker-pd-envoy-callouts short r1 --callouts
```

Audit ISL4000 and Mooncake with the corresponding workload argument. The wrapper checks plan hash, dataset hash, Ready replicas, absence of active Jobs and competing gateways, and equality of Envoy's 16 decode Pod clusters with the current Ready Pod IPs. The fixture snapshots those clusters from the vCluster at deploy time; refresh it after Pod churn. Production must replace this static snapshot with CDS/xDS or equivalent dynamic discovery. Both the component graph and descriptor come from the same `dynamo-pd-contracts` ConfigMap as the direct host.

Raw AIPerf exports are preserved unchanged under local ignored `raw_aiperf/` and `/shared/nix/aiperf/results/nixpdc-<workload>-pd-envoy-callouts-r1/` on the vCluster NFS. Committed audits, summaries, Job specs, dataset hashes, cache evidence, and before/after node occupancy are sufficient to locate and validate those exports; the raw files are intentionally not committed.
