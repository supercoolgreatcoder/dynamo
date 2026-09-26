<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Record-level ISL4000 gateway-host comparison

The earlier [summary-only P/D comparison](../2026-09-26-mocker-pd-envoy-generic/README.md) could not attribute a small ISL4000 difference because AGW and Envoy clients started at different offsets. This distinct [record-level plan](benchmark_plan.json), SHA-256 `0f0644639ba99446fa1ee95fe07a166658da1e2735f312cd747b9b0be7a157e9`, keeps the same dataset, six client Pods on the same two nodes, concurrency 128 per client, 45-second AIPerf 0.12.0 phase, synthetic P/D workers, preprocessor/selector counts, and Nix-built gateway bundle. It changes only the export to `--export-level records --slice-duration 1`, so its absolute RPS must not be compared as a gain or loss against the older summary-only series.

For **each Job independently**, the [streaming auditor](../../audit-nix-mocker-pd-overlap.py) sets `t0` to the latest of its six AIPerf profiling starts plus two seconds and counts only successful records whose request start **and** end are within `[t0, t0+30 seconds]`. Every client must remain in its profiling window for two more seconds beyond that interval. This gives each arm an identical-length, fully overlapping six-client interior window despite separate absolute run times. It is not a wall-clock-simultaneous A/B test.

| Arm / trial | Audit | Total successful records | Interior-window requests | Interior-window RPS | Client-start spread |
| --- | --- | ---: | ---: | ---: | ---: |
| AGW generic r1 | Valid | 440,542 | 288,766 | 9,625.53 | 1.509 s |
| Envoy generic r1 | Valid | 426,108 | 282,645 | 9,421.50 | 0.021 s |
| AGW generic r2 | Valid | 451,604 | 292,446 | 9,748.20 | 2.422 s |
| AGW generic r3 | Valid | 433,972 | 282,695 | 9,423.17 | 1.601 s |

The single Envoy result is **2.12% below** the AGW pilot median of 9,625.53 RPS, but AGW's own three valid runs span 9,423.17–9,748.20 RPS. The [analysis](analysis-r1.json) defines an empirical half-range noise floor of 1.69% and an operational minimum detectable effect of 3.38% (the full three-run range divided by its median, **not** a confidence interval). The observed difference is inside that spread. Verdict: **inconclusive, no demonstrated Envoy-specific regression**. Another repeat is not justified solely by this small delta; a stronger decision would need improved load synchronization and a separately controlled series.

## Reproduce inside the vCluster

Use only explicit `VCLUSTER_KUBECONFIG=/tmp/dynamo-components-vcluster.kubeconfig`, `VCLUSTER_EXPECTED_SERVER=https://gateway-poc.mkhadkevich-dev:443`, and `VCLUSTER_NAMESPACE=dynamo-components-v2`; the wrapper rejects another API server or namespace. Set the existing vCluster Nix NFS server/path and `ENVSUBST_BIN` as described in the [main fixture runbook](../2026-09-26-mocker-pd-generic/README.md). The shared synthetic P/D fixture and both gateway hosts must already pass `smoke-nix-mocker-pd.sh`.

With no active benchmark Job, scale exactly one gateway to 1 and the other to 0 in that vCluster, then run sequentially:

```bash
bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-trial.sh isl4000 r1 agw-records
python3 deploy/component-pipelines/k8s/vcluster/audit-nix-mocker-pd-overlap.py \
  deploy/component-pipelines/k8s/vcluster/results/2026-09-26-mocker-pd-isl-overlap agw r1

bash deploy/component-pipelines/k8s/vcluster/run-nix-mocker-pd-trial.sh isl4000 r1 envoy-records
python3 deploy/component-pipelines/k8s/vcluster/audit-nix-mocker-pd-overlap.py \
  deploy/component-pipelines/k8s/vcluster/results/2026-09-26-mocker-pd-isl-overlap envoy r1
```

The two additional AGW runs (`r2`, `r3`) were a once-per-series noise-floor pilot, authorized by the pre-pilot [repeat decision](repeat-decision.json) after the first valid pair. Repeat them with `agw-records` and audit each before recomputing `compare r1`. The runner freezes the plan and dataset SHA-256, checks component readiness and inactive competing gateways, server-dry-runs the generated Job, records Job/pod identity and client-node occupancy, and refuses an existing Job name. The auditor checks six raw summary exports, record counts, unique request IDs, phase identity, timestamps, finite nonnegative latencies, zero errors/cancellations, and complete window coverage before analysis.

The four Jobs' unmodified AIPerf JSONL/CSV/JSON exports total about 3.2 GB and are excluded from Git; they remain at `/shared/nix/aiperf/results/<job>/` on the vCluster NFS and in local ignored `raw_aiperf/`. Committed summaries carry the SHA-256 of every raw summary and request-record file. The synthetic workers test gateway/component overhead, not GPU inference, NIXL, or production serving capacity. This custom gateway/mocker campaign adapts the repository's AIPerf configuration, run, and analysis quality gates; it is not a DynamoGraphDeployment recipe-optimization run.
