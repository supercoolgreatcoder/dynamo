<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Rebased Dynamo P/D mocker recheck, September 27, 2026

This is the completed disaggregated prefill/decode **gateway and component**
characterization for the rebased Dynamo branch
`feat/dynamo-component-pipelines-upstream-20260927`. The source used to build
the Nix bundle is `b140462e2c674b98c4102db0464e0e48660d96e8`; the bundle is
`/nix/store/1jycyv3idc78rz9mn67z3ygv6a1gg6rd-dynamo-component-pipelines-b140462e2c`.
The matching Nix flake branch is
`dynamo-nix-envs:feat/dynamo-component-pipeline-upstream-20260927` at
`40cebd4f0edf7ea66ab56217f98d896d5868b787`.

Only the explicit vCluster API `https://gateway-poc.mkhadkevich-dev:443`,
namespace `dynamo-components-v2`, was used. Four Dynamo-backed preprocessors,
four InferencePool-backed selectors, four synthetic prefill workers, and 16
synthetic decode workers were Ready. A decode mocker rejects a request without
the prefill handoff; the streamed smoke test passed through every host before
measurement. The four one-replica gateway arms were isolated and rotated in
three interleaved passes per workload.

The [frozen plan](benchmark_plan.json) pins AIPerf 0.12.0, six clients placed
three each on the same two CPU nodes, concurrency 128 per client for short and
ISL4000, 45-second measured phases, and the 46-second fixed Mooncake trace.
The three prebuilt datasets matched the reference SHA-256 hashes, recorded in
[`dataset_sha256.tsv`](dataset_sha256.tsv). Short and ISL4000 replay raw OpenAI
request bodies; every one of the 72 Mooncake clients reported a memory-mapped
dataset cache HIT and skipped tokenizer/composer work. Mooncake completed all
136,194 scheduled requests in every Job.

The [audit](benchmark_summary.json) validated all **36 of 36** Jobs: six raw
exports and retained Pod placements each, the pinned image and nodes, start
spread at most three seconds, zero request errors/cancellations, full Mooncake
trace counts, and all cache hits. The applied Job commands and placements are
in [`benchmark_execution.json`](benchmark_execution.json); unchanged AIPerf
exports and their hashes are under `raw_aiperf/`.

Median of six client-reported successful-request rates, requests/second:

| Workload | AGW static | AGW generic | Envoy direct | Envoy callouts |
| --- | ---: | ---: | ---: | ---: |
| Short | 9,468.3 | 10,534.7 | 10,728.2 | 10,860.5 |
| ISL4000 | 6,012.5 | 9,485.5 | 8,788.2 | 9,792.8 |
| Mooncake | 3,023.8 | 3,024.5 | 3,024.0 | 3,023.5 |

The older P/D report used a different metric: successful requests divided by
the global observed six-client window. On that metric, the new medians were:

| Workload | AGW static | AGW generic | Envoy direct | Envoy callouts |
| --- | ---: | ---: | ---: | ---: |
| Short | 9,170.2 | 10,308.0 | 10,493.7 | 10,846.2 |
| ISL4000 | 5,929.3 | 8,994.9 | 8,785.7 | 9,676.1 |
| Mooncake | 2,976.1 | 2,912.4 | 2,955.3 | 3,021.2 |

The prior callout report recorded 11,770 short, 10,024 ISL4000, and 2,958
Mooncake global-window RPS. Those numbers used a different client-node pair,
separate plans, and one run per arm; they are historical context, not a
same-series regression estimate. The short gap motivated the same-day
[historical control](../2026-09-27-upstream-rebase-pd-control/README.md).
That control was interrupted at the operator's request after two short and
one ISL4000 Jobs. Its initial short rates exceed this campaign's callout
median, so performance parity is **not yet established**.

## Reproduce

Build `.#component-pipeline-v2` from the Nix branch above, stage its closure
into the existing vCluster store with `stage-nix-closure.sh`, and set
`VCLUSTER_KUBECONFIG=/tmp/dynamo-components-vcluster.kubeconfig`,
`VCLUSTER_EXPECTED_SERVER=https://gateway-poc.mkhadkevich-dev:443`,
`VCLUSTER_NAMESPACE=dynamo-components-v2`, and `COMPONENT_BUNDLE` to the
immutable bundle above. Set the existing vCluster NFS server/path,
`TOKENIZER_STORE_BASENAME`, `NIX_STAGER_POD`, `ENVSUBST_BIN`, and the two
`AIPERF_NODE_*` values from the plan. `run-nix-mocker-pd.sh` renders the
fixture; `run-nix-mocker-pd-callouts.sh` refreshes Envoy's 16 decode Pod-IP
clusters; `smoke-nix-mocker-pd.sh` verifies the streamed handoff. With
`RESULT_DIR` set to a new run-scoped directory, invoke
`run-nix-mocker-pd-recheck.sh` for each `WORKLOAD` and `TRIAL` (`r110`–`r112`).
After completion, run `capture-nix-mocker-execution.sh` with explicit
`JOB_NAMES`, `capture-nix-mocker-pd-cache.sh`, then
`summarize-nix-mocker-pd-recheck.py`. The runner and capture scripts reject a
different API server or namespace. Do not use the host-cluster context.

This is a closed-loop synthetic-worker measurement, not real GPU or NIXL
throughput. AIPerf summary exports cannot independently reconstruct merged
per-request percentiles or duplicate/missing request IDs. Real vLLM and
SGLang correctness on this rebased bundle remains to be rerun after the pause.
