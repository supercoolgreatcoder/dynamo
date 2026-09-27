<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Dynamo upstream rebase audit, September 27, 2026

The benchmarked reference branch `feat/dynamo-component-pipelines-repro`
at `744d16bf0178df135f34ab20fe135fe88b362ef2` remains untouched.
This new branch rebases its 138 feature commits from shared base
`da27aa78dcadbb2239273cd9b4c0a0167e14b507` onto upstream Dynamo
`main` at `81a9871abd08f368eaf6a743650915898e80ce8b`. The rebase
completed without a textual conflict. Upstream main had advanced 52
commits; clean patch application alone did not prove API compatibility.

The first `cargo test --locked -p dynamo-component-facades` run compiled
but failed both selector integration tests: the new Dynamo KV router
requires a host-supplied default worker-selection policy factory. The
fix imports `dynamo-custom-policy-builtin` as a workspace dependency and
supplies `dynamo_custom_policy_builtin::default_factory()` at the
standalone selector's construction boundary, matching Dynamo's EPP
initialization. No policy implementation was copied into the facade.
Cargo updated only that one dependency entry in `Cargo.lock`.

With `protoc` and `LIBCLANG_PATH` supplied by Nix, these tests pass on
rebased main:

- `cargo test --locked -p dynamo-generic-pipeline`: 92 unit and 2 graph tests.
- `cargo test --locked -p dynamo-component-facades`: 14 unit/integration tests.
- `cargo test --locked -p dynamo-static-pipeline -p dynamo-pipeline-grpc`:
  2 static and 7 gRPC transport tests.

## Nix build and vCluster smoke

The separate `dynamo-nix-envs` branch
`feat/dynamo-component-pipeline-upstream-20260927` at
`40cebd4f0edf7ea66ab56217f98d896d5868b787` pins Dynamo
`b140462e2c674b98c4102db0464e0e48660d96e8`. Its checked-in
Agentgateway lockfile was regenerated after patching the host with the new
Dynamo source; the Cargo vendor hash was updated. All four leaf Nix outputs
and `.#component-pipeline-v2` built. The assembled bundle is
`/nix/store/1jycyv3idc78rz9mn67z3ygv6a1gg6rd-dynamo-component-pipelines-b140462e2c`.
The facade CLI and Envoy version command started successfully. Agentgateway
started and reported its release Rust toolchain, although its `--version`
build metadata said `unknown`; this packaging metadata issue is not a serving
failure.

Only the vCluster API `https://gateway-poc.mkhadkevich-dev:443`, namespace
`dynamo-components-v2`, was used for deployment. The bundle closure was staged
with `stage-nix-closure.sh` under Job `nixstage-b140-upstream-r1`, then
`rollout-nix-bundle.sh` moved the 16 mock workers, four preprocessors, one
selector, and gateway hosts to the new paths. One streamed OpenAI chat request
through Envoy direct produced four content chunks and `[DONE]`.

## Frozen aggregate-mocker benchmark

Each valid cell used one gateway replica, six AIPerf 0.12.0 clients on the
same two load nodes, concurrency 128 per client, a 45-second measured
interval, and three interleaved passes. Short and ISL4000 replay the frozen
raw OpenAI bodies; Mooncake uses the prepared mmap cache. The measured clients
do not synthesize the short/ISL4000 prompts. All 36 valid Jobs had six
retained client exports, zero request errors, and no cancellations.
`benchmark_execution.json` records the applied Job commands and complete Pod
placement; `benchmark_summary.json` normalizes the raw exports under
`raw_aiperf/`.

Median aggregate successful requests/second (new versus the previous
`2026-09-25-accf6af-bundle-recheck` campaign):

| Workload | AGW static | AGW generic | Envoy direct | Envoy callouts |
| --- | ---: | ---: | ---: | ---: |
| Short, new | 11,514.9 | 11,760.4 | 12,367.7 | 12,065.9 |
| Short, previous | 12,339.7 | 12,396.6 | 12,726.6 | 12,970.7 |
| ISL4000, new | 8,730.8 | 10,524.3 | 10,264.9 | 10,579.1 |
| ISL4000, previous | 8,930.0 | 10,707.9 | 10,550.3 | 11,768.3 |
| Mooncake, new | 3,023.6 | 3,023.8 | 3,023.6 | 3,022.6 |
| Mooncake, previous | 3,023.6 | 3,023.3 | 3,023.6 | 3,023.7 |

Historical short throughput fell across all four arms during this campaign.
The previous `accf6af` bundle, rerun on the same day and fixture, delivered
11,937.1 RPS on short Envoy direct and 11,732.6 RPS on short AGW generic;
the new bundle's latest passes were 12,032.7 and 11,760.4 RPS respectively.
For the larger historical ISL4000 callout gap, a same-day old-module control
delivered 10,138.6 RPS, below the new module's 10,579.1 RPS median. These
controls support environmental/campaign drift rather than a demonstrated
rebase regression; they do not prove an absolute gateway ceiling. Their raw
exports and execution ledger are retained separately in
`../2026-09-27-upstream-rebase-control/`.

One ISL4000 AGW-static Job, `nixv2-isl4000-agw-static-r103`, had 6-second
client-start skew (three clients on each load node formed two start-time
clusters) and was rejected despite zero request errors. Its raw exports are
preserved under `invalid_raw_aiperf/`; the fresh `r105` Job passed with
2-second skew and replaced it in the valid three-run series. Do not count the
rejected Job when comparing medians.

This campaign covers the aggregate mocker fixture. The rebased branch still
needs a separate disaggregated mocker matrix and real vLLM/SGLang correctness
recheck before claiming full end-to-end parity.
