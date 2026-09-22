<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Dynamo gRPC components

Goal: implement fresh gRPC facades around canonical Dynamo preprocessing, selection,
and postprocessing, retaining existing vLLM/SGLang workers or Dynamo wrappers.

Reference branch: `feat/dynamo-grpc-components`, retained at `ce04286ef7`.
Implementation branch: `feat/dynamo-grpc-components-v2`, created directly from
upstream `origin/main` at `7d4c346fa0`.

Detailed Sol handoff: [SOL-IMPLEMENTATION-PLAN.md](SOL-IMPLEMENTATION-PLAN.md).
User clarified four required gateway variants: AGW static, AGW generic, Envoy
generic, and Envoy generic with callouts (priority candidate). Implementation is in
progress against this checklist.

## Acceptance criteria

- No copied tokenizer, routing, reasoning, tool parsing, or generation algorithms.
- Workspace dependencies follow the checked-out Dynamo revision and shared lockfile.
- Bounded batches, per-item errors, deadlines, cancellation, streaming flow control,
  health checks, model readiness, and preserved request metadata.
- Existing worker protocols and backend implementations remain authoritative.
- Differential tests compare facade outputs with direct canonical Dynamo calls.
- Build, deploy, end-to-end integration, and a reproducible performance comparison.
- Compare equivalent mock workloads separately from real-engine correctness tests.

## Initial findings

- `OpenAIPreprocessor::preprocess_request` already renders, tokenizes, and reports
  prompt-injected reasoning using model-specific canonical logic.
- `OpenAIPreprocessor::postprocessor_parsing_stream` and
  `transform_postprocessor_stream` implement the canonical response path.
- `SelectionService` exposes transport-independent selection, worker lifecycle,
  reservation, and indexer operations.
- Prototype tokenizer/sidecar services import canonical crates but duplicate parts
  of frontend policy. New facades must use the full shared processing entry points.
- Sandbox commands fail because bwrap cannot establish its mount namespace.
  apply_patch works inside an approved unsandboxed interactive shell.

## Implementation progress

- Added `dynamo-component-facades` as a normal workspace crate with workspace path
  dependencies on `dynamo-llm`, `dynamo-kv-router`, and `dynamo-runtime`.
- Extracted the existing chat normalization/preparation sequence into public
  `OpenAIPreprocessor` methods; the existing frontend calls the same implementation.
- Added versioned protobuf services for bounded preprocessing batches, canonical
  selector operations/lifecycle, and multiplexed streaming postprocessing.
- Added one runnable binary with preprocessor, selector, and postprocessor modes,
  gRPC health services, limits, per-item errors, and deadlines where applicable.
- The build now emits the canonical protobuf descriptor set and every facade serves
  it through gRPC reflection. Generic gateway variants therefore bind to the same
  contract as generated clients instead of maintaining a second schema.
- Renamed the postprocessor input field to `annotated_chunk_json` before freezing
  v1, then tightened the boundary further: it is now
  `annotated_backend_chunk_json`, an `Annotated<BackendOutput>`. The canonical
  postprocessor owns response generation, usage accounting, reasoning/tool parsing,
  and OpenAI chunk construction.
- Added a transport-only `WorkerFacade` over a caller-supplied Dynamo backend
  `ServiceEngine`. Existing wrappers remain responsible for generation, P/D, and
  detokenization; the bridge provides bounded output, request identity, and cancel.
- Added `ChatWorkerFacade`, the preferred server-streaming gateway boundary, which
  composes that supplied engine with the canonical postprocessor. Added unary facade
  methods beside batch methods so the generic host can fold calls using contract
  metadata rather than custom service knowledge.
- Added `deploy/component-pipelines/generic-core` to the Dynamo workspace. Its 88
  domain-independence/validation/batching/streaming tests pass. Added a lean dynamic
  gRPC transport that resolves the emitted facade descriptor; its six tests pass.
- Added and load-time validated the canonical aggregate graph and OpenAPI binding
  documents. It uses Dynamo facade payloads and contains no tokenizer/routing/parser
  implementation.
- Added a compiled typed aggregate pipeline for AGW static mode. Both typed and
  generic paths consume the facade-provided selector request, so model/routing field
  translation remains inside Dynamo rather than leaking into gateway adapters.
- Documented the ownership boundary and rebase/upgrade procedure in `README.md`.
- Focused facade tests pass: preprocessing 3, selector 2, postprocessor gRPC 1,
  worker bridge gRPC 1; 7 passed and 0 failed. The crate and binary compile against
  Dynamo 1.6 at `7d4c346fa0` with descriptor generation and reflection.
- Strict CODEOWNERS generation reports 100% coverage; the new crate is shared by
  frontend, router, and runtime owners.
- Private push is currently rejected because upstream `main` references seven Git
  LFS video objects that return 404 upstream and are absent from the private mirror.
  GitHub's pre-receive hook rejects the inherited pointers even with local incomplete
  push enabled. Local commits remain authoritative until the base/mirror is repaired.

## Recorded prototype comparison targets

One trial, six load generators, concurrency 128 each, 45-second cells. These are
mock-engine results; matching them does not demonstrate real-engine performance.

| Topology | Dataset | RPS | Reported latency (ms) | CPU us/request |
| --- | --- | ---: | ---: | ---: |
| agg | short | 10745.4 | 55 | 912 |
| agg | isl4000 | 9105.8 | 73 | 1091 |
| agg | mooncake | 3011.3 | 97 | 3211 |
| disagg | short | 9165.6 | 74 | 1062 |
| disagg | isl4000 | 7886.8 | 89 | 1246 |
| disagg | mooncake | 3004.2 | 153 | 3272 |

The original harness labels a field p50; verify raw aggregation before using that
label in the new report. Persist raw results, error counts, CPU allocations, model
and binary identities, token counts, and actual transport configuration.

## Remaining work

1. Connect an existing Dynamo backend wrapper without a replacement worker engine.
2. Implement AGW static, AGW generic, Envoy generic, and Envoy generic with callouts.
3. Build artifacts/manifests and deploy an isolated cluster stack.
4. Run real-engine E2E plus the repeated 24-cell mock benchmark matrix.
5. Investigate performance gaps, complete update rehearsal, and commit evidence.
6. Push once the inherited upstream Git LFS objects/private base are available.
