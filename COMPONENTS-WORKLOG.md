<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Dynamo gRPC components

Goal: implement fresh gRPC facades around canonical Dynamo preprocessing, selection,
and postprocessing, retaining existing vLLM/SGLang workers or Dynamo wrappers.

Branch: `feat/dynamo-grpc-components`, starting at `88ef69a41c`.

Detailed Sol handoff: [SOL-IMPLEMENTATION-PLAN.md](SOL-IMPLEMENTATION-PLAN.md).
User clarified four required gateway variants: AGW static, AGW generic, Envoy
generic, and Envoy generic with callouts (priority candidate). Current turn produces
the execution plan; implementation and validation remain pending.

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

1. Define versioned contracts and minimal shared API extraction where necessary.
2. Implement canonical preprocessing, selection, and postprocessing facades.
3. Connect an existing Dynamo backend wrapper without a replacement worker engine.
4. Differential and transport lifecycle tests, local build, and cluster deployment.
5. Equivalent-work benchmarks and investigation of any performance regression.
6. Document upstream-update workflow and validation evidence; commit and push branch.
