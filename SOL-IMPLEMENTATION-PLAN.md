<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Sol execution handoff: Dynamo components and four gateway variants

## 1. User intent and mandatory scope

Build a fresh implementation on `feat/dynamo-grpc-components`, based on the
prototype's demonstrated capabilities but backed by the canonical Dynamo source.
Decompose preprocessing/tokenization, worker selection, and worker-side
postprocessing into independently deployable components with batched gRPC facades.
Keep inference engines unchanged: use vLLM, SGLang, or their existing Dynamo
wrappers. Do not replace an engine with a custom worker implementation.

Implement and validate ALL FOUR gateway variants:

| ID | Variant | Required distinction |
| --- | --- | --- |
| A | AGW static | agentgateway with a compiled, typed orchestration state machine |
| B | AGW generic | agentgateway executing a validated, declarative pipeline graph |
| C | Envoy generic | Envoy dynamic module executing that graph with independent client transport |
| D | Envoy generic with callouts | Same graph, with stage traffic using Envoy-owned cluster callouts/streams |

D is the user's most promising candidate and gets priority after the shared
components work. A, B, and C remain required deliverables, not optional baselines.
"Static" describes the orchestration implementation, not static endpoint selection:
all variants must support the same canonical KV-aware selector and live discovery.
The generic runtime must stay domain-independent; Dynamo policy belongs in Dynamo.

The newest request is to prepare this handoff for Sol, not to execute the full
implementation in the planning turn. The receiving Sol session should execute it.

## 2. Starting state and authority

- Repository: `/work/dynamo-epp`; branch already created from `88ef69a41c`.
- `COMPONENTS-WORKLOG.md` records initial findings and historical benchmark targets.
- No new facade or gateway implementation has been written on this branch yet.
- Earlier prototype code is reference material and a comparison oracle, not the new
  components' dependency or source of copied tokenizer/parser/routing algorithms.
- Preserve untracked `result` and `review/`; the latter contains large nested repos.
  The earlier request to commit work does not authorize publishing arbitrary review
  checkouts, build outputs, credentials, or unrelated files. Stage explicit paths.
- User explicitly authorized build, deployment, benchmarking, commits, and pushes.
  Use DCO-signed commits. Private destination is `supercoolgreatcoder/dynamo-epp`.
  Do not publish upstream issues/PRs or push to `ai-dynamo/*` without permission.
- Do not invoke Claude. If more historical context is needed, parse the saved
  transcript for session `c39a6ca7-525e-4707-894d-c6c825ee6d87` directly.
- Do not use Kubernetes port-forward. Use in-cluster test/load pods.
- Read applicable AGENTS.md and skills before working in their covered areas.
  New docs/examples/recipes require the documentation instructions. Architecture
  changes eventually need a DEP; draft it locally/private, seek permission before
  filing publicly. Follow test model-size guardrails.

## 3. Architecture and ownership

Aggregate request path:

```text
OpenAI client -> gateway A/B/C/D -> tokenizer facade -> selector facade
                     |                                   |
                     +---- selected worker-side adapter -+
                                    |
                       existing Dynamo backend/engine
                                    |
                       canonical postprocessor -> SSE client
```

Disaggregated path adds prefill selection, actual backend prefill, opaque backend
transfer metadata, decode selection, and actual backend decode. The exact order
must follow the supported engine/Dynamo contract, not a guessed universal P/D
protocol. Preserve engine-owned transfer descriptors without reinterpretation.

Ownership rules:

- Tokenizer facade calls canonical Dynamo request preparation: chat template,
  tokenization, sampling/stopping settings, model-specific prompt reasoning,
  tool-choice/guided-decoding policy, and supported multimodal handling.
- Selector facade calls canonical `SelectionService`: selection, KV awareness,
  worker catalog, reservations, lifecycle, readiness, and capacity accounting.
- Postprocessor facade maintains one canonical response-processing state per
  request: incremental decoding where applicable, reasoning, tools, finish reasons,
  usage, and terminal events. Verify where detokenization currently happens;
  never decode twice when the existing backend already emits text.
- Worker adapter is transport glue only. It invokes existing engine/wrapper
  request, stream, cancel, and P/D mechanisms. No fake prefill success, fabricated
  transfer handles, replacement generation loop, or duplicated backend policy.
- Gateways own graph scheduling, transport, request lifetime, routing to selected
  endpoints, and response streaming; they do not implement model behavior.
- KV payloads/media tensors do not flow through gateway JSON. Preserve canonical
  handles and supported direct-transfer mechanisms.

## 4. Canonical entry points and minimal extraction

Inspect these at the checked-out revision before choosing exact signatures:

- `lib/llm/src/preprocessor.rs`: `OpenAIPreprocessor::new`,
  `preprocess_request`, `transform_postprocessor_stream`, and
  `postprocessor_parsing_stream`.
- The frontend operator in that same file applies additional policy, including
  `apply_tool_choice_guided_decoding`, tracker setup, prompt length accounting,
  multimodal counts, annotations, and context attachment. Calling just tokenize
  or preprocess_request is NOT automatically full frontend parity.
- `lib/llm/src/model_card.rs`: canonical model configuration/tokenizer loading.
- `lib/llm/src/protocols/common/llm_backend.rs`: `PreprocessedRequest`,
  `BackendOutput`, finish reasons, usage, engine-owned disaggregation metadata.
- `lib/llm/src/protocols/openai/`: response generators and OpenAI protocol types.
- `lib/kv-router/src/services/selection/{service,types,input,error}.rs`:
  `SelectionServiceBuilder` and selection/catalog/reservation APIs.
- `components/src/dynamo/{vllm,sglang}/`: existing wrapper entry points and backend
  transport contracts. Trace the actual token-in and cancellation paths.

Where the canonical API is too coupled to the monolithic frontend, extract a small
shared preparation/response-session API into Dynamo itself, and make BOTH the
existing frontend and new facades call it. Preserve existing frontend behavior and
tests. Do not expose many unrelated internals merely to make the facade compile.

Suggested layout, adjustable after dependency review:

```text
lib/component-facades/          workspace crate: shared adapters + gRPC servers
  proto/dynamo/components/v1/   contracts and checked compatibility fixtures
  src/{preprocess,select,postprocess,worker_bridge,server}.rs
  tests/                       direct-vs-gRPC and lifecycle tests
deploy/component-pipelines/     four gateway integrations/configs and manifests
benchmarks/component-pipelines/ reproducible runner, schemas, result summaries
```

Use workspace path dependencies and one Dynamo lockfile. Avoid absolute developer
paths and separately pinned copies of Dynamo/tokenizer/parser crates. Inspect
default features: `dynamo-llm` may otherwise pull GPU/block-manager dependencies
into CPU-only services. External gateway repositories can keep their own build
systems, but their dependency/artifact identities must be pinned and reproducible.

## 5. Contract specification before implementation

Write a short ADR and protobuf schema, then test the schema with generated clients.
Version the transport envelope separately from canonical Dynamo payload schemas.
Choose typed fields for stable, hot-path data; where necessary use a documented,
versioned canonical payload envelope rather than manually mirroring every backend
field. Measure JSON/bytes conversion costs; do not commit to expensive repeated
serialization without profiling. Preserve metadata across every boundary.

Specify all of the following:

1. Request ID, batch item ID, model/tokenizer revision, stage role, protocol version,
   tracing context, absolute deadline, per-stage budget, supported capabilities.
2. Preprocess batch request/response: canonical prepared request plus the exact
   context required to resume postprocessing, including prompt length, reasoning
   and structural-tag flags, tools/options, and multimodal accounting.
3. Select batch request/response: canonical selectors and results, endpoint identity,
   role, reservation identity, and explicit no-capacity/error outcomes.
4. Worker lifecycle/reservation RPCs needed by actual integration. Prefer typed
   methods corresponding to canonical APIs, not an arbitrary method-name tunnel.
5. Stateful postprocessing stream: open request context, ordered backend chunks,
   per-request sequence identity, terminal output/error, and explicit cancellation.
   Batch envelopes may multiplex independent streams; maintain separate parser
   state and per-request ordering, with fair scheduling between requests.
6. Existing backend bridge: prepared input in, canonical backend output out;
   bounded streaming and propagation of cancel/deadline, including prefill/decode.
7. Transport error versus per-item application error. Define whole-batch rejection
   for malformed/oversized envelopes and isolation for valid individual failures.
8. Limits: items, bytes, token count, active sessions, queue depth, maximum batch
   wait, and tenant/model compatibility. Flush on size, timer, or imminent deadline.
9. Batch fairness and cancellation: canceling one item must not abort unrelated
   items; canceling the enclosing RPC terminates its owned work. No unbounded task
   spawning or accumulating output behind a slow reader.
10. Retry/idempotency classification. Never retry generation or reservation creation
    blindly; ambiguous completion must not create duplicate work or leak capacity.
11. Health versus readiness, graceful drain, model reload behavior, authentication
    boundary, and supported revision negotiation. Do not claim production security
    merely because the experiment runs within a cluster.

## 6. Implementation milestones and exit gates

### M0 — inventory and freeze comparison conditions

- Record git SHAs, dirty files, toolchains, hardware, cluster context/namespace,
  deployed image/store identities, replicas, CPU limits/affinity, and model assets.
- Inventory prototype features with source/test evidence: batch behavior, token
  transport, selection, tools/reasoning, metadata, P/D, cancel, errors, and retries.
  Mark claimed-but-unverified behavior explicitly instead of carrying claims forward.
- Locate/pin AGW static and generic sources and their patches. `/work/tmp/agw-src`,
  `/work/tmp/build-agw.sh`, and prototype patches are discovery hints, not a durable
  build specification. Inspect their actual revisions and local modifications.
- Freeze an isolated reference deployment and raw benchmark inputs. Keep new
  deployments separate to avoid accidental reference contamination.
- Exit: written capability matrix, pinned reference configurations, agreed contract
  draft, clean build recipe locations, and explicit known unsupported features.

### M1 — canonical shared APIs and direct differential tests

- Extract only missing preparation/postprocessing interfaces, used by old and new
  frontend paths. Keep selector logic unchanged behind a facade adapter.
- Add direct-call differential tests before transport work. Exercise the same
  request/model/chunk fixtures through old frontend behavior and shared APIs.
- Include tools, reasoning, stop/EOS, partial UTF-8, usage, metadata, empty/error
  streams, and model configuration. Reuse canonical fixtures under `lib/llm/tests`.
- Exit: canonical regression tests and new differential tests pass; no copied
  model-specific algorithm exists in adapters.

### M2 — three gRPC component facades and existing-worker bridge

- Implement protobuf generation and CPU-service binaries with configurable binds,
  model paths, limits, batching, health/readiness, metrics, and graceful shutdown.
- Implement tokenizer batches, selector batches/lifecycle, and stateful batched
  postprocessor streams using the shared APIs from M1.
- Connect a real existing Dynamo backend wrapper. Choose the first supported small
  model/backend using the repo guardrails and available GPUs; then exercise the
  other available backend. Record unavailable backend coverage as a limitation.
- Add bounded transport/backpressure, cancellation cleanup, and reservation cleanup.
  Use structured errors; avoid panic paths, hidden retries, or silent dropping.
- Exit: generated gRPC clients produce equivalent output to direct canonical calls,
  and an aggregate request reaches a real backend without a replacement worker.

### M3 — all four gateway integrations

- Define one logical stage contract and one set of component endpoints. A has typed
  compiled scheduling; B/C/D consume the same validated generic graph semantics.
- A: connect AGW's typed scheduler to the new gRPC services and existing worker
  bridge; remove dependence on prototype preprocessing/postprocessing algorithms.
- B: generic graph validation, descriptor binding, bounded execution, streaming
  terminal response, error edges, and stage-specific cleanup through AGW transport.
- C: Envoy dynamic module with generic runtime and explicit independent gRPC/client
  transport. Document separate pools/runtime/CPU cost; do not label it callout mode.
- D: Envoy cluster-backed transport for component and worker stage calls. Unary gRPC
  and streaming gRPC must correctly handle framing, grpc-status/trailers, headers,
  cancellation, backpressure, and thread-affine Envoy callbacks.
- For D, verify dynamic-module ABI capability at startup. Prototype patches for
  callout options/trailers are under `gateway-orchestration-prototype/patches/`
  and `/work/tmp/`. Pin any required patched Envoy build and SDK. Missing required
  callbacks must fail readiness/startup, not silently fall back to tonic/reqwest.
- D must demonstrate cluster traffic through counters/traces and tests for actual
  timeouts, circuit breakers, discovery changes, and configured TLS where enabled.
  Do not assume router retry policy applies to direct callouts. Retry only stages
  whose semantics permit it. If an SDK cannot support streaming, implement the
  narrowly scoped extension or report the blocker; do not relabel variant C as D.
- Implement D early after the first shared-component smoke test to expose ABI risks;
  bring A/B/C to parity before final benchmark comparison.
- Exit: identical semantic fixture suite passes through A/B/C/D in aggregate and
  supported disaggregated modes, with proof of their distinct execution paths.

### M4 — deployment and fault/end-to-end tests

- Check in builds, pinned gateway patches/revisions, manifests, component settings,
  health probes, resource allocations, and one-command smoke/teardown procedures.
- Deploy into an isolated namespace/name prefix. Do not delete unrelated pods or
  replace the historical reference fleet. Confirm exact kubectl context first.
- Use in-cluster clients, no port-forward. Test streaming and nonstreaming OpenAI
  requests, actual token-in execution, tools/reasoning, termination, and token usage.
- Test real aggregate backend first, then real supported P/D with transfer evidence.
  A mock prefill returning an opaque-looking blob is not disaggregation evidence.
- Test client disconnects, per-item cancel, deadline expiry at each stage, slow
  readers, mixed-validity batches, overload, selector failure/no capacity, worker
  restart/drain, duplicate completion, and cleanup after partial P/D failure.
- Assert no reservation/session leaks and no unrelated batch-item cancellation.
  Inspect memory/queue/active-request metrics during slow-consumer and soak tests.
- Run applicable interconnect validation for actual disaggregation. If GPUs/RDMA or
  model assets are unavailable, report the exact blocker and continue mock/system
  validation, but do not claim the real-engine acceptance gate passed.
- Exit: evidence directory with commands, manifests, logs, assertions, and artifact
  identities for all four variants; explicit backend capability/support matrix.

### M5 — fair benchmarks and optimization

- Implement the benchmark design below. First verify correctness/error counts and
  raw latency aggregation, then run performance comparisons.
- Profile failures against target by stage: CPU/tokenization, serialization/copies,
  batching wait, selection, connection scheduling, Envoy callback dispatch, and
  stream buffers. Fix shared bottlenecks once in the canonical path where relevant.
- After every optimization rerun differential/lifecycle tests and affected cells.
  Do not improve RPS by dropping parsing, usage, tools, cancellation, or real work.
- Exit: performance gates met, or a transparent documented remaining gap; a gap
  means the performance objective remains unfinished, not an automatic waiver.

### M6 — maintainability and final delivery

- Add an upstream-update procedure: update Dynamo revision, resolve minimal API
  extraction patches, regenerate contracts if required, rebuild against the shared
  lockfile, and run canonical + facade + four-variant contract tests.
- Where safe, test an upstream update in a temporary worktree; never reset the
  user's branch. Record the exact old/new upstream SHAs and compatibility results.
  No design can guarantee all future upstream changes require zero adaptation.
- Add CI tiers: offline unit/differential/contract tests; CPU integration; opt-in
  GPU real-engine/P-D; benchmark jobs with stored raw artifacts and regression rules.
- Review dependency ownership and diff for copied algorithms, absolute paths,
  private artifacts, secrets, accidental generated bulk, and prototype dependencies.
- Commit focused DCO changes and push the private branch. Report commit SHAs,
  builds/deployment identities, test evidence, benchmark matrix, limitations, and
  exact rerun commands. No public PR or DEP without user approval.

## 7. Benchmark protocol and pass criteria

Use TWO separate tracks; do not compare mock numbers to real-engine numbers.

### Track 1: historical mock-engine parity

Compare the retained prototype, a canonical Dynamo frontend reference when feasible,
and A/B/C/D with identical mock engine behavior, tokenizer/model revisions, replicas,
KV state policy, input datasets, output lengths, transport requirements, and total
CPU resources. Use mock workers only for this controlled frontend measurement.

Required primary matrix: 4 variants x 2 topologies x 3 datasets = 24 cells per trial,
plus reference cells. Datasets: short (historically ISL 128 +/-16, OSL 50), ISL4000
(historically +/-100, OSL 50), and the exact historical Mooncake trace (historically
median ISL about 6249, mean OSL about 171; recover distribution and checksum).
Historical six load generators used concurrency 128 each and 45-second measurement.
Retain those cells for continuity; add a longer steady-state run where 45 seconds is
too noisy. Separate warmup, startup, cold-cache, and warm-cache measurements.

Historical single-trial values, NOT sufficient evidence of parity by themselves:

| Topology | Dataset | RPS | Reported latency ms | CPU us/request |
| --- | --- | ---: | ---: | ---: |
| aggregate | short | 10745.4 | 55 | 912 |
| aggregate | ISL4000 | 9105.8 | 73 | 1091 |
| aggregate | Mooncake | 3011.3 | 97 | 3211 |
| disaggregate | short | 9165.6 | 74 | 1062 |
| disaggregate | ISL4000 | 7886.8 | 89 | 1246 |
| disaggregate | Mooncake | 3004.2 | 153 | 3272 |

Verify whether the historical latency field is truly p50; never average percentiles
and present them as a global percentile. Recover raw samples/histograms, or label
the historic field as unverified. Recover exact offered load for Mooncake: its
approximately 3000 RPS may be load-generator limited, not a capacity ceiling.

Run at least 3 interleaved trials per cell, extending to 5+ if variance obscures
results. Randomize order, avoid competing background benchmarks, pin CPU/replicas,
and count BOTH gateway and component/sidecar CPU. Record hidden runtime threads.
Publish errors, successful/completed requests, prompt/output token counts, RPS,
tokens/sec, TTFT, inter-token latency, end-to-end p50/p95/p99, queue delay,
batch size/wait, CPU us/successful request, RSS, and network volume.

Proposed operational definition of "match" (explicit default, not a previously
user-approved relaxation): at matched resources and equivalent successful work,
median throughput at least 95% of the freshly rerun reference, latency p95/p99 no
more than 110%, CPU/request no more than 110%, and zero unexpected request errors.
Report uncertainty across runs; extend trials for borderline results. Also show
absolute historical targets above, and explain baseline drift rather than hiding it.
All four variants must be measured and discrepancies investigated. Prioritize D's
performance, but do not silently waive A/B/C parity. If the user requires tighter
thresholds, use them. Do not call a near-ceiling offered-load result a speedup.

Secondary sweeps: low/medium/high concurrency, batch disabled/enabled and batch-wait
sweep, tokenizer colocation if supported, CPU-matched gateway thread sweeps, and a
sustained overload/slow-consumer run. Separate these from the primary fair matrix.

### Track 2: real-engine correctness and performance

Compare canonical frontend versus A/B/C/D using the same existing backend, model,
GPU allocation, decode settings, supported P/D topology, seeds where meaningful,
and workloads. Run vLLM and SGLang where available. Establish actual generated-token
and semantic parity; account for legitimate nondeterminism without masking errors.
Report GPU utilization and end-to-end metrics separately from mock results. Do not
use GPU-bound throughput alone to claim low frontend overhead.

## 8. Build/deploy recovery hints (verify; not immutable requirements)

Previously usable local build inputs:

```text
Rust: /nix/store/gpiwvkwdh78s3h6hfsg4j0jgializa7p-rust-default-1.97.0/bin
CARGO_HOME=/work/tmp/cargohome
PROTOC=/nix/store/4hdzvycn5mkg3xm1ggscp3mxz8005c3m-protobuf-36.1/bin/protoc
CUDARC_CUDA_VERSION=13010
NIXL_PREFIX=/nix/store/d5rbkfa442rcq9bamzqxrm6b0glkjkla-nixl-cu13-1.4.1
LIBCLANG_PATH=/nix/store/a3kjvvlm7f8abcmxh6f8dndiwc4vp726-clang-21.1.8-lib/lib
KUBECONFIG=/work/tmp/vc-direct.kubeconfig
Reference namespace=gwo
```

Discover existing Nix packages under `/work/envs/gateway-pipeline` and wrapper/build
scripts under `/work/tmp`. Prototype sources of particular interest:

- `gateway-orchestration-prototype/{tok-svc,detok-sidecar}`: identify features and
  duplicated policy to eliminate, not implementation dependencies.
- `gateway-orchestration-prototype/{pipeline-core,pipeline-grpc}`: graph semantics
  and generic transport reference; audit before reuse outside inference policy.
- `gateway-orchestration-prototype/envoy-generic-module`: client and cluster-backed
  transport reference, thread-affinity and optional ABI behavior.
- `gateway-orchestration-prototype/patches`: gateway/Envoy integration changes.
- `gateway-orchestration-prototype/grpc-worker`: NOT a real-worker acceptance path;
  historical prototype includes mock forwarding/synthetic prefill behavior.
- `gateway-orchestration-prototype/grpc-mocker`: mock benchmark only.
- `gateway-orchestration-prototype/k8s/envalldatasets.sh`, `/work/tmp/cell-lib.sh`,
  `/work/tmp/lib.sh`, `/work/tmp/par.sh`: historical harness reference. Audit metric
  calculations, hardcoded artifacts, retries, and semantic checks before use.

Existing services were deployed using Nix closures staged through a store bridge.
Reconstruct a checked-in, idempotent build/staging recipe; do not depend on transient
store-path text files or copy unknown whole directories. Never print kubeconfig or
credentials. Existing environment sandbox commands may fail with bwrap mount errors;
request approved escalation rather than bypassing permissions. Use apply_patch for
edits; it has worked inside an approved interactive unsandboxed shell here.

## 9. Handoff execution checklist

- [ ] M0 reference inventory and feature matrix committed.
- [ ] M1 canonical shared APIs and direct differential tests pass.
- [ ] M2 tokenizer, selector, postprocessor gRPC facades and backend bridge pass.
- [ ] M3 A AGW static implemented and validated.
- [ ] M3 B AGW generic implemented and validated.
- [ ] M3 C Envoy generic independent transport implemented and validated.
- [ ] M3 D Envoy generic callouts implemented; no fallback; ABI/stream tests pass.
- [ ] M4 all variants deployed; E2E, faults, cancellation, and cleanup pass.
- [ ] M4 real backend aggregate/P-D validation recorded, limitations explicit.
- [ ] M5 full 24-cell mock matrix plus references repeated with raw artifacts.
- [ ] M5 real-engine comparisons and performance gaps resolved/documented honestly.
- [ ] M6 clean build/upstream-update procedure and CI coverage verified.
- [ ] M6 focused signed commits pushed to private branch; final evidence linked.

Suggested opening instruction to Sol:

> Execute SOL-IMPLEMENTATION-PLAN.md on feat/dynamo-grpc-components. Implement all
> four gateway variants; prioritize Envoy generic with Envoy-owned callouts without
> dropping the other three. Keep inference logic in canonical Dynamo and workers in
> existing engines/wrappers. Build, deploy, run differential/E2E/fault tests and fair
> repeated benchmarks. Keep COMPONENTS-WORKLOG.md current. Commit and push scoped
> changes privately. Do not claim completion while required gates remain unmet.
