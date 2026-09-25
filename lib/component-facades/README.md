<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Dynamo component facades

This crate exposes bounded gRPC transport around Dynamo's canonical preprocessing,
selection, and response-postprocessing implementations. It does not implement model
templates, tokenization, routing policy, reasoning parsing, tool parsing, or inference.

## Ownership boundary

| Facade | Canonical implementation |
| --- | --- |
| Preprocessor | `dynamo_llm::preprocessor::OpenAIPreprocessor` |
| Selector | `dynamo_kv_router::services::selection::SelectionService` |
| Worker bridge | A caller-supplied canonical Dynamo backend `ServiceEngine`; the executable can compose Dynamo's native vLLM or SGLang sidecar engine |
| Postprocessor | `OpenAIPreprocessor::postprocess_backend_chat_stream` |
| Workers | Existing Dynamo vLLM/SGLang/TRT-LLM wrappers and sidecars |

The protobuf messages are transport envelopes. Evolving Dynamo request and selector
types cross the boundary as their canonical JSON representation, so the facade does
not maintain a field-by-field shadow model. Stable envelope fields carry request IDs,
deadlines, parser continuation state, per-item errors, and batching controls.

The worker bridge accepts a canonical `PreprocessedRequest` and relays the
`Annotated<BackendOutput>` stream from a supplied Dynamo backend pipeline. It has no
generation implementation. Incremental token decoding and hidden-stop handling remain
in `dynamo_llm::backend::Backend`, where Dynamo already requires them to run. The
postprocessor consumes that canonical backend stream and performs Dynamo's response
generation, usage accounting, reasoning/tool parsing, and role normalization without
decoding a token twice.

The descriptor set emitted at build time is registered with gRPC reflection by every
standalone facade. Descriptor-driven gateways therefore consume the exact generated
contract rather than carrying a separately maintained protobuf model.

For gateway graphs, `ChatWorkerFacade` composes the supplied backend engine with the
same canonical postprocessor behind one server-streaming RPC. This is the preferred
sidecar boundary: backend chunks never become gateway policy, and dropping the client
stream drops the underlying Dynamo stream. Its `GenerateRaw` RPC preserves the
canonical annotated backend stream, including an opaque prefill KV handoff,
without OpenAI postprocessing. `WorkerBridge.Process` remains available for
bidirectional cancellation. Both raw methods accept packed prompt token IDs.
`ChatWorkerBridge.Generate` also accepts the opaque prefill handoff in a
separate protobuf field and inserts it into Dynamo's `PreprocessedRequest`
before invoking the decode engine.

## Native worker-side gRPC facade

For a vLLM or SGLang engine exposing its native gRPC service, the
executable can build the worker bridge directly from Dynamo's own sidecar
engine, `dynamo_backend_common::EngineAdapter`, and
`dynamo_llm::backend::Backend`. For example, colocate the process with a
native vLLM gRPC server and run:

```bash
dynamo-component-facade --listen 0.0.0.0:50052 vllm-worker \
  --model-path /model -- --grpc-endpoint 127.0.0.1:50051
```

Use `sglang-worker` for SGLang. The native sidecar arguments after `--` are
parsed by the corresponding Dynamo sidecar crate, including its gRPC connection
and startup-timeout options. The worker-side gRPC health service becomes
serving only after engine discovery and startup succeed. No Dynamo distributed
runtime is started by this facade; Kubernetes owns worker lifecycle and the
selector discovers the serving Pod through its InferencePool.

`--disaggregation-mode prefill` and `--disaggregation-mode decode` are passed
through to the same Dynamo sidecar and `EngineAdapter`; the facade no longer
forces aggregated mode. The CPU-only
[`smoke-vllm-pd-mocker.sh`](tests/smoke-vllm-pd-mocker.sh) test runs two of
Dynamo's native vLLM gRPC mockers, proves prefill returns an opaque handoff,
proves decode rejects a missing handoff, then forwards that handoff and checks
the decode OpenAI stream. Build `dynamo-component-facade` and
`dynamo-vllm-mocker-server`, set `GRPCURL_BIN` if needed, and run the script.
The 2026-09-25 run passed with eight decode chunks. A generic-core regression
test separately checks that the configured gateway graph keeps the prefill
handoff out of the public stream and passes it to decode. Neither test moves
real KV data, proves NIXL readiness, or exercises the deployed gateway P/D
path. Those GPU-backed and deployed-graph checks remain pending.

The facade still does not install Dynamo runtime endpoint registration,
KV-event publishers, or dynamic LoRA discovery. Runtime-less equivalents for
those capabilities require separate validation. Do not infer real-engine or
disaggregated performance from the synthetic `benchmark-worker` measurements.

## Upgrade procedure

1. Fetch and rebase the feature branch onto the desired `ai-dynamo/dynamo` `main`.
2. Resolve only changes to the narrow shared methods on `OpenAIPreprocessor`.
3. Regenerate protobuf bindings through `cargo build`; generated files are not checked in.
4. Run `cargo test -p dynamo-component-facades` and the existing `dynamo-llm` parser tests.
5. Build the existing backend sidecars and run the component pipeline end-to-end suite.
6. Rerun the pinned mock and real-engine benchmark matrices before deployment promotion.

All Dynamo dependencies are workspace path dependencies and share the repository lockfile.
There is no separately versioned Dynamo fork or copied policy implementation to update.
