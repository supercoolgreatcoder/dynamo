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
| Worker bridge | A caller-supplied canonical Dynamo backend `ServiceEngine` |
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
stream drops the underlying Dynamo stream.

## Upgrade procedure

1. Fetch and rebase the feature branch onto the desired `ai-dynamo/dynamo` `main`.
2. Resolve only changes to the narrow shared methods on `OpenAIPreprocessor`.
3. Regenerate protobuf bindings through `cargo build`; generated files are not checked in.
4. Run `cargo test -p dynamo-component-facades` and the existing `dynamo-llm` parser tests.
5. Build the existing backend sidecars and run the component pipeline end-to-end suite.
6. Rerun the pinned mock and real-engine benchmark matrices before deployment promotion.

All Dynamo dependencies are workspace path dependencies and share the repository lockfile.
There is no separately versioned Dynamo fork or copied policy implementation to update.
