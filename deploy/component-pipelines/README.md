<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Dynamo component pipelines

This directory contains gateway orchestration shared by the four required hosts:
AGW static, AGW generic, Envoy generic with independent transport, and Envoy generic
with Envoy-owned callouts. All hosts use the same component protobuf descriptor and
the same aggregate/disaggregated graph semantics.

`generic-core` is the domain-independent validated DAG runtime proven in the retained
prototype branch, imported here as one Dynamo workspace crate and audited to exclude
tokenizer, selector, worker, and inference policy. Its 88 unit tests remain intact.
`grpc-transport` is the small descriptor-driven transport baseline; it resolves methods
from the descriptor emitted by `dynamo-component-facades`, not generated shadow clients.

The transport supports two explicit protobuf/JSON bridge extensions:

- `x-grpc-json-bytes: true` converts structured graph values into protobuf `bytes`
  fields and decodes JSON-bearing response bytes. The facade still carries canonical
  Dynamo JSON bytes, while the graph can bind fields inside those values.
- `x-grpc-response-body: field_name` unwraps one response-envelope field. The worker
  graph uses it to expose the canonical OpenAI chunk produced by Dynamo rather than a
  gateway-defined response model.

`graphs/aggregate.yaml` is the first shared graph. It prepares through Dynamo's
`OpenAIPreprocessor`, selects through Dynamo's `SelectionService`, and calls the
worker-side composition of a supplied Dynamo backend engine plus Dynamo's canonical
postprocessor. The gateway does not render prompts, compute KV policy, decode tokens,
or parse reasoning/tools.

Upstream host pins for the adapter work are:

- agentgateway `b14ca87d0a1670b3a59859494b06539489118630`
- Envoy `1f921705de13c94326ba72d406fc4113ed81ab70` (`1.39.0-dev`)

The host adapters and disaggregated graph are the next implementation increment; the
shared runtime and contracts here do not by themselves claim those variants complete.
