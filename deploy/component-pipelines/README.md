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

The host changes are committed as portable patches, rather than vendored source trees.
Clone each upstream next to this `dynamo-epp` checkout so the Agentgateway patch's
relative workspace dependencies resolve, then verify and apply the patches:

```bash
export DYNAMO_EPP_ROOT="$(pwd)"

git clone https://github.com/agentgateway/agentgateway.git ../agentgateway
git -C ../agentgateway checkout b14ca87d0a1670b3a59859494b06539489118630
git -C ../agentgateway apply --check \
  "$DYNAMO_EPP_ROOT/deploy/component-pipelines/hosts/agentgateway/agentgateway-b14ca87.patch"
git -C ../agentgateway apply \
  "$DYNAMO_EPP_ROOT/deploy/component-pipelines/hosts/agentgateway/agentgateway-b14ca87.patch"

git clone https://github.com/envoyproxy/envoy.git ../envoy
git -C ../envoy checkout 1f921705de13c94326ba72d406fc4113ed81ab70
git -C ../envoy apply --check \
  "$DYNAMO_EPP_ROOT/deploy/component-pipelines/hosts/envoy-generic/patches/envoy-callout-options.patch"
git -C ../envoy apply \
  "$DYNAMO_EPP_ROOT/deploy/component-pipelines/hosts/envoy-generic/patches/envoy-callout-options.patch"
```

`git apply --check` is intentional: it fails immediately when an upstream pin or patch
artifact has drifted, before an expensive host build starts.

`hosts/envoy-generic` embeds that same runtime in Envoy. `envoy-independent.yaml`
uses the transport's own tonic channels; `envoy-callouts.yaml` sends every mapped hop
through Envoy clusters. Streaming callouts use Envoy's stock HTTP-stream ABI. Unary
gRPC retries and trailer-aware callouts use the pinned additive patch in
`hosts/envoy-generic/patches`; configurations that request those features fail to load
unless the matching Envoy ABI is present.

Export the descriptor used by either host from the facade binary:

```bash
dynamo-component-facade descriptor --output components_descriptor.bin
```

The Agentgateway patch contains both the static compiled aggregate pipeline and the
descriptor-driven generic pipeline. Together with the two Envoy transport modes this
provides the four benchmark arms: AGW static, AGW generic, Envoy generic with independent
tonic transport, and Envoy generic with Envoy-owned callouts. The vCluster packaging and
benchmark procedure is documented in `k8s/vcluster/README.md`.
