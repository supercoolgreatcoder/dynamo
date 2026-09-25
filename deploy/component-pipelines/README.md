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

`graphs/disaggregated.yaml` is an initial vLLM-style two-role graph. It
drains the prefill worker's canonical raw stream without emitting its handoff
to the HTTP caller, selects a decode worker, and forwards the opaque handoff
through a gRPC envelope field that the facade maps into Dynamo's request.
Its prefill endpoint is currently a Kubernetes Service, not a KV-aware
prefill selector. It does not yet implement SGLang's early bootstrap handoff,
where decode must begin before the prefill stream completes. Do not treat
the graph unit test or CPU mocker smoke as a real-GPU P/D proof.

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

## Build the four-host bundle

Run the following from the Dynamo checkout after applying both patches above. The
tested toolchain uses Rust 1.97, Bazel 8.7.0 (the pinned Envoy source's
`.bazelversion`), `protoc`, `pkg-config`, OpenSSL development headers, and libclang
for the Envoy SDK's bindgen step. Set `LIBCLANG_PATH` to the directory containing
`libclang.so` if bindgen cannot find it. Both host checkouts must remain siblings
of this checkout because the Agentgateway patch has relative path dependencies
on the shared pipeline crates.

```bash
export DYNAMO_EPP_ROOT="$(pwd)"
export AGW_ROOT="$(realpath ../agentgateway)"
export ENVOY_ROOT="$(realpath ../envoy)"

cargo build --release --locked -p dynamo-component-facades \
  --bin dynamo-component-facade
cargo build --release --manifest-path "$AGW_ROOT/Cargo.toml" \
  -p agentgateway-app --bin agentgateway
(cd "$ENVOY_ROOT" && bazel build //source/exe:envoy-static)

# The callout module must compile against the patched SDK in ENVOY_ROOT.
# Build a committed-source copy so Cargo can update its local dependency lock
# without changing the lockfile in this checkout. The shared crates inherit
# dependencies from the root Cargo workspace, so the complete tree is needed.
export MODULE_BUILD_ROOT="$(mktemp -d)"
git archive HEAD | tar -x -C "$MODULE_BUILD_ROOT"
cargo --config "patch.\"https://github.com/envoyproxy/envoy\".envoy-proxy-dynamic-modules-rust-sdk.path=\"$ENVOY_ROOT/source/extensions/dynamic_modules/sdk/rust\"" \
  build --release --features envoy-callout-options \
  --manifest-path "$MODULE_BUILD_ROOT/deploy/component-pipelines/hosts/envoy-generic/Cargo.toml"

nix build --impure --no-link --print-out-paths --expr \
  "import ./deploy/component-pipelines/k8s/vcluster/package.nix {
    facadePath = $DYNAMO_EPP_ROOT/target/release/dynamo-component-facade;
    agentgatewayPath = $AGW_ROOT/target/release/agentgateway;
    envoyPath = $ENVOY_ROOT/bazel-bin/source/exe/envoy-static;
    envoyModulePath = $MODULE_BUILD_ROOT/deploy/component-pipelines/hosts/envoy-generic/target/release/libenvoy_generic_pipeline.so;
  }"
```

The Nix output contains `bin/dynamo-component-facade`, `bin/agentgateway`,
`bin/envoy-static`, and `lib/libgeneric_pipeline.so`. Keep the returned store path
as `COMPONENT_BUNDLE` for the vCluster templates. The upstream Envoy source patch
changes the host ABI; using an unpatched Envoy binary with the callout module is
not a valid build. A fresh build needs the usual Cargo and Bazel dependency access
and can take substantial time. The vCluster deployment and benchmark procedure is
in `k8s/vcluster/README.md`.

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
