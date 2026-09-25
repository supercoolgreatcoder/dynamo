<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Real vLLM prefill/decode correctness in the vCluster

On 2026-09-25, the two-GPU `real-vllm-pd-*` fixture passed a streamed
OpenAI-compatible request through AGW generic, the Dynamo fastokens
preprocessor, InferencePool-backed selector, and separate vLLM prefill and
decode workers. Both workers ran the pinned upstream `vllm-rs` frontend plus
headless vLLM engine, with `NixlConnector` `kv_producer`/`kv_consumer` roles
and UCX enabled. The graph sent the prefill worker's NIXL handoff JSON over
the gRPC facade to the decode worker. It did not use NATS or the Dynamo
runtime for discovery.

The deployment used Dynamo source commit `accf6af699`, the Nix facade
`/nix/store/v4ag5hc0vwvn665f0c1gswd1pyydi8d1-dynamo-component-facade-1.6.0-accf6af699`,
and the AGW bundle
`/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699`.
The vCluster API was
`https://gateway-poc.mkhadkevich-dev:443`, namespace
`dynamo-components-v2`. Prefill and decode Pods were Ready on different GPU
nodes and published runtime KV metadata through Pod annotations; each
reported block size 16 and 8,979 total KV blocks.

The first request failed despite HTTP 200: decode logged a NIXL remote
engine-ID mismatch because its handoff advertised `localhost:5600`. The
runtime-less fixture was missing `VLLM_NIXL_SIDE_CHANNEL_HOST`. Following
the upstream `lib/sidecar/vllm/deploy/disagg.yaml` pattern, both engine Pods
now obtain that variable from the Downward API `status.podIP`. After rollout,
the decode engine logged `NIXL compatibility check passed` and no handshake
or KV-load error for the passing request. The passing smoke output was:

```json
{
  "result": "pass",
  "engine": "real vLLM NIXL prefill/decode",
  "gateway": "AGW generic",
  "tokenizer": "Dynamo fastokens",
  "generated_text": "Hello! How can I assist you today?",
  "stream_chunks": 10,
  "finish_reason": "stop"
}
```

Reproduce with `run-real-vllm-pd.sh` and `smoke-real-vllm-pd.sh` in this
directory's parent, after staging both Nix closures and setting the exact
vCluster, facade, and bundle variables described in the parent README.
The smoke script checks the two runtime KV annotations and requires both
response content and a terminal finish reason. This proves functional
cross-pod NIXL handoff for one request; it is not a throughput result or
an RDMA performance qualification. The engine warned that accelerated IB
support was unavailable, so fabric performance must be measured separately.
