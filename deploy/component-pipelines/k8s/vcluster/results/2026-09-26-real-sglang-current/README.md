<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Real SGLang aggregate correctness, 2026-09-26

The current unified Nix bundle
`/nix/store/if70chidxbz35bw239gda006wif3c9nx-dynamo-component-pipelines-cb970285af`
supplied the Dynamo-derived facade, preprocessor, selector, and AGW generic
gateway. The native SGLang gRPC engine used the pinned
`/nix/store/cra3bmqp5hvv4c7wxbkh67f4gwx8mkdr-sglang-worker-env` and the
cached Qwen3-0.6B model. The deployment and smoke test used only the explicit
vCluster API `https://gateway-poc.mkhadkevich-dev:443` in namespace
`dynamo-components-v2`; the scripts reject a different API server. No NATS
or Dynamo distributed runtime was started.

After the engine, facade, preprocessor, selector, and gateway rolled out
Ready, `smoke-real-sglang-split.sh` exercised preprocessing, selection, and
worker streaming directly over gRPC, then a streamed OpenAI request through
AGW. It checked that the InferencePool selector returned the Ready worker
Pod's IP, and that the worker's live annotation contained its Pod UID and
positive runtime KV block size and capacity. The observed output was:

```json
{
  "result": "pass",
  "engine": "SGLang native gRPC",
  "tokenizer": "Dynamo fastokens",
  "prompt_tokens": 19,
  "stream_chunks": 10,
  "finish_reason": "stop",
  "generated_text": "Hello! How can I assist you today?",
  "selector_endpoint": "http://10.0.18.104:50051",
  "gateway_stream_chunks": 10,
  "gateway_finish_reason": "stop",
  "runtime_block_size": 64,
  "runtime_total_kv_blocks": 2401
}
```

For reproduction, set `VCLUSTER_KUBECONFIG`,
`VCLUSTER_EXPECTED_SERVER`, `VCLUSTER_NAMESPACE`, the three immutable Nix
store paths above (`FACADE_STORE_PATH` and `GATEWAY_BUNDLE_PATH` both use the
unified bundle), `NIX_STORE_NFS_SERVER/PATH`, and `MODEL_NFS_SERVER/PATH` from
the existing fixture. Set `NIX_STAGER_POD` to the live store stager and run
`run-real-sglang-split.sh`; then set `GRPCURL_BIN` and run
`smoke-real-sglang-split.sh`. The deployment script performs a server-side
dry-run before applying the manifests. The temporary fixture Deployments
were scaled to zero after the pass, releasing the GPU.

This proves aggregate real-worker compatibility on the current facade pin;
it is not a SGLang P/D, GPU throughput, dynamic post-start KV update, or
multi-model benchmark.
