<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Real vLLM split-path correctness, 2026-09-25

This is a functional test, **not** a throughput benchmark. Everything was
deployed inside the vCluster API
`https://gateway-poc.mkhadkevich-dev:443`, namespace
`dynamo-components-v2`; the four test Deployments were scaled to zero afterward.
No NATS or Dynamo runtime was used for component discovery.

## Pinned runtime and path

- vLLM source: `engines/vllm` submodule revision
  `3284af6bf1be8429c332bd5fafba579c2d7557da` in the dedicated Nix
  branch `feat/dynamo-component-pipeline-builds`.
- Matching upstream `vllm-rs` binary built by the Nix branch's `#vllm-rs`:
  `/nix/store/japwwgwlasw0ll8rgivkhcimr7rv0r9s-vllm-rs-0.1.0`.
- Lean Python headless-engine environment built by that branch's `#vllm`:
  `/nix/store/k95br0pgw95nkxzrarf4hnwgll0bs101-vllm-worker-env`.
- Dynamo facade:
  `/nix/store/gig2imm94kjrv2jbdqkyysfpgqnc1bmv-dynamo-component-facade-1.6.0-8468212130`.
- AGW bundle:
  `/nix/store/j7n9hn14s34c2595q1nr4h9w0055y24x-dynamo-component-pipelines-8468212130`.
- Model: cached `Qwen/Qwen3-0.6B` on one B200 GPU.

The request path was AGW generic graph -> Dynamo fastokens preprocessor ->
selector watching the `real-vllm-split` InferencePool and Pod annotation ->
thin gRPC facade around Dynamo's upstream vLLM sidecar -> upstream `vllm-rs`
`vllm.Inference`/`vllm.Control` -> matching headless Python vLLM engine.
The worker facade published runtime KV capacity to its own Pod annotation
after engine startup.

## Result

`smoke-real-vllm-split.sh` passed twice: first with the temporary Python
environment containing `vllm[grpc]`, then with the final lean environment
above, which omits that unused Python gRPC extra. The final run returned:

```json
{
  "result": "pass",
  "tokenizer": "Dynamo fastokens",
  "prompt_tokens": 19,
  "stream_chunks": 10,
  "finish_reason": "stop",
  "generated_text": "Hello! How can I assist you today?",
  "selector_endpoint": "http://10.0.12.129:50051",
  "gateway_stream_chunks": 10,
  "gateway_finish_reason": "stop",
  "runtime_block_size": 16,
  "runtime_total_kv_blocks": 8979
}
```

The smoke script asserts nonempty generated text, a terminal `stop` both
directly at the worker facade and through AGW, selector routing to the actual
Ready Pod, and a Pod-UID-associated worker annotation with positive block size
and KV capacity. It does not establish performance parity or disaggregated
vLLM correctness.

## Protocol compatibility lesson

The Python `vllm.entrypoints.grpc_server` from this same source revision
started and loaded the model, but Dynamo's sidecar rejected it:
`Unknown service: vllm.Control`. Its gRPC service set is not the sidecar
contract. The matched upstream `vllm-rs` frontend resolved this without
patching Dynamo core or maintaining a parallel vLLM protocol implementation.

## Reproduction

Build `#vllm-rs` and `#vllm` from the dedicated Nix branch
(`path:/work/worktrees/envs-dynamo-component-pipelines-20260924` in this
run), stage both closures into the vCluster's Nix NFS store using
`stage-nix-closure.sh`, then set the explicit vCluster kubeconfig, expected
server, namespace, Nix paths, model NFS path, and run
`run-real-vllm-split.sh`. Run `smoke-real-vllm-split.sh` with `GRPCURL_BIN`
pointing to grpcurl 1.9.4. The scripts reject non-vCluster API servers and
concurrent benchmark fixtures. `run-real-vllm-split.sh` checks store paths
before applying any manifest.

Separate mocker and stock-reference RPS evidence is in
[`../2026-09-25-facade-846821-recheck/README.md`](../2026-09-25-facade-846821-recheck/README.md)
and
[`../2026-09-25-stock-reference-recheck/README.md`](../2026-09-25-stock-reference-recheck/README.md).
