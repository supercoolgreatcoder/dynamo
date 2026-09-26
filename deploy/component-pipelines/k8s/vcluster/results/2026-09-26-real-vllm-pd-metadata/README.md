# Real vLLM P/D metadata correctness, 2026-09-26

The corrected disaggregated graph and Dynamo worker facades were built from
`cb970285af` in the unified Nix bundle
`/nix/store/if70chidxbz35bw239gda006wif3c9nx-dynamo-component-pipelines-cb970285af`.
The previously validated two-GPU Qwen3-0.6B vLLM/NIXL prefill/decode fixture
was deployed entirely inside the vCluster API
`https://gateway-poc.mkhadkevich-dev:443`, namespace
`dynamo-components-v2`. The engine images and model cache remained pinned
to the prior real vLLM fixture; only the Dynamo facade, gateway, and graph
were upgraded. No NATS or Dynamo runtime was used for discovery.

`run-real-vllm-pd.sh` passed its server-side dry-run and all five Deployments
rolled out Ready. `smoke-real-vllm-pd.sh` then made a streamed OpenAI request
with `stream_options.include_usage=true`. It checked both workers' live Pod
annotations for positive runtime KV block size/capacity and Pod-UID identity,
then asserted generated content, a terminal finish reason, and positive
worker-side prompt-token usage. The observed result was:

```json
{
  "result": "pass",
  "engine": "real vLLM NIXL prefill/decode",
  "gateway": "AGW generic",
  "tokenizer": "Dynamo fastokens",
  "generated_text": "Hello! How can I assist you today?",
  "stream_chunks": 11,
  "finish_reason": "stop",
  "prompt_tokens": 19
}
```

This verifies the corrected graph forwards the preprocessor's prompt-token
count to Dynamo's canonical worker-side postprocessor on a real split vLLM
path. It is a functional check, not a GPU throughput benchmark, multimodal
test, or RDMA qualification. The five temporary test Deployments were scaled
back to zero after the pass, releasing the two GPUs without deleting their
reproducible manifests.
