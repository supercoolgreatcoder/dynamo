<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Native SGLang split-pipeline correctness, 2026-09-25

This is a correctness test, **not** a throughput benchmark. It ran entirely
inside the validated vCluster, using a B200 worker Pod with the cached
Qwen/Qwen3-0.6B snapshot. The worker used the native SGLang gRPC server and
the thin `dynamo-component-facade sglang-worker`; the preprocessor and
selector were separate CPU Deployments. AGW generic was the HTTP entry point.
No Dynamo distributed runtime or NATS was started for this path.

The facade is Nix-built from Dynamo commit `0d7e775709` at
`/nix/store/pvwyh2bdw1jvqanwfmb2h6a0ka4ihc9r-dynamo-component-facade-1.6.0-0d7e775709`.
The AGW binary was the previously benchmarked immutable bundle
`/nix/store/g7achknzv9ibixmfdaxgjy4a3pp33dp5-dynamo-component-pipelines-98676215fa`.
The newer complete bundle also built, but was not rolled into this isolated
route; its AGW source pin is unchanged. Both engine and facade ran from a
read-only Nix store mounted into slim BusyBox containers.

After applying `run-real-sglang-split.sh`, the repeatable test was:

```bash
VCLUSTER_KUBECONFIG=/path/to/vcluster.kubeconfig \
VCLUSTER_EXPECTED_SERVER=https://your-vcluster-api.example:443 \
VCLUSTER_NAMESPACE=dynamo-components-v2 \
GRPCURL_BIN=/path/to/grpcurl \
  bash deploy/component-pipelines/k8s/vcluster/smoke-real-sglang-split.sh
```

Observed successful output:

```json
{
  "result": "pass",
  "engine": "SGLang native gRPC",
  "tokenizer": "Dynamo fastokens",
  "prompt_tokens": 19,
  "stream_chunks": 10,
  "finish_reason": "stop",
  "generated_text": "Hello! How can I assist you today?",
  "selector_endpoint": "http://<ready-worker-pod-ip>:50051",
  "gateway_stream_chunks": 10,
  "gateway_finish_reason": "stop"
}
```

The script additionally checks that the selector's InferencePool-discovered
endpoint equals the Ready worker Pod's IP and target port, and that AGW's SSE
stream ends with `[DONE]`. Its request includes `enable_thinking=false` and
`temperature=0`, so both direct gRPC and full-gateway paths return final
answer content containing “hello” and a normal `stop` reason.

This verifies one aggregate SGLang replica and one model. It does not prove
multi-model routing, dynamic KV-capacity publication, a native vLLM split
worker, disaggregated prefill/decode, or performance parity with GPU workers.
