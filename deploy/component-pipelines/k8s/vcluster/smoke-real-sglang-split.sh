#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Exercise Dynamo preprocessing and a native SGLang engine through their thin
# gRPC facades. This script only port-forwards vCluster Services; it never
# contacts the host-cluster API or creates Kubernetes resources.
set -euo pipefail

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server URL}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"

kubectl_bin=${KUBECTL_BIN:-kubectl}
grpcurl_bin=${GRPCURL_BIN:-grpcurl}
preprocessor_port=${PREPROCESSOR_PORT:-16052}
worker_port=${WORKER_PORT:-16053}
selector_port=${SELECTOR_PORT:-16054}
gateway_port=${GATEWAY_PORT:-18081}
command -v "$kubectl_bin" >/dev/null
command -v "$grpcurl_bin" >/dev/null
command -v jq >/dev/null
command -v curl >/dev/null

actual_server=$(
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" \
    config view --minify --output jsonpath='{.clusters[0].cluster.server}'
)
if [[ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]]; then
  echo "refusing non-vCluster API: expected $VCLUSTER_EXPECTED_SERVER, got $actual_server" >&2
  exit 2
fi

"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  rollout status deployment/real-qwen3-preprocessor --timeout=10s >/dev/null
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  rollout status deployment/real-sglang-split --timeout=10s >/dev/null
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  rollout status deployment/real-qwen3-selector --timeout=10s >/dev/null
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  rollout status deployment/real-qwen3-agw-generic --timeout=10s >/dev/null

"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  port-forward service/real-qwen3-preprocessor "$preprocessor_port:50051" \
  --address 127.0.0.1 >/dev/null 2>&1 &
preprocessor_pid=$!
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  port-forward service/real-sglang-split "$worker_port:50051" \
  --address 127.0.0.1 >/dev/null 2>&1 &
worker_pid=$!
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  port-forward service/real-qwen3-selector "$selector_port:50051" \
  --address 127.0.0.1 >/dev/null 2>&1 &
selector_pid=$!
"$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
  port-forward service/real-qwen3-agw-generic "$gateway_port:8080" \
  --address 127.0.0.1 >/dev/null 2>&1 &
gateway_pid=$!
trap 'kill "$preprocessor_pid" "$worker_pid" "$selector_pid" "$gateway_pid" 2>/dev/null || true' EXIT

for attempt in {1..30}; do
  if "$grpcurl_bin" -plaintext "127.0.0.1:$preprocessor_port" list >/dev/null 2>&1 &&
    "$grpcurl_bin" -plaintext "127.0.0.1:$worker_port" list >/dev/null 2>&1 &&
    "$grpcurl_bin" -plaintext "127.0.0.1:$selector_port" list >/dev/null 2>&1 &&
    curl --silent --max-time 1 --output /dev/null "http://127.0.0.1:$gateway_port/"; then
    break
  fi
  if ! kill -0 "$preprocessor_pid" 2>/dev/null ||
     ! kill -0 "$worker_pid" 2>/dev/null ||
     ! kill -0 "$selector_pid" 2>/dev/null ||
     ! kill -0 "$gateway_pid" 2>/dev/null; then
    echo "a vCluster port-forward exited before gRPC became ready" >&2
    exit 1
  fi
  if [[ "$attempt" == 30 ]]; then
    echo "timed out waiting for both vCluster gRPC port-forwards" >&2
    exit 1
  fi
  sleep 1
done

openai_request='{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"Say hello in one short sentence."}],"max_tokens":32,"temperature":0,"stream":true,"chat_template_kwargs":{"enable_thinking":false}}'
prepared=$(
  jq -n --arg request "$openai_request" \
    '{itemId:"real-sglang-split-smoke",openaiRequestJson:($request|@base64),packedTokensOnly:true}' |
    "$grpcurl_bin" -plaintext -d @ "127.0.0.1:$preprocessor_port" \
      dynamo.components.v1.Preprocessor/Prepare
)
if [[ -n "$(jq -r '.error // empty' <<<"$prepared")" ]]; then
  echo "preprocessor error: $(jq -r '.error' <<<"$prepared")" >&2
  exit 1
fi

selector_request=$(
  jq -c '{itemId:.itemId,payloadJson:.selectorRequestJson,tokenIdsLe:.tokenIdsLe}' \
    <<<"$prepared"
)
selection=$(
  "$grpcurl_bin" -plaintext -d "$selector_request" "127.0.0.1:$selector_port" \
    dynamo.components.v1.Selector/Select
)
if [[ -n "$(jq -r '.error // empty' <<<"$selection")" ]]; then
  echo "selector error: $(jq -r '.error' <<<"$selection")" >&2
  exit 1
fi
worker_endpoint=$(jq -r '.payloadJson | @base64d | fromjson | .endpoint' <<<"$selection")
worker_pod=$(
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" \
    get pods -l app=real-sglang-split -o json
)
worker_ip=$(jq -r '.items[0].status.podIP' <<<"$worker_pod")
if [[ "$worker_endpoint" != "http://$worker_ip:50051" ]]; then
  echo "selector returned $worker_endpoint, expected real worker Pod $worker_ip:50051" >&2
  exit 1
fi
worker_metadata=$(jq -r '.items[0].metadata.annotations["dynamo.nvidia.com/worker-metadata"]' <<<"$worker_pod")
worker_uid=$(jq -r '.items[0].metadata.uid' <<<"$worker_pod")
if ! jq -e --arg uid "$worker_uid" '
  .model_name == "Qwen/Qwen3-0.6B" and
  .stable_routing_id == $uid and
  .block_size > 0 and .total_kv_blocks > 0
' <<<"$worker_metadata" >/dev/null; then
  echo "native worker did not publish its runtime KV capacity to its own Pod" >&2
  exit 1
fi
if ! jq -e '.promptTokens | tonumber > 0' <<<"$prepared" >/dev/null; then
  echo "preprocessor returned no prompt tokens" >&2
  exit 1
fi

worker_request=$(
  jq -c '{requestId:.itemId,backendRequestJson:.backendRequestJson,
          normalizedOpenaiRequestJson:.normalizedOpenaiRequestJson,
          promptInjectedReasoning:(.promptInjectedReasoning // false),
          usesToolCallStructuralTag:(.usesToolCallStructuralTag // false),
          promptTokens:.promptTokens,tokenIdsLe:.tokenIdsLe}
        | with_entries(select(.value != null))' <<<"$prepared"
)
worker_stream=$(
  "$grpcurl_bin" -plaintext -d "$worker_request" "127.0.0.1:$worker_port" \
    dynamo.components.v1.ChatWorkerBridge/Generate
)
if [[ -n "$(jq -r '.error // empty' <<<"$worker_stream")" ]]; then
  echo "worker facade error: $(jq -r '.error // empty' <<<"$worker_stream")" >&2
  exit 1
fi
chunks=$(jq -r '.openaiChunkJson // empty | @base64d' <<<"$worker_stream")
if ! jq -e -s '
  length > 0 and
  (map(.choices[0].delta.content // "") | join("") | ascii_downcase | contains("hello")) and
  any(.[]; .choices[0].finish_reason == "stop")
' <<<"$chunks" >/dev/null; then
  echo "worker returned no content or no terminal finish reason" >&2
  exit 1
fi

gateway_stream=$(
  curl --fail-with-body --silent --show-error --no-buffer --max-time 30 \
    -H 'content-type: application/json' -d "$openai_request" \
    "http://127.0.0.1:$gateway_port/v1/chat/completions"
)
if [[ "$gateway_stream" != *'data: [DONE]'* ]]; then
  echo "gateway stream did not terminate with [DONE]" >&2
  exit 1
fi
gateway_chunks=$(sed -n 's/^data: //p' <<<"$gateway_stream" | sed '/^\[DONE\]$/d')
if ! jq -e -s '
  length > 0 and
  (map(.choices[0].delta.content // "") | join("") | ascii_downcase | contains("hello")) and
  any(.[]; .choices[0].finish_reason == "stop")
' <<<"$gateway_chunks" >/dev/null; then
  echo "gateway returned no content or no terminal finish reason" >&2
  exit 1
fi

jq -n --argjson prompt_tokens "$(jq -r '.promptTokens' <<<"$prepared")" \
  --argjson chunks "$(jq -s 'length' <<<"$chunks")" \
  --arg finish_reason "$(jq -r 'select(.choices[0].finish_reason != null) | .choices[0].finish_reason' <<<"$chunks" | tail -1)" \
  --arg text "$(jq -r '.choices[0].delta.content // empty' <<<"$chunks" | tr -d '\n')" \
  --arg selector_endpoint "$worker_endpoint" \
  --argjson gateway_chunks "$(jq -s 'length' <<<"$gateway_chunks")" \
  --arg gateway_finish_reason "$(jq -r 'select(.choices[0].finish_reason != null) | .choices[0].finish_reason' <<<"$gateway_chunks" | tail -1)" \
  --argjson block_size "$(jq -r '.block_size' <<<"$worker_metadata")" \
  --argjson total_kv_blocks "$(jq -r '.total_kv_blocks' <<<"$worker_metadata")" \
  '{result:"pass",engine:"SGLang native gRPC",tokenizer:"Dynamo fastokens",
    prompt_tokens:$prompt_tokens,stream_chunks:$chunks,finish_reason:$finish_reason,
    generated_text:$text,selector_endpoint:$selector_endpoint,
    gateway_stream_chunks:$gateway_chunks,gateway_finish_reason:$gateway_finish_reason,
    runtime_block_size:$block_size,runtime_total_kv_blocks:$total_kv_blocks}'
