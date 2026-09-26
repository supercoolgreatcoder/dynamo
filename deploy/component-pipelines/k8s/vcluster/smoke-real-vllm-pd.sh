#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Real two-GPU KV handoff through the vCluster-only AGW P/D graph.
set -euo pipefail
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2

kubectl_bin=${KUBECTL_BIN:-kubectl}
gateway_port=${GATEWAY_PORT:-18083}
actual_server=$(
  "$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" \
    config view --minify -o jsonpath='{.clusters[0].cluster.server}'
)
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || {
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
}
vc=("$kubectl_bin" --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
for deployment in real-vllm-pd-prefill real-vllm-pd-decode \
  real-vllm-pd-preprocessor real-vllm-pd-selector real-vllm-pd-agw; do
  "${vc[@]}" rollout status "deployment/$deployment" --timeout=10s >/dev/null
done

for role in prefill decode; do
  pod=$("${vc[@]}" get pods -l "app=real-vllm-pd-$role" -o json)
  metadata=$(jq -r '.items[0].metadata.annotations["dynamo.nvidia.com/worker-metadata"]' \
    <<<"$pod")
  uid=$(jq -r '.items[0].metadata.uid' <<<"$pod")
  jq -e --arg uid "$uid" '
    .model_name == "Qwen/Qwen3-0.6B" and
    .stable_routing_id == $uid and
    .block_size > 0 and .total_kv_blocks > 0
  ' <<<"$metadata" >/dev/null || {
    echo "$role worker did not publish runtime KV metadata" >&2
    exit 1
  }
done

if (echo >/dev/tcp/127.0.0.1/"$gateway_port") 2>/dev/null; then
  echo "refusing occupied local port $gateway_port" >&2
  exit 2
fi
"${vc[@]}" port-forward service/real-vllm-pd-agw "$gateway_port:8080" \
  --address 127.0.0.1 >/dev/null 2>&1 &
forward_pid=$!
cleanup() { kill "$forward_pid" 2>/dev/null || true; wait "$forward_pid" 2>/dev/null || true; }
trap cleanup EXIT
for attempt in {1..30}; do
  if curl --silent --max-time 1 --output /dev/null \
    "http://127.0.0.1:$gateway_port/"; then
    break
  fi
  kill -0 "$forward_pid" 2>/dev/null || {
    echo "gateway port-forward exited" >&2
    exit 1
  }
  [[ $attempt != 30 ]] || { echo "gateway port-forward timed out" >&2; exit 1; }
  sleep 1
done

request='{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"Say hello in one short sentence."}],"max_tokens":32,"temperature":0,"stream":true,"stream_options":{"include_usage":true},"chat_template_kwargs":{"enable_thinking":false}}'
stream=$(
  curl --fail-with-body --silent --show-error --no-buffer --max-time 120 \
    -H 'content-type: application/json' -d "$request" \
    "http://127.0.0.1:$gateway_port/v1/chat/completions"
)
[[ $stream == *'data: [DONE]'* ]] || {
  echo "P/D gateway stream did not terminate with [DONE]" >&2
  printf '%s\n' "$stream" >&2
  exit 1
}
chunks=$(sed -n 's/^data: //p' <<<"$stream" | sed '/^\[DONE\]$/d')
jq -es '
  length > 0 and
  (map(.choices[0].delta.content // "") | join("") | ascii_downcase | contains("hello")) and
  any(.[]; .choices[0].finish_reason != null) and
  any(.[]; (.usage.prompt_tokens // 0) > 0)
' <<<"$chunks" >/dev/null || {
  echo "P/D gateway returned no content or no terminal finish reason" >&2
  printf '%s\n' "$stream" >&2
  exit 1
}

jq -n --arg text "$(jq -sr '[.[].choices[0].delta.content // ""] | join("")' \
    <<<"$chunks")" \
  --arg finish_reason "$(jq -sr '[.[] | .choices[0].finish_reason // empty][-1]' \
    <<<"$chunks")" \
  --argjson chunks "$(jq -s 'length' <<<"$chunks")" \
  --argjson prompt_tokens "$(jq -sr '[.[] | .usage.prompt_tokens // empty][-1]' <<<"$chunks")" \
  '{result:"pass",engine:"real vLLM NIXL prefill/decode",gateway:"AGW generic",
    tokenizer:"Dynamo fastokens",generated_text:$text,
    stream_chunks:$chunks,finish_reason:$finish_reason,prompt_tokens:$prompt_tokens}'
