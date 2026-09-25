#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# CPU-only proof that Dynamo's vLLM prefill/decode engines exchange an opaque
# handoff through the facades. This does not move KV data or exercise NIXL.
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
facade_bin=${FACADE_BIN:-$repo_root/target/debug/dynamo-component-facade}
mocker_bin=${MOCKER_BIN:-$repo_root/target/debug/dynamo-vllm-mocker-server}
grpcurl_bin=${GRPCURL_BIN:-grpcurl}
model_path=${MODEL_PATH:-$repo_root/lib/llm/tests/data/sample-models/mock-llama-3.1-8b-instruct}
prefill_engine_port=${PREFILL_ENGINE_PORT:-30051}
decode_engine_port=${DECODE_ENGINE_PORT:-30052}
preprocessor_port=${PREPROCESSOR_PORT:-50060}
prefill_facade_port=${PREFILL_FACADE_PORT:-50061}
decode_facade_port=${DECODE_FACADE_PORT:-50062}

for bin in "$facade_bin" "$mocker_bin" "$grpcurl_bin"; do
  command -v "$bin" >/dev/null || { echo "missing executable: $bin" >&2; exit 2; }
done
command -v jq >/dev/null
test -d "$model_path"

tcp_ready() { (echo >/dev/tcp/127.0.0.1/"$1") 2>/dev/null; }
for port in "$prefill_engine_port" "$decode_engine_port" "$preprocessor_port" \
  "$prefill_facade_port" "$decode_facade_port"; do
  if tcp_ready "$port"; then
    echo "refusing occupied loopback port $port" >&2
    exit 2
  fi
done

log_dir=$(mktemp -d -t dynamo-vllm-pd-mocker.XXXXXXXX)
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

"$mocker_bin" --listen "127.0.0.1:$prefill_engine_port" --model "$model_path" \
  --disaggregation-mode prefill \
  --extra-engine-args '{"speedup_ratio":1000,"enable_prefix_caching":false}' \
  >"$log_dir/prefill-engine.log" 2>&1 &
pids+=("$!")
"$mocker_bin" --listen "127.0.0.1:$decode_engine_port" --model "$model_path" \
  --disaggregation-mode decode \
  --extra-engine-args '{"speedup_ratio":1000,"enable_prefix_caching":false}' \
  >"$log_dir/decode-engine.log" 2>&1 &
pids+=("$!")
"$facade_bin" --listen "127.0.0.1:$preprocessor_port" preprocessor \
  --model-path "$model_path" >"$log_dir/preprocessor.log" 2>&1 &
pids+=("$!")
"$facade_bin" --listen "127.0.0.1:$prefill_facade_port" vllm-worker \
  --model-path "$model_path" -- \
  --grpc-endpoint "127.0.0.1:$prefill_engine_port" --disaggregation-mode prefill \
  >"$log_dir/prefill-facade.log" 2>&1 &
pids+=("$!")
"$facade_bin" --listen "127.0.0.1:$decode_facade_port" vllm-worker \
  --model-path "$model_path" -- \
  --grpc-endpoint "127.0.0.1:$decode_engine_port" --disaggregation-mode decode \
  >"$log_dir/decode-facade.log" 2>&1 &
pids+=("$!")

for port in "$preprocessor_port" "$prefill_facade_port" "$decode_facade_port"; do
  ready=false
  for attempt in {1..120}; do
    if tcp_ready "$port"; then ready=true; break; fi
    for pid in "${pids[@]}"; do
      if ! kill -0 "$pid" 2>/dev/null; then
        echo "a test process exited; logs: $log_dir" >&2
        exit 1
      fi
    done
    sleep 1
  done
  if [[ $ready != true ]]; then
    echo "timed out waiting for port $port; logs: $log_dir" >&2
    exit 1
  fi
done

openai_request=$(jq -nc --arg model "$model_path" '{model:$model,messages:[{role:"user",content:"Say hello."}],max_tokens:8,temperature:0,stream:true}')
prepared=$(
  jq -nc --arg request "$openai_request" '{itemId:"cpu-pd-1",openaiRequestJson:($request|@base64),packedTokensOnly:true}' |
    "$grpcurl_bin" -plaintext -d @ "127.0.0.1:$preprocessor_port" \
      dynamo.components.v1.Preprocessor/Prepare
)
jq -e '.error == null and (.promptTokens|tonumber) > 0' <<<"$prepared" >/dev/null

prefill_request=$(jq -nc --argjson p "$prepared" '{requestId:$p.itemId,backendRequestJson:$p.backendRequestJson,tokenIdsLe:$p.tokenIdsLe,promptTokens:$p.promptTokens}')
prefill_output=$(
  "$grpcurl_bin" -plaintext -d "$prefill_request" "127.0.0.1:$prefill_facade_port" \
    dynamo.components.v1.ChatWorkerBridge/GenerateRaw
)
handoff=$(jq -sce '
  (last.finished == true) and
  ([.[] | select(.annotatedBackendChunkJson != null)] | length == 1) and
  ([.[] | select(.annotatedBackendChunkJson != null)][0].annotatedBackendChunkJson
    | @base64d | fromjson | .data.disaggregated_params | type == "object")
' <<<"$prefill_output")
test "$handoff" = true
handoff_json=$(jq -sc '[.[] | select(.annotatedBackendChunkJson != null)][0].annotatedBackendChunkJson | @base64d | fromjson | .data.disaggregated_params' <<<"$prefill_output")

missing_status=0
missing_output=$(
  "$grpcurl_bin" -plaintext -d "$(jq -nc --argjson p "$prepared" '{requestId:"cpu-pd-missing",backendRequestJson:$p.backendRequestJson,normalizedOpenaiRequestJson:$p.normalizedOpenaiRequestJson,tokenIdsLe:$p.tokenIdsLe,promptTokens:$p.promptTokens}')" \
    "127.0.0.1:$decode_facade_port" dynamo.components.v1.ChatWorkerBridge/Generate 2>&1
) || missing_status=$?
if [[ $missing_status == 0 || $missing_output != *'missing the prefill_result'* ]]; then
  echo "decode did not reject missing handoff: $missing_output" >&2
  exit 1
fi

decode_request=$(jq -nc --argjson p "$prepared" --argjson handoff "$handoff_json" '
  {requestId:$p.itemId,
   backendRequestJson:$p.backendRequestJson,
   prefillResultJson:($handoff | tojson | @base64),
   normalizedOpenaiRequestJson:$p.normalizedOpenaiRequestJson,
   tokenIdsLe:$p.tokenIdsLe,promptTokens:$p.promptTokens}
')
decode_output=$(
  "$grpcurl_bin" -plaintext -d "$decode_request" "127.0.0.1:$decode_facade_port" \
    dynamo.components.v1.ChatWorkerBridge/Generate
)
jq -se '
  (map(.openaiChunkJson | @base64d | fromjson) |
   (map(.choices[0].delta.content // "") | join("") | length > 0) and
   any(.[]; .choices[0].finish_reason != null))
' <<<"$decode_output" >/dev/null

jq -n --arg logs "$log_dir" --argjson prefill "$handoff_json" \
  --argjson chunks "$(jq -s 'length' <<<"$decode_output")" \
  '{result:"pass",engine:"Dynamo vLLM CPU mocker",prefill_handoff:$prefill,
    decode_chunks:$chunks,decode_without_handoff:"rejected",logs:$logs}'
