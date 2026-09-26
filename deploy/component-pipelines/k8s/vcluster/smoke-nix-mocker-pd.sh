#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# The decode benchmark worker rejects requests without the prefill marker, so
# a complete OpenAI stream proves that the graph relayed its gRPC handoff.
set -euo pipefail
: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || {
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
}
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
service=${PD_GATEWAY_SERVICE:-dynamo-pd-agw-generic}
[[ $service == dynamo-pd-agw-generic || $service == dynamo-pd-envoy-generic ]] || exit 2
"${vc[@]}" get deployment "$service" -o json |
  jq -e '.status.readyReplicas == 1' >/dev/null

port=${PD_LOCAL_PORT:-18081}
[[ $port =~ ^[1-9][0-9]{3,4}$ ]] || exit 2
tmp_dir=$(mktemp -d -t dynamo-mocker-pd-smoke.XXXXXXXX)
"${vc[@]}" port-forward "service/$service" "$port:8080" \
  > "$tmp_dir/port-forward.log" 2>&1 &
forward_pid=$!
cleanup() {
  kill "$forward_pid" 2>/dev/null || true
  wait "$forward_pid" 2>/dev/null || true
  rm -r -- "$tmp_dir"
}
trap cleanup EXIT
ready=0
for _ in {1..40}; do
  code=$(curl -sS -o /dev/null -w '%{http_code}' \
    "http://127.0.0.1:$port/v1/models" 2>/dev/null || true)
  if [[ $code != 000 && -n $code ]]; then ready=1; break; fi
  sleep 0.25
done
[[ $ready == 1 ]] || {
  echo "gateway port-forward did not become ready" >&2
  cat "$tmp_dir/port-forward.log" >&2
  exit 1
}
response=$(curl --no-buffer -fsS --max-time 120 \
  -H 'Content-Type: application/json' \
  -d '{"model":"Qwen/Qwen2.5-0.5B-Instruct","messages":[{"role":"user","content":"Reply briefly."}],"max_tokens":4,"stream":true}' \
  "http://127.0.0.1:$port/v1/chat/completions")
[[ $response == *'"content":"x"'* ]] || {
  echo "missing decoded token in OpenAI stream" >&2
  printf '%s\n' "$response" >&2
  exit 1
}
[[ $response == *'"finish_reason":"length"'* ]] || {
  echo "missing terminal length finish reason" >&2
  printf '%s\n' "$response" >&2
  exit 1
}
[[ $response == *'[DONE]'* ]] || {
  echo "missing OpenAI stream terminator" >&2
  printf '%s\n' "$response" >&2
  exit 1
}
echo "synthetic P/D handoff and streamed decode passed inside $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
