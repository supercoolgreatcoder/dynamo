#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# One isolated stock-Dynamo frontend/mocker trial. Restart both Deployments
# before every cell so stale frontend KV/tokenizer state cannot carry over.
set -euo pipefail
shopt -s nullglob

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${RESULT_DIR:?set a local result directory}"
: "${TRIAL:?set a fresh rN trial identifier}"
: "${WORKLOAD:?set short, isl4000, or mooncake}"
[[ "$TRIAL" =~ ^r[1-9][0-9]*$ ]] || { echo "TRIAL must be rN" >&2; exit 2; }
case "$WORKLOAD" in short|isl4000|mooncake) ;; *) echo "invalid WORKLOAD" >&2; exit 2 ;; esac
test -d "$RESULT_DIR"

actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [[ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]]; then
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
fi
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
job="nixv2-${WORKLOAD}-dynamo-reference-${TRIAL}"
if [[ -d "$RESULT_DIR/raw_aiperf/$job" ]] || "${vc[@]}" get job "$job" >/dev/null 2>&1; then
  echo "refusing to reuse prior benchmark evidence: $job" >&2
  exit 2
fi
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
    echo "another benchmark Job is active" >&2
    exit 2
  }
for inactive in agw-static agw-generic envoy-independent envoy-callouts \
  real-qwen3-agw-generic real-qwen3-preprocessor real-qwen3-selector real-sglang-split; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null || {
      echo "$inactive is active; refusing a confounded reference benchmark" >&2
      exit 2
    }
done
for component in dynamo-preprocessor:4 dynamo-selector:1 dynamo-benchmark-worker:16; do
  name=${component%:*}
  expected=${component#*:}
  "${vc[@]}" get deployment "$name" -o json |
    jq -e --argjson expected "$expected" \
      '.spec.replicas == $expected and .status.readyReplicas == $expected' >/dev/null || {
        echo "$name is not $expected/$expected Ready" >&2
        exit 2
      }
done
"${vc[@]}" get deployment etcd -o json |
  jq -e '.spec.replicas == 1 and .status.readyReplicas == 1' >/dev/null
"${vc[@]}" get deployment nats -o json |
  jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null

cleanup() {
  "${vc[@]}" scale deployment/dynamo-frontend-reference \
    deployment/dynamo-reference-worker --replicas=0
}
trap cleanup EXIT

wait_pods_gone() {
  local label=$1 attempt
  for attempt in {1..60}; do
    if "${vc[@]}" get pods -l "app=$label" -o json |
      jq -e '.items | length == 0' >/dev/null; then
      return 0
    fi
    sleep 5
  done
  echo "timed out draining $label Pods" >&2
  return 1
}

cleanup
wait_pods_gone dynamo-frontend-reference
wait_pods_gone dynamo-reference-worker
"${vc[@]}" scale deployment/dynamo-reference-worker --replicas=4
"${vc[@]}" rollout status deployment/dynamo-reference-worker --timeout=300s
"${vc[@]}" scale deployment/dynamo-frontend-reference --replicas=1
"${vc[@]}" rollout status deployment/dynamo-frontend-reference --timeout=300s

bash "$(dirname "$0")/run-nix-mocker-trial.sh" \
  dynamo-reference "$WORKLOAD" "$TRIAL"
