#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Run one isolated six-client capacity trial. Scale and verify the chosen gateway
# before invoking this script; the runner never changes component deployments.
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 {agw-static|agw-generic|envoy-generic|envoy-callouts|dynamo-reference|pd-agw-generic|pd-envoy-generic|pd-envoy-callouts} {short|isl4000|mooncake} rN" >&2
  exit 2
fi

arm=$1
workload=$2
trial=$3
case "$arm" in
  agw-static) service=agw-static ;;
  agw-generic) service=agw-generic ;;
  envoy-generic) service=envoy-independent ;;
  envoy-callouts) service=envoy-callouts ;;
  dynamo-reference) service=dynamo-frontend-reference ;;
  pd-agw-generic) service=dynamo-pd-agw-generic ;;
  pd-envoy-generic) service=dynamo-pd-envoy-generic ;;
  pd-envoy-callouts) service=dynamo-pd-envoy-callouts ;;
  *) echo "unsupported arm: $arm" >&2; exit 2 ;;
esac
case "$workload" in
  short) dataset=short-claude-sonnet-raw.jsonl ;;
  isl4000) dataset=isl4000-claude-sonnet-raw.jsonl ;;
  mooncake) dataset= ;;
  *) echo "unsupported workload: $workload" >&2; exit 2 ;;
esac
[[ "$trial" =~ ^r[1-9][0-9]*$ ]] || { echo "trial must be rN" >&2; exit 2; }
record_export=${PD_RECORD_EXPORT:-0}
[[ $record_export == 0 || $record_export == 1 ]] || exit 2
if [[ $record_export == 1 ]]; then
  [[ $workload == isl4000 && ( $arm == pd-agw-generic || $arm == pd-envoy-generic ) ]] || exit 2
fi

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig path}"
: "${VCLUSTER_EXPECTED_SERVER:?set the expected vCluster API server URL}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${AIPERF_NODE_A:?set load-generator node A}"
: "${AIPERF_NODE_B:?set load-generator node B}"
: "${NIX_STORE_NFS_SERVER:?set the existing vCluster NFS server}"
: "${NIX_STORE_NFS_PATH:?set the existing vCluster NFS export}"
: "${TOKENIZER_STORE_BASENAME:?set the staged tokenizer store basename}"
: "${RESULT_DIR:?set a local result directory}"
envsubst_bin=${ENVSUBST_BIN:-envsubst}
command -v "$envsubst_bin" >/dev/null || { echo "envsubst is unavailable: $envsubst_bin" >&2; exit 2; }
test -f "$VCLUSTER_KUBECONFIG"
test "$AIPERF_NODE_A" != "$AIPERF_NODE_B"
test -d "$RESULT_DIR"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]; then
  echo "kubeconfig server $actual_server does not match expected vCluster server" >&2
  exit 2
fi

kubectl_vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
if [[ $record_export == 1 ]]; then
  job="nixpdr-${workload}-${arm}-${trial}"
elif [ "$arm" = pd-envoy-generic ]; then
  job="nixpde-${workload}-${arm}-${trial}"
elif [ "$arm" = pd-envoy-callouts ]; then
  job="nixpdc-${workload}-${arm}-${trial}"
elif [ "$arm" = pd-agw-generic ] && [ "$workload" = mooncake ] && [ "${BENCHMARK_DURATION:-45}" = 46 ]; then
  job="nixpdg-${workload}-${arm}-${trial}"
elif [ "$arm" = pd-agw-generic ]; then
  job="nixpd-${workload}-${arm}-${trial}"
else
  job="nixv2-${workload}-${arm}-${trial}"
fi
if "${kubectl_vc[@]}" get job "$job" >/dev/null 2>&1; then
  echo "refusing to reuse existing Job $job" >&2
  exit 2
fi
"${kubectl_vc[@]}" get deployment "$service" -o json |
  jq -e '(.spec.replicas == 1) and (.status.readyReplicas == 1)' >/dev/null || {
    echo "gateway deployment $service must be exactly 1/1 Ready" >&2
    exit 2
  }
if [ "$arm" = pd-agw-generic ] || [ "$arm" = pd-envoy-generic ] || [ "$arm" = pd-envoy-callouts ]; then
  components=(dynamo-pd-preprocessor:4 dynamo-pd-selector:4 dynamo-pd-prefill:4 dynamo-pd-decode:16)
else
  components=(dynamo-preprocessor:4 dynamo-selector:1 dynamo-benchmark-worker:16)
fi
for component in "${components[@]}"; do
  name=${component%:*}
  expected=${component#*:}
  "${kubectl_vc[@]}" get deployment "$name" -o json |
    jq -e --argjson expected "$expected" \
      '(.spec.replicas == $expected) and (.status.readyReplicas == $expected)' >/dev/null || {
        echo "deployment $name must be $expected/$expected Ready" >&2
        exit 2
      }
done
if [ "$arm" = dynamo-reference ]; then
  "${kubectl_vc[@]}" get deployment dynamo-reference-worker -o json |
    jq -e '(.spec.replicas == 4) and (.status.readyReplicas == 4)' >/dev/null || {
      echo "Dynamo reference workers must be 4/4 Ready" >&2
      exit 2
    }
fi

export VCLUSTER_NAMESPACE AIPERF_NODE_A AIPERF_NODE_B
export NIX_STORE_NFS_SERVER NIX_STORE_NFS_PATH
export BENCHMARK_START_UNIX=$(( $(date -u +%s) + 90 ))
if [ "$workload" = mooncake ]; then
  export BENCHMARK_DURATION=${BENCHMARK_DURATION:-45}
  [[ $BENCHMARK_DURATION == 45 ]] || {
    [[ ( $arm == pd-agw-generic || $arm == pd-envoy-generic || $arm == pd-envoy-callouts ) && $BENCHMARK_DURATION == 46 ]] || exit 2
  }
  export JOB_NAME=$job ARM_NAME=$arm
  export TARGET_URL="http://${service}:8080/v1/chat/completions"
  "$envsubst_bin" '${JOB_NAME} ${VCLUSTER_NAMESPACE} ${ARM_NAME} ${AIPERF_NODE_A} ${AIPERF_NODE_B} ${BENCHMARK_START_UNIX} ${TARGET_URL} ${BENCHMARK_DURATION} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}' \
    < "$(dirname "$0")/mooncake-job.yaml.tmpl" | "${kubectl_vc[@]}" apply -f -
else
  export RUN_NAME=$job ARM_SERVICE=$service DATASET=$dataset
  export TOKENIZER_STORE_BASENAME
  rendered=$("$envsubst_bin" '${RUN_NAME} ${VCLUSTER_NAMESPACE} ${ARM_SERVICE} ${DATASET} ${AIPERF_NODE_A} ${AIPERF_NODE_B} ${BENCHMARK_START_UNIX} ${TOKENIZER_STORE_BASENAME} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}' \
    < "$(dirname "$0")/frozen-raw-capacity-job.yaml.tmpl")
  if [[ $record_export == 1 ]]; then
    rendered=$(sed 's/--export-level summary/--export-level records --slice-duration 1/' <<<"$rendered")
    [[ $rendered == *'--export-level records --slice-duration 1'* ]] || exit 2
  fi
  printf '%s\n' "$rendered" | "${kubectl_vc[@]}" apply --dry-run=server -f - >/dev/null
  printf '%s\n' "$rendered" | "${kubectl_vc[@]}" apply -f -
fi

echo "waiting for $job (start barrier $BENCHMARK_START_UNIX)" >&2
"${kubectl_vc[@]}" wait --for=condition=complete "job/$job" --timeout=900s
out="$RESULT_DIR/raw_aiperf/$job"
mkdir -p "$out"
if [[ $record_export == 1 ]]; then
  "${kubectl_vc[@]}" exec -i dynamo-component-store-stager -- \
    sh -c "cd /shared/nix/aiperf/results/$job && tar -cf - ?/profile_export_aiperf.json ?/profile_export_aiperf.csv ?/profile_export_console.txt ?/profile_export.jsonl" |
    tar -C "$out" -xf -
else
  "${kubectl_vc[@]}" exec -i dynamo-component-store-stager -- \
    sh -c "cd /shared/nix/aiperf/results/$job && tar -cf - ?/profile_export_aiperf.json ?/profile_export_aiperf.csv ?/profile_export_console.txt" |
    tar -C "$out" -xf -
fi
jq -es --arg job "$job" '{job:$job,clients:length,rps:(map(.request_throughput.avg)|add),requests:(map(.request_count.avg)|add),errors:(map(.error_summary|map(.count)|add // 0)|add),cancelled:(map(.was_cancelled)|any)}' \
  "$out"/?/profile_export_aiperf.json
jq -es 'length == 6 and all(.[]; (.error_summary | length) == 0 and .was_cancelled == false)' \
  "$out"/?/profile_export_aiperf.json >/dev/null || {
    echo "benchmark $job completed but its six-client exports are not error-free" >&2
    exit 1
  }
