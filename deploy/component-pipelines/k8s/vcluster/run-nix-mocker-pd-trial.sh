#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Execute one frozen, six-client P/D replay and retain its vCluster execution
# identity even when the AIPerf export validation fails.
set -euo pipefail
[[ $# -ge 2 && $# -le 3 && $1 =~ ^(short|isl4000|mooncake)$ && $2 =~ ^r[1-9][0-9]*$ ]] || {
  echo "usage: $0 {short|isl4000|mooncake} rN [grace|envoy]" >&2
  exit 2
}
workload=$1
trial=$2
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(git rev-parse --show-toplevel)
if [[ ${3:-} == grace && $workload == mooncake ]]; then
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-mooncake-grace
  export BENCHMARK_DURATION=46
  plan_sha256=66375e549497c63ee944eca1c499f959371e339eae78b8fa5762067d9ced458c
  job_prefix=nixpdg
  arm=pd-agw-generic
elif [[ ${3:-} == envoy ]]; then
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-envoy-generic
  plan_sha256=9d0c3b7fef51ff82173d97623016f8b99869a58e78e41cfa12ddf3bc2aff3d68
  job_prefix=nixpde
  arm=pd-envoy-generic
elif [[ $# == 2 ]]; then
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-generic
  export BENCHMARK_DURATION=45
  plan_sha256=36e98edcd8ae9007724870ae3072e46d45b0037a53e21485c1b0f695a2b6dccc
  job_prefix=nixpd
  arm=pd-agw-generic
else
  echo "grace is supported only for Mooncake" >&2
  exit 2
fi
plan=$RESULT_DIR/benchmark_plan.json
echo "$plan_sha256  $plan" | sha256sum --check --status
export BENCHMARK_DURATION
BENCHMARK_DURATION=$(jq -er --arg workload "$workload" \
  '.workloads[$workload].benchmark_duration_seconds // .execution.benchmark_duration_seconds' "$plan")
series_id=$(jq -er --arg workload "$workload" \
  '.benchmark_series_id_by_workload[$workload]' "$plan")
dataset_path=$(jq -er --arg workload "$workload" '.workloads[$workload].path' "$plan")
dataset_sha256=$(jq -er --arg workload "$workload" '.workloads[$workload].sha256' "$plan")
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
for inactive in agw-static agw-generic envoy-independent envoy-callouts \
  dynamo-frontend-reference real-vllm-pd-agw; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null
done
if [[ $arm == pd-envoy-generic ]]; then
  inactive_pd=dynamo-pd-agw-generic
else
  inactive_pd=dynamo-pd-envoy-generic
fi
"${vc[@]}" get deployment "$inactive_pd" -o json |
  jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null
export AIPERF_NODE_A AIPERF_NODE_B
AIPERF_NODE_A=$(jq -er '.execution.aiperf_nodes[0]' "$plan")
AIPERF_NODE_B=$(jq -er '.execution.aiperf_nodes[1]' "$plan")
export TOKENIZER_STORE_BASENAME=wjq1b3wfjpzak4yd4rmj9arwqk1gkiir-qwen-tokenizer
job=${job_prefix}-${workload}-${arm}-${trial}
actual_dataset_sha256=$("${vc[@]}" exec dynamo-component-store-stager -- \
  sha256sum "/shared/nix${dataset_path#/shared}" | awk '{print $1}')
[[ $actual_dataset_sha256 == "$dataset_sha256" ]] || {
  echo "dataset SHA-256 does not match frozen plan: $workload" >&2
  exit 2
}
printf '%s\t%s\t%s\n' "$workload" "$dataset_path" "$actual_dataset_sha256" \
  > "$RESULT_DIR/dataset-sha-$job.tsv"
"${vc[@]}" get pods -A -o json |
  jq --arg a "$AIPERF_NODE_A" --arg b "$AIPERF_NODE_B" '
    {captured_at:(now|todateiso8601),client_nodes:[$a,$b],
     pods:[.items[] | select(.spec.nodeName == $a or .spec.nodeName == $b) |
       {namespace:.metadata.namespace,name:.metadata.name,node:.spec.nodeName,
        phase:.status.phase}]}
  ' > "$RESULT_DIR/occupancy-before-$job.json"

status=completed
if ! bash "$script_dir/run-nix-mocker-trial.sh" "$arm" "$workload" "$trial"; then
  status=failed
fi
"${vc[@]}" get pods -A -o json |
  jq --arg a "$AIPERF_NODE_A" --arg b "$AIPERF_NODE_B" '
    {captured_at:(now|todateiso8601),client_nodes:[$a,$b],
     pods:[.items[] | select(.spec.nodeName == $a or .spec.nodeName == $b) |
       {namespace:.metadata.namespace,name:.metadata.name,node:.spec.nodeName,
        phase:.status.phase}]}
  ' > "$RESULT_DIR/occupancy-after-$job.json"
job_json=$("${vc[@]}" get job "$job" -o json)
pods_json=$("${vc[@]}" get pods -l "job-name=$job" -o json)
jq -n --argjson job "$job_json" --argjson pods "$pods_json" \
  --arg status "$status" --arg server "$actual_server" \
  --arg plan "$plan" --arg hash "$plan_sha256" \
  --arg series "$series_id" --arg workload "$workload" '
  {status:$status,vcluster_api_server:$server,
   plan_path:$plan,plan_sha256:$hash,benchmark_series_id:$series,
   performance_question:"P/D generic gateway host characterization on frozen reference traffic",
   workload:$workload,
   job:{name:$job.metadata.name,uid:$job.metadata.uid,
     created_at:$job.metadata.creationTimestamp,
     completed_at:$job.status.completionTime,
     succeeded:($job.status.succeeded//0),failed:($job.status.failed//0),
     spec:$job.spec},
   pods:[$pods.items[] | {name:.metadata.name,node:.spec.nodeName,
     phase:.status.phase,started_at:.status.startTime,
     finished_at:.status.containerStatuses[0].state.terminated.finishedAt}]}
' > "$RESULT_DIR/execution-$job.json"
if [[ $workload == mooncake ]]; then
  cache_file=$RESULT_DIR/cache-$job.tsv
  : > "$cache_file"
  while IFS= read -r pod; do
    hit=$("${vc[@]}" logs "$pod" | rg -F 'Memory-mapped dataset cache HIT') || {
      echo "mmap cache HIT not proven for $pod" >&2
      exit 1
    }
    [[ $hit == *'skipping tokenizer + composer'* ]] || exit 1
    printf '%s\t%s\n' "$pod" "$hit" >> "$cache_file"
  done < <(jq -r '.pods[].name' "$RESULT_DIR/execution-$job.json")
  [[ $(wc -l < "$cache_file") == 6 ]] || exit 1
fi
[[ $status == completed ]] || exit 1
echo "P/D $workload AIPerf Job and execution evidence complete: $job"
