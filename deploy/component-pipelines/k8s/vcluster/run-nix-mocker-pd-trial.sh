#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Execute one frozen, six-client P/D replay and retain its vCluster execution
# identity even when the AIPerf export validation fails.
set -euo pipefail
[[ $# -ge 2 && $# -le 3 && $1 =~ ^(short|isl4000|mooncake)$ && $2 =~ ^r[1-9][0-9]*$ ]] || {
  echo "usage: $0 {short|isl4000|mooncake} rN [grace|envoy|callouts|static|generic-refresh|generic-metadata|generic-unified|generic-unified-rest|agw-records|envoy-records]" >&2
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
  export PD_RECORD_EXPORT=0
elif [[ ${3:-} == agw-records || ${3:-} == envoy-records ]]; then
  [[ $workload == isl4000 ]] || exit 2
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-isl-overlap
  plan_sha256=0f0644639ba99446fa1ee95fe07a166658da1e2735f312cd747b9b0be7a157e9
  job_prefix=nixpdr
  arm=pd-${3%-records}-generic
  export PD_RECORD_EXPORT=1
elif [[ ${3:-} == envoy ]]; then
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-envoy-generic
  plan_sha256=9d0c3b7fef51ff82173d97623016f8b99869a58e78e41cfa12ddf3bc2aff3d68
  job_prefix=nixpde
  arm=pd-envoy-generic
  export PD_RECORD_EXPORT=0
elif [[ ${3:-} == callouts ]]; then
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-envoy-callouts
  plan_sha256=6ffb6d54ad1ab37b8aee979b90e8f2e80a1c684fbc34b67567ee226c7a8ad001
  job_prefix=nixpdc
  arm=pd-envoy-callouts
  export PD_RECORD_EXPORT=0
elif [[ ${3:-} == static ]]; then
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-agw-static-logwarn
  plan_sha256=5688eba75b15413353f993fe222ad282e6271e44a679036df27077ae6d5eca3c
  job_prefix=nixpds
  arm=pd-agw-static
  export PD_RECORD_EXPORT=0
elif [[ ${3:-} == generic-refresh ]]; then
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-agw-generic-refresh
  plan_sha256=d02979978da7967bfd382b359a34029cfda4ae81368aae8ae5085a8934d9d7b3
  job_prefix=nixpd
  arm=pd-agw-generic
  export PD_RECORD_EXPORT=0
elif [[ ${3:-} == generic-metadata ]]; then
  [[ $workload == isl4000 ]] || exit 2
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-agw-generic-metadata
  plan_sha256=049b0d440ed5516c07e60dc1c5c39999704e2731a0bcfb5665c55425bce8900a
  job_prefix=nixpd
  arm=pd-agw-generic
  export PD_RECORD_EXPORT=0
elif [[ ${3:-} == generic-unified ]]; then
  [[ $workload == isl4000 ]] || exit 2
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-agw-generic-unified
  plan_sha256=0bcc9e931bb183fd12db3e7ddd32bc437c3945dba14a7304af9d4d404771b1cc
  job_prefix=nixpd
  arm=pd-agw-generic
  export PD_RECORD_EXPORT=0
elif [[ ${3:-} == generic-unified-rest ]]; then
  [[ $workload == short || $workload == mooncake ]] || exit 2
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-agw-generic-unified-rest
  plan_sha256=9c09dd9ff683f874c7c27099ccfead3709ff9dd66c5929751c21951201b428d0
  if [[ $workload == mooncake ]]; then job_prefix=nixpdg; else job_prefix=nixpd; fi
  arm=pd-agw-generic
  export PD_RECORD_EXPORT=0
elif [[ $# == 2 ]]; then
  export RESULT_DIR=$script_dir/results/2026-09-26-mocker-pd-generic
  export BENCHMARK_DURATION=45
  plan_sha256=36e98edcd8ae9007724870ae3072e46d45b0037a53e21485c1b0f695a2b6dccc
  job_prefix=nixpd
  arm=pd-agw-generic
  export PD_RECORD_EXPORT=0
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
  '.benchmark_series_id // .benchmark_series_id_by_workload[$workload]' "$plan")
dataset_path=$(jq -er --arg workload "$workload" \
  '.workload.path // .workloads[$workload].path' "$plan")
dataset_sha256=$(jq -er --arg workload "$workload" \
  '.workload.sha256 // .workloads[$workload].sha256' "$plan")
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
: "${NIX_STORE_NFS_SERVER:?}"
: "${NIX_STORE_NFS_PATH:?}"
envsubst_bin=${ENVSUBST_BIN:-envsubst}
command -v "$envsubst_bin" >/dev/null || {
  echo "envsubst is unavailable: $envsubst_bin" >&2
  exit 2
}
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
for inactive in agw-static agw-generic envoy-independent envoy-callouts \
  dynamo-frontend-reference real-vllm-pd-agw; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null
done
for gateway in dynamo-pd-agw-static dynamo-pd-agw-generic dynamo-pd-envoy-generic dynamo-pd-envoy-callouts; do
  [[ $gateway == dynamo-${arm} ]] && continue
  inactive_json=$("${vc[@]}" get deployment "$gateway" --ignore-not-found -o json)
  if [[ -n $inactive_json ]]; then
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' \
      <<<"$inactive_json" >/dev/null
  fi
done
if [[ $arm == pd-agw-static ]]; then
  expected_binary=$(jq -r '.candidate.agentgateway_nix_output + "/bin/agentgateway"' "$plan")
  expected_linger=$(jq -r '.candidate.preprocess_batch_linger_us // 200 | tostring' "$plan")
  expected_threads=$(jq -r '.candidate.gateway_worker_threads | tostring' "$plan")
  expected_timing=$(jq -r '.candidate.stage_timing_every // 0 | tostring' "$plan")
  expected_rust_log=$(jq -r '.candidate.rust_log // ""' "$plan")
  "${vc[@]}" get deployment dynamo-pd-agw-static -o json |
    jq -e --arg binary "$expected_binary" --arg linger "$expected_linger" --arg timing "$expected_timing" --arg rust_log "$expected_rust_log" '
      .spec.template.spec.containers[0].command[0] == $binary
      and any(.spec.template.spec.containers[0].env[];
        .name == "DYN_PREFILL_ENDPOINT" and .value == "http://dynamo-pd-prefill:50051")
      and any(.spec.template.spec.containers[0].env[];
        .name == "DYN_PREPROCESS_BATCH_LINGER_US" and .value == $linger)
      and ($timing == "0" or any(.spec.template.spec.containers[0].env[];
        .name == "DYN_STATIC_STAGE_TIMING_EVERY" and .value == $timing))
      and ($rust_log == "" or any(.spec.template.spec.containers[0].env[];
        .name == "RUST_LOG" and .value == $rust_log))
    ' >/dev/null
  "${vc[@]}" get configmap dynamo-pd-agw-static -o json |
    jq -e --arg threads "$expected_threads" '
      .data["config.yaml"] | contains("workerThreads: " + $threads)
    ' >/dev/null
fi
if [[ $arm == pd-envoy-callouts ]]; then
  pod_ips=$("${vc[@]}" get pods -l app=dynamo-pd-decode -o json |
    jq -r '.items[] | select(.metadata.deletionTimestamp == null) |
      select(.status.phase == "Running") |
      select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
      .status.podIP' | sort)
  cluster_ips=$("${vc[@]}" get configmap dynamo-pd-envoy-callouts -o json |
    jq -r '.data["envoy.yaml"] | scan("(?m)^    - name: \"([0-9.]+)\"$") | .[0]' | sort)
  [[ $(wc -l <<<"$pod_ips") == 16 && $pod_ips == "$cluster_ips" ]] || {
    echo "Envoy callout clusters do not match 16 Ready decode Pod IPs; refresh fixture" >&2
    exit 2
  }
fi
export AIPERF_NODE_A AIPERF_NODE_B
AIPERF_NODE_A=$(jq -er '.execution.aiperf_nodes[0]' "$plan")
AIPERF_NODE_B=$(jq -er '.execution.aiperf_nodes[1]' "$plan")
export TOKENIZER_STORE_BASENAME=wjq1b3wfjpzak4yd4rmj9arwqk1gkiir-qwen-tokenizer
job=${job_prefix}-${workload}-${arm}-${trial}
actual_dataset_sha256=$("${vc[@]}" exec "$stager_pod" -- \
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

log_pid=
cleanup_log_follow() {
  if [[ -n $log_pid ]]; then
    kill "$log_pid" 2>/dev/null || true
    wait "$log_pid" 2>/dev/null || true
    log_pid=
  fi
}
trap cleanup_log_follow EXIT
if [[ $arm == pd-agw-static && $expected_timing != 0 ]]; then
  gateway_pod=$("${vc[@]}" get pods -l app=dynamo-pd-agw-static -o json |
    jq -er '[.items[] | select(.metadata.deletionTimestamp == null) |
      select(.status.phase == "Running") |
      select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) |
      .metadata.name] | if length == 1 then .[0] else error("expected one Ready static gateway") end')
  "${vc[@]}" logs "$gateway_pod" --follow --since=1s \
    > "$RESULT_DIR/raw-gateway-$job.log" \
    2> "$RESULT_DIR/raw-gateway-$job.stderr" &
  log_pid=$!
  sleep 1
  kill -0 "$log_pid"
fi
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
cleanup_log_follow
if [[ $arm == pd-agw-static && $expected_timing != 0 ]]; then
  rg '^static_stage_us prepare=[0-9]+ prefill=[0-9]+ select=[0-9]+ decode=[0-9]+ total=[0-9]+$' \
    "$RESULT_DIR/raw-gateway-$job.log" > "$RESULT_DIR/stage-timing-$job.log" || true
fi
jq -n --argjson job "$job_json" --argjson pods "$pods_json" \
  --arg status "$status" --arg server "$actual_server" \
  --arg plan "$plan" --arg hash "$plan_sha256" \
  --arg series "$series_id" --arg workload "$workload" \
  --arg question "$(jq -r '.question' "$plan")" '
  {status:$status,vcluster_api_server:$server,
   plan_path:$plan,plan_sha256:$hash,benchmark_series_id:$series,
   performance_question:$question,
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
if [[ $arm == pd-agw-static && $expected_timing != 0 ]]; then
  [[ $(wc -l < "$RESULT_DIR/stage-timing-$job.log") -ge 20 ]] || {
    echo "fewer than 20 static stage timing samples" >&2
    exit 1
  }
fi
[[ $status == completed ]] || exit 1
echo "P/D $workload AIPerf Job and execution evidence complete: $job"
