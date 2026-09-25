#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Reproduce the clean 12-client Mooncake capacity point on one Nix-built
# Envoy-with-callouts gateway. The six-client parity trace is offered-rate
# limited; this is a separate series with twice the aggregate arrivals.
set -euo pipefail
shopt -s nullglob

if [ "$#" -ne 1 ] || ! [[ "$1" =~ ^r[1-9][0-9]*$ ]]; then
  echo "usage: $0 rN" >&2
  exit 2
fi
trial=$1
job="ceilv2-mooncake-envoy-callouts-c12-${trial}"
bundle=/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699
plan_sha256=0d88eea5e51bb20ebc4dc62d6aa09e7b0dd75e122c344ffd5b52ca19d6948ad9
trace_sha256=28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550
clients=12

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${AIPERF_NODE_A:?set load-generator node A}"
: "${AIPERF_NODE_B:?set load-generator node B}"
: "${NIX_STORE_NFS_SERVER:?set the existing vCluster NFS server}"
: "${NIX_STORE_NFS_PATH:?set the existing vCluster NFS export}"
: "${RESULT_DIR:?set a new, existing result directory}"
envsubst_bin=${ENVSUBST_BIN:-envsubst}
command -v "$envsubst_bin" >/dev/null || {
  echo "envsubst is unavailable: $envsubst_bin" >&2
  exit 2
}
test -d "$RESULT_DIR"
test "$AIPERF_NODE_A" != "$AIPERF_NODE_B"
test "$(sha256sum "$RESULT_DIR/benchmark_plan.json" | cut -d' ' -f1)" = "$plan_sha256" || {
  echo "benchmark plan identity changed; start a new series" >&2
  exit 2
}
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
test "$actual_server" = "$VCLUSTER_EXPECTED_SERVER" || {
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
}
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
test ! -e "$RESULT_DIR/raw_aiperf/$job" || {
  echo "refusing to overwrite existing raw result $job" >&2
  exit 2
}
"${vc[@]}" get job "$job" >/dev/null 2>&1 && {
  echo "refusing to reuse existing vCluster Job $job" >&2
  exit 2
}
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
    echo "another benchmark Job is active" >&2
    exit 2
  }

for node in "$AIPERF_NODE_A" "$AIPERF_NODE_B"; do
  kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" get node "$node" -o json |
    jq -e '(.metadata.labels["topology.unikorn-cloud.org/node-pool"] == "cpu-pool") and
      ((.spec.unschedulable // false) == false) and
      any(.status.conditions[]; .type == "Ready" and .status == "True")' >/dev/null || {
      echo "load-generator node is not Ready: $node" >&2
      exit 2
    }
done
"${vc[@]}" get deployment envoy-callouts -o json |
  jq -e --arg bundle "$bundle" '
    .spec.replicas == 1 and .status.readyReplicas == 1 and
    .spec.template.spec.containers[0].command[0] == ($bundle + "/bin/envoy-static") and
    .spec.template.spec.containers[0].args[3] == "20" and
    any(.spec.template.spec.containers[0].env[]?;
      .name == "GENERIC_PIPELINE_THREADS" and .value == "20")
  ' >/dev/null || {
    echo "Envoy callouts must run the exact bundle at 20+20 threads, 1/1 Ready" >&2
    exit 2
  }
for component in dynamo-preprocessor:8 dynamo-selector:8 dynamo-benchmark-worker:32; do
  name=${component%:*}
  expected=${component#*:}
  "${vc[@]}" get deployment "$name" -o json |
    jq -e --argjson expected "$expected" --arg bundle "$bundle" '
      .spec.replicas == $expected and .status.readyReplicas == $expected and
      .spec.template.spec.containers[0].command[0] == ($bundle + "/bin/dynamo-component-facade")
    ' >/dev/null || {
      echo "$name must be $expected/$expected Ready from the exact bundle" >&2
      exit 2
    }
done
for inactive in agw-static agw-generic envoy-independent dynamo-frontend-reference \
  real-qwen3-agw-generic real-qwen3-preprocessor real-qwen3-selector \
  real-sglang real-sglang-split real-vllm-split real-vllm-agw-generic \
  real-vllm-pd-prefill real-vllm-pd-decode real-vllm-pd-preprocessor \
  real-vllm-pd-selector real-vllm-pd-agw; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null || {
      echo "$inactive is active; refusing confounded benchmark" >&2
      exit 2
    }
done
actual_trace_sha=$("${vc[@]}" exec dynamo-component-store-stager -- \
  sha256sum /shared/nix/aiperf/mooncake-qwen2.5-0.5b/mooncake-512-context.jsonl | cut -d' ' -f1)
test "$actual_trace_sha" = "$trace_sha256" || {
  echo "Mooncake trace hash differs from frozen plan" >&2
  exit 2
}

export JOB_NAME=$job ARM_NAME=envoy-callouts
export TARGET_URL=http://envoy-callouts:8080/v1/chat/completions
export VCLUSTER_NAMESPACE AIPERF_NODE_A AIPERF_NODE_B
export NIX_STORE_NFS_SERVER NIX_STORE_NFS_PATH
export BENCHMARK_START_UNIX=$(( $(date -u +%s) + 120 ))
"$envsubst_bin" '${JOB_NAME} ${VCLUSTER_NAMESPACE} ${ARM_NAME} ${AIPERF_NODE_A} ${AIPERF_NODE_B} ${BENCHMARK_START_UNIX} ${TARGET_URL} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}' \
  < "$(dirname "$0")/mooncake-job.yaml.tmpl" |
  "${vc[@]}" create --dry-run=client -f - -o json |
  jq --argjson clients "$clients" '
    .spec.completions = $clients |
    .spec.parallelism = $clients |
    .spec.template.metadata.labels.benchmark = "mooncake-capacity" |
    .spec.template.spec.topologySpreadConstraints[0].labelSelector.matchLabels.benchmark = "mooncake-capacity" |
    .spec.template.spec.containers[0].args[0] |= gsub("--record-processors 16"; "--record-processors 1")
  ' | "${vc[@]}" apply -f -

echo "waiting for $job ($clients clients, barrier $BENCHMARK_START_UNIX)" >&2
"${vc[@]}" wait --for=condition=complete "job/$job" --timeout=900s
out="$RESULT_DIR/raw_aiperf/$job"
mkdir -p "$out"
"${vc[@]}" exec -i dynamo-component-store-stager -- \
  sh -c "cd /shared/nix/aiperf/results/$job && tar -cf - ?/profile_export_aiperf.json ?/profile_export_aiperf.csv ?/profile_export_console.txt ??/profile_export_aiperf.json ??/profile_export_aiperf.csv ??/profile_export_console.txt" |
  tar -C "$out" -xf -

"${vc[@]}" get jobs,pods -o json |
  jq --arg job "$job" --arg server "$actual_server" '
    .items as $items | {
      vcluster_api_server: $server,
      job: ($items[] | select(.kind == "Job" and .metadata.name == $job) |
        {name:.metadata.name,uid:.metadata.uid,created_at:.metadata.creationTimestamp,
         completed_at:.status.completionTime,succeeded:(.status.succeeded//0),
         failed:(.status.failed//0),spec:.spec.template.spec,
         completions:.spec.completions,parallelism:.spec.parallelism}),
      pods: [$items[] | select(.kind == "Pod" and .metadata.labels["job-name"] == $job) |
        {name:.metadata.name,node:.spec.nodeName,phase:.status.phase,
         started_at:.status.startTime,
         finished_at:.status.containerStatuses[0].state.terminated.finishedAt}]
    }
  ' > "$RESULT_DIR/execution-$job.json"
jq -e --argjson clients "$clients" --arg a "$AIPERF_NODE_A" --arg b "$AIPERF_NODE_B" '
  .job.succeeded == $clients and .job.failed == 0 and (.pods|length) == $clients and
  ([.pods[].node] | group_by(.) | map(length) | sort) == [($clients/2),($clients/2)] and
  ([.pods[].node] | unique | sort) == ([$a,$b] | sort)
' "$RESULT_DIR/execution-$job.json" >/dev/null

summaries=("$out"/*/profile_export_aiperf.json)
test "${#summaries[@]}" -eq "$clients"
jq -es --arg job "$job" --argjson clients "$clients" '
  {
    job:$job,clients:length,version:(map(.aiperf_version)|unique),
    phase_types:(map(.input_config.phases[0].type)|unique),
    requests_scheduled:(map(.input_config.phases[0].requests)|add),
    requests_successful:(map(.request_count.avg)|add),
    rps:(map(.request_throughput.avg)|add),
    output_tps:(map(.output_token_throughput.avg)|add),
    errors:(map(.error_summary|map(.count)|add // 0)|add),
    cancelled:(map(.was_cancelled)|any)
  }
' "${summaries[@]}" > "$RESULT_DIR/summary-$job.json"
bash "$(dirname "$0")/verify-nix-mooncake-cache.sh" "$job"
jq -e --argjson clients "$clients" '
  .clients == $clients and .version == ["0.12.0"] and
  .phase_types == ["fixed_schedule"] and .errors == 0 and .cancelled == false
' "$RESULT_DIR/summary-$job.json" >/dev/null

jq . "$RESULT_DIR/summary-$job.json"
