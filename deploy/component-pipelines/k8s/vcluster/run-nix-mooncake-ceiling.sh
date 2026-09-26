#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Reproduce the clean 12-client Mooncake capacity point on one Nix-built
# Envoy-with-callouts gateway. The six-client parity trace is offered-rate
# limited; this is a separate series with twice the aggregate arrivals.
set -euo pipefail
shopt -s nullglob

reader_mode=original
placement_mode=two
if [ "$#" -eq 1 ] && [[ "$1" =~ ^r[1-9][0-9]*$ ]]; then
  clients=12
  trial=$1
  job="ceilv2-mooncake-envoy-callouts-c12-${trial}"
  plan_sha256=0d88eea5e51bb20ebc4dc62d6aa09e7b0dd75e122c344ffd5b52ca19d6948ad9
elif [ "$#" -eq 3 ] && [ "$1" = fixed ] && [[ "$2" =~ ^(12|18|24)$ ]] && [[ "$3" =~ ^r[1-9][0-9]*$ ]]; then
  reader_mode=fixed
  clients=$2
  trial=$3
  job="ceilfix-mooncake-envoy-callouts-c${clients}-${trial}"
  plan_sha256=787bb593a7841345c4ccdc25405ce91454ccddc7ddaef402deee28383ce06a7e
elif [ "$#" -eq 3 ] && [ "$1" = fixed3 ] && [ "$2" = 24 ] && [[ "$3" =~ ^r[1-9][0-9]*$ ]]; then
  reader_mode=fixed
  placement_mode=three
  clients=24
  trial=$3
  job="ceilplace-mooncake-envoy-callouts-c24-${trial}"
  plan_sha256=58ad8caa1175ff8458e023f3786393129c8c2fe7eb7ba7099b064ae86d2dca53
else
  echo "usage: $0 rN | fixed {12|18|24} rN | fixed3 24 rN" >&2
  exit 2
fi
bundle=/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699
trace_sha256=28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550
patch_sha256=f03b650796bb1de5f4ed3dea2bd65545a102e9d0fd089e3e436b574cec38e418
patch_file="$(dirname "$0")/bench_patches/sitecustomize.py"
patch_configmap=aiperf-mmap-slice-f03b6507

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${AIPERF_NODE_A:?set load-generator node A}"
: "${AIPERF_NODE_B:?set load-generator node B}"
client_nodes=("$AIPERF_NODE_A" "$AIPERF_NODE_B")
if [ "$placement_mode" = three ]; then
  : "${AIPERF_NODE_C:?set load-generator node C}"
  client_nodes+=("$AIPERF_NODE_C")
fi
: "${NIX_STORE_NFS_SERVER:?set the existing vCluster NFS server}"
: "${NIX_STORE_NFS_PATH:?set the existing vCluster NFS export}"
: "${RESULT_DIR:?set a new, existing result directory}"
envsubst_bin=${ENVSUBST_BIN:-envsubst}
command -v "$envsubst_bin" >/dev/null || {
  echo "envsubst is unavailable: $envsubst_bin" >&2
  exit 2
}
test -d "$RESULT_DIR"
if [ "$reader_mode" = fixed ]; then
  test "$(sha256sum "$patch_file" | cut -d' ' -f1)" = "$patch_sha256" || {
    echo "AIPerf mmap overlay differs from frozen plan" >&2
    exit 2
  }
fi
test "$(printf '%s\n' "${client_nodes[@]}" | sort -u | wc -l)" -eq "${#client_nodes[@]}" || {
  echo "load-generator nodes must be distinct" >&2
  exit 2
}
test "$(sha256sum "$RESULT_DIR/benchmark_plan.json" | cut -d' ' -f1)" = "$plan_sha256" || {
  echo "benchmark plan identity changed; start a new series" >&2
  exit 2
}
if [ "$placement_mode" = three ]; then
  jq -e --arg a "$AIPERF_NODE_A" --arg b "$AIPERF_NODE_B" --arg c "$AIPERF_NODE_C" '
    (.placement.nodes | sort) == ([$a,$b,$c] | sort)
  ' "$RESULT_DIR/benchmark_plan.json" >/dev/null || {
    echo "three client nodes differ from frozen placement plan" >&2
    exit 2
  }
fi
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

for node in "${client_nodes[@]}"; do
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
if [ "$reader_mode" = fixed ]; then
  if ! "${vc[@]}" get configmap "$patch_configmap" >/dev/null 2>&1; then
    "${vc[@]}" create configmap "$patch_configmap" --from-file="sitecustomize.py=$patch_file"
  fi
  cm_sha=$("${vc[@]}" get configmap "$patch_configmap" -o json |
    jq -j '.data["sitecustomize.py"]' | sha256sum | cut -d' ' -f1)
  test "$cm_sha" = "$patch_sha256" || {
    echo "vCluster AIPerf mmap ConfigMap differs from frozen patch" >&2
    exit 2
  }
fi
if [ "$placement_mode" = three ]; then
  kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" get pods -A -o json |
    jq --arg a "$AIPERF_NODE_A" --arg b "$AIPERF_NODE_B" --arg c "$AIPERF_NODE_C" '
      {captured_at:now|todateiso8601,
       pods:[.items[] | select(.status.phase == "Running") |
         select(.spec.nodeName == $a or .spec.nodeName == $b or .spec.nodeName == $c) |
         {namespace:.metadata.namespace,name:.metadata.name,node:.spec.nodeName,phase:.status.phase}]}
    ' > "$RESULT_DIR/occupancy-before-$job.json"
fi

export JOB_NAME=$job ARM_NAME=envoy-callouts
export TARGET_URL=http://envoy-callouts:8080/v1/chat/completions
export VCLUSTER_NAMESPACE AIPERF_NODE_A AIPERF_NODE_B
export NIX_STORE_NFS_SERVER NIX_STORE_NFS_PATH
export BENCHMARK_START_UNIX=$(( $(date -u +%s) + 120 ))
"$envsubst_bin" '${JOB_NAME} ${VCLUSTER_NAMESPACE} ${ARM_NAME} ${AIPERF_NODE_A} ${AIPERF_NODE_B} ${BENCHMARK_START_UNIX} ${TARGET_URL} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}' \
  < "$(dirname "$0")/mooncake-job.yaml.tmpl" |
  "${vc[@]}" create --dry-run=client -f - -o json |
  jq --argjson clients "$clients" --arg reader_mode "$reader_mode" --arg placement_mode "$placement_mode" \
    --arg c "${AIPERF_NODE_C:-}" --arg patch_configmap "$patch_configmap" '
    .spec.completions = $clients |
    .spec.parallelism = $clients |
    .spec.template.metadata.labels.benchmark = (if $placement_mode == "three" then "mooncake-placement" else "mooncake-capacity" end) |
    .spec.template.spec.topologySpreadConstraints[0].labelSelector.matchLabels.benchmark = .spec.template.metadata.labels.benchmark |
    .spec.template.spec.containers[0].args[0] |= gsub("--record-processors 16"; "--record-processors 1") |
    if $placement_mode == "three" then
      .spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].values += [$c]
    else . end |
    if $reader_mode == "fixed" then
      .spec.template.spec.containers[0].env += [{"name":"PYTHONPATH","value":"/opt/aiperf-mmap-patch"}] |
      .spec.template.spec.containers[0].volumeMounts += [{"name":"aiperf-mmap-patch","mountPath":"/opt/aiperf-mmap-patch","readOnly":true}] |
      .spec.template.spec.volumes += [{"name":"aiperf-mmap-patch","configMap":{"name":$patch_configmap}}] |
      .spec.template.spec.containers[0].args[0] |= sub("exec aiperf profile";
        "python3 -c \u0027from aiperf.dataset.memory_map_utils import MemoryMapDatasetClient; assert MemoryMapDatasetClient.get_conversation.__module__ == \"sitecustomize\"\u0027\nexec aiperf profile")
    else . end
  ' | "${vc[@]}" apply -f -

echo "waiting for $job ($clients clients, barrier $BENCHMARK_START_UNIX)" >&2
"${vc[@]}" wait --for=condition=complete "job/$job" --timeout=900s
if [ "$placement_mode" = three ]; then
  kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" get pods -A -o json |
    jq --arg a "$AIPERF_NODE_A" --arg b "$AIPERF_NODE_B" --arg c "$AIPERF_NODE_C" '
      {captured_at:now|todateiso8601,
       pods:[.items[] | select(.status.phase == "Running") |
         select(.spec.nodeName == $a or .spec.nodeName == $b or .spec.nodeName == $c) |
         {namespace:.metadata.namespace,name:.metadata.name,node:.spec.nodeName,phase:.status.phase}]}
    ' > "$RESULT_DIR/occupancy-after-$job.json"
fi
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
jq -e --argjson clients "$clients" --arg a "$AIPERF_NODE_A" --arg b "$AIPERF_NODE_B" \
  --arg c "${AIPERF_NODE_C:-}" --argjson node_count "${#client_nodes[@]}" '
  .job.succeeded == $clients and .job.failed == 0 and (.pods|length) == $clients and
  ([.pods[].node] | group_by(.) | map(length) | all(. == ($clients/$node_count))) and
  ([.pods[].node] | unique | sort) == ((if $node_count == 3 then [$a,$b,$c] else [$a,$b] end) | sort)
' "$RESULT_DIR/execution-$job.json" >/dev/null

summaries=("$out"/*/profile_export_aiperf.json)
test "${#summaries[@]}" -eq "$clients"
jq -es --arg job "$job" --argjson clients "$clients" '
  def start_seconds:
    . as $timestamp |
    (($timestamp | split(".")[0] + "Z" | fromdateiso8601) +
     ("0." + ($timestamp | split(".")[1]) | tonumber));
  {
    job:$job,clients:length,version:(map(.aiperf_version)|unique),
    phase_types:(map(.input_config.phases[0].type)|unique),
    requests_scheduled:(map(.input_config.phases[0].requests)|add),
    requests_successful:(map(.request_count.avg)|add),
    rps:(map(.request_throughput.avg)|add),
    rps_semantics:"sum of per-client rates measured over potentially different windows",
    output_tps:(map(.output_token_throughput.avg)|add),
    errors:(map(.error_summary|map(.count)|add // 0)|add),
    cancelled:(map(.was_cancelled)|any),
    replay_degraded_clients:(map(select(.replay_sched_degraded.avg == 1))|length),
    replay_lag_p99_ms_max:(map(.replay_sched_lag_p99.avg)|max),
    measured_start_min:(map(.start_time)|min),
    measured_start_max:(map(.start_time)|max),
    measured_start_spread_seconds:((map(.start_time|start_seconds)|max)-(map(.start_time|start_seconds)|min))
  }
' "${summaries[@]}" > "$RESULT_DIR/summary-$job.json"
bash "$(dirname "$0")/verify-nix-mooncake-cache.sh" "$job"
jq . "$RESULT_DIR/summary-$job.json"
jq -e --argjson clients "$clients" '
  .clients == $clients and .version == ["0.12.0"] and
  .phase_types == ["fixed_schedule"] and .errors == 0 and .cancelled == false and
  .replay_degraded_clients == 0 and .measured_start_spread_seconds <= 3
' "$RESULT_DIR/summary-$job.json" >/dev/null || {
  echo "benchmark artifacts retained, but replay or phase synchronization failed the capacity gate" >&2
  exit 1
}
