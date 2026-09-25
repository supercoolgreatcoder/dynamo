#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Scale AIPerf clients without changing the frozen short request bodies or the
# single Envoy-direct gateway. Keep this series distinct from the six-client
# parity matrix: offered load intentionally changes.
set -euo pipefail
shopt -s nullglob

if [ "$#" -ne 2 ] || ! [[ "$1" =~ ^[0-9]+$ ]] || ! [[ "$2" =~ ^r[1-9][0-9]*$ ]]; then
  echo "usage: $0 CLIENTS rN (for example: 12 r1)" >&2
  exit 2
fi
clients=$1
trial=$2
if [ "$clients" -lt 6 ] || [ "$clients" -gt 24 ] || [ $((clients % 3)) -ne 0 ]; then
  echo "CLIENTS must be a multiple of 3 between 6 and 24" >&2
  exit 2
fi

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig path}"
: "${VCLUSTER_EXPECTED_SERVER:?set the expected vCluster API server URL}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${AIPERF_NODE_A:?set dedicated load-generator node A}"
: "${AIPERF_NODE_B:?set dedicated load-generator node B}"
: "${AIPERF_NODE_C:?set dedicated load-generator node C}"
: "${NIX_STORE_NFS_SERVER:?set the existing vCluster NFS server}"
: "${NIX_STORE_NFS_PATH:?set the existing vCluster NFS export}"
: "${TOKENIZER_STORE_BASENAME:?set the staged tokenizer basename}"
: "${RESULT_DIR:?set the local result directory}"
envsubst_bin=${ENVSUBST_BIN:-envsubst}
command -v "$envsubst_bin" >/dev/null
test -f "$VCLUSTER_KUBECONFIG"
test -d "$RESULT_DIR"
test "$AIPERF_NODE_A" != "$AIPERF_NODE_B"
test "$AIPERF_NODE_A" != "$AIPERF_NODE_C"
test "$AIPERF_NODE_B" != "$AIPERF_NODE_C"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]; then
  echo "kubeconfig server $actual_server does not match expected vCluster server" >&2
  exit 2
fi
kubectl_vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
for node in "$AIPERF_NODE_A" "$AIPERF_NODE_B" "$AIPERF_NODE_C"; do
  kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" get node "$node" -o json |
    jq -e '
      (.metadata.labels["topology.unikorn-cloud.org/node-pool"] == "cpu-pool") and
      ((.spec.unschedulable // false) == false) and
      ((.spec.taints // []) | all(.[]; .effect != "NoSchedule")) and
      any(.status.conditions[]; .type == "Ready" and .status == "True")
    ' >/dev/null || {
      echo "load-generator node $node is not schedulable and Ready" >&2
      exit 2
    }
done

job="ceilv1-short-envoy-direct-c${clients}-${trial}"
if "${kubectl_vc[@]}" get job "$job" >/dev/null 2>&1; then
  echo "refusing to reuse Job $job" >&2
  exit 2
fi
"${kubectl_vc[@]}" get jobs -o json |
  jq -e '[.items[] | select(.metadata.name | startswith("ceilv1-")) | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
    echo "another ceiling Job is active" >&2
    exit 2
  }
"${kubectl_vc[@]}" get deployment envoy-independent -o json |
  jq -e '(.spec.replicas == 1) and (.status.readyReplicas == 1)' >/dev/null || {
    echo "envoy-independent must be exactly 1/1 Ready" >&2
    exit 2
  }
for component in dynamo-preprocessor:4 dynamo-selector:1 dynamo-benchmark-worker:16; do
  name=${component%:*}
  expected=${component#*:}
  "${kubectl_vc[@]}" get deployment "$name" -o json |
    jq -e --argjson expected "$expected" '(.spec.replicas == $expected) and (.status.readyReplicas == $expected)' >/dev/null || {
      echo "deployment $name must be $expected/$expected Ready" >&2
      exit 2
    }
done
"${kubectl_vc[@]}" get deployment agw-static agw-generic envoy-callouts dynamo-frontend-reference -o json |
  jq -e 'all(.items[]; .spec.replicas == 0)' >/dev/null || {
    echo "another benchmark gateway has replicas" >&2
    exit 2
  }

export RUN_NAME=$job ARM_SERVICE=envoy-independent DATASET=short-claude-sonnet-raw.jsonl
export VCLUSTER_NAMESPACE AIPERF_NODE_A AIPERF_NODE_B TOKENIZER_STORE_BASENAME
export NIX_STORE_NFS_SERVER NIX_STORE_NFS_PATH
export BENCHMARK_START_UNIX=$(( $(date -u +%s) + 120 ))
"$envsubst_bin" '${RUN_NAME} ${VCLUSTER_NAMESPACE} ${ARM_SERVICE} ${DATASET} ${AIPERF_NODE_A} ${AIPERF_NODE_B} ${BENCHMARK_START_UNIX} ${TOKENIZER_STORE_BASENAME} ${NIX_STORE_NFS_SERVER} ${NIX_STORE_NFS_PATH}' \
  < "$(dirname "$0")/frozen-raw-capacity-job.yaml.tmpl" |
  "${kubectl_vc[@]}" create --dry-run=client -f - -o json |
  jq --argjson clients "$clients" --arg node_c "$AIPERF_NODE_C" '
    .spec.completions = $clients |
    .spec.parallelism = $clients |
    .spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].values += [$node_c]
  ' |
  "${kubectl_vc[@]}" apply -f -

echo "waiting for $job ($clients clients, barrier $BENCHMARK_START_UNIX)" >&2
"${kubectl_vc[@]}" wait --for=condition=complete "job/$job" --timeout=900s
execution="$RESULT_DIR/execution-${job}.json"
"${kubectl_vc[@]}" get jobs,pods -o json |
  jq --arg job "$job" '
    .items as $items |
    {
      job: ($items[] | select(.kind == "Job" and .metadata.name == $job) |
        {name: .metadata.name, uid: .metadata.uid,
         created_at: .metadata.creationTimestamp,
         completed_at: .status.completionTime,
         succeeded: (.status.succeeded // 0),
         image: .spec.template.spec.containers[0].image,
         command: .spec.template.spec.containers[0].command,
         args: .spec.template.spec.containers[0].args,
         completions: .spec.completions,
         parallelism: .spec.parallelism,
         node_affinity: .spec.template.spec.affinity.nodeAffinity,
         topology_spread: .spec.template.spec.topologySpreadConstraints}),
      pods: [$items[] | select(.kind == "Pod" and .metadata.labels["job-name"] == $job) |
        {name: .metadata.name, node: .spec.nodeName, phase: .status.phase,
         started_at: .status.startTime,
         finished_at: .status.containerStatuses[0].state.terminated.finishedAt}]
    }
  ' > "$execution"
jq -e --argjson clients "$clients" \
  --arg a "$AIPERF_NODE_A" --arg b "$AIPERF_NODE_B" --arg c "$AIPERF_NODE_C" '
    (.job.succeeded == $clients) and (.pods | length == $clients) and
    ([.pods[].node] | group_by(.) | map(length) | sort == [($clients/3),($clients/3),($clients/3)]) and
    (([.pods[].node] | unique | sort) == ([$a,$b,$c] | sort))
  ' "$execution" >/dev/null || {
    echo "missing or skewed Pod placement evidence for $job" >&2
    exit 1
  }
out="$RESULT_DIR/raw_aiperf/$job"
mkdir -p "$out"
"${kubectl_vc[@]}" exec -i dynamo-component-store-stager -- \
  sh -c "cd /shared/nix/aiperf/results/$job && tar -cf - */profile_export_aiperf.json */profile_export_aiperf.csv */profile_export_console.txt" |
  tar -C "$out" -xf -
summaries=("$out"/*/profile_export_aiperf.json)
if [ "${#summaries[@]}" -ne "$clients" ]; then
  echo "expected $clients client exports, found ${#summaries[@]}" >&2
  exit 1
fi
jq -es --arg job "$job" --argjson clients "$clients" '
  {job:$job,clients:length,rps:(map(.request_throughput.avg)|add),
   effective_concurrency:(map(.effective_concurrency.avg)|add),
   requests:(map(.request_count.avg)|add),
   errors:(map(.error_summary|map(.count)|add // 0)|add),
   cancelled:(map(.was_cancelled)|any)}
' "${summaries[@]}"
jq -es --argjson clients "$clients" '
  length == $clients and all(.[]; (.error_summary | length) == 0 and .was_cancelled == false)
' "${summaries[@]}" >/dev/null || {
  echo "Job $job completed but client exports are not error-free" >&2
  exit 1
}
