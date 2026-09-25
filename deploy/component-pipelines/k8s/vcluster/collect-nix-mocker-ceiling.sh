#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Collect a completed ceiling Job, including after the launching shell exited.
set -euo pipefail
shopt -s nullglob
if [ "$#" -ne 2 ] || ! [[ "$1" =~ ^ceilv1-short-envoy-direct-c[0-9]+-r[1-9][0-9]*(-selector4|-preprocessor8|-gateway12|-isolated|-envoy12|-profile)?$ ]] ||
  ! [[ "$2" =~ ^[0-9]+$ ]]; then
  echo "usage: $0 JOB CLIENTS" >&2
  exit 2
fi
job=$1
clients=$2
if [ "$clients" -lt 6 ] || [ "$clients" -gt 24 ] || [ $((clients % 3)) -ne 0 ] ||
  [[ "$job" != ceilv1-short-envoy-direct-c"${clients}"-r* ]]; then
  echo "JOB and CLIENTS must match, with a multiple of 3 clients from 6 to 24" >&2
  exit 2
fi
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
: "${AIPERF_NODE_A:?}"
: "${AIPERF_NODE_B:?}"
: "${AIPERF_NODE_C:?}"
: "${RESULT_DIR:?}"
test -d "$RESULT_DIR"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]; then
  echo "kubeconfig server $actual_server does not match expected vCluster server" >&2
  exit 2
fi
kubectl_vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${kubectl_vc[@]}" get job "$job" -o json |
  jq -e --argjson clients "$clients" '(.status.succeeded == $clients) and (.spec.completions == $clients)' >/dev/null
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
