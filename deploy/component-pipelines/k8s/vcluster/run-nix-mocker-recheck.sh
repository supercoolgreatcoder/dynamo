#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# New, non-overlapping trials for a newly rolled Nix bundle. A failed or
# interrupted Job is never silently reused or counted as a valid repeat.
set -euo pipefail
shopt -s nullglob

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${RESULT_DIR:?set a local result directory}"
test -d "$RESULT_DIR"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [[ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]]; then
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
fi
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
    echo "refusing overlapping benchmark Jobs" >&2
    exit 2
  }
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
for inactive in real-qwen3-agw-generic real-qwen3-preprocessor \
  real-qwen3-selector real-sglang real-sglang-split real-vllm-split \
  real-vllm-agw-generic real-vllm-pd-prefill \
  real-vllm-pd-decode real-vllm-pd-preprocessor real-vllm-pd-selector \
  real-vllm-pd-agw dynamo-frontend-reference dynamo-reference-worker; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null || {
      echo "$inactive is active; refusing a confounded mocker benchmark" >&2
      exit 2
    }
done

trial=${TRIAL:-r20}
workload=${WORKLOAD:-short}
[[ "$trial" =~ ^r[1-9][0-9]*$ ]] || { echo "TRIAL must be rN" >&2; exit 2; }
case "$workload" in short|isl4000|mooncake) ;; *) echo "invalid WORKLOAD" >&2; exit 2 ;; esac

# Alternating orders reduce systematic warm/cold bias across clean repeats.
# r20-r24 belong to the earlier 846821 facade campaign. The accf6af bundle
# uses new r28-r30 Job names so evidence cannot be confused or overwritten.
case "$trial" in
  r20) arms=(envoy-generic agw-generic envoy-callouts agw-static) ;;
  r21) arms=(agw-static envoy-callouts agw-generic envoy-generic) ;;
  r22) arms=(envoy-callouts envoy-generic agw-static agw-generic) ;;
  r23) arms=(agw-generic agw-static envoy-generic envoy-callouts) ;;
  r24) arms=(envoy-callouts envoy-generic agw-static agw-generic) ;;
  r28) arms=(agw-static envoy-callouts agw-generic envoy-generic) ;;
  r29) arms=(envoy-generic agw-generic envoy-callouts agw-static) ;;
  r30) arms=(agw-generic agw-static envoy-generic envoy-callouts) ;;
  *) echo "this frozen recheck reserves r20-r24 and r28-r30 only" >&2; exit 2 ;;
esac

service_for_arm() {
  case "$1" in
    agw-static|agw-generic|envoy-callouts) echo "$1" ;;
    envoy-generic) echo envoy-independent ;;
    *) return 2 ;;
  esac
}

for arm in "${arms[@]}"; do
  job="nixv2-${workload}-${arm}-${trial}"
  out="$RESULT_DIR/raw_aiperf/$job"
  exports=("$out"/?/profile_export_aiperf.json)
  if [[ "${#exports[@]}" -eq 6 ]]; then
    jq -es 'length == 6 and all(.[]; (.error_summary | length) == 0 and .was_cancelled == false)' \
      "${exports[@]}" >/dev/null || {
        echo "existing local exports are invalid: $job" >&2
        exit 2
      }
    echo "already collected $job" >&2
    continue
  fi
  if [[ -d "$out" ]] || "${vc[@]}" get job "$job" >/dev/null 2>&1; then
    echo "partial or remote-only $job exists; inspect before retrying" >&2
    exit 2
  fi

  "${vc[@]}" scale deployment/agw-static deployment/agw-generic \
    deployment/envoy-independent deployment/envoy-callouts --replicas=0
  service=$(service_for_arm "$arm")
  "${vc[@]}" scale "deployment/$service" --replicas=1
  "${vc[@]}" rollout status "deployment/$service" --timeout=300s
  bash "$(dirname "$0")/run-nix-mocker-trial.sh" "$arm" "$workload" "$trial"
done

"${vc[@]}" scale deployment/agw-static deployment/agw-generic \
  deployment/envoy-independent deployment/envoy-callouts --replicas=0
echo "completed $workload $trial with four isolated gateway arms" >&2
