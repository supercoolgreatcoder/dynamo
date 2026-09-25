#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Complete three valid trials per gateway arm/workload after the initial r1
# campaign; preserve invalid stock Dynamo Mooncake repeats for diagnosis.
# Every Kubernetes operation uses the explicit vCluster kubeconfig and namespace.
set -euo pipefail
shopt -s nullglob

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig path}"
: "${VCLUSTER_EXPECTED_SERVER:?set the expected vCluster API server URL}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${RESULT_DIR:?set a local result directory}"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]; then
  echo "kubeconfig server $actual_server does not match expected vCluster server" >&2
  exit 2
fi
kubectl_vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${kubectl_vc[@]}" get jobs -o json |
  jq -e '[.items[] | select(.metadata.name | startswith("nixv2-")) | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
    echo "another nixv2 benchmark Job is active; refusing overlapping trials" >&2
    exit 2
  }

service_for_arm() {
  case "$1" in
    agw-static) echo agw-static ;;
    agw-generic) echo agw-generic ;;
    envoy-generic) echo envoy-independent ;;
    envoy-callouts) echo envoy-callouts ;;
    dynamo-reference) echo dynamo-frontend-reference ;;
    *) return 2 ;;
  esac
}

run_cell() {
  local arm=$1 workload=$2 trial=$3 job out service
  job="nixv2-${workload}-${arm}-${trial}"
  out="$RESULT_DIR/raw_aiperf/$job"
  local exports=("$out"/?/profile_export_aiperf.json)
  if [ "${#exports[@]}" -eq 6 ]; then
    jq -es 'length == 6 and all(.[]; (.error_summary | length) == 0 and .was_cancelled == false)' \
      "${exports[@]}" >/dev/null || {
        echo "existing local evidence is invalid for $job" >&2
        exit 2
      }
    echo "already collected $job" >&2
    return
  fi
  if [ -d "$out" ]; then
    echo "partial local evidence exists for $job; inspect before rerunning" >&2
    exit 2
  fi

  service=$(service_for_arm "$arm")
  "${kubectl_vc[@]}" scale \
    deployment/agw-static deployment/agw-generic \
    deployment/envoy-independent deployment/envoy-callouts \
    deployment/dynamo-frontend-reference deployment/dynamo-reference-worker \
    --replicas=0
  if [ "$arm" = dynamo-reference ]; then
    "${kubectl_vc[@]}" scale deployment/dynamo-reference-worker --replicas=4
    "${kubectl_vc[@]}" rollout status deployment/dynamo-reference-worker --timeout=300s
  fi
  "${kubectl_vc[@]}" scale "deployment/$service" --replicas=1
  "${kubectl_vc[@]}" rollout status "deployment/$service" --timeout=300s
  bash "$(dirname "$0")/run-nix-mocker-trial.sh" "$arm" "$workload" "$trial"
}

# Rotating the arm order limits warm/cold and time-of-day bias. Existing valid
# exports are skipped, so this can resume after an interrupted campaign.
for workload in short isl4000 mooncake; do
  for arm in agw-generic envoy-generic envoy-callouts agw-static dynamo-reference; do
    if [ "$workload" = mooncake ] && [ "$arm" = dynamo-reference ]; then
      # This campaign's r2 completed with two AIPerf request errors. Keep its
      # six exports for audit, but do not count or silently retry that Job.
      excluded=("$RESULT_DIR"/raw_aiperf/nixv2-mooncake-dynamo-reference-r2/?/profile_export_aiperf.json)
      if [ "${#excluded[@]}" -ne 6 ]; then
        echo "missing six-client evidence for excluded Dynamo Mooncake r2" >&2
        exit 2
      fi
      jq -es '(map(.error_summary | map(.count) | add // 0) | add) == 2' \
        "${excluded[@]}" >/dev/null || {
          echo "excluded Dynamo Mooncake r2 error count changed" >&2
          exit 2
        }
      echo "excluded nixv2-mooncake-dynamo-reference-r2 (two request errors)" >&2
      continue
    fi
    run_cell "$arm" "$workload" r2
  done
done
for workload in short isl4000 mooncake; do
  for arm in dynamo-reference agw-static envoy-callouts envoy-generic agw-generic; do
    if [ "$workload" = mooncake ] && [ "$arm" = dynamo-reference ]; then
      # r3 also completed with two empty-content request errors. Its records-
      # level diagnostic is preserved separately; do not count it as valid.
      excluded=("$RESULT_DIR"/raw_aiperf/nixv2-mooncake-dynamo-reference-r3/?/profile_export_aiperf.json)
      if [ "${#excluded[@]}" -ne 6 ]; then
        echo "missing six-client evidence for excluded Dynamo Mooncake r3" >&2
        exit 2
      fi
      jq -es '(map(.error_summary | map(.count) | add // 0) | add) == 2' \
        "${excluded[@]}" >/dev/null || {
          echo "excluded Dynamo Mooncake r3 error count changed" >&2
          exit 2
        }
      echo "excluded nixv2-mooncake-dynamo-reference-r3 (two request errors)" >&2
      continue
    fi
    run_cell "$arm" "$workload" r3
  done
done
# AGW static's Mooncake r1 failed before traffic; r2 was its first valid pass.
run_cell agw-static mooncake r4

"${kubectl_vc[@]}" scale \
  deployment/agw-static deployment/agw-generic \
  deployment/envoy-independent deployment/envoy-callouts \
  deployment/dynamo-frontend-reference deployment/dynamo-reference-worker \
  --replicas=0
echo "three valid trials per gateway arm/workload collected under $RESULT_DIR/raw_aiperf" >&2
echo "stock Dynamo Mooncake remains diagnostic-only beyond its valid r1" >&2
