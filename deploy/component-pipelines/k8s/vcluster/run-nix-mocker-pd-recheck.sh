#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Recheck one frozen P/D workload across four isolated hosts on a pinned Nix
# bundle. run-nix-mocker-trial.sh owns the immutable AIPerf Job and exports.
set -euo pipefail
shopt -s nullglob

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the exact vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${COMPONENT_BUNDLE:?set the staged four-artifact Nix bundle}"
: "${RESULT_DIR:?set a run-scoped local result directory}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
[[ $COMPONENT_BUNDLE == /nix/store/* && ${COMPONENT_BUNDLE#/nix/store/} != */* ]] || exit 2
test -d "$RESULT_DIR"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || {
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
}
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null || {
    echo "refusing overlapping benchmark Jobs" >&2
    exit 2
  }

for component in dynamo-pd-preprocessor:4 dynamo-pd-selector:4 \
  dynamo-pd-prefill:4 dynamo-pd-decode:16; do
  name=${component%:*}
  expected=${component#*:}
  "${vc[@]}" get deployment "$name" -o json |
    jq -e --argjson expected "$expected" --arg facade "$COMPONENT_BUNDLE/bin/dynamo-component-facade" \
      '.spec.replicas == $expected and .status.readyReplicas == $expected and
       .spec.template.spec.containers[0].command[0] == $facade' >/dev/null || {
        echo "$name is not $expected/$expected Ready on $COMPONENT_BUNDLE" >&2
        exit 2
      }
done
for inactive in dynamo-benchmark-worker dynamo-preprocessor dynamo-selector \
  real-qwen3-agw-generic real-qwen3-preprocessor real-qwen3-selector \
  real-sglang real-sglang-split real-vllm-split real-vllm-agw-generic \
  real-vllm-pd-prefill real-vllm-pd-decode real-vllm-pd-preprocessor \
  real-vllm-pd-selector real-vllm-pd-agw dynamo-frontend-reference \
  dynamo-reference-worker; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null || {
      echo "$inactive is active; refusing a confounded P/D benchmark" >&2
      exit 2
    }
done
for arm in agw-static agw-generic envoy-generic envoy-callouts; do
  gateway=dynamo-pd-$arm
  if [[ $arm == agw-* ]]; then binary=$COMPONENT_BUNDLE/bin/agentgateway
  else binary=$COMPONENT_BUNDLE/bin/envoy-static; fi
  "${vc[@]}" get deployment "$gateway" -o json |
    jq -e --arg binary "$binary" '.spec.template.spec.containers[0].command[0] == $binary' >/dev/null || {
      echo "$gateway is not pinned to $COMPONENT_BUNDLE" >&2
      exit 2
    }
done

trial=${TRIAL:-r110}
workload=${WORKLOAD:-short}
case "$workload" in short|isl4000|mooncake) ;; *) echo "invalid WORKLOAD" >&2; exit 2 ;; esac
if [[ $workload == mooncake ]]; then
  # The fixed P/D trace needs the same 46-second grace used by the prior
  # campaign to complete every scheduled request. Short/ISL4000 stay at 45s.
  export BENCHMARK_DURATION=46
fi
case "$trial" in
  r110) arms=(agw-static envoy-callouts agw-generic envoy-generic) ;;
  r111) arms=(envoy-generic agw-generic envoy-callouts agw-static) ;;
  r112) arms=(agw-generic agw-static envoy-generic envoy-callouts) ;;
  *) echo "this P/D recheck reserves r110-r112 only" >&2; exit 2 ;;
esac

for arm in "${arms[@]}"; do
  case "$arm" in
    agw-static) prefix=nixpds ;;
    agw-generic)
      if [[ $workload == mooncake ]]; then prefix=nixpdg
      else prefix=nixpd; fi ;;
    envoy-generic) prefix=nixpde ;;
    envoy-callouts) prefix=nixpdc ;;
  esac
  job=$prefix-$workload-pd-$arm-$trial
  out=$RESULT_DIR/raw_aiperf/$job
  exports=("$out"/?/profile_export_aiperf.json)
  if [[ ${#exports[@]} == 6 ]]; then
    jq -es 'length == 6 and all(.[]; (.error_summary | length) == 0 and .was_cancelled == false)' \
      "${exports[@]}" >/dev/null || {
        echo "existing local exports are invalid: $job" >&2
        exit 2
      }
    echo "already collected $job" >&2
    continue
  fi
  if [[ -d $out ]] || "${vc[@]}" get job "$job" >/dev/null 2>&1; then
    echo "partial or remote-only $job exists; inspect before retrying" >&2
    exit 2
  fi

  "${vc[@]}" scale deployment/dynamo-pd-agw-static deployment/dynamo-pd-agw-generic \
    deployment/dynamo-pd-envoy-generic deployment/dynamo-pd-envoy-callouts --replicas=0
  gateway=dynamo-pd-$arm
  "${vc[@]}" scale "deployment/$gateway" --replicas=1
  "${vc[@]}" rollout status "deployment/$gateway" --timeout=300s
  bash "$(dirname "$0")/run-nix-mocker-trial.sh" "pd-$arm" "$workload" "$trial"
done

"${vc[@]}" scale deployment/dynamo-pd-agw-static deployment/dynamo-pd-agw-generic \
  deployment/dynamo-pd-envoy-generic deployment/dynamo-pd-envoy-callouts --replicas=0
echo "completed P/D $workload $trial with four isolated gateway arms" >&2
