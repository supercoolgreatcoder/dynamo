#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Capture comparable selector and prefill facade samples under one AGW gateway.
set -euo pipefail
[[ $# == 2 && ( $1 == pd-agw-static || $1 == pd-agw-generic ) && $2 =~ ^r[1-9][0-9]*$ ]] || exit 2
arm=$1
trial=$2
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
: "${PD_RPC_BINARY:?}"
: "${AIPERF_NODE_A:?}"
: "${AIPERF_NODE_B:?}"
: "${NIX_STORE_NFS_SERVER:?}"
: "${NIX_STORE_NFS_PATH:?}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
[[ $PD_RPC_BINARY == /nix/store/*/bin/dynamo-component-facade ]] || exit 2
correlated_timing=${PD_RPC_CORRELATED_TIMING:-0}
[[ $correlated_timing == 0 || $correlated_timing == 1 ]] || exit 2
if [[ $correlated_timing == 1 ]]; then
  : "${PD_CORRELATED_GATEWAY_BINARY:?}"
  [[ $PD_CORRELATED_GATEWAY_BINARY == /nix/store/*/bin/agentgateway ]] || exit 2
fi
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
for component in selector prefill; do
  "${vc[@]}" get deployment "dynamo-pd-$component" -o json |
    jq -e --arg binary "$PD_RPC_BINARY" --arg correlated "$correlated_timing" '
      .spec.replicas == 4 and .status.readyReplicas == 4
      and .spec.template.spec.containers[0].command[0] == $binary
      and any(.spec.template.spec.containers[0].env[];
        .name == "DYN_COMPONENT_RPC_SAMPLE_EVERY" and .value == "1000")
      and any(.spec.template.spec.containers[0].env[];
        .name == "RUST_LOG" and .value == "warn,dynamo_component_rpc_sample=debug")
      and (if $correlated == "0" then
        all(.spec.template.spec.containers[0].env[]; .name != "DYN_COMPONENT_CORRELATED_TIMING")
        else any(.spec.template.spec.containers[0].env[];
          .name == "DYN_COMPONENT_CORRELATED_TIMING" and .value == "1") end)
    ' >/dev/null
done
"${vc[@]}" get deployment dynamo-pd-preprocessor -o json |
  jq -e '.spec.replicas == 4 and .status.readyReplicas == 4
    and .spec.template.spec.containers[0].command[0] == "/nix/store/ip5h1yq7nz5gdbg9yja58b0fcng537gj-dynamo-component-pipelines-cb970285af/bin/dynamo-component-facade"' >/dev/null
gateway=dynamo-${arm}
"${vc[@]}" get deployment "$gateway" -o json |
  jq -e '.spec.replicas == 1 and .status.readyReplicas == 1' >/dev/null
if [[ $correlated_timing == 1 ]]; then
  "${vc[@]}" get deployment "$gateway" -o json |
    jq -e --arg binary "$PD_CORRELATED_GATEWAY_BINARY" --arg arm "$arm" '
      .spec.template.spec.containers[0].command[0] == $binary
      and any(.spec.template.spec.containers[0].env[];
        .name == (if $arm == "pd-agw-static" then "DYN_STATIC_CORRELATED_TIMING" else "DYN_GENERIC_CORRELATED_TIMING" end)
        and .value == "1")
      and any(.spec.template.spec.containers[0].env[];
        .name == "RUST_LOG" and .value == (if $arm == "pd-agw-static" then
          "warn,dynamo_static_rpc_split=debug,dynamo_static_selector_rpc=debug"
          else "warn,dynamo_generic_rpc_split=debug" end))
    ' >/dev/null
fi
for inactive in dynamo-pd-agw-static dynamo-pd-agw-generic dynamo-pd-envoy-generic dynamo-pd-envoy-callouts; do
  [[ $inactive == "$gateway" ]] && continue
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null
done

script_dir=$(cd "$(dirname "$0")" && pwd)
export RESULT_DIR=${PD_RPC_RESULT_DIR:-$script_dir/results/2026-09-26-mocker-pd-rpc-timing}
export TOKENIZER_STORE_BASENAME=wjq1b3wfjpzak4yd4rmj9arwqk1gkiir-qwen-tokenizer
export ENVSUBST_BIN=${ENVSUBST_BIN:-/nix/store/g4ylgfr6jw3wrvgqh0ifvvjpnn6rabzv-gettext-1.0/bin/envsubst}
export PD_RECORD_EXPORT=0
mkdir -p "$RESULT_DIR"
if [[ $arm == pd-agw-static ]]; then
  job=nixpds-isl4000-pd-agw-static-$trial
else
  job=nixpd-isl4000-pd-agw-generic-$trial
fi
for component in preprocessor selector prefill; do
  "${vc[@]}" get deployment "dynamo-pd-$component" -o json \
    > "$RESULT_DIR/$component-$job.json"
done
"${vc[@]}" get deployment "$gateway" -o json > "$RESULT_DIR/gateway-$job.json"
dataset=/shared/nix/aiperf/static-qwen2.5-0.5b/isl4000-claude-sonnet-raw.jsonl
actual_sha=$("${vc[@]}" exec "${NIX_STAGER_POD:-dynamo-component-store-stager}" -- \
  sha256sum "$dataset" | awk '{print $1}')
[[ $actual_sha == 3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743 ]] || exit 2
printf '%s\t%s\n' "$dataset" "$actual_sha" > "$RESULT_DIR/dataset-sha-$job.tsv"
status=completed
if ! bash "$script_dir/run-nix-mocker-trial.sh" "$arm" isl4000 "$trial"; then
  status=failed
fi
"${vc[@]}" get job "$job" -o json > "$RESULT_DIR/job-$job.json"
"${vc[@]}" logs "deployment/$gateway" > "$RESULT_DIR/gateway-$job.log"
for component in selector prefill; do
  "${vc[@]}" get pods -l "app=dynamo-pd-$component" -o json |
    jq -r '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name' |
    while IFS= read -r pod; do
      "${vc[@]}" logs "$pod" > "$RESULT_DIR/$component-$job-$pod.log"
    done
done
jq -es --arg status "$status" --arg job "$job" '
  {status:$status,job:$job,clients:length,
   exported_rps:(map(.request_throughput.avg)|add),
   requests:(map(.request_count.avg)|add),
   errors:(map(.error_summary|map(.count)|add // 0)|add),
   cancelled:(map(.was_cancelled)|any)}
' "$RESULT_DIR/raw_aiperf/$job"/?/profile_export_aiperf.json \
  > "$RESULT_DIR/summary-$job.json"
[[ $status == completed ]] || exit 1
