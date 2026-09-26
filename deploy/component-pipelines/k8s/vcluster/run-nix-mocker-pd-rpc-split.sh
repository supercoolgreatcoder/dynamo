#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Capture one frozen ISL4000 run with the sampled static prefill RPC split.
set -euo pipefail
[[ $# == 1 && $1 =~ ^r[1-9][0-9]*$ ]] || exit 2
trial=$1
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
: "${PD_SPLIT_BINARY:?}"
: "${AIPERF_NODE_A:?}"
: "${AIPERF_NODE_B:?}"
: "${NIX_STORE_NFS_SERVER:?}"
: "${NIX_STORE_NFS_PATH:?}"
: "${NIX_STAGER_POD:?}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
[[ $PD_SPLIT_BINARY == /nix/store/*/bin/agentgateway ]] || exit 2
lazy_channels=${PD_LAZY_CHANNELS:-0}
[[ $lazy_channels == 0 || $lazy_channels == 1 ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
"${vc[@]}" get deployment dynamo-pd-agw-static -o json |
  jq -e --arg binary "$PD_SPLIT_BINARY" --arg lazy "$lazy_channels" '
    .spec.replicas == 1 and .status.readyReplicas == 1
    and .spec.template.spec.containers[0].command[0] == $binary
    and any(.spec.template.spec.containers[0].env[];
      .name == "DYN_STATIC_STAGE_TIMING_EVERY" and .value == "1000")
    and any(.spec.template.spec.containers[0].env[];
      .name == "RUST_LOG" and .value == "warn,dynamo_static_rpc_split=debug")
    and (if $lazy == "0" then
      all(.spec.template.spec.containers[0].env[]; .name != "DYN_STATIC_LAZY_CHANNELS")
      else any(.spec.template.spec.containers[0].env[];
        .name == "DYN_STATIC_LAZY_CHANNELS" and .value == $lazy) end)
  ' >/dev/null
for inactive in dynamo-pd-agw-generic dynamo-pd-envoy-generic dynamo-pd-envoy-callouts; do
  "${vc[@]}" get deployment "$inactive" -o json |
    jq -e '.spec.replicas == 0 and (.status.readyReplicas // 0) == 0' >/dev/null
done

script_dir=$(cd "$(dirname "$0")" && pwd)
export RESULT_DIR=${PD_SPLIT_RESULT_DIR:-$script_dir/results/2026-09-26-mocker-pd-static-rpc-split}
export TOKENIZER_STORE_BASENAME=wjq1b3wfjpzak4yd4rmj9arwqk1gkiir-qwen-tokenizer
export ENVSUBST_BIN=${ENVSUBST_BIN:-/nix/store/g4ylgfr6jw3wrvgqh0ifvvjpnn6rabzv-gettext-1.0/bin/envsubst}
export PD_RECORD_EXPORT=0
mkdir -p "$RESULT_DIR"
job=nixpds-isl4000-pd-agw-static-$trial
"${vc[@]}" get deployment dynamo-pd-agw-static -o json > "$RESULT_DIR/gateway-$job.json"
"${vc[@]}" get deployment dynamo-pd-preprocessor -o json > "$RESULT_DIR/preprocessor-$job.json"
"${vc[@]}" get deployment dynamo-pd-selector -o json > "$RESULT_DIR/selector-$job.json"
"${vc[@]}" get deployment dynamo-pd-prefill -o json > "$RESULT_DIR/prefill-$job.json"
dataset=/shared/nix/aiperf/static-qwen2.5-0.5b/isl4000-claude-sonnet-raw.jsonl
actual_sha=$("${vc[@]}" exec "$NIX_STAGER_POD" -- sha256sum "$dataset" | awk '{print $1}')
[[ $actual_sha == 3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743 ]] || exit 2
printf '%s\t%s\n' "$dataset" "$actual_sha" > "$RESULT_DIR/dataset-sha-$job.tsv"
status=completed
if ! bash "$script_dir/run-nix-mocker-trial.sh" pd-agw-static isl4000 "$trial"; then
  status=failed
fi
"${vc[@]}" get job "$job" -o json > "$RESULT_DIR/job-$job.json"
"${vc[@]}" logs deployment/dynamo-pd-agw-static > "$RESULT_DIR/gateway-$job.log"
jq -es --arg status "$status" --arg job "$job" '
  {status:$status,job:$job,clients:length,
   exported_rps:(map(.request_throughput.avg)|add),
   requests:(map(.request_count.avg)|add),
   errors:(map(.error_summary|map(.count)|add // 0)|add),
   cancelled:(map(.was_cancelled)|any)}
' "$RESULT_DIR/raw_aiperf/$job"/?/profile_export_aiperf.json \
  > "$RESULT_DIR/summary-$job.json"
[[ $status == completed ]] || exit 1
