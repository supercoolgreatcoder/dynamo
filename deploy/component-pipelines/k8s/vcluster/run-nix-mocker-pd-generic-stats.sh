#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Roll one diagnostic AGW binary over the existing synthetic P/D graph.
set -euo pipefail
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
: "${PD_AGW_BINARY:?}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
[[ $PD_AGW_BINARY == /nix/store/*/bin/agentgateway ]] || exit 2
stats_interval=${PD_GENERIC_STATS_INTERVAL_SECS:-0}
[[ $stats_interval =~ ^[0-9]+$ ]] && (( stats_interval <= 3600 )) || exit 2
rust_log=${PD_RUST_LOG:-warn,dynamo_generic_pipeline_stats=debug}
[[ $rust_log == warn,dynamo_generic_pipeline_stats=debug ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
for component in dynamo-pd-preprocessor:4 dynamo-pd-selector:4 dynamo-pd-prefill:4 dynamo-pd-decode:16; do
  name=${component%:*}
  replicas=${component#*:}
  "${vc[@]}" get deployment "$name" -o json |
    jq -e --argjson replicas "$replicas" \
      '.spec.replicas == $replicas and .status.readyReplicas == $replicas' >/dev/null
done
"${vc[@]}" get configmap dynamo-pd-contracts -o json |
  jq -e '.data["disaggregated.yaml"] | contains("operationId: generateRaw")' >/dev/null
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
"${vc[@]}" exec "$stager_pod" -- test -x "/shared/nix${PD_AGW_BINARY#/nix}"
source_deployment=$("${vc[@]}" get deployment dynamo-pd-agw-generic -o json)
deployment=$(jq -c --arg binary "$PD_AGW_BINARY" \
  --arg interval "$stats_interval" --arg rust_log "$rust_log" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-agw-generic"},spec:.spec}
  | .spec.replicas=1
  | .spec.template.spec.containers[0].command[0]=$binary
  | .spec.template.spec.containers[0].env |=
      (map(select(.name != "DYN_GENERIC_STATS_INTERVAL_SECS" and .name != "RUST_LOG"))
       + [{name:"RUST_LOG",value:$rust_log}]
       + (if $interval == "0" then []
          else [{name:"DYN_GENERIC_STATS_INTERVAL_SECS",value:$interval}] end))
' <<<"$source_deployment")
printf '%s\n' "$deployment" | "${vc[@]}" apply --dry-run=server -f - >/dev/null
printf '%s\n' "$deployment" | "${vc[@]}" apply -f -
"${vc[@]}" rollout restart deployment/dynamo-pd-agw-generic
"${vc[@]}" rollout status deployment/dynamo-pd-agw-generic --timeout=900s
echo "AGW generic stats fixture ready in $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
