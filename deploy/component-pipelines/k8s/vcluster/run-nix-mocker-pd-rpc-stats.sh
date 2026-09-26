#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Roll one opt-in selector or prefill facade diagnostic inside the vCluster.
set -euo pipefail
[[ $# == 1 && ( $1 == selector || $1 == prefill ) ]] || exit 2
component=$1
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
: "${PD_RPC_BINARY:?}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
[[ $PD_RPC_BINARY == /nix/store/*/bin/dynamo-component-facade ]] || exit 2
sample_every=${PD_RPC_SAMPLE_EVERY:-0}
[[ $sample_every == 0 || $sample_every == 1000 ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
"${vc[@]}" exec "$stager_pod" -- test -x "/shared/nix${PD_RPC_BINARY#/nix}"
name=dynamo-pd-$component
source_deployment=$("${vc[@]}" get deployment "$name" -o json)
deployment=$(jq -c --arg name "$name" --arg binary "$PD_RPC_BINARY" \
  --arg interval "$sample_every" '
  {apiVersion,kind,metadata:{name:$name},spec:.spec}
  | .spec.replicas=4
  | .spec.template.spec.containers[0].command[0]=$binary
  | .spec.template.spec.containers[0].env |=
      (map(select(.name != "DYN_COMPONENT_RPC_SAMPLE_EVERY" and .name != "RUST_LOG"))
       + [{name:"RUST_LOG",value:(if $interval == "0" then "warn" else "warn,dynamo_component_rpc_sample=debug" end)}]
       + (if $interval == "0" then []
          else [{name:"DYN_COMPONENT_RPC_SAMPLE_EVERY",value:$interval}] end))
' <<<"$source_deployment")
printf '%s\n' "$deployment" | "${vc[@]}" apply --dry-run=server -f - >/dev/null
printf '%s\n' "$deployment" | "${vc[@]}" apply -f -
"${vc[@]}" rollout status "deployment/$name" --timeout=900s
echo "$component facade ready in $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
