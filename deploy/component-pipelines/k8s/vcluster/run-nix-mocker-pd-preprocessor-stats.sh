#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Roll an isolated facade diagnostic over the existing vCluster P/D fixture.
set -euo pipefail
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
: "${PD_PREPROCESSOR_BINARY:?}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
[[ $PD_PREPROCESSOR_BINARY == /nix/store/*/bin/dynamo-component-facade ]] || exit 2
stats_interval=${PD_PREPROCESSOR_STATS_INTERVAL_SECS:-0}
[[ $stats_interval == 0 || $stats_interval == 10 ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
"${vc[@]}" get jobs -o json |
  jq -e '[.items[] | select((.status.active // 0) > 0)] | length == 0' >/dev/null
stager_pod=${NIX_STAGER_POD:-dynamo-component-store-stager}
"${vc[@]}" exec "$stager_pod" -- test -x "/shared/nix${PD_PREPROCESSOR_BINARY#/nix}"
source_deployment=$("${vc[@]}" get deployment dynamo-pd-preprocessor -o json)
deployment=$(jq -c --arg binary "$PD_PREPROCESSOR_BINARY" \
  --arg interval "$stats_interval" '
  {apiVersion,kind,metadata:{name:"dynamo-pd-preprocessor"},spec:.spec}
  | .spec.replicas=4
  | .spec.template.spec.containers[0].command[0]=$binary
  | .spec.template.spec.containers[0].env |=
      (map(select(.name != "DYN_PREPROCESSOR_STATS_INTERVAL_SECS" and .name != "RUST_LOG"))
       + [{name:"RUST_LOG",value:(if $interval == "0" then "warn" else "warn,dynamo_preprocessor_batch_stats=debug" end)}]
       + (if $interval == "0" then []
          else [{name:"DYN_PREPROCESSOR_STATS_INTERVAL_SECS",value:$interval}] end))
' <<<"$source_deployment")
printf '%s\n' "$deployment" | "${vc[@]}" apply --dry-run=server -f - >/dev/null
printf '%s\n' "$deployment" | "${vc[@]}" apply -f -
"${vc[@]}" rollout status deployment/dynamo-pd-preprocessor --timeout=900s
echo "Preprocessor facade ready in $VCLUSTER_EXPECTED_SERVER/$VCLUSTER_NAMESPACE"
