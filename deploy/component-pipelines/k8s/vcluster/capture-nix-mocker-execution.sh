#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Snapshot applied Job commands and Pod placements before Kubernetes TTL GC.
set -euo pipefail

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig path}"
: "${VCLUSTER_EXPECTED_SERVER:?set the expected vCluster API server URL}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${RESULT_DIR:?set the local result directory}"
test -f "$VCLUSTER_KUBECONFIG"
test -d "$RESULT_DIR"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
if [ "$actual_server" != "$VCLUSTER_EXPECTED_SERVER" ]; then
  echo "kubeconfig server $actual_server does not match expected vCluster server" >&2
  exit 2
fi
out="$RESULT_DIR/benchmark_execution.json"
if [ -e "$out" ]; then
  echo "refusing to overwrite existing execution ledger $out" >&2
  exit 2
fi
captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE" get jobs,pods -o json |
  jq --arg captured_at "$captured_at" --arg server "$actual_server" --arg namespace "$VCLUSTER_NAMESPACE" '
    .items as $items |
    [$items[] | select(.kind == "Job" and (.metadata.name | startswith("nixv2-") or startswith("diag-mooncake-")))] as $jobs |
    [$items[] | select(.kind == "Pod")] as $pods |
    {
      captured_at: $captured_at,
      vcluster_api_server: $server,
      namespace: $namespace,
      jobs: [
        $jobs[] | .metadata.name as $name | {
          name: $name,
          uid: .metadata.uid,
          created_at: .metadata.creationTimestamp,
          completed_at: .status.completionTime,
          succeeded: (.status.succeeded // 0),
          failed: (.status.failed // 0),
          image: .spec.template.spec.containers[0].image,
          command: .spec.template.spec.containers[0].command,
          args: .spec.template.spec.containers[0].args,
          node_affinity: .spec.template.spec.affinity.nodeAffinity,
          topology_spread: .spec.template.spec.topologySpreadConstraints,
          placement_evidence_complete: ([$pods[] | select(.metadata.labels["job-name"] == $name)] | length == 6),
          pods: [$pods[] | select(.metadata.labels["job-name"] == $name) | {
            name: .metadata.name,
            node: .spec.nodeName,
            phase: .status.phase,
            started_at: .status.startTime,
            finished_at: .status.containerStatuses[0].state.terminated.finishedAt
          }]
        }
      ]
    }
  ' > "$out"
jq -e '.jobs | length > 0 and all(.[]; .succeeded == 6 or .failed > 0)' "$out" >/dev/null || {
  echo "execution ledger contains active or unresolved Jobs: $out" >&2
  exit 1
}
echo "captured $(jq '.jobs | length' "$out") completed Jobs in $out"
echo "Jobs lacking six retained Pod placements: $(jq '[.jobs[] | select(.placement_evidence_complete == false)] | length' "$out")" >&2
