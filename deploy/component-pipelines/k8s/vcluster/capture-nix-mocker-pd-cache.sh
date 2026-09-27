#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Preserve one AIPerf mmap-cache HIT line for each of the 72 Mooncake clients.
set -euo pipefail
shopt -s nullglob

: "${VCLUSTER_KUBECONFIG:?set the explicit vCluster kubeconfig}"
: "${VCLUSTER_EXPECTED_SERVER:?set the vCluster API server}"
: "${VCLUSTER_NAMESPACE:?set the vCluster namespace}"
: "${RESULT_DIR:?set the local result directory}"
[[ $VCLUSTER_EXPECTED_SERVER == https://gateway-poc.mkhadkevich-dev:443 ]] || exit 2
[[ $VCLUSTER_NAMESPACE == dynamo-components-v2 ]] || exit 2
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" \
  config view --minify -o jsonpath='{.clusters[0].cluster.server}')
[[ $actual_server == "$VCLUSTER_EXPECTED_SERVER" ]] || exit 2
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")

dirs=("$RESULT_DIR"/raw_aiperf/nixpd*-mooncake-pd-*-r11[012])
[[ ${#dirs[@]} == 12 ]] || {
  echo "expected 12 complete Mooncake P/D Jobs, found ${#dirs[@]}" >&2
  exit 2
}
for dir in "${dirs[@]}"; do
  job=${dir##*/}
  target=$RESULT_DIR/cache-$job.tsv
  [[ ! -e $target ]] || { echo "refusing to overwrite $target" >&2; exit 2; }
  tmp=$(mktemp "$RESULT_DIR/.cache-$job.XXXXXX")
  trap 'rm -f -- "$tmp"' EXIT
  "${vc[@]}" get pods -l "job-name=$job" -o json |
    jq -r '.items | sort_by(.metadata.labels["batch.kubernetes.io/job-completion-index"] | tonumber) |
      .[] | [.metadata.labels["batch.kubernetes.io/job-completion-index"],.metadata.name,.status.phase] | @tsv' |
    while IFS=$'\t' read -r index pod phase; do
      [[ $phase == Succeeded ]] || { echo "$pod is not Succeeded" >&2; exit 1; }
      line=$("${vc[@]}" logs "$pod" | rg -F 'Memory-mapped dataset cache HIT (key=')
      [[ $(wc -l <<<"$line") == 1 ]] || { echo "expected one cache HIT for $pod" >&2; exit 1; }
      printf '%s\t%s\t%s\n' "$index" "$pod" "$line"
    done > "$tmp"
  [[ $(wc -l < "$tmp") == 6 ]] || { echo "expected six cache hits for $job" >&2; exit 1; }
  mv -- "$tmp" "$target"
  trap - EXIT
  echo "captured six mmap-cache hits for $job" >&2
done
