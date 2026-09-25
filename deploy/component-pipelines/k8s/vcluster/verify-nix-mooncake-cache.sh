#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Verify cache hits separately from Job creation so a collector failure never
# requires rerunning or overwriting a completed AIPerf measurement.
set -euo pipefail

if [ "$#" -ne 1 ] || ! [[ "$1" =~ ^ceilv2-mooncake-envoy-callouts-c12-r[1-9][0-9]*$ ]]; then
  echo "usage: $0 ceilv2-mooncake-envoy-callouts-c12-rN" >&2
  exit 2
fi
job=$1
: "${VCLUSTER_KUBECONFIG:?}"
: "${VCLUSTER_EXPECTED_SERVER:?}"
: "${VCLUSTER_NAMESPACE:?}"
: "${RESULT_DIR:?}"
actual_server=$(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" config view --minify -o jsonpath='{.clusters[0].cluster.server}')
test "$actual_server" = "$VCLUSTER_EXPECTED_SERVER" || {
  echo "refusing non-vCluster API: $actual_server" >&2
  exit 2
}
execution="$RESULT_DIR/execution-$job.json"
test -s "$execution"
jq -e '.job.succeeded == 12 and (.pods | length) == 12' "$execution" >/dev/null
vc=(kubectl --kubeconfig "$VCLUSTER_KUBECONFIG" -n "$VCLUSTER_NAMESPACE")
out="$RESULT_DIR/cache-$job.tsv"
tmp=$(mktemp "$RESULT_DIR/cache-$job.XXXXXX")
trap 'rm -f "$tmp"' EXIT
while IFS= read -r pod; do
  # Do not use `rg -m1` here: with pipefail it can close kubectl's pipe early,
  # falsely turning a genuine HIT into a SIGPIPE failure.
  hit=$("${vc[@]}" logs "$pod" | rg -F 'Memory-mapped dataset cache HIT') || {
    echo "mmap cache HIT not proven for $pod" >&2
    exit 1
  }
  [[ "$hit" == *'skipping tokenizer + composer'* ]] || {
    echo "cache log did not prove tokenizer/composer skip for $pod" >&2
    exit 1
  }
  printf '%s\t%s\n' "$pod" "$hit" >> "$tmp"
done < <(jq -r '.pods[].name' "$execution")
test "$(wc -l < "$tmp")" -eq 12
if [ -s "$out" ]; then
  echo "refusing to overwrite existing nonempty cache evidence: $out" >&2
  exit 2
fi
mv "$tmp" "$out"
echo "proved 12 AIPerf mmap cache HITs without tokenizer/composer work: $out"
