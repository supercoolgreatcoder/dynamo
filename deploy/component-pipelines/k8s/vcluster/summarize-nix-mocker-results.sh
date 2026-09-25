#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Normalize the six immutable AIPerf summary exports from every collected Job.
set -euo pipefail
shopt -s nullglob

: "${RESULT_DIR:?set the local result directory}"
series_id=${SERIES_ID:-nix-component-pipeline-mocker-parity-2026-09-24}
execution_file=${EXECUTION_FILE:-benchmark_execution.json}
summary_file=${SUMMARY_FILE:-benchmark_summary.json}
[[ $series_id =~ ^[a-z0-9][-a-z0-9]*$ ]] || {
  echo "SERIES_ID must be a DNS-safe series name" >&2
  exit 2
}
for basename in "$execution_file" "$summary_file"; do
  [[ $basename =~ ^[a-z0-9][a-z0-9_.-]*\.json$ ]] || {
    echo "execution and summary files must be JSON basenames" >&2
    exit 2
  }
done
test -f "$RESULT_DIR/$execution_file"
test ! -e "$RESULT_DIR/$summary_file" || {
  echo "refusing to overwrite $summary_file" >&2
  exit 2
}

run_records=$(mktemp)
trap 'rm -f "$run_records"' EXIT
for dir in "$RESULT_DIR"/raw_aiperf/nixv2-*; do
  job=${dir##*/}
  summaries=("$dir"/?/profile_export_aiperf.json)
  csvs=("$dir"/?/profile_export_aiperf.csv)
  consoles=("$dir"/?/profile_export_console.txt)
  if [ "${#summaries[@]}" -ne 6 ] || [ "${#csvs[@]}" -ne 6 ] || [ "${#consoles[@]}" -ne 6 ]; then
    echo "incomplete six-client exports for $job" >&2
    exit 1
  fi
  jq -es '
    length == 6 and
    (map(.aiperf_version) | unique) == ["0.12.0"] and
    (map(.input_config.endpoint.urls[0]) | unique | length) == 1 and
    (map(.input_config.datasets[0]) | unique | length) == 1 and
    (map(.input_config.phases[0].duration) | unique) == [45] and
    (map(.input_config.phases[0].concurrency) | unique) == [128]
  ' "${summaries[@]}" >/dev/null || {
    echo "AIPerf version, endpoint, workload, or load policy drift in $job" >&2
    exit 1
  }
  jq -cs --arg job "$job" '
    {
      job: $job,
      clients: length,
      aiperf_version: .[0].aiperf_version,
      endpoint: .[0].input_config.endpoint.urls[0],
      dataset: .[0].input_config.datasets[0],
      phase: .[0].input_config.phases[0],
      aggregate_rps: (map(.request_throughput.avg) | add),
      successful_requests: (map(.request_count.avg) | add),
      failed_requests: (map(.error_summary | map(.count) | add // 0) | add),
      cancelled: (map(.was_cancelled) | any),
      effective_concurrency: (map(.effective_concurrency.avg) | add),
      mean_request_latency_ms: ((map(.request_latency.avg * .request_count.avg) | add) / (map(.request_count.avg) | add)),
      error_summary: [.[].error_summary[]?]
    }
  ' "${summaries[@]}" >> "$run_records"
done

jq -s --arg series_id "$series_id" '
  map(. + (.job | capture("^nixv2-(?<workload>short|isl4000|mooncake)-(?<arm>.+)-(?<trial>r[0-9]+)$"))) as $runs |
  {
    series_id: $series_id,
    metric: "aggregate successful request throughput",
    unit: "requests/second",
    runs: $runs,
    cells: (
      $runs | group_by([.workload, .arm]) |
      map(. as $group |
        ($group | map(select(.failed_requests == 0 and .cancelled == false))) as $valid |
        {
          workload: $group[0].workload,
          arm: $group[0].arm,
          valid_jobs: ($valid | map(.job)),
          invalid_jobs: ($group | map(select(.failed_requests > 0 or .cancelled == true) | .job)),
          valid_runs: ($valid | length),
          median_rps: ($valid | map(.aggregate_rps) | sort | .[length / 2 | floor]),
          min_rps: ($valid | map(.aggregate_rps) | min),
          max_rps: ($valid | map(.aggregate_rps) | max)
        }
      )
    )
  }
' "$run_records" > "$RESULT_DIR/$summary_file"

echo "normalized $(jq '.runs | length' "$RESULT_DIR/$summary_file") Jobs into $summary_file"
