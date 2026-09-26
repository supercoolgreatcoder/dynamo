#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Build one fixed-schedule dataset equivalent to 24 clients replaying the same
# 22,699-row prepared Mooncake trace. This is a mechanical byte-for-byte repeat:
# AIPerf's single timing manager, not 24 independent clocks, owns the schedule.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 SOURCE_JSONL NEW_OUTPUT_JSONL" >&2
  exit 2
fi

source_file=$1
output_file=$2
source_sha=28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550
source_rows=22699
copies=24

test -s "$source_file"
test ! -e "$output_file" || {
  echo "refusing to overwrite: $output_file" >&2
  exit 2
}
test "$(sha256sum "$source_file" | cut -d' ' -f1)" = "$source_sha"
test "$(wc -l < "$source_file")" -eq "$source_rows"

output_dir=$(dirname "$output_file")
test -d "$output_dir"
temporary_file=$(mktemp "$output_dir/.mooncake-x24.XXXXXXXX")
trap 'rm -f -- "$temporary_file"' EXIT

for ((copy=0; copy<copies; copy++)); do
  # Mechanical dataset expansion; no content or timestamp is modified.
  awk '{print}' "$source_file" >> "$temporary_file"
done

test "$(wc -l < "$temporary_file")" -eq "$((source_rows * copies))"
mv -- "$temporary_file" "$output_file"
trap - EXIT
sha256sum "$output_file"
