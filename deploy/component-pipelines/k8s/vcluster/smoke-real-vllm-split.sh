#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Reuse the exact SGLang correctness assertions against the isolated vLLM
# worker, selector, and gateway resources.
set -euo pipefail
exec env \
  REAL_WORKER_NAME=real-vllm-split \
  REAL_SELECTOR_NAME=real-vllm-selector \
  REAL_GATEWAY_NAME=real-vllm-agw-generic \
  REAL_ENGINE_LABEL='vLLM via upstream vllm-rs gRPC' \
  bash "$(dirname "$0")/smoke-real-sglang-split.sh"
