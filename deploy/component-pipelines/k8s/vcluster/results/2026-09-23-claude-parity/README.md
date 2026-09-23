<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Claude-parity prototype results

The final AGW static run reached 3,023.40 requests/second with zero errors on the
same four-preprocessor, sixteen-mock-worker topology reconstructed from the retained
Claude session. The context-only Claude result was approximately 3,016 requests/second.
The difference is small and no series noise floor was measured, so treat this as parity,
not a statistically established improvement.

| Orchestrator | RPS | Completed | Errors |
|---|---:|---:|---:|
| AGW static | 3,023.40 | 136,074 | 0 |
| AGW generic | 1,717.79 | 77,808 | 0 |
| Envoy generic | 1,601.23 | 72,716 | 0 |
| Envoy generic with callouts | 2,125.79 | 96,246 | 0 |

The critical comparability fix was pinning `DYN_TOKENIZER=fastokens` with fallback
disabled. The earlier facade run used Dynamo's correct but slower default HuggingFace
backend and reached only 1,914.56 requests/second. Generic variants also carry token IDs
as one opaque little-endian byte field instead of materializing thousands of dynamic JSON
numbers; that changed AGW generic from 789.50 to 1,717.79 requests/second and Envoy
callouts from 351.66 to 2,125.79 requests/second.

These are prototype results. See `benchmark_audit.json`: full per-request AIPerf exports
were not persisted, so the matrix does not meet this repository's decision-grade audit
standard.
