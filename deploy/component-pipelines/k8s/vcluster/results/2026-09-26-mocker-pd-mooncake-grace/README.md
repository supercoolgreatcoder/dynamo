<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Mooncake completion-grace diagnostic

The original 45-second fixed-schedule Mooncake replay ended with 136,074 successful requests out of 136,194 scheduled, despite zero AIPerf errors or cancellations. This separate, immutable [46-second plan](benchmark_plan.json) preserves the same 22,699 trace timestamps per client, six clients, prebuilt mmap cache, source dataset SHA-256, gateway, component counts, and synthetic P/D workers. Only the AIPerf phase duration changes to allow boundary-timestamp requests to finish. Plan SHA-256: `66375e549497c63ee944eca1c499f959371e339eae78b8fa5762067d9ced458c`.

Job `nixpdg-mooncake-pd-agw-generic-r1` passed the [audit](audit-nixpdg-mooncake-pd-agw-generic-r1.json): six clients completed on the two pinned vCluster nodes, all six used the prebuilt mmap cache without tokenizing during measurement, and all **136,194 / 136,194** scheduled requests succeeded with zero errors or cancellations. Summed-client RPS was **3,024.60**; globally normalized successful RPS was **2,963.39**. These are summary-level descriptive measurements, not per-request percentile evidence.

Reproduce only inside the explicitly checked vCluster with the environment and fixture documented in the [main replay README](../2026-09-26-mocker-pd-generic/README.md): `run-nix-mocker-pd-trial.sh mooncake rN grace`, then `audit-nix-mocker-pd.py <this-directory> mooncake rN --grace`. The wrapper and auditor keep this Job and series distinct from the 45-second reference. The changed measurement window means its RPS is **not** a same-series comparison to the 45-second number. Raw exports are retained on the vCluster NFS and in local ignored `raw_aiperf/`; the committed summary records their SHA-256 hashes.
