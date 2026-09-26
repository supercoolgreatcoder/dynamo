<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# AGW static 16-item batch-cap diagnostic

`nixpds-isl4000-pd-agw-static-r47` tested the same pinned Nix AGW binary,
frozen ISL4000 payload, six-client load, vCluster P/D topology, and opt-in
periodic batch summaries as the audited 32-item summary-on control. Only
`DYN_PREPROCESS_BATCH_MAX` changed from 32 to 16. The synthetic P/D handoff
smoke passed. The raw AIPerf audit is valid: 281,374 successful requests,
zero errors/cancellations, and **6,102.32 globally normalized RPS**. This is
below the three-run 32-item summary-on median of 6,275.40 RPS. One run does
not provide a variance estimate and should not be treated as a final cap
recommendation.

During the busy window (15:12:34-15:13:14 UTC), differences of the cumulative
`batch-summary-*.log` counters give approximately 13.2 items/batch, 2.0 ms
collection time/batch, and 41 ms/batched preprocessing RPC. The comparable
32-item runs yielded approximately 21 items/batch and 3.2 ms collection time.
Smaller batches shortened collection but increased batch count; they did not
lift end-to-end throughput. Collection clocks include async scheduling waits,
not just gateway CPU work. This result motivates an isolated parallel-batcher
experiment, not an inference that the preprocessor service is saturated.

The frozen plan, exact dataset hash, six raw client summaries, execution
record, audit, and gateway batch snapshots are retained here. Reproduce by
deploying the pinned AGW output with `PD_PREPROCESS_BATCH_MAX=16`,
`PD_BATCH_SUMMARY_SECS=10`, and the other settings in `benchmark_plan.json`,
then running `run-nix-mocker-pd-trial.sh isl4000 rN static-batch16` from
`deploy/component-pipelines/k8s/vcluster` inside the vCluster. Use
`audit-nix-mocker-pd.py <this-dir> isl4000 rN --static` to verify the raw
export.
