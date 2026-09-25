<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Nix facade 846821 mocker recheck

This run rechecks the frozen short, ISL4000, and Mooncake mocker workloads
after rolling `/nix/store/j7n9hn14s34c2595q1nr4h9w0055y24x-dynamo-component-pipelines-8468212130`
into the existing vCluster benchmark topology. Its gateway/Envoy binaries are
unchanged from the earlier Nix parity bundle; the Dynamo facade is now pinned
to `8468212130`, which adds native-worker metadata publication outside the
mocker request path.

The runner uses the same six AIPerf 0.12.0 clients, two CPU nodes, concurrency
128 per client, 45-second measured interval, 90-second shared start barrier,
frozen raw OpenAI bodies for short/ISL4000, and prepared Mooncake mmap as the
previous campaign. The measured clients do not synthesize or tokenize prompts
on the hot path. The four gateway arms share four `fastokens` preprocessors,
one selector, and 16 synthetic workers; one gateway replica is active per cell.
The `r20` short pass is diagnostic-only: a separate real-model AGW Pod was
scheduled on one AIPerf node during that pass. After all four `r20` Jobs
finished, the four real-model correctness Deployments were scaled to zero and
their Pods were confirmed gone. The clean comparison uses three interleaved
repeats `r21`–`r23` per gateway/workload. Raw six-client summary
exports are collected under `raw_aiperf/` and are accepted only if all six
exports are present, error-free, and not cancelled.

The remaining workloads will be added after their Jobs complete. These
closed-loop achieved-throughput numbers do not by themselves prove the
single-gateway saturation ceiling. Mooncake's trace offers roughly 3,026 RPS.

## Short workload: completed clean series

All 12 clean `r21`–`r23` short Jobs completed with exactly six AIPerf 0.12.0
JSON/CSV/console exports each, configured concurrency 128 per client and
45-second measured phases. Every export reports zero request errors and no
cancellation. The median is the middle of the three aggregate Job RPS values,
not a mean of clients or a median of all individual requests.

| Gateway | Clean three-run median RPS | Range RPS | Saved full-bundle point RPS |
|---|---:|---:|---:|
| AGW static | 11,783.14 | 11,719.53–12,163.33 | 12,279.18 |
| AGW generic | 11,733.00 | 11,655.38–12,111.29 | 11,960.96 |
| Envoy generic/direct | 11,913.69 | 11,905.58–12,008.33 | 12,186.33 |
| Envoy generic/callouts | 12,321.92 | 12,066.04–12,554.56 | 12,433.44 |

The saved points are from the previous
[full-bundle matrix](../2026-09-25-full-bundle-all-arms/README.md), mostly one
run per arm; they are not paired three-run controls. For example, the current
callout `r23` result exceeds its saved point, while the earlier Envoy-direct
series already ranged from about 10.3k to 12.2k RPS. The current static median
is roughly 4.0% below its saved point, but its `r23` is within 1%; this
descriptive gap does not establish a facade regression. The diagnostic `r20`
pass also has six valid exports per arm but is excluded because an unrelated
real-model AGW shared one AIPerf node at the time. That fixture was scaled to
zero and its Pods were confirmed gone before the clean series.

The saved versus current callout short exports show lower mean request latency
in the current `r21` run (17.31 vs. 17.80 ms), despite lower achieved
throughput; effective in-flight concurrency was also lower (209 vs. 222).
These are closed-loop achieved-throughput observations, not a measured gateway
ceiling or a causal old/new binary A–B. ISL4000 and Mooncake rechecks remain
pending.
