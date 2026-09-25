<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Stock Dynamo Mooncake empty-response diagnostic

`diag-mooncake-dynamo-records-r1` used the same six-client Mooncake trace,
45-second schedule, stock frontend, and mocker topology as the parity run, but
changed AIPerf's export level from `summary` to `records`. It is diagnostic,
not a performance comparison. The six JSON/CSV/console summaries are in
[`diag-mooncake-dynamo-records-r1/`](diag-mooncake-dynamo-records-r1/). The
larger per-request JSONL files remain on vCluster NFS at
`/shared/aiperf/results/diag-mooncake-dynamo-records-r1/{0..5}/profile_export.jsonl`.

Two of 136,086 attempted requests had `InvalidInferenceResultError`: AIPerf
received no content-bearing response, only metadata/terminal frames. The
failed request IDs were `0ecefdc1-f6d2-4181-8d15-253089c1eb5c`
(`session_016030`) and `5a7ef92a-da55-477b-824e-d082db9ed84b`
(`session_011639`). Their trace rows request 6,966/6,648 input tokens and
one output token, respectively. Both exact rows succeeded with one output
token on each of the other five clients. The trace has 22,699 rows, no
zero-output rows, and 91 one-output rows. This rules out invalid zero-output
input and shows that the failure is intermittent; it does not prove whether
the missing content originated in the mocker, frontend streaming path, or
AIPerf's response parser.

The stock frontend Mooncake parity `r1` had zero errors, but `r2` had one
AIPerf mmap decode error plus one empty-content response, and `r3` had two
empty-content responses. The invalid `r2`/`r3` runs remain in `raw_aiperf/`
and are excluded from valid medians. The four Nix-built gateway arms did not
show this error on the original trace. A focused one-token reproduction or a
raw-response capture is needed before claiming the stock reference is stable.
