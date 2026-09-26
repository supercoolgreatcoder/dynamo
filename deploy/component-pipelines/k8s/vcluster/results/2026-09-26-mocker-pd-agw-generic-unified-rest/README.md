# Unified metadata-correct AGW generic: short and Mooncake

This finishes the three-workload AGW generic P/D matrix for the single-revision
Dynamo `cb970285af` Nix bundle; the ISL4000 result and its plan are in the
adjacent `2026-09-26-mocker-pd-agw-generic-unified` directory. Gateway,
preprocessor, selector, and synthetic P/D worker facade all use the same
bundle. The corrected graph forwards the preprocessor metadata needed by
worker-side Dynamo postprocessing.

Both runs passed the six-client AIPerf 0.12.0 audit with zero errors and
cancellations:

| Workload | Job | Successful requests | Globally normalized requests/s |
| --- | --- | ---: | ---: |
| Short | `nixpd-short-pd-agw-generic-r13` | 499,633 | 10,745.12 |
| Mooncake, 46-second grace | `nixpdg-mooncake-pd-agw-generic-r14` | 136,194 | 3,004.76 |

Mooncake used the frozen raw trace and prebuilt mmap dataset cache. All six
client logs proved cache hits that skipped tokenizer and composer work, and
all 136,194 scheduled requests succeeded. The prior valid generic short run
was 10,856.92 normalized requests/s; the prior valid 46-second Mooncake
grace run was 2,963.39. These single runs are descriptive, not a variance
estimate. Do not compare the 46-second Mooncake RPS with a 45-second series
as a same-series performance delta. Raw AIPerf exports remain local and on
the vCluster benchmark store; the frozen plan, execution records, summaries,
cache proofs, and validation audits are retained here.
