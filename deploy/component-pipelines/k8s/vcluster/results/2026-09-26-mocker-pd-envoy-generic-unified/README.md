# Unified metadata-correct Envoy generic P/D matrix

The Envoy dynamic module, gateway graph, and all P/D facades use Dynamo
`cb970285af`; the Envoy binary retains its separately pinned ABI patch.
The vCluster fixture had one Envoy gateway, four preprocessors, four
selectors, four prefill workers, and 16 decode workers. Streamed P/D handoff
passed the smoke test before measurement.

All three six-client AIPerf 0.12.0 runs passed the audit with zero errors or
cancellations:

| Workload | Job | Successful requests | Globally normalized requests/s |
| --- | --- | ---: | ---: |
| Short | `nixpde-short-pd-envoy-generic-r19` | 528,720 | 11,720.57 |
| ISL4000 | `nixpde-isl4000-pd-envoy-generic-r18` | 392,308 | 8,702.23 |
| Mooncake, 46-second grace | `nixpde-mooncake-pd-envoy-generic-r20` | 136,194 | 2,954.93 |

Mooncake completed every scheduled trace request and proved all six
prebuilt mmap cache hits. For context, the same-revision AGW generic matrix
reported 10,745.12, 9,370.98, and 3,004.76 normalized requests/s. Envoy
is ahead on short prompts but behind on ISL4000 in these single runs;
Mooncake is trace-paced and close in both arms. This is not a variance
estimate or causal attribution. Raw AIPerf exports remain local and on the
vCluster benchmark store; the frozen plan, execution records, summaries,
cache proofs, and audits are retained here.

A later ISL4000 diagnostic, `nixpde-isl4000-pd-envoy-generic-r22`, enabled
gateway stage counters and passed the same six-client audit with 403,365
successful requests, zero errors or cancellations, and 8,943.45 globally
normalized requests/s. This is a diagnostic run, not an additional matrix
sample or a new ceiling estimate; the per-stage gateway log was not
retained in this result directory.
