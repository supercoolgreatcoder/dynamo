# Unified metadata-correct AGW static P/D matrix

The static gateway and all facades used the same Nix bundle built from Dynamo
`cb970285af`. The vCluster fixture had one static gateway, four preprocessors,
four selectors, four prefill workers, and 16 decode workers. Gateway settings
were 12 worker threads and 200 µs preprocessor batch linger. The streamed P/D
handoff smoke test passed before measurement.

All three six-client AIPerf 0.12.0 runs passed the audit with zero errors or
cancellations:

| Workload | Job | Successful requests | Globally normalized requests/s |
| --- | --- | ---: | ---: |
| Short | `nixpds-short-pd-agw-static-r16` | 438,779 | 9,281.66 |
| ISL4000 | `nixpds-isl4000-pd-agw-static-r15` | 269,371 | 5,822.19 |
| Mooncake, 46-second grace | `nixpds-mooncake-pd-agw-static-r17` | 136,194 | 2,947.98 |

The Mooncake run proved all six prebuilt mmap cache hits and completed every
scheduled trace request. For context, the unified generic AGW runs on the
same frozen fixtures reported 10,745.12, 9,370.98, and 3,004.76 normalized
requests/s respectively. Thus static remains substantially behind generic
on ISL4000 despite identical facade and gateway source pins; Mooncake is
close to its trace-driven ceiling in both arms. These are single unpaired
runs, not a variance estimate or causal attribution. Raw AIPerf exports
remain local and on the vCluster benchmark store; the frozen plan, execution
records, summaries, cache proofs, and audits are retained here.
