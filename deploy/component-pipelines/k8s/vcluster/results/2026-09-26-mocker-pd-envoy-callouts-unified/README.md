# Unified metadata-correct Envoy callout P/D matrix

The Envoy dynamic module, graph, and facades use Dynamo `cb970285af` from
the same Nix bundle; the Envoy binary retains its separately pinned ABI
patch. One Envoy gateway uses six Envoy workers and eight generic-pipeline
threads. Its 16 static HTTP/2 decode clusters were rendered from the 16
Ready decode Pod IPs and checked against those Pods before each trial. This
is a benchmark snapshot, not production CDS/xDS. Streamed selected-Pod P/D
handoff passed the smoke test before measurement.

All three six-client AIPerf 0.12.0 runs passed the audit with zero errors or
cancellations:

| Workload | Job | Successful requests | Globally normalized requests/s |
| --- | --- | ---: | ---: |
| Short | `nixpdc-short-pd-envoy-callouts-r22` | 540,763 | 11,974.52 |
| ISL4000 | `nixpdc-isl4000-pd-envoy-callouts-r21` | 464,888 | 10,314.11 |
| Mooncake, 46-second grace | `nixpdc-mooncake-pd-envoy-callouts-r23` | 136,194 | 3,022.76 |

Mooncake completed all scheduled trace requests and proved all six prebuilt
mmap cache hits. In these single unpaired runs, callouts led the four
current-pin gateway arms on short and ISL4000; Mooncake was near the
trace-driven ceiling for all arms. These results are descriptive and do not
establish variance or causal attribution. Raw AIPerf exports remain local
and on the vCluster benchmark store; the frozen plan, execution records,
summaries, cache proofs, and audits are retained here.
