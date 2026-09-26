# Corrected Dynamo-backed P/D gateway matrix, 2026-09-26

The gateway graph forwards the Dynamo preprocessor's prompt and multimodal
metadata to the Dynamo worker-side postprocessor. Gateway, facades, and
Envoy's dynamic module are built from Dynamo `cb970285af` by the Nix flake
on `feat/dynamo-component-pipeline-static-pd`; patched Envoy and AGW retain
their independently pinned upstream engine sources. Each gateway arm ran
alone in the vCluster against the same four preprocessor, four selector,
four synthetic prefill, and 16 synthetic decode replicas. All arms passed
streamed P/D handoff smoke tests before measurement.

The table reports globally normalized successful requests/s, not the sum
of client-local throughput. Every cell has a valid six-client AIPerf 0.12.0
audit, zero errors/cancellations, and the frozen dataset SHA. Mooncake used
the prebuilt mmap cache, skipped load-generator tokenization/composition,
proved six cache hits, and completed all 136,194 scheduled trace requests
with the established 46-second completion grace.

| Gateway, one replica | Short | ISL4000 | Mooncake, 46 s |
| --- | ---: | ---: | ---: |
| AGW static | 9,281.66 | 5,822.19 | 2,947.98 |
| AGW generic | 10,745.12 | 9,370.98 | 3,004.76 |
| Envoy generic | 11,720.57 | 8,702.23 | 2,954.93 |
| Envoy generic + callouts | 11,974.52 | 10,314.11 | 3,022.76 |

Evidence and exact plans: [AGW static](2026-09-26-mocker-pd-agw-static-unified/README.md),
[AGW generic ISL4000](2026-09-26-mocker-pd-agw-generic-unified/README.md),
[AGW generic short/Mooncake](2026-09-26-mocker-pd-agw-generic-unified-rest/README.md),
[Envoy generic](2026-09-26-mocker-pd-envoy-generic-unified/README.md), and
[Envoy callouts](2026-09-26-mocker-pd-envoy-callouts-unified/README.md).

These are one-run-per-cell diagnostic measurements, not paired medians or a
statistical performance claim. The 46-second Mooncake grace is not a
same-series RPS comparison to a 45-second prototype trace. Synthetic Dynamo
AsyncEngine workers remove GPU/NIXL capacity from this gateway comparison;
real vLLM/SGLang correctness and GPU serving performance require separate
validation. The retained Claude aggregate-front-end reference uses a
different topology, so it cannot establish direct P/D gateway parity.
Callout decode clusters are a snapshot of Ready worker Pod IPs; production
needs CDS/xDS or equivalent dynamic cluster updates.
