<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Historical P/D control — paused before completion

This is a **partial** same-day control for the rebased P/D mocker matrix. The
operator requested a pause after two short and one ISL4000 Jobs. The original
[plan](benchmark_plan.json) reserved three trials per workload, but the
unstarted Jobs were not launched. Do not quote a three-run median or call this
a completed A/B campaign.

The reference P/D fixture was reconstructed from the historical source
`995c04e74f785a498470f305d37875703fb72b86`: its facade package was
`/nix/store/w77ac5nlyc9543kgkwdxpf9a7yk5l7y6-dynamo-component-facade-1.6.0-995c04e74f`
and its Envoy/Agentgateway bundle was
`/nix/store/d9n60x9aylvjvj9j56654640xshsai1x-dynamo-component-pipelines-accf6af699`.
The old disaggregated graph from that exact Git revision was mounted after
replacing only the three service names with their P/D fixture names. Both the
transformed source and the live ConfigMap hashed to
`84e242ccc5769538a00fe6f9f2cb1c9e5b4e7d624f2b77b3d04786a58cf15002`.
The old Envoy callout host had 16 clusters matching the Ready decode Pod IPs;
its streamed prefill→decode smoke test passed.

This correction matters: a first attempted control mistakenly used the old
aggregate bundle as the facade, but it lacked `--benchmark-mode`. After
restoring the rebased fixture, the correct historical facade was deployed.
The first smoke then failed because the current graph contained newly added
multimodal fields that the old Envoy module could not encode. Restoring the
historical graph made the smoke pass. **Neither failed setup produced a
benchmark result**; only the three Jobs below were measured.

The 18 retained AIPerf 0.12.0 summary exports report zero errors or
cancellations. Each completed Job had six successful Pods, placed three on
each of the same two CPU nodes used by the rebased matrix; see
[`benchmark_execution.json`](benchmark_execution.json). The short/ISL4000
raw payloads and 45-second, concurrency-128-per-client policy were unchanged.

| Completed Job | Summed-client RPS | Requests | Errors |
| --- | ---: | ---: | ---: |
| Short `r120` | 12,027.98 | 543,250 | 0 |
| ISL4000 `r120` | 10,298.69 | 464,447 | 0 |
| Short `r121` | 11,670.49 | 526,488 | 0 |

For context, the rebased Envoy callout's **three-run medians** were 10,860.47
short and 9,792.80 ISL4000 summed-client RPS. The two historical short
observations and one ISL4000 observation are higher, but the incomplete
control, changed graph, and different facade binaries do not isolate a single
cause. The remaining planned control Jobs and a new-bundle run using the old
text-only graph would be the next tests when work resumes.

Only the vCluster API `https://gateway-poc.mkhadkevich-dev:443`, namespace
`dynamo-components-v2`, was used. The experiment loop was stopped; the last
already-started Job was allowed to finish and its raw exports were collected.
No benchmark Jobs remained active when the temporary P/D gateway, preprocessor,
selector, prefill, and decode Deployments were scaled to zero. The historical
fixture configuration remains in the vCluster but is inactive; a resumed run
must reapply the intended bundle and refresh Envoy's Pod-IP clusters before
measurement. Real vLLM/SGLang correctness on the new bundle was not run
before this pause.
