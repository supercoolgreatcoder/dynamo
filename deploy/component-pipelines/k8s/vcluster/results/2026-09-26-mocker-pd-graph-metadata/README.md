# Generic P/D graph runtime-metadata correction

The generic disaggregated graph previously sent packed tokens and backend
JSON to prefill but omitted `prompt_tokens` and the multimodal token/count
fields that the static graph already forwarded. Dynamo commit `021ad0bcd4`
adds the bindings and declares them in `generateRaw`'s worker schema. The
generic-core suite passed 94 tests, including a contract assertion that
prefill and decode use the same expressions for those fields.

Only the graph and `chat-worker.yaml` keys of the `dynamo-pd-contracts`
ConfigMap were updated, inside the `gateway-poc` vCluster namespace
`dynamo-components-v2`. The installed worker schema SHA256 matched source:
`a9a78bf60ce815e05ea52c1d57a5e4202b086ccd290c4438adbac420fda79215`.
The installed graph SHA256 was
`8bcfb6b75f68dfb38369e2dbc8ae9e4822ff04ee6d2972e1cabe29df78a003cc`
after substituting the P/D service names. A streamed P/D smoke passed; with
the opt-in prefill diagnostic set to sample every request, the `GenerateRaw`
handler recorded `prompt_tokens=32` for the smoke request. That directly
confirms the field reached the gRPC facade. It does not establish behavior
for multimodal inputs or real GPU workers.

One six-client 45-second ISL4000 mock-worker run then used the same frozen
raw dataset as the prior generic diagnostic, SHA256
`3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`.
All six AIPerf 0.12.0 clients completed without errors or cancellation;
the Kubernetes Job succeeded 6/6 with clients split 3/3 across the two
planned CPU nodes.

| Generic graph | Job | Requests | Sum of exported client RPS |
| --- | --- | ---: | ---: |
| Before correction | `nixpd-isl4000-pd-agw-generic-r72` | 423,983 | 9,401.88 |
| Corrected | `nixpd-isl4000-pd-agw-generic-r73` | 433,193 | 9,609.23 |

The corrected graph is within the prior throughput range, but these are
single runs, not an interleaved A/B; the 2.2% difference is not claimed as
an improvement. The six raw AIPerf exports, Job and Pod snapshots are
retained here. AIPerf collection initially failed because the runner's
default store-stager Pod had completed. The Job itself succeeded; its exports
were recovered from the shared store through the Running
`dynamo-component-store-stager-pool-r1` Pod without a rerun. The runner now
checks stager readiness before creating a Job. For future trials, set
`NIX_STAGER_POD` to a Running, Ready Pod and use a fresh trial ID.

After the smoke, the one generic gateway was scaled to zero and the four
prefill replicas were restored to the pinned default facade. The corrected
ConfigMap remains installed for the next graph test.
