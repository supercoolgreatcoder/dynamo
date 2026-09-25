<!--
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
-->

# Full Nix bundle: AGW and Envoy-callout mocker matrix

The complete Nix bundle
`/nix/store/g7achknzv9ibixmfdaxgjy4a3pp33dp5-dynamo-component-pipelines-98676215fa`
is staged and running in vCluster namespace `dynamo-components-v2`.
The [Envoy-direct full-bundle campaign](../2026-09-25-full-bundle-parity/README.md)
measured all three frozen workloads and reached 16,263.81 RPS in the
18-client short saturation probe. This follow-up uses the same four
preprocessors, one selector, 16 synthetic workers, frozen short/ISL4000
raw payloads, prepared Mooncake mmap, AIPerf 0.12.0, and six-client 45-second
policy to measure the remaining three orchestrator arms. Exactly one gateway
replica is active at a time. The Envoy-callout worker map was refreshed after
the worker rollout; a two-token streaming smoke request passed before load.

The nine Jobs used AIPerf 0.12.0, six clients at configured concurrency 128,
and an approximately 45-second measurement per client. All 54 AIPerf Pods
completed with zero reported request errors or cancellations. The retained
[`benchmark_execution.json`](benchmark_execution.json) records each Job's
actual command and complete 3/3 client placement across the two selected CPU
nodes. Each Job's six JSON/CSV/console summaries are under
[`raw_aiperf/`](raw_aiperf/).

| Full-bundle gateway arm | Short RPS | ISL4000 RPS | Mooncake RPS |
|---|---:|---:|---:|
| AGW static, packed-token path | 12,279.18 | 9,078.95 | 3,023.38 |
| AGW generic | 11,960.96 | 10,234.04 | 3,023.45 |
| Envoy generic/callouts | 12,433.44 | 11,162.98 | 3,023.54 |
| Envoy generic/direct | 12,186.33 later short confirmation | 9,350.45 | 3,022.64 |

The direct row links to the [separate full-bundle campaign](../2026-09-25-full-bundle-parity/README.md)
and its [preprocessor A–B–A return leg](../2026-09-25-preprocessor-binary-ab/README.md):
the same complete bundle gave 10,272.88–12,186.33 short RPS across three
six-client runs, while its isolated 18-client short run reached 16,263.81
RPS. Direct and callout short/ISL cells were not paired or interleaved; the
six-client series has appreciable temporal variation. In particular, the
callout ISL4000 point is one valid run, not proof of a 20% intrinsic gain over
its earlier 9,255.85 RPS median. AGW generic's ISL4000 point closely matches
its earlier 10,190.12 median; AGW static's packed-token ISL4000 point closely
matches its earlier 8,982.09 median. All Mooncake cells track the trace's
approximately 3,026 RPS offered rate and do not measure a gateway ceiling.
The subsequent [direct/callout ISL4000 crossover](../2026-09-25-isl-callout-direct-crossover/README.md)
measured a smaller approximately 7.4% callout lead against two bracketing
direct runs; matching Envoy worker count did not close it in one probe.

The same verified frozen inputs were used throughout: short SHA256
`ab030551a31fb4ac8e7a864a940862a68bc0d386b152b4aba7bc249322833426`,
ISL4000 SHA256 `3ad382586f5417f4255d7eea774700d9817bc39acbc7b537e2efa7d89145c743`,
and normalized Mooncake trace SHA256
`28a94e9bc1b63fdc88e5217bdd5c647a0487c08c83bdc64a814ec0840562f550`.
Short/ISL4000 replay frozen raw OpenAI payloads; Mooncake uses its prepared
content-addressed mmap. The benchmark clients do no prompt synthesis in the
measured phase. AIPerf exported summaries only, so per-request records and
actual output-length distributions are unavailable for a promotion-grade
SLO audit. The stock Dynamo frontend uses `dynamo.mocker`, whereas these
gateway arms use the facade's synthetic `ServiceEngine`; its older number is
an architectural reference, not an identical-backend A/B.
