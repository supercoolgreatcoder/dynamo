# Mooncake saturation result

The measured zero-error ceiling is about **3.4–3.5k RPS** for the current single-Envoy-callout, single-selector topology. The baseline (4 fastokens preprocessors, 16 mock workers) peaks at **3,446.86 RPS**. A single 32-worker run reached 3,631.49 RPS, but its confirmation fell to 3,464.39 RPS; the two-run mean is 3,547.94 RPS, only 4.8% above the baseline at the same load and inside the campaign's prior ~6% spread.

| AIPerf clients | Offered RPS | Achieved RPS | Baseline result |
|---:|---:|---:|---|
| 6 | 3,026.53 | 3,016.99 | Tracks offered load |
| 8 | 4,035.38 | 3,387.48 | Saturation knee |
| 12 | 6,053.07 | **3,446.86** | Baseline peak |
| 16 | 8,070.76 | 3,385.35 | No gain; latency rises |

At 16 AIPerf clients, doubling preprocessors from 4 to 8 produced 3,474.93 RPS (+2.65%). Doubling workers from 16 to 32 produced 3,631.49 and 3,464.39 RPS (mean 3,547.94, +4.80%). At the 12-client peak, 32 workers produced 3,452.86 RPS (+0.17%). Combining 8 preprocessors and 32 workers produced 3,548.35 RPS. None of these changes establishes a material, reproducible ceiling increase.

To exclude AIPerf replica contention, a reusable doubled-arrival trace was synthesized once. Six clients then offered the same ~6.05k RPS that normally requires twelve clients. All six logged an mmap cache hit and skipped tokenizer/composer work, yet throughput still plateaued at 3,346.98 RPS. The extra AIPerf processes are therefore not the primary ceiling.

All included runs completed with zero request errors and zero cancellations. Saturation appears as backpressure: p50 latency rises from about 53–61 ms at six clients to about 398–413 ms at twelve and 569–573 ms at sixteen.

The experiment does not prove which remaining stage is limiting because Kubernetes CPU metrics were unavailable. Since neither tokenizer nor worker replica doubling materially moves the plateau, the next bounded experiment should scale the still-singleton Envoy callout gateway and then the selector independently. Results use mock workers and describe orchestration capacity, not real-GPU inference capacity.
