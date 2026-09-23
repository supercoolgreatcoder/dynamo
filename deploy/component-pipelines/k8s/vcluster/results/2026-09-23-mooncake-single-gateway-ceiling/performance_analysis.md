# Single-gateway Mooncake ceiling

The bottleneck is the single Envoy generic-with-callouts gateway, not the selector. With selector/preprocessor/worker replicas scaled to 8/8/32, the best clean result from one gateway pod was **4,671.15 requests/s** at 20 Envoy workers plus 20 callout pipeline threads. All 12 clients hit the prebuilt mmap cache, skipped tokenizer/composer work, and completed with zero errors and cancellations.

## Bottleneck matrix

| Selector | Preprocessor | Worker | Gateway threads | RPS | Errors |
|---:|---:|---:|---:|---:|---:|
| 1 | 4 | 16 | 8+8 | 3,560.91 | 0 |
| 4 | 4 | 16 | 8+8 | 3,432.42 | 0 |
| 8 | 4 | 16 | 8+8 | 3,315.80 | 0 |
| 8 | 8 | 32 | 8+8 | 3,336.88 | 0 |

Selector scaling did not increase throughput. At the final 8+8 topology point, the gateway consumed about 13.13 cores during the request rate while all eight selectors consumed 1.14 cores total. Raising only the gateway to 12+12 increased same-series throughput by 24.60% to 4,157.74 RPS.

## Clean vertical-ceiling matrix

| Gateway threads | RPS | Output tokens/s | Gateway cores | Errors |
|---:|---:|---:|---:|---:|
| 20+20 | **4,671.15** | **803,161.78** | ~24.1 in the exploratory attribution run | 0 |
| 24+24 | 4,565.63 | 784,313.08 | 24.44 | 0 |
| 28+28 | 4,533.34 | 779,432.12 | 24.78 | 0 |

The extra threads after 20+20 do not increase effective CPU utilization materially. They increase CPU per request and reduce throughput, locating the observed one-pod ceiling near 4.6–4.7k RPS on this 32-CPU node.

## Measurement correction

AIPerf 0.12 uses `seek()` followed by `read()` on a shared mmap object in `MemoryMapDatasetClient.get_conversation()`. Six clients with 16 record processors occasionally crossed offsets and emitted load-generator JSON decode errors. Those exploratory runs remain preserved but are excluded from the clean ceiling claim. The corrected series used 12 original-trace clients with one record processor each, which preserves the same 6,053.07 aggregate offered RPS and produced zero errors.

## Uncertainty and next step

The 20+20 lead over 24+24 is 2.26%, and this series has no measured noise floor. The bottleneck attribution is clear, but selecting a production default from that small difference requires two more 20+20 repetitions (n=3 total). After that, independently vary Envoy workers and callout pipeline threads to find which pool can be reduced without losing throughput.
