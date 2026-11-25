# Scheduler multi-seed evaluation (issue #5)

- seeds: 20 (base 2026), jobs/seed: 120, workers: 3
- trace params: ttft 2.937s, decode 0.602s | aging_cap 12s, rescue_window 35s, min_rescue 40000 tokens

**semantics**: `legacy` = issue-#5 baseline (no TTL renewal); `renewal` = sliding-TTL write-back on every run; `renewal_guarded` = renewal + guardrails (only session_guarded_dual).

## Regime: normal

### semantics: legacy

| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |
|---|---|---|---|---|---|---|---|---|
| dual_cache_ttl_aware | 100.000 ± 0.000 | 1.950 ± 1.293 | 102.270 ± 2.400 | 44.468 ± 1.572 | 100.000 ± 0.000 | 1446.430 ± 24.733 | 71.458 ± 1.595 | 43.709 ± 0.096 |
| fifo | 14.500 ± 2.643 | 23.750 ± 0.802 | 130.456 ± 3.864 | 123.245 ± 3.866 | 63.358 ± 1.132 | 4681.660 ± 112.848 | 39.542 ± 2.149 | 35.998 ± 0.190 |
| priority_aging | 16.334 ± 1.887 | 0.000 ± 0.000 | 128.360 ± 3.714 | 34.109 ± 0.642 | 64.144 ± 0.808 | 4612.935 ± 83.637 | 34.584 ± 2.523 | 36.482 ± 0.166 |
| warm_env_affinity | 17.667 ± 1.901 | 0.000 ± 0.000 | 123.207 ± 3.737 | 33.679 ± 0.631 | 64.715 ± 0.813 | 4563.515 ± 83.060 | 81.917 ± 1.799 | 37.605 ± 0.160 |

### semantics: legacy_guarded

| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |
|---|---|---|---|---|---|---|---|---|
| session_guarded_dual | 99.667 ± 0.480 | 0.050 ± 0.105 | 101.539 ± 2.223 | 40.913 ± 1.045 | 99.857 ± 0.206 | 1455.645 ± 23.019 | 77.666 ± 1.858 | 43.912 ± 0.147 |

### semantics: renewal

| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |
|---|---|---|---|---|---|---|---|---|
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 87.342 ± 3.330 | 31.741 ± 0.769 | 100.000 ± 0.000 | 776.605 ± 14.169 | 78.707 ± 1.787 | 46.785 ± 0.151 |
| fifo | 93.166 ± 1.788 | 20.100 ± 1.279 | 92.979 ± 3.382 | 87.878 ± 3.393 | 97.070 ± 0.766 | 1067.570 ± 68.495 | 84.083 ± 2.210 | 44.833 ± 0.227 |
| priority_aging | 95.168 ± 1.289 | 0.000 ± 0.000 | 92.146 ± 3.327 | 31.566 ± 0.588 | 97.926 ± 0.553 | 989.810 ± 51.001 | 45.876 ± 1.728 | 45.074 ± 0.190 |
| warm_env_affinity | 95.667 ± 1.144 | 0.000 ± 0.000 | 88.703 ± 3.318 | 30.554 ± 0.519 | 98.141 ± 0.490 | 984.380 ± 49.661 | 84.458 ± 1.525 | 46.341 ± 0.136 |

### semantics: renewal_guarded

| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |
|---|---|---|---|---|---|---|---|---|
| session_guarded_dual | 99.834 ± 0.348 | 0.000 ± 0.000 | 88.022 ± 3.143 | 30.719 ± 0.729 | 99.929 ± 0.150 | 755.895 ± 17.909 | 75.874 ± 1.342 | 46.653 ± 0.114 |

## Regime: overload

### semantics: legacy

| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |
|---|---|---|---|---|---|---|---|---|
| dual_cache_ttl_aware | 99.667 ± 0.480 | 24.300 ± 1.401 | 123.539 ± 1.831 | 59.575 ± 1.599 | 99.857 ± 0.206 | 1455.990 ± 31.164 | 84.499 ± 1.008 | 44.090 ± 0.151 |
| fifo | 10.499 ± 1.456 | 27.400 ± 0.535 | 159.069 ± 1.854 | 150.672 ± 1.865 | 61.645 ± 0.625 | 4834.970 ± 64.971 | 36.334 ± 1.998 | 35.716 ± 0.138 |
| priority_aging | 13.167 ± 1.474 | 0.000 ± 0.000 | 156.455 ± 2.023 | 39.316 ± 1.154 | 62.788 ± 0.632 | 4727.270 ± 72.861 | 31.501 ± 1.935 | 36.217 ± 0.153 |
| warm_env_affinity | 13.667 ± 1.330 | 0.000 ± 0.000 | 151.685 ± 1.982 | 37.629 ± 1.103 | 63.003 ± 0.570 | 4716.220 ± 75.537 | 84.833 ± 1.032 | 37.443 ± 0.169 |

### semantics: legacy_guarded

| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |
|---|---|---|---|---|---|---|---|---|
| session_guarded_dual | 83.334 ± 3.037 | 6.300 ± 0.730 | 130.470 ± 2.867 | 65.805 ± 2.569 | 92.858 ± 1.301 | 2028.420 ± 126.930 | 76.333 ± 1.365 | 42.466 ± 0.332 |

### semantics: renewal

| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |
|---|---|---|---|---|---|---|---|---|
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 113.853 ± 2.095 | 35.155 ± 0.904 | 100.000 ± 0.000 | 726.200 ± 17.467 | 80.917 ± 1.278 | 46.998 ± 0.088 |
| fifo | 90.333 ± 1.422 | 24.750 ± 0.501 | 121.364 ± 1.670 | 114.859 ± 1.649 | 95.855 ± 0.609 | 1179.890 ± 55.933 | 84.583 ± 1.700 | 44.508 ± 0.150 |
| priority_aging | 93.000 ± 1.423 | 0.000 ± 0.000 | 120.166 ± 1.866 | 34.535 ± 0.889 | 96.998 ± 0.610 | 1072.185 ± 59.388 | 42.375 ± 1.641 | 44.746 ± 0.176 |
| warm_env_affinity | 94.000 ± 1.396 | 0.000 ± 0.000 | 115.513 ± 1.681 | 33.422 ± 0.758 | 97.426 ± 0.598 | 1069.205 ± 57.196 | 85.958 ± 1.305 | 46.210 ± 0.166 |

### semantics: renewal_guarded

| policy | rescue rate % | starved (>45s) | FG p95 JCT s | BG max wait s | hit rate % | cost units | warm reuse % | throughput/min |
|---|---|---|---|---|---|---|---|---|
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 114.410 ± 2.115 | 34.666 ± 1.022 | 100.000 ± 0.000 | 712.565 ± 10.985 | 81.209 ± 0.795 | 47.034 ± 0.125 |

## Acceptance (session_guarded_dual, renewal_guarded, overload)

- rescue rate: 100.000 ± 0.000 (need >= 95%): PASS
- starved max across seeds: 0 (need = 0): PASS
- **overall: PASS**
