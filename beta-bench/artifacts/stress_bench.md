# Scheduler stress benchmark (issue #5)

- seeds/cell: 20 (base 7700), jobs: 240 stress / 120 baseline, workers per cell
- trace: ttft 2.937s, decode 0.602s | aging_cap 12s, min_rescue 40000 tokens
- semantics: sliding-TTL renewal for all policies; session_guarded_dual + guardrails
- acceptance dual-constraint: rescue rate >= 95% AND starved(>45s) == 0

## Scenario matrix

| id | story | knobs | workers | offered/min (measured) |
|---|---|---|---|---|
| S0_normal | issue-#5 baseline generator, cross-check vs sched_eval | baseline trace, overload=false | 3 | 137.0 |
| S0_overload | issue-#5 baseline generator, cross-check vs sched_eval | baseline trace, overload=true | 3 | 238.4 |
| S1_load_0.5x | arrival-rate sweep -> saturation curve | mean_gap 2.73s (~22 jobs/min offered) | 3 | 22.4 |
| S1_load_0.75x | arrival-rate sweep -> saturation curve | mean_gap 1.88s (~32 jobs/min offered) | 3 | 32.5 |
| S1_load_1.0x | arrival-rate sweep -> saturation curve | mean_gap 1.40s (~43 jobs/min offered) | 3 | 43.6 |
| S1_load_1.25x | arrival-rate sweep -> saturation curve | mean_gap 1.11s (~54 jobs/min offered) | 3 | 55.0 |
| S1_load_1.5x | arrival-rate sweep -> saturation curve | mean_gap 0.92s (~65 jobs/min offered) | 3 | 66.4 |
| S1_load_2.0x | arrival-rate sweep -> saturation curve | mean_gap 0.70s (~86 jobs/min offered) | 3 | 87.2 |
| S2_burst_storm | clustered bursts (one user action fans out) at matched mean rate | mean_gap 1.40s, bursts of 3-8 jobs | 3 | 45.3 |
| S3_hot_expiry_storm | rescue-pressure extreme: most fg prefixes near expiry | mean_gap 1.40s, hot_share 0.60 | 3 | 41.3 |
| S4_bg_flood | batch ingestion floods the pool, fairness/guardrail under test | mean_gap 1.40s, bg_share 0.55 | 3 | 42.3 |
| S5_short_ttl_60 | TTL sensitivity: rescue window compressed, renewal races tighten | mean_gap 1.40s, ttl 60s, rescue_window 12s | 3 | 43.6 |
| S6a_worker_scarcity_2 | capacity shrinks below demand | mean_gap 1.88s (~32/min), workers=2 | 2 | 32.5 |
| S6b_worker_scale_6 | capacity doubles, pressure scales with it | mean_gap 0.70s (~86/min), workers=6 | 6 | 87.2 |
| S7_tenant_skew | multi-tenant fairness: hot Space vs cold Spaces | mean_gap 1.40s, tenant_skew 0.70 | 3 | 49.8 |

## S1 load sweep — saturation curves

### starved jobs (>45s), mean

| load cell | fifo | priority_aging | warm_env_affinity | dual_cache_ttl_aware | session_guarded_dual |
|---|---|---|---|---|---|
| S1_load_0.5x (~22/min) | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| S1_load_0.75x (~32/min) | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| S1_load_1.0x (~44/min) | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| S1_load_1.25x (~55/min) | 29.2 | 0.0 | 0.0 | 0.0 | 0.0 |
| S1_load_1.5x (~66/min) | 51.4 | 0.1 | 0.0 | 0.0 | 0.0 |
| S1_load_2.0x (~87/min) | 60.5 | 6.1 | 6.2 | 7.2 | 0.8 |

### rescue rate %

| load cell | fifo | priority_aging | warm_env_affinity | dual_cache_ttl_aware | session_guarded_dual |
|---|---|---|---|---|---|
| S1_load_0.5x (~22/min) | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 |
| S1_load_0.75x (~32/min) | 100.0 | 100.0 | 100.0 | 100.0 | 100.0 |
| S1_load_1.0x (~44/min) | 98.3 | 99.1 | 99.5 | 100.0 | 100.0 |
| S1_load_1.25x (~55/min) | 89.8 | 90.5 | 92.0 | 100.0 | 100.0 |
| S1_load_1.5x (~66/min) | 85.9 | 86.7 | 87.8 | 100.0 | 100.0 |
| S1_load_2.0x (~87/min) | 83.8 | 84.2 | 84.2 | 100.0 | 99.9 |

### FG p95 JCT s

| load cell | fifo | priority_aging | warm_env_affinity | dual_cache_ttl_aware | session_guarded_dual |
|---|---|---|---|---|---|
| S1_load_0.5x (~22/min) | 6.1 | 5.8 | 5.7 | 5.7 | 5.9 |
| S1_load_0.75x (~32/min) | 8.8 | 7.2 | 7.1 | 7.1 | 8.4 |
| S1_load_1.0x (~44/min) | 20.2 | 18.2 | 16.1 | 16.0 | 19.8 |
| S1_load_1.25x (~55/min) | 65.8 | 71.4 | 62.5 | 57.6 | 60.8 |
| S1_load_1.5x (~66/min) | 109.0 | 115.1 | 107.0 | 100.2 | 102.5 |
| S1_load_2.0x (~87/min) | 159.4 | 160.4 | 152.5 | 144.9 | 146.0 |

### cost units

| load cell | fifo | priority_aging | warm_env_affinity | dual_cache_ttl_aware | session_guarded_dual |
|---|---|---|---|---|---|
| S1_load_0.5x (~22/min) | 1797 | 1797 | 1797 | 1797 | 1797 |
| S1_load_0.75x (~32/min) | 1720 | 1720 | 1720 | 1720 | 1720 |
| S1_load_1.0x (~44/min) | 1809 | 1757 | 1724 | 1696 | 1694 |
| S1_load_1.25x (~55/min) | 2426 | 2376 | 2271 | 1680 | 1680 |
| S1_load_1.5x (~66/min) | 2685 | 2639 | 2561 | 1668 | 1662 |
| S1_load_2.0x (~87/min) | 2833 | 2809 | 2813 | 1623 | 1621 |

## Per-scenario scorecards

### S0_normal — issue-#5 baseline generator, cross-check vs sched_eval

(knobs: baseline trace, overload=false, workers: 3)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 91.499 ± 1.558 | 20.600 ± 1.265 | 92.961 ± 4.450 | 95.453 ± 4.545 | 87.027 ± 4.266 | 96.355 ± 0.667 | 1131.415 ± 62.443 | 84.374 ± 2.896 | FAIL |
| priority_aging | 95.001 ± 1.075 | 0.000 ± 0.000 | 91.718 ± 4.031 | 94.299 ± 4.223 | 31.787 ± 0.646 | 97.855 ± 0.461 | 997.225 ± 40.661 | 46.708 ± 2.175 | PASS |
| warm_env_affinity | 95.168 ± 1.072 | 0.000 ± 0.000 | 88.101 ± 3.991 | 91.461 ± 4.023 | 31.627 ± 0.795 | 97.926 ± 0.459 | 1002.485 ± 47.294 | 83.374 ± 1.704 | PASS |
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 86.605 ± 3.905 | 90.076 ± 3.796 | 31.586 ± 0.603 | 100.000 ± 0.000 | 776.400 ± 14.555 | 77.666 ± 1.511 | PASS |
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 86.952 ± 3.532 | 90.370 ± 3.778 | 30.365 ± 0.712 | 100.000 ± 0.000 | 747.540 ± 13.596 | 76.124 ± 1.316 | PASS |

### S0_overload — issue-#5 baseline generator, cross-check vs sched_eval

(knobs: baseline trace, overload=true, workers: 3)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 89.333 ± 1.568 | 25.000 ± 0.626 | 121.266 ± 2.563 | 124.494 ± 2.630 | 114.435 ± 2.558 | 95.428 ± 0.671 | 1217.465 ± 64.092 | 83.457 ± 2.676 | FAIL |
| priority_aging | 91.832 ± 1.288 | 0.000 ± 0.000 | 120.143 ± 2.509 | 123.781 ± 2.595 | 34.325 ± 1.198 | 96.498 ± 0.552 | 1122.415 ± 53.825 | 43.208 ± 1.626 | FAIL |
| warm_env_affinity | 92.832 ± 1.047 | 0.000 ± 0.000 | 115.583 ± 2.661 | 120.268 ± 2.547 | 32.891 ± 1.115 | 96.926 ± 0.449 | 1094.530 ± 39.268 | 84.292 ± 1.061 | FAIL |
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 112.842 ± 2.457 | 117.282 ± 2.447 | 34.560 ± 1.093 | 100.000 ± 0.000 | 712.065 ± 8.533 | 79.000 ± 1.658 | PASS |
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 113.367 ± 2.437 | 116.372 ± 2.427 | 34.445 ± 1.389 | 100.000 ± 0.000 | 711.630 ± 8.481 | 81.459 ± 0.838 | PASS |

### S2_burst_storm — clustered bursts (one user action fans out) at matched mean rate

(knobs: mean_gap 1.40s, bursts of 3-8 jobs, workers: 3)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 92.332 ± 2.652 | 6.950 ± 7.291 | 45.457 ± 9.131 | 49.965 ± 9.280 | 46.282 ± 9.880 | 97.052 ± 1.014 | 2233.645 ± 192.522 | 32.562 ± 1.291 | FAIL |
| priority_aging | 92.751 ± 2.792 | 0.000 ± 0.000 | 48.869 ± 10.652 | 52.588 ± 10.464 | 26.100 ± 2.654 | 97.230 ± 1.071 | 2197.715 ± 201.772 | 33.313 ± 1.371 | FAIL |
| warm_env_affinity | 93.540 ± 3.064 | 0.000 ± 0.000 | 45.885 ± 10.355 | 50.352 ± 10.246 | 25.620 ± 2.609 | 97.552 ± 1.155 | 2132.085 ± 210.487 | 62.916 ± 1.964 | FAIL |
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 44.058 ± 8.683 | 48.113 ± 8.493 | 25.832 ± 2.271 | 100.000 ± 0.000 | 1701.810 ± 56.940 | 60.876 ± 2.407 | PASS |
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 47.015 ± 8.451 | 52.037 ± 8.536 | 23.255 ± 2.440 | 100.000 ± 0.000 | 1696.920 ± 57.276 | 60.458 ± 1.898 | PASS |

### S3_hot_expiry_storm — rescue-pressure extreme: most fg prefixes near expiry

(knobs: mean_gap 1.40s, hot_share 0.60, workers: 3)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 98.338 ± 0.820 | 0.000 ± 0.000 | 18.738 ± 2.800 | 20.815 ± 2.763 | 17.113 ± 2.752 | 98.944 ± 0.522 | 2065.135 ± 112.587 | 32.917 ± 1.172 | PASS |
| priority_aging | 99.409 ± 0.474 | 0.000 ± 0.000 | 15.874 ± 3.227 | 18.417 ± 3.389 | 16.424 ± 0.953 | 99.623 ± 0.302 | 1935.055 ± 87.767 | 33.667 ± 1.018 | PASS |
| warm_env_affinity | 99.468 ± 0.384 | 0.000 ± 0.000 | 14.099 ± 2.610 | 17.692 ± 3.167 | 16.685 ± 1.014 | 99.658 ± 0.243 | 1926.100 ± 81.107 | 50.333 ± 3.029 | PASS |
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 14.540 ± 2.662 | 18.180 ± 2.619 | 17.383 ± 1.155 | 100.000 ± 0.000 | 1860.545 ± 50.392 | 47.770 ± 2.564 | PASS |
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 18.143 ± 2.945 | 22.242 ± 3.151 | 14.918 ± 1.534 | 100.000 ± 0.000 | 1860.545 ± 50.392 | 47.604 ± 2.304 | PASS |

### S4_bg_flood — batch ingestion floods the pool, fairness/guardrail under test

(knobs: mean_gap 1.40s, bg_share 0.55, workers: 3)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 95.699 ± 2.994 | 0.000 ± 0.000 | 19.499 ± 4.235 | 21.634 ± 4.465 | 18.204 ± 4.275 | 98.261 ± 1.248 | 1576.380 ± 163.802 | 33.334 ± 1.794 | PASS |
| priority_aging | 97.471 ± 2.246 | 0.000 ± 0.000 | 16.362 ± 6.223 | 18.862 ± 6.561 | 17.248 ± 1.861 | 99.003 ± 0.882 | 1493.210 ± 129.102 | 32.186 ± 1.382 | PASS |
| warm_env_affinity | 97.924 ± 2.128 | 0.000 ± 0.000 | 14.294 ± 5.524 | 17.674 ± 6.259 | 16.859 ± 1.766 | 99.173 ± 0.842 | 1474.285 ± 132.478 | 45.813 ± 3.168 | PASS |
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 14.040 ± 5.038 | 16.942 ± 5.494 | 17.190 ± 1.661 | 100.000 ± 0.000 | 1377.380 ± 81.287 | 44.500 ± 2.604 | PASS |
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 16.416 ± 4.879 | 19.881 ± 5.261 | 16.651 ± 1.761 | 100.000 ± 0.000 | 1377.995 ± 80.906 | 45.625 ± 2.978 | PASS |

### S5_short_ttl_60 — TTL sensitivity: rescue window compressed, renewal races tighten

(knobs: mean_gap 1.40s, ttl 60s, rescue_window 12s, workers: 3)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 49.849 ± 4.905 | 4.600 ± 3.794 | 42.242 ± 6.581 | 45.216 ± 6.807 | 40.225 ± 6.706 | 76.070 ± 4.176 | 6078.310 ± 543.662 | 32.396 ± 1.650 | FAIL |
| priority_aging | 62.050 ± 6.710 | 0.000 ± 0.000 | 38.400 ± 7.352 | 41.687 ± 7.517 | 22.404 ± 1.892 | 81.524 ± 4.647 | 5169.150 ± 653.090 | 33.688 ± 0.916 | FAIL |
| warm_env_affinity | 66.820 ± 5.228 | 0.000 ± 0.000 | 30.756 ± 6.027 | 34.878 ± 6.374 | 21.727 ± 1.592 | 85.082 ± 3.435 | 4707.875 ± 541.412 | 56.938 ± 2.907 | FAIL |
| dual_cache_ttl_aware | 95.574 ± 1.009 | 0.000 ± 0.000 | 20.716 ± 3.068 | 25.048 ± 3.471 | 20.548 ± 1.220 | 98.226 ± 0.418 | 2516.615 ± 90.521 | 48.040 ± 2.190 | PASS |
| session_guarded_dual | 93.588 ± 1.265 | 0.000 ± 0.000 | 26.206 ± 3.634 | 32.347 ± 4.762 | 18.327 ± 1.495 | 97.219 ± 0.647 | 2653.520 ± 102.760 | 47.812 ± 1.503 | FAIL |

### S6a_worker_scarcity_2 — capacity shrinks below demand

(knobs: mean_gap 1.88s (~32/min), workers=2, workers: 2)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 89.055 ± 2.366 | 15.800 ± 7.575 | 56.522 ± 6.317 | 59.675 ± 6.275 | 55.633 ± 6.466 | 95.594 ± 1.092 | 2518.175 ± 222.833 | 32.271 ± 1.609 | FAIL |
| priority_aging | 89.735 ± 2.720 | 0.000 ± 0.000 | 62.724 ± 7.755 | 66.995 ± 7.891 | 28.761 ± 1.898 | 95.815 ± 1.267 | 2473.940 ± 252.270 | 32.125 ± 1.691 | FAIL |
| warm_env_affinity | 91.243 ± 2.208 | 0.000 ± 0.000 | 52.804 ± 6.865 | 57.578 ± 6.774 | 28.287 ± 1.620 | 96.450 ± 1.000 | 2351.410 ± 203.792 | 59.417 ± 1.705 | FAIL |
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 48.014 ± 6.536 | 52.913 ± 6.893 | 27.260 ± 1.759 | 100.000 ± 0.000 | 1715.745 ± 56.296 | 56.291 ± 1.275 | PASS |
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 57.548 ± 7.398 | 64.665 ± 7.771 | 19.036 ± 2.414 | 100.000 ± 0.000 | 1709.200 ± 56.447 | 52.249 ± 1.402 | PASS |

### S6b_worker_scale_6 — capacity doubles, pressure scales with it

(knobs: mean_gap 0.70s (~86/min), workers=6, workers: 6)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 100.000 ± 0.000 | 0.000 ± 0.000 | 12.035 ± 1.244 | 13.214 ± 1.324 | 9.380 ± 1.187 | 100.000 ± 0.000 | 1696.015 ± 61.868 | 32.521 ± 1.160 | PASS |
| priority_aging | 100.000 ± 0.000 | 0.000 ± 0.000 | 8.383 ± 1.170 | 9.802 ± 1.313 | 13.414 ± 0.774 | 100.000 ± 0.000 | 1696.015 ± 61.868 | 32.562 ± 1.051 | PASS |
| warm_env_affinity | 100.000 ± 0.000 | 0.000 ± 0.000 | 7.656 ± 0.915 | 9.810 ± 1.333 | 13.728 ± 1.013 | 100.000 ± 0.000 | 1696.015 ± 61.868 | 48.645 ± 3.409 | PASS |
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 7.901 ± 1.023 | 10.035 ± 1.520 | 13.584 ± 1.055 | 100.000 ± 0.000 | 1696.015 ± 61.868 | 46.376 ± 3.098 | PASS |
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 8.515 ± 0.924 | 11.028 ± 1.524 | 13.249 ± 0.989 | 100.000 ± 0.000 | 1696.015 ± 61.868 | 49.229 ± 3.512 | PASS |

### S7_tenant_skew — multi-tenant fairness: hot Space vs cold Spaces

(knobs: mean_gap 1.40s, tenant_skew 0.70, workers: 3)

| policy | rescue % | starved | FG p95 s | FG p99 s | BG max wait s | hit % | cost | warm % | dual-ok |
|---|---|---|---|---|---|---|---|---|---|
| fifo | 98.575 ± 1.192 | 0.050 ± 0.105 | 19.192 ± 4.248 | 21.212 ± 4.157 | 17.611 ± 4.143 | 99.438 ± 0.469 | 1746.615 ± 96.387 | 51.709 ± 1.945 | FAIL |
| priority_aging | 98.918 ± 1.189 | 0.000 ± 0.000 | 16.597 ± 4.667 | 18.993 ± 4.760 | 16.531 ± 1.429 | 99.579 ± 0.465 | 1722.440 ± 96.321 | 52.374 ± 2.137 | PASS |
| warm_env_affinity | 98.873 ± 1.220 | 0.000 ± 0.000 | 15.948 ± 4.245 | 19.529 ± 4.302 | 16.814 ± 1.410 | 99.541 ± 0.491 | 1728.265 ± 96.371 | 62.084 ± 2.737 | PASS |
| dual_cache_ttl_aware | 100.000 ± 0.000 | 0.000 ± 0.000 | 15.348 ± 3.585 | 18.748 ± 3.795 | 16.987 ± 1.173 | 100.000 ± 0.000 | 1640.260 ± 53.321 | 61.354 ± 2.888 | PASS |
| session_guarded_dual | 100.000 ± 0.000 | 0.000 ± 0.000 | 19.048 ± 3.905 | 23.774 ± 4.433 | 14.262 ± 1.516 | 100.000 ± 0.000 | 1640.260 ± 53.321 | 61.335 ± 2.599 | PASS |

## Verdicts — max sustainable load (dual-constraint: rescue >= 95% and starved = 0)

| policy | max sustainable (jobs/min) | cell | first violation (jobs/min) | cell |
|---|---|---|---|---|
| fifo | 43 | S1_load_1.0x | 54 | S1_load_1.25x |
| priority_aging | 43 | S1_load_1.0x | 54 | S1_load_1.25x |
| warm_env_affinity | 43 | S1_load_1.0x | 54 | S1_load_1.25x |
| dual_cache_ttl_aware | 65 | S1_load_1.5x | 86 | S1_load_2.0x |
| session_guarded_dual | 65 | S1_load_1.5x | 86 | S1_load_2.0x |

## S7 tenant-skew fairness snapshot (last seed)

| run | tenant | jobs | FG p95 JCT s | BG max wait s | starved |
|---|---|---|---|---|---|
| S7_tenant_skew/dual_cache_ttl_aware | tenant-0 | 160 | 10.3 | 15.2 | 0 |
| S7_tenant_skew/dual_cache_ttl_aware | tenant-1 | 36 | 9.3 | 15.0 | 0 |
| S7_tenant_skew/dual_cache_ttl_aware | tenant-2 | 44 | 9.7 | 15.6 | 0 |
| S7_tenant_skew/fifo | tenant-0 | 160 | 13.1 | 10.9 | 0 |
| S7_tenant_skew/fifo | tenant-1 | 36 | 12.7 | 8.2 | 0 |
| S7_tenant_skew/fifo | tenant-2 | 44 | 13.3 | 9.5 | 0 |
| S7_tenant_skew/priority_aging | tenant-0 | 160 | 11.6 | 14.5 | 0 |
| S7_tenant_skew/priority_aging | tenant-1 | 36 | 10.2 | 14.5 | 0 |
| S7_tenant_skew/priority_aging | tenant-2 | 44 | 9.7 | 13.0 | 0 |
| S7_tenant_skew/session_guarded_dual | tenant-0 | 160 | 13.8 | 9.7 | 0 |
| S7_tenant_skew/session_guarded_dual | tenant-1 | 36 | 16.9 | 3.9 | 0 |
| S7_tenant_skew/session_guarded_dual | tenant-2 | 44 | 15.6 | 7.5 | 0 |
| S7_tenant_skew/warm_env_affinity | tenant-0 | 160 | 10.7 | 13.7 | 0 |
| S7_tenant_skew/warm_env_affinity | tenant-1 | 36 | 9.3 | 14.3 | 0 |
| S7_tenant_skew/warm_env_affinity | tenant-2 | 44 | 9.8 | 14.7 | 0 |

