# Ranked experiment backlog (re-read after context compaction)

Evaluation: correctness = fixtures + dev answer + multiseed vs oracle (NCPU=4 locally).
Timing = bench.yml on x86 CI: tune on seed 7, validate on held-out seed 11 (never tune on 11). Mac timings are unreliable.

Latest x86 profile (20M events, 4 vCPU, commit 384fc9c): person-table misses 21%, memchr 17%, timestamps 11%, Ev sort 8%, links phase 0.38 s.

1. process_person allocates a Vec<Sess> plus a 21-slot cat_rev Vec for every session; switch to flat reusable arrays (sessions phase).
2. Fast F1/F2 timestamp parse: cached month-start day table, branchless digit check.
3. Pre-size bucket Vecs (estimate from bytes); try P=64 to cut scatter streams.
4. Radix or two-level sort for Ev/Ck in the sessions phase; store the cid key so ties skip byte compares.
5. Links: batch-prefetch the union-find intern loop.
6. madvise/MAP_POPULATE on input mmaps; huge pages for big buffers (kernel time ~5%).
7. Overlap the links phase with event tokenization.
8. target-cpu x86-64-v4 / 64-byte AVX-512 masks.
9. Shrink Ev from 40 to 32 B; INT32 parquet columns; parallel campaign aggregation.
10. Try 16 threads on 8 vCPU (oversubscription) vs 8.
