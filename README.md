# Shopping Sessions (Databricks track, Builders Buildathon)

A from-scratch Rust pipeline: memory-mapped CSV, a hand-written parser that follows `rules.md` exactly,
union-find for linked IDs, parallel dedup and sessionization, and a minimal Parquet writer. No data engine.

## Layout

- `src/main.rs`: pipeline (setup, links, parse, dedup, sessions and attribution, output)
- `src/ts.rs`: timestamp and price parsing
- `src/parquet.rs`: Parquet writer (PLAIN, uncompressed)
- `scripts/`: fixture runner, comparer, oracle and multi-seed checks
- `fixtures/`: the 36 official edge cases

## Test

    cargo build --release
    python3 scripts/run_fixtures.py fixtures -- target/release/shopping
    DATA_DIR=dev OUTPUT_DIR=out target/release/shopping && python3 scripts/compare.py out dev-answer

Set `SS_TIMING=1` for per-phase timings on stderr.
