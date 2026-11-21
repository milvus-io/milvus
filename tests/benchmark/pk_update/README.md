# PK update native benchmark

The benchmark uses one production sealed segment per process on one request
thread. It compares a segment receiving `ApplyUpdates` with an independent
forward-replayed mirror containing zero deltas. Each pair uses identical
ClientIP and URL residency. The raw configuration loads both columns. The
STL_SORT configuration loads both indexes and obtains base values by index
reverse lookup. It keeps the PK and timestamp columns in both configurations.

Use Python 3.11+ and PyArrow to prepare fixtures from the first ClickBench
`hits_0.parquet` shard. The generator checks the source SHA-256. Use a new
fixture root for each generation.

```sh
python tests/benchmark/pk_update/prepare_clickbench.py \
  --parquet /path/to/hits_0.parquet --output-dir "$fixtures/1pct-1x" --percent 1 --rounds 1
python tests/benchmark/pk_update/prepare_clickbench.py \
  --parquet /path/to/hits_0.parquet --output-dir "$fixtures/5pct-1x" --percent 5 --rounds 1
python tests/benchmark/pk_update/prepare_clickbench.py \
  --parquet /path/to/hits_0.parquet --output-dir "$fixtures/5pct-3x" --percent 5 --rounds 3
```

Build `all_tests` and its matching shared core in Release or RelWithDebInfo
using the repository's native test build. Run the matrix against those binaries.

```sh
python tests/benchmark/pk_update/run_native.py \
  --binary "$native_build/unittest/all_tests" \
  --core-library "$native_core_library" \
  --fixtures "$fixtures" --output-dir "$new_results"
```

Each configuration checks the aligned offsets, PKs and projections for the
100 literal sets before timing. The candidate is also checked at all earlier
fixture ticks. The separate snapshot test covers duplicate PKs and offsets,
NULL assignments and future-only updates. The repeated fixture also times
100 PK equality plans with empty and with INT64/VARCHAR projections.

The measured interval wraps production `Segment::Retrieve`. Load, apply,
plan construction, assertions and 100 warmup calls are outside it. Each
conjunction series observes the same two production predicate leaves in all
1,000 calls with the expression result cache disabled. The scoped, thread-local
observer records actual native predicate and update batch work. Raw rows are
rows passed to native evaluators before their per-row candidate mask. Overlay
input rows are the size of the incoming spans before membership or candidate
filtering. Visited bitmap bits report requested logical size. Repeated
configurations include an unobserved timing comparison for the candidate.

The runner preserves per-process command, exit code, GTest XML and complete
latency series. It verifies fixture hashes and records binary and core digests.
Check that the supplied core is the one loaded by the test executable when
collecting results.
