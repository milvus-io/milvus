# Async Growing Index Catch-Up Budget Plan

**Goal:** Prevent a non-converging async growing-index catch-up from causing a large locked finalize, while retaining complete-index publication semantics and falling back safely to raw search after a bounded deadline.

**Base:** `e17e9abb4e449538ac5174a662345b915f485bdf` (`foxspy/growing-index-async-pr` exact head)

## Contract

- Measure cumulative rows and wall time spent in actual CatchUp `AddRange` calls.
- Estimate locked-finalize time as `latest_gap / cumulative_rows_per_ms`.
- Attempt finalize only when the estimate is within `asyncGrowingFinalizeBudgetMs`.
- After acquiring `append_mutex_`, recompute the frozen gap and estimate; publish only if it is still within budget.
- If CatchUp elapsed time exceeds `asyncGrowingCatchupDeadlineMs`, discard the unpublished interim index, enter `kDisabled`, and keep raw search permanently for the segment.
- Remove the eight-stall-round large-gap forced-finalize behavior.
- Log rate samples, decisions, frozen gap, estimated time, lock wait/hold, and deadline fallback.

## Implementation sequence

1. Add focused failing C++ tests for budget-gated finalization and deadline fallback, plus configuration default/bridge coverage.
2. Add the two query-node configuration parameters and propagate them into the segcore configuration snapshot used by newly created growing indexes.
3. Replace `CatchUp()` stall-round logic with cumulative-rate estimation, lock-time budget recheck, and safe deadline abort cleanup.
4. Run focused segcore and Go parameter tests, formatting/diff checks, and the relevant broader build/test targets.
5. Commit and push the dedicated branch only after fresh verification.
6. With immediate user confirmation of exact QTP parameters, build the VDC image, deploy a separate AWS UAT 2-CU capacity instance, and run a comparable i8g Strong streaming test.

## Streaming acceptance evidence

- No large-gap forced locked finalize in logs.
- Correct row visibility and zero query failures.
- Insert ack P99/P99.9/MAX, query QPS/P99/P99.9/MAX, catch-up rate/ETA decisions, and raw-fallback behavior are captured.
- Compare against the prior 3000 rows/s Strong result where async insert P99.9/MAX reached 19.18s/43.50s.
