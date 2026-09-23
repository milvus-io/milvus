# Native segment Load phases

## Key packages

- `internal/core/src/monitor/SegmentLoadMetrics.{h,cpp}` owns the fixed phase histogram and per-attempt timing state.
- `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp` places phase boundaries in `Load`, `ApplyLoadDiff`, and `PrepareLoadDiffForReopen`.
- `internal/core/src/monitor/SegmentLoadMetricsTest.cpp` checks successful and failed attempts against the exported registry.

## Completion population

`internal_core_segment_load_duration_seconds{stage,result}` emits one sample
for every phase and `total` for each completed invocation of
`ChunkedSegmentSealedImpl::Load`. All phases share the invocation's final
`success` or `error` outcome. Escaping exceptions, including native cancellation,
produce `error` observations. A failure recovered within Load would not change
its final outcome.

There are 14 phases. They partition elapsed wall time on the calling Load
thread, including waits for its parallel workers and cleanup on an exceptional
exit. Phases not reached contribute zero. A reached phase with an empty diff
still includes its condition checks and bookkeeping. Reopen calls do not pass
a timing object and are outside this metric's population.

| Phase | Boundary |
|---|---|
| `lock_wait` | Acquire the existing reopen mutex |
| `prepare` | Capture published state, copy load info and compute its diff |
| `clone_state` | Clone runtime/published state and construct the staged committer |
| `indexes` | Load and replace index batches, including worker waits |
| `reload_columns` | Reload existing columns |
| `column_groups` | Reader creation and eager/lazy manifest column groups |
| `text_lob` | Initialize text LOB paths |
| `field_data` | Load and replace binlog batches |
| `text_indexes` | Load text-index batches |
| `json_stats` | Load and replace JSON stats |
| `default_fields` | Fill default-value fields |
| `create_text_indexes` | Create missing text indexes |
| `finalize` | Drop/retire obsolete resources and finalize staged state |
| `publish` | Compact runtime info, build/publish the state delta and return cleanup |

The histogram uses seconds, with finite buckets from 1 microsecond to 600
seconds followed by `+Inf`. Labels are fixed phase and outcome names; no
collection, segment, field, or request identifiers are exported.

## Interpretation

Use the same component, instance, outcome and time filters for phase means:

```promql
sum by (stage, result) (
  increase(internal_core_segment_load_duration_seconds_sum[5m])
)
/
sum by (stage, result) (
  increase(internal_core_segment_load_duration_seconds_count[5m])
)
```

For a completed population with equal counts, the 14 phase means add to the
native total mean. The `total` series is the parent and must not be added again.
Phase quantiles are not additive. Parallel worker durations, Go scheduler
attempts, manifest reads and client requests have different populations.
This metric does not measure their end-to-end latency or isolate S3 retries.

Observations are emitted at completion, so an unfinished load is absent. A
scrape may occur between phase observations; take a final scrape after the
workload settles before comparing counts and sums. The timer records time up
to sampling before exporting the observations, excluding its own export cost.

## Validation

The C++ test covers successful and exceptional completion, equal phase counts,
zero for unreached phases, idempotent End, and phase-sum/total equality.
Full native integration is needed to exercise actual storage/index failures;
the timer test alone is not a performance or request-level coverage result.
