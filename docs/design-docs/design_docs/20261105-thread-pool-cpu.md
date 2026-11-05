# Sampled CPU accounting by thread pool

## Goal and existing behavior

Keep Milvus's built-in five-second thread watcher and the existing
`milvus_thread_num` and `milvus_thread_cpu_active_num_by_pool` gauges. Add
`milvus_thread_pool_cpu_seconds_total{thread_pool="..."}` to distinguish how
many threads were active from how much CPU they consumed. The active gauge
continues to include threads whose CPU advanced or whose sampled state is R/D;
D can mean waiting for I/O, not executing on CPU.

## Collection and accounting

On Linux, resolve `SC_CLK_TCK` once through the existing go-sysconf dependency.
Read `/proc/uptime` before enumerating `/proc/self/task`. Read each thread's
`stat` once: comm, state, utime, stime, and starttime come from the same record.
Names with spaces or parentheses are parsed using the outer comm delimiters.
No TID or raw thread name is exported as a label. Kubernetes labels continue
to come from Prometheus discovery, as on the existing thread panel.

Keep a baseline per TID containing its starttime, pool, and cumulative CPU
ticks. Compare both TID and starttime before subtracting. Sum positive tick
deltas per pool, then divide by SC_CLK_TCK and add to the Prometheus counter.
Only the watcher goroutine mutates the baselines; Prometheus handles concurrent
counter reads. Memory is O(live threads + fixed pools), and scanning costs one
uptime read, one directory enumeration, and one stat read per thread per tick.
This replaces the previous comm + stat pair of reads.

| Event | Accounting |
|---|---|
| First successful scan | Establish baselines; do not import historic CPU. |
| Same TID and starttime | Add utime + stime delta. |
| Newly observed identity born after the previous scan began | Add its observed lifetime CPU. |
| Newly observed identity with older/ambiguous starttime | Establish a baseline only. |
| Pool name changes | Attribute that interval to the previous pool; future intervals use the new pool. Never transfer historic totals. |
| CPU ticks go backwards | Establish a fresh baseline; never subtract or wrap unsigned values. |
| Thread stat missing, malformed, or TID mismatches directory | Keep any prior baseline while the TID remains enumerated. Skip this sample. |
| Thread disappears from directory | Retire its baseline; retain all accumulated pool CPU. |
| Whole scan / uptime read fails | Keep all metrics and baselines unchanged and retry next tick. |
| Clock frequency unavailable | Warn once, retain thread gauges, do not publish CPU counters. |

The uptime boundary is rounded upward to clock ticks. Threads born on that
boundary tick are conservatively baselined, since procfs does not provide a
transactional snapshot. Recovery after a failed read can publish multiple
intervals' CPU in one update; rates during that period may spike.

## Fixed classification

Retain existing Knowhere build/search/fetch, RocksDB, Milvus search/load,
HIGH/MIDD/LOW segcore, CGO and file_write groups. Add:

| comm prefix | Pool |
|---|---|
| MILVUS_ASYNC_LOA | async_load |
| MILVUS_LF_IO | local_file_io |
| MILVUS_PREFETCH | prefetch |
| AwsEventLoop | aws_crt_event_loop |
| AwsHostResolver | aws_crt_resolver |
| EvntLoopCleanup | aws_crt_cleanup |

Prefixes account for Linux's 15-byte visible thread names. AWS event loops
with and without a space before their number share one group. Names were
checked against local aws-c-io sources (event_loop.c and host_resolver.c).
Unmatched threads remain `unclassified`. Classification is by OS worker, not
by task: Go goroutines, nested OpenMP workers, or shared executors cannot be
reliably attributed to a calling business operation from thread names alone.
Knowhere pool names are already supplied by milvus-common; no Knowhere change
is needed for these three pools.

## Dashboard and sampling limits

The new CPU cores panel uses
`sum by (pod, thread_pool) (rate(milvus_thread_pool_cpu_seconds_total[$__rate_interval]))`
with the existing deployment selectors. A value of 2 means two CPU cores on
average; do not multiply by 100. Keep sampling at five seconds, retain the
existing Prometheus scrape interval, and use a rate window covering at least
four scrapes.

A separate five-minute comparison plots all sampled pools, including
unclassified, against the same Pod's `process_cpu_seconds_total` rate. These
are not synchronized snapshots. Do not interpret their difference as an exact
missing pool or force it into unclassified. Threads created and destroyed
between scans are invisible. CPU after a thread's final sample and before
exit is also lost. Historic CPU before the initial baseline is intentionally
excluded. Shorter sampling reduces these gaps at higher scanning cost.

## Verification

Focused tests cover stat parsing (including unusual comm values), real Linux
procfs, every added classification and HIGH/MIDD/LOW/Knowhere, initial and new
thread baselines, TID reuse, renames, rollback, partial/whole scan failures,
recovery, counter persistence after exit, tick-to-second conversion, metric
registration and bounded labels. A synthetic 1,000-thread directory benchmark
measures parsing and file-read overhead; it is not a production procfs benchmark.

Full package validation requires C++ headers/libraries matching this checkout.
Deployment validation should compare a stable workload's sampled CPU and
process CPU over several minutes, then exercise rapid thread creation/exit to
measure expected undercount. No claim of production coverage follows from
unit tests or the synthetic benchmark alone.

### Local validation (2026-11-05)

- Collector file tests passed with `-tags dynamic,test -gcflags='all=-N -l'`.
  A real thread's proc stat delta measured 0.110 CPU seconds against 0.110
  seconds of independent kernel scheduler runtime (`/proc/thread-self/schedstat`).
- `go test` of the `pkg/metrics` package passed with the same required flags.
- Dashboard JSON, unique panel IDs, CPU units and deployment selectors passed
  structural checks; no Grafana rendering or live Prometheus scrape was performed.
- A five-iteration synthetic scan of 1,000 thread files averaged 16.1 ms and
  1.55 MB allocated per scan with compiler optimization disabled. These fixture
  results do not establish real procfs cost at production scale.
- Full `internal/util/metrics` package testing is blocked before these changes
  by stale installed C++ headers/library lacking `SegcoreSetPrefetchThreadPoolNum`.
  No C++ rebuild or production workload comparison was performed.
- On the latest master baseline, collector file tests, `pkg/metrics` tests and
  targeted golangci-lint passed again. Full `make static-check` could not pass
  type checking because the locally installed C++ headers lack
  `LoonFilesystemMetricsSources` and the related `loon_filesystem_*metrics_sources`
  interfaces; the complete metrics package remains blocked by the prefetch symbol.
