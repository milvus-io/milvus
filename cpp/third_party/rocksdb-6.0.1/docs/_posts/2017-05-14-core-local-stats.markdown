---
title: Core-local Statistics
layout: post
author: ajkr
category: blog
---

## Origins: Global Atomics

Until RocksDB 4.12, ticker/histogram statistics were implemented with std::atomic values shared across the entire program. A ticker consists of a single atomic, while a histogram consists of several atomics to represent things like min/max/per-bucket counters. These statistics could be updated by all user/background threads.

For concurrent/high-throughput workloads, cache line bouncing of atomics caused high CPU utilization. For example, we have tickers that count block cache hits and misses. Almost every user read increments these tickers a few times. Many concurrent user reads would cause the cache lines containing these atomics to bounce between cores.

### Performance

Here are perf results for 32 reader threads where most reads (99%+) are served by uncompressed block cache. Such a scenario stresses the statistics code heavily.

Benchmark command: `TEST_TMPDIR=/dev/shm/ perf record -g ./db_bench -statistics -use_existing_db=true -benchmarks=readrandom -threads=32 -cache_size=1048576000 -num=1000000 -reads=1000000 && perf report -g --children`

Perf snippet for "cycles" event:

```
  Children  Self    Command   Shared Object  Symbol
+   30.33%  30.17%  db_bench  db_bench       [.] rocksdb::StatisticsImpl::recordTick
+    3.65%   0.98%  db_bench  db_bench       [.] rocksdb::StatisticsImpl::measureTime
```

Perf snippet for "cache-misses" event:

```
  Children  Self    Command   Shared Object  Symbol
+   19.54%  19.50%  db_bench  db_bench 	     [.] rocksdb::StatisticsImpl::recordTick
+    3.44%   0.57%  db_bench  db_bench       [.] rocksdb::StatisticsImpl::measureTime
```

The high CPU overhead for updating tickers and histograms corresponds well to the high cache misses.

## Thread-locals: Faster Updates

Since RocksDB 4.12, ticker/histogram statistics use thread-local storage. Each thread has a local set of atomic values that no other thread can update. This prevents the cache line bouncing problem described above. Even though updates to a given value are always made by the same thread, atomics are still useful to synchronize with aggregations for querying statistics.

Implementing this approach involved a couple challenges. First, each query for a statistic's global value must aggregate all threads' local values. This adds some overhead, which may pass unnoticed if statistics are queried infrequently. Second, exited threads' local values are still needed to provide accurate statistics. We handle this by merging a thread's local values into process-wide variables upon thread exit.

### Performance

Update benchmark setup is same as before. CPU overhead improved 7.8x compared to global atomics, corresponding to a 17.8x reduction in cache-misses overhead.

Perf snippet for "cycles" event:

```
  Children  Self    Command   Shared Object  Symbol
+    2.96%  0.87%   db_bench  db_bench       [.] rocksdb::StatisticsImpl::recordTick
+    1.37%  0.10%   db_bench  db_bench       [.] rocksdb::StatisticsImpl::measureTime
```

Perf snippet for "cache-misses" event:

```
  Children  Self    Command   Shared Object  Symbol
+    1.21%  0.65%   db_bench  db_bench       [.] rocksdb::StatisticsImpl::recordTick
     0.08%  0.00%   db_bench  db_bench       [.] rocksdb::StatisticsImpl::measureTime
```

To measure statistics query latency, we ran sysbench with 4K OLTP clients concurrently with one client that queries statistics repeatedly. Times shown are in milliseconds.

```
 min: 18.45
 avg: 27.91
 max: 231.65
 95th percentile: 55.82
```

## Core-locals: Faster Querying

The thread-local approach is working well for applications calling RocksDB from only a few threads, or polling statistics infrequently. Eventually, though, we found use cases where those assumptions do not hold. For example, one application has per-connection threads and typically runs into performance issues when connection count grows very high. For debugging such issues, they want high-frequency statistics polling to correlate issues in their application with changes in RocksDB's state.

Once [PR #2258](https://github.com/facebook/rocksdb/pull/2258) lands, ticker/histogram statistics will be local to each CPU core. Similarly to thread-local, each core updates only its local values, thus avoiding cache line bouncing. Local values are still atomics to make aggregation possible. With this change, query work depends only on number of cores, not the number of threads. So, applications with many more threads than cores can no longer impact statistics query latency.

### Performance

Update benchmark setup is same as before. CPU overhead worsened ~23% compared to thread-local, while cache performance was unchanged.

Perf snippet for "cycles" event:

```
  Children  Self    Command   Shared Object  Symbol
+    2.96%  0.87%   db_bench  db_bench       [.] rocksdb::StatisticsImpl::recordTick
+    1.37%  0.10%   db_bench  db_bench       [.] rocksdb::StatisticsImpl::measureTime
```

Perf snippet for "cache-misses" event:

```
  Children  Self    Command   Shared Object  Symbol
+    1.21%  0.65%   db_bench  db_bench       [.] rocksdb::StatisticsImpl::recordTick
     0.08%  0.00%   db_bench  db_bench       [.] rocksdb::StatisticsImpl::measureTime
```

Query latency is measured same as before with times in milliseconds. Average latency improved by 6.3x compared to thread-local.

```
 min: 2.47
 avg: 4.45
 max: 91.13
 95th percentile: 7.56
```
