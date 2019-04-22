---
title: RocksDB 5.6.1 Released!
layout: post
author: yiwu
category: blog
---

### Public API Change
* Scheduling flushes and compactions in the same thread pool is no longer supported by setting `max_background_flushes=0`. Instead, users can achieve this by configuring their high-pri thread pool to have zero threads. See https://github.com/facebook/rocksdb/wiki/Thread-Pool for more details.
* Replace `Options::max_background_flushes`, `Options::max_background_compactions`, and `Options::base_background_compactions` all with `Options::max_background_jobs`, which automatically decides how many threads to allocate towards flush/compaction.
* options.delayed_write_rate by default take the value of options.rate_limiter rate.
* Replace global variable `IOStatsContext iostats_context` with `IOStatsContext* get_iostats_context()`; replace global variable `PerfContext perf_context` with `PerfContext* get_perf_context()`.

### New Features
* Change ticker/histogram statistics implementations to use core-local storage. This improves aggregation speed compared to our previous thread-local approach, particularly for applications with many threads. See http://rocksdb.org/blog/2017/05/14/core-local-stats.html for more details.
* Users can pass a cache object to write buffer manager, so that they can cap memory usage for memtable and block cache using one single limit.
* Flush will be triggered when 7/8 of the limit introduced by write_buffer_manager or db_write_buffer_size is triggered, so that the hard threshold is hard to hit. See https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager for more details.
* Introduce WriteOptions.low_pri. If it is true, low priority writes will be throttled if the compaction is behind. See https://github.com/facebook/rocksdb/wiki/Low-Priority-Write for more details.
* `DB::IngestExternalFile()` now supports ingesting files into a database containing range deletions.

### Bug Fixes
* Shouldn't ignore return value of fsync() in flush.
