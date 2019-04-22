---
title: RocksDB 5.10.2 Released!
layout: post
author: siying
category: blog
---

### Public API Change
* When running `make` with environment variable `USE_SSE` set and `PORTABLE` unset, will use all machine features available locally. Previously this combination only compiled SSE-related features.

### New Features
* CRC32C is now using the 3-way pipelined SSE algorithm `crc32c_3way` on supported platforms to improve performance. The system will choose to use this algorithm on supported platforms automatically whenever possible. If PCLMULQDQ is not supported it will fall back to the old Fast_CRC32 algorithm.
* Provide lifetime hints when writing files on Linux. This reduces hardware write-amp on storage devices supporting multiple streams.
* Add a DB stat, `NUMBER_ITER_SKIP`, which returns how many internal keys were skipped during iterations (e.g., due to being tombstones or duplicate versions of a key).
* Add PerfContext counters, `key_lock_wait_count` and `key_lock_wait_time`, which measure the number of times transactions wait on key locks and total amount of time waiting.

### Bug Fixes
* Fix IOError on WAL write doesn't propagate to write group follower
* Make iterator invalid on merge error.
* Fix performance issue in `IngestExternalFile()` affecting databases with large number of SST files.
* Fix possible corruption to LSM structure when `DeleteFilesInRange()` deletes a subset of files spanned by a `DeleteRange()` marker.
* Fix DB::Flush() keep waiting after flush finish under certain condition.
