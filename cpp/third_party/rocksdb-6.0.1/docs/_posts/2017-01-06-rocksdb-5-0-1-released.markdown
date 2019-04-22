---
title: RocksDB 5.0.1 Released!
layout: post
author: yiwu
category: blog
---

### Public API Change

  * Options::max_bytes_for_level_multiplier is now a double along with all getters and setters.
  * Support dynamically change `delayed_write_rate` and `max_total_wal_size` options via SetDBOptions().
  * Introduce DB::DeleteRange for optimized deletion of large ranges of contiguous keys.
  * Support dynamically change `delayed_write_rate` option via SetDBOptions().
  * Options::allow_concurrent_memtable_write and Options::enable_write_thread_adaptive_yield are now true by default.
  * Remove Tickers::SEQUENCE_NUMBER to avoid confusion if statistics object is shared among RocksDB instance. Alternatively DB::GetLatestSequenceNumber() can be used to get the same value.
  * Options.level0_stop_writes_trigger default value changes from 24 to 32.
  * New compaction filter API: CompactionFilter::FilterV2(). Allows to drop ranges of keys.
  * Removed flashcache support.
  * DB::AddFile() is deprecated and is replaced with DB::IngestExternalFile(). DB::IngestExternalFile() remove all the restrictions that existed for DB::AddFile.

### New Features

  * Add avoid_flush_during_shutdown option, which speeds up DB shutdown by not flushing unpersisted data (i.e. with disableWAL = true). Unpersisted data will be lost. The options is dynamically changeable via SetDBOptions().
  * Add memtable_insert_with_hint_prefix_extractor option. The option is mean to reduce CPU usage for inserting keys into memtable, if keys can be group by prefix and insert for each prefix are sequential or almost sequential. See include/rocksdb/options.h for more details.
  * Add LuaCompactionFilter in utilities.  This allows developers to write compaction filters in Lua.  To use this feature, LUA_PATH needs to be set to the root directory of Lua.
  * No longer populate "LATEST_BACKUP" file in backup directory, which formerly contained the number of the latest backup. The latest backup can be determined by finding the highest numbered file in the "meta/" subdirectory.
