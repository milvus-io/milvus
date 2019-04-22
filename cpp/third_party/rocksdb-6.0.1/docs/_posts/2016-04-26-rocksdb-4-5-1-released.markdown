---
title: RocksDB 4.5.1 Released!
layout: post
author: sdong
category: blog
redirect_from:
  - /blog/3179/rocksdb-4-5-1-released/
---

## 4.5.1 (3/25/2016)

### Bug Fixes

  *  Fix failures caused by the destorying order of singleton objects.

<br/>

## 4.5.0 (2/5/2016)

### Public API Changes

  * Add a new perf context level between kEnableCount and kEnableTime. Level 2 now does not include timers for mutexes.
  * Statistics of mutex operation durations will not be measured by default. If you want to have them enabled, you need to set Statistics::stats_level_ to kAll.
  * DBOptions::delete_scheduler and NewDeleteScheduler() are removed, please use DBOptions::sst_file_manager and NewSstFileManager() instead

### New Features
  * ldb tool now supports operations to non-default column families.
  * Add kPersistedTier to ReadTier. This option allows Get and MultiGet to read only the persited data and skip mem-tables if writes were done with disableWAL = true.
  * Add DBOptions::sst_file_manager. Use NewSstFileManager() in include/rocksdb/sst_file_manager.h to create a SstFileManager that can be used to track the total size of SST files and control the SST files deletion rate.

<br/>

<!--truncate-->

## 4.4.0 (1/14/2016)

### Public API Changes

  * Change names in CompactionPri and add a new one.
  * Deprecate options.soft_rate_limit and add options.soft_pending_compaction_bytes_limit.
  * If options.max_write_buffer_number > 3, writes will be slowed down when writing to the last write buffer to delay a full stop.
  * Introduce CompactionJobInfo::compaction_reason, this field include the reason to trigger the compaction.
  * After slow down is triggered, if estimated pending compaction bytes keep increasing, slowdown more.
  * Increase default options.delayed_write_rate to 2MB/s.
  * Added a new parameter --path to ldb tool. --path accepts the name of either MANIFEST, SST or a WAL file. Either --db or --path can be used when calling ldb.

<br/>

## 4.3.0 (12/8/2015)

### New Features

  * CompactionFilter has new member function called IgnoreSnapshots which allows CompactionFilter to be called even if there are snapshots later than the key.
  * RocksDB will now persist options under the same directory as the RocksDB database on successful DB::Open, CreateColumnFamily, DropColumnFamily, and SetOptions.
  * Introduce LoadLatestOptions() in rocksdb/utilities/options_util.h. This function can construct the latest DBOptions / ColumnFamilyOptions used by the specified RocksDB intance.
  * Introduce CheckOptionsCompatibility() in rocksdb/utilities/options_util.h. This function checks whether the input set of options is able to open the specified DB successfully.

### Public API Changes

  * When options.db_write_buffer_size triggers, only the column family with the largest column family size will be flushed, not all the column families.
