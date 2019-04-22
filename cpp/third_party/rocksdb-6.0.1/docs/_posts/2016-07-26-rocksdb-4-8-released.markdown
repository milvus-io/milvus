---
title: RocksDB 4.8 Released!
layout: post
author: yiwu
category: blog
redirect_from:
  - /blog/3239/rocksdb-4-8-released/
---

## 4.8.0 (5/2/2016)

### [](https://github.com/facebook/rocksdb/blob/master/HISTORY.md#public-api-change-1)Public API Change

  * Allow preset compression dictionary for improved compression of block-based tables. This is supported for zlib, zstd, and lz4. The compression dictionary's size is configurable via CompressionOptions::max_dict_bytes.
  * Delete deprecated classes for creating backups (BackupableDB) and restoring from backups (RestoreBackupableDB). Now, BackupEngine should be used for creating backups, and BackupEngineReadOnly should be used for restorations. For more details, see [https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB%3F](https://github.com/facebook/rocksdb/wiki/How-to-backup-RocksDB%3F)
  * Expose estimate of per-level compression ratio via DB property: "rocksdb.compression-ratio-at-levelN".
  * Added EventListener::OnTableFileCreationStarted. EventListener::OnTableFileCreated will be called on failure case. User can check creation status via TableFileCreationInfo::status.

### [](https://github.com/facebook/rocksdb/blob/master/HISTORY.md#new-features-2)New Features

  * Add ReadOptions::readahead_size. If non-zero, NewIterator will create a new table reader which performs reads of the given size.

<br/>

<!--truncate-->

## [](https://github.com/facebook/rocksdb/blob/master/HISTORY.md#470-482016)4.7.0 (4/8/2016)

### [](https://github.com/facebook/rocksdb/blob/master/HISTORY.md#public-api-change-2)Public API Change

  * rename options compaction_measure_io_stats to report_bg_io_stats and include flush too.
  * Change some default options. Now default options will optimize for server-workloads. Also enable slowdown and full stop triggers for pending compaction bytes. These changes may cause sub-optimal performance or significant increase of resource usage. To avoid these risks, users can open existing RocksDB with options extracted from RocksDB option files. See [https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File) for how to use RocksDB option files. Or you can call Options.OldDefaults() to recover old defaults. DEFAULT_OPTIONS_HISTORY.md will track change history of default options.

<br/>

## [](https://github.com/facebook/rocksdb/blob/master/HISTORY.md#460-3102016)4.6.0 (3/10/2016)

### [](https://github.com/facebook/rocksdb/blob/master/HISTORY.md#public-api-changes-1)Public API Changes

  * Change default of BlockBasedTableOptions.format_version to 2. It means default DB created by 4.6 or up cannot be opened by RocksDB version 3.9 or earlier
  * Added strict_capacity_limit option to NewLRUCache. If the flag is set to true, insert to cache will fail if no enough capacity can be free. Signature of Cache::Insert() is updated accordingly.
  * Tickers [NUMBER_DB_NEXT, NUMBER_DB_PREV, NUMBER_DB_NEXT_FOUND, NUMBER_DB_PREV_FOUND, ITER_BYTES_READ] are not updated immediately. The are updated when the Iterator is deleted.
  * Add monotonically increasing counter (DB property "rocksdb.current-super-version-number") that increments upon any change to the LSM tree.

### [](https://github.com/facebook/rocksdb/blob/master/HISTORY.md#new-features-3)New Features

  * Add CompactionPri::kMinOverlappingRatio, a compaction picking mode friendly to write amplification.
  * Deprecate Iterator::IsKeyPinned() and replace it with Iterator::GetProperty() with prop_name="rocksdb.iterator.is.key.pinned"
