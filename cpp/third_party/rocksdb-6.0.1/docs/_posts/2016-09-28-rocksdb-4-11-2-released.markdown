---
title: RocksDB 4.11.2 Released!
layout: post
author: sdong
category: blog
---
We abandoned release candidates 4.10.x and directly go to 4.11.2 from 4.9, to make sure the latest release is stable. In 4.11.2, we fixed several data corruption related bugs introduced in 4.9.0.

## 4.11.2 (9/15/2016)

### Bug fixes

  * Segfault when failing to open an SST file for read-ahead iterators.
  * WAL without data for all CFs is not deleted after recovery.

<!--truncate-->

## 4.11.1 (8/30/2016)

### Bug Fixes

  * Mitigate the regression bug of deadlock condition during recovery when options.max_successive_merges hits.
  * Fix data race condition related to hash index in block based table when putting indexes in the block cache.

## 4.11.0 (8/1/2016)

### Public API Change

  * options.memtable_prefix_bloom_huge_page_tlb_size => memtable_huge_page_size. When it is set, RocksDB will try to allocate memory from huge page for memtable too, rather than just memtable bloom filter.

### New Features

  * A tool to migrate DB after options change. See include/rocksdb/utilities/option_change_migration.h.
  * Add ReadOptions.background_purge_on_iterator_cleanup. If true, we avoid file deletion when destorying iterators.

## 4.10.0 (7/5/2016)

### Public API Change

  * options.memtable_prefix_bloom_bits changes to options.memtable_prefix_bloom_bits_ratio and deprecate options.memtable_prefix_bloom_probes
  * enum type CompressionType and PerfLevel changes from char to unsigned char. Value of all PerfLevel shift by one.
  * Deprecate options.filter_deletes.

### New Features

  * Add avoid_flush_during_recovery option.
  * Add a read option background_purge_on_iterator_cleanup to avoid deleting files in foreground when destroying iterators. Instead, a job is scheduled in high priority queue and would be executed in a separate background thread.
  * RepairDB support for column families. RepairDB now associates data with non-default column families using information embedded in the SST/WAL files (4.7 or later). For data written by 4.6 or earlier, RepairDB associates it with the default column family.
  * Add options.write_buffer_manager which allows users to control total memtable sizes across multiple DB instances.
