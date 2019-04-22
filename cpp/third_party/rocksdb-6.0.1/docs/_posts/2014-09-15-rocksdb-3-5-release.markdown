---
title: RocksDB 3.5 Release!
layout: post
author: leijin
category: blog
redirect_from:
  - /blog/1547/rocksdb-3-5-release/
---

New RocksDB release - 3.5!


**New Features**


  1. Add include/utilities/write_batch_with_index.h, providing a utility class to query data out of WriteBatch when building it.


  2. new ReadOptions.total_order_seek to force total order seek when block-based table is built with hash index.

<!--truncate-->

**Public API changes**


  1. The Prefix Extractor used with V2 compaction filters is now passed user key to SliceTransform::Transform instead of unparsed RocksDB key.


  2. Move BlockBasedTable related options to BlockBasedTableOptions from Options. Change corresponding JNI interface. Options affected include: no_block_cache, block_cache, block_cache_compressed, block_size, block_size_deviation, block_restart_interval, filter_policy, whole_key_filtering. filter_policy is changed to shared_ptr from a raw pointer.


  3. Remove deprecated options: disable_seek_compaction and db_stats_log_interval


  4. OptimizeForPointLookup() takes one parameter for block cache size. It now builds hash index, bloom filter, and block cache.


[https://github.com/facebook/rocksdb/releases/tag/v3.5](https://github.com/facebook/rocksdb/releases/tag/rocksdb-3.5)
