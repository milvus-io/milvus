---
title: RocksDB 5.2.1 Released!
layout: post
author: sdong
category: blog
---

### Public API Change
* NewLRUCache() will determine number of shard bits automatically based on capacity, if the user doesn't pass one. This also impacts the default block cache when the user doesn't explict provide one.
* Change the default of delayed slowdown value to 16MB/s and further increase the L0 stop condition to 36 files.

### New Features
* Added new overloaded function GetApproximateSizes that allows to specify if memtable stats should be computed only without computing SST files' stats approximations.
* Added new function GetApproximateMemTableStats that approximates both number of records and size of memtables.
* (Experimental) Two-level indexing that partition the index and creates a 2nd level index on the partitions. The feature can be enabled by setting kTwoLevelIndexSearch as IndexType and configuring index_per_partition.

### Bug Fixes
* RangeSync() should work if ROCKSDB_FALLOCATE_PRESENT is not set
* Fix wrong results in a data race case in Get()
* Some fixes related to 2PC.
* Fix several bugs in Direct I/O supports.
* Fix a regression bug which can cause Seek() to miss some keys if the return key has been updated many times after the snapshot which is used by the iterator.
