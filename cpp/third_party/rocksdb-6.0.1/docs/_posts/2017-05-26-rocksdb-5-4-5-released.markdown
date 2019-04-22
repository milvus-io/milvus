---
title: RocksDB 5.4.5 Released!
layout: post
author: sagar0
category: blog
---

### Public API Change
* Support dynamically changing `stats_dump_period_sec` option via SetDBOptions().
* Added ReadOptions::max_skippable_internal_keys to set a threshold to fail a request as incomplete when too many keys are being skipped while using iterators.
* DB::Get in place of std::string accepts PinnableSlice, which avoids the extra memcpy of value to std::string in most of cases.
    * PinnableSlice releases the pinned resources that contain the value when it is destructed or when ::Reset() is called on it.
    * The old API that accepts std::string, although discouraged, is still supported.
* Replace Options::use_direct_writes with Options::use_direct_io_for_flush_and_compaction. See Direct IO wiki for details.

### New Features
* Memtable flush can be avoided during checkpoint creation if total log file size is smaller than a threshold specified by the user.
* Introduce level-based L0->L0 compactions to reduce file count, so write delays are incurred less often.
* (Experimental) Partitioning filters which creates an index on the partitions. The feature can be enabled by setting partition_filters when using kFullFilter. Currently the feature also requires two-level indexing to be enabled. Number of partitions is the same as the number of partitions for indexes, which is controlled by metadata_block_size.
* DB::ResetStats() to reset internal stats.
* Added CompactionEventListener and EventListener::OnFlushBegin interfaces.
* Added DB::CreateColumnFamilie() and DB::DropColumnFamilies() to bulk create/drop column families.
* Facility for cross-building RocksJava using Docker.

### Bug Fixes
* Fix WriteBatchWithIndex address use after scope error.
* Fix WritableFile buffer size in direct IO.
* Add prefetch to PosixRandomAccessFile in buffered io.
* Fix PinnableSlice access invalid address when row cache is enabled.
* Fix huge fallocate calls fail and make XFS unhappy.
* Fix memory alignment with logical sector size.
* Fix alignment in ReadaheadRandomAccessFile.
* Fix bias with read amplification stats (READ_AMP_ESTIMATE_USEFUL_BYTES and READ_AMP_TOTAL_READ_BYTES).
* Fix a manual / auto compaction data race.
* Fix CentOS 5 cross-building of RocksJava.
* Build and link with ZStd when creating the static RocksJava build.
* Fix snprintf's usage to be cross-platform.
* Fix build errors with blob DB.
* Fix readamp test type inconsistency.
