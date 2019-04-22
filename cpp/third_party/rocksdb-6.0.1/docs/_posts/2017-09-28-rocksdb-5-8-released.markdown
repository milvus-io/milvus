---
title: RocksDB 5.8 Released!
layout: post
author: maysamyabandeh
category: blog
---

### Public API Change
* Users of `Statistics::getHistogramString()` will see fewer histogram buckets and different bucket endpoints.
* `Slice::compare` and BytewiseComparator `Compare` no longer accept `Slice`s containing nullptr.
* `Transaction::Get` and `Transaction::GetForUpdate` variants with `PinnableSlice` added.

### New Features
* Add Iterator::Refresh(), which allows users to update the iterator state so that they can avoid some initialization costs of recreating iterators.
* Replace dynamic_cast<> (except unit test) so people can choose to build with RTTI off. With make, release mode is by default built with -fno-rtti and debug mode is built without it. Users can override it by setting USE_RTTI=0 or 1.
* Universal compactions including the bottom level can be executed in a dedicated thread pool. This alleviates head-of-line blocking in the compaction queue, which cause write stalling, particularly in multi-instance use cases. Users can enable this feature via `Env::SetBackgroundThreads(N, Env::Priority::BOTTOM)`, where `N > 0`.
* Allow merge operator to be called even with a single merge operand during compactions, by appropriately overriding `MergeOperator::AllowSingleOperand`.
* Add `DB::VerifyChecksum()`, which verifies the checksums in all SST files in a running DB.
* Block-based table support for disabling checksums by setting `BlockBasedTableOptions::checksum = kNoChecksum`.

### Bug Fixes
* Fix wrong latencies in `rocksdb.db.get.micros`, `rocksdb.db.write.micros`, and `rocksdb.sst.read.micros`.
* Fix incorrect dropping of deletions during intra-L0 compaction.
* Fix transient reappearance of keys covered by range deletions when memtable prefix bloom filter is enabled.
* Fix potentially wrong file smallest key when range deletions separated by snapshot are written together.
