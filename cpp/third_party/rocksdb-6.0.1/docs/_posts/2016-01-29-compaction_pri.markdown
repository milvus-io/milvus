---
title: Option of Compaction Priority
layout: post
author: sdong
category: blog
redirect_from:
  - /blog/2921/compaction_pri/
---

The most popular compaction style of RocksDB is level-based compaction, which is an improved version of LevelDB's compaction algorithm. Page 9- 16 of this [slides](https://github.com/facebook/rocksdb/blob/gh-pages/talks/2015-09-29-HPTS-Siying-RocksDB.pdf) gives an illustrated introduction of this compaction style. The basic idea that: data is organized by multiple levels with exponential increasing target size. Except a special level 0, every level is key-range partitioned into many files. When size of a level exceeds its target size, we pick one or more of its files, and merge the file into the next level.

<!--truncate-->

Which file to pick to compact is an interesting question. LevelDB only uses one thread for compaction and it always picks files in round robin manner. We implemented multi-thread compaction in RocksDB by picking multiple files from the same level and compact them in parallel. We had to move away from LevelDB's file picking approach. Recently, we created an option [options.compaction_pri](https://github.com/facebook/rocksdb/blob/d6c838f1e130d8860407bc771fa6d4ac238859ba/include/rocksdb/options.h#L83-L93), which indicated three different algorithms to pick files to compact.

Why do we need to multiple algorithms to choose from? Because there are different factors to consider when picking the files, and we now don't yet know how to balance them automatically, so we expose it to users to choose. Here are factors to consider:

**Write amplification**

When we estimate write amplification, we usually simplify the problem by assuming keys are uniformly distributed inside each level. In reality, it is not the case, even if user updates are uniformly distributed across the whole key range. For instance, when we compact one file of a level to the next level, it creates a hole. Over time, incoming compaction will fill data to the hole, but the density will still be lower for a while. Picking a file with keys least densely populated is more expensive to get the file to the next level, because there will be more overlapping files in the next level so we need to rewrite more data. For example, assume a file is 100MB, if an L2 file overlaps with 8 L3 files, we need to rewrite about 800MB of data to get the file to L3. If the file overlaps with 12 L3 files, we'll need to rewrite about 1200MB to get a file of the same size out of L2. It uses 50% more writes. (This analysis ignores the key density of the next level, because the range covers N times of files in that level so one hole only impacts write amplification by 1/N)

If all the updates are uniformly distributed, LevelDB's approach optimizes write amplification, because a file being picked covers a range whose last compaction time to the next level is the oldest, so the range will accumulated keys from incoming compactions for the longest and the density is the highest.

We created a compaction priority **kOldestSmallestSeqFirst** for the same effect. With this mode, we always pick the file covers the oldest updates in the level, which usually is contains the densest key range. If you have a use case where writes are uniformly distributed across the key space and you want to reduce write amplification, you should set options.compaction_pri=kOldestSmallestSeqFirst.

**Optimize for small working set**

We are assuming updates are uniformly distributed across the whole key space in previous analysis. However, in many use cases, there are subset of keys that are frequently updated while other key ranges are very cold. In this case, keeping hot key ranges from compacting to deeper levels will benefit write amplification, as well as space amplification. For example, if in a DB only key 150-160 are updated and other keys are seldom updated. If level 1 contains 20 keys, we want to keep 150-160 all stay in level 1. Because when next level 0 -> 1 compaction comes, it will simply overwrite existing keys so size level 1 doesn't increase, so no need to schedule further compaction for level 1->2. On the other hand, if we compact key 150-155 to level2, when a new Level 1->2 compaction comes, it increases the size of level 1, making size of level 1 exceed target size and more compactions will be needed, which generates more writes.

The compaction priority **kOldestLargestSeqFirst** optimizes this use case. In this mode, we will pick a file whose latest update is the oldest. It means there is no incoming data for the range for the longest. Usually it is the coldest range. By compacting coldest range first, we leave the hot ranges in the level. If your use case is to overwrite existing keys in a small range, try options.compaction_pri=kOldestLargestSeqFirst**.**

**Drop delete marker sooner**

If one file contains a lot of delete markers, it may slow down iterating over this area, because we still need to iterate those deleted keys just to ignore them. Furthermore, the sooner we compact delete keys into the last level, the sooner the disk space is reclaimed, so it is good for space efficiency.

Our default compaction priority **kByCompensatedSize** considers the case. If number of deletes in a file exceeds number of inserts, it is more likely to be picked for compaction. The more number of deletes exceed inserts, the more likely it is being compacted. The optimization is added to avoid the worst performance of space efficiency and query performance when a large percentage of the DB is deleted.

**Efficiency of compaction filter**

Usually people use [compaction filters](https://github.com/facebook/rocksdb/blob/v4.1/include/rocksdb/options.h#L201-L226) to clean up old data to free up space. Picking files to compact may impact space efficiency. We don't yet have a a compaction priority to optimize this case. In some of our use cases, we solved the problem in a different way: we have an external service checking modify time of all SST files. If any of the files is too old, we force the single file to compaction by calling DB::CompactFiles() using the single file. In this way, we can provide a time bound of data passing through compaction filters.


In all, there three choices of compaction priority modes optimizing different scenarios. if you have a new use case, we suggest you start with `options.compaction_pri=kOldestSmallestSeqFirst` (note it is not the default one for backward compatible reason). If you want to further optimize your use case, you can try other two use cases if your use cases apply.

If you have good ideas about better compaction picker approach, you are welcome to implement and benchmark it. We'll be glad to review and merge your a pull requests.

### Comments

**[Mark Callaghan](mdcallag@gmail.com)**

Performance results for compaction_pri values and linkbench are explained at [http://smalldatum.blogspot.com/2016/02/compaction-priority-in-rocksdb.html](http://smalldatum.blogspot.com/2016/02/compaction-priority-in-rocksdb.html)
