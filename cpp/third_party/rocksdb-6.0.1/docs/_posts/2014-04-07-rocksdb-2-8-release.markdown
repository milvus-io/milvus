---
title: RocksDB 2.8 release
layout: post
author: icanadi
category: blog
redirect_from:
  - /blog/371/rocksdb-2-8-release/
---

Check out the new RocksDB 2.8 release on [Github](https://github.com/facebook/rocksdb/releases/tag/2.8.fb).

RocksDB 2.8. is mostly focused on improving performance for in-memory workloads. We are seeing read QPS as high as 5M (we will write a separate blog post on this).

<!--truncate-->

Here is the summary of new features:

  * Added a new table format called PlainTable, which is optimized for RAM storage (ramfs or tmpfs). You can read more details about it on [our wiki](https://github.com/facebook/rocksdb/wiki/PlainTable-Format).


  * New prefixed memtable format HashLinkedList, which is optimized for cases where there are only a few keys for each prefix.


  * Merge operator supports a new function PartialMergeMulti() that allows users to do partial merges against multiple operands. This function enables big speedups for workloads that use merge operators.


  * Added a V2 compaction filter interface. It buffers the kv-pairs sharing the same key prefix, process them in batches, and return the batched results back to DB.


  * Geo-spatial support for locations and radial-search.


  * Improved read performance using thread local cache for frequently accessed data.


  * Stability improvements -- we're now ignoring partially written tailing record to MANIFEST or WAL files.



We have also introduced small incompatible API changes (mostly for advanced users). You can see full release notes in our [HISTORY.my](https://github.com/facebook/rocksdb/blob/2.8.fb/HISTORY.md) file.
