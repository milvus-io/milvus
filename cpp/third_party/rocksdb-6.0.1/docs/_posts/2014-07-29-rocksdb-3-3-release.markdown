---
title: RocksDB 3.3 Release
layout: post
author: yhciang
category: blog
redirect_from:
  - /blog/1301/rocksdb-3-3-release/
---

Check out new RocksDB release on [GitHub](https://github.com/facebook/rocksdb/releases/tag/rocksdb-3.3)!

New Features in RocksDB 3.3:

  * **JSON API prototype**.


  * **Performance improvement on HashLinkList**:  We addressed performance outlier of HashLinkList caused by skewed bucket by switching data in the bucket from linked list to skip list. Add parameter threshold_use_skiplist in NewHashLinkListRepFactory().

<!--truncate-->

  * **More effective on storage space reclaim**:  RocksDB is now able to reclaim storage space more effectively during the compaction process.  This is done by compensating the size of each deletion entry by the 2X average value size, which makes compaction to be triggerred by deletion entries more easily.


  * **TimeOut API to write**:  Now WriteOptions have a variable called timeout_hint_us.  With timeout_hint_us set to non-zero, any write associated with this timeout_hint_us may be aborted when it runs longer than the specified timeout_hint_us, and it is guaranteed that any write completes earlier than the specified time-out will not be aborted due to the time-out condition.


  * **rate_limiter option**: We added an option that controls total throughput of flush and compaction. The throughput is specified in bytes/sec. Flush always has precedence over compaction when available bandwidth is constrained.



Public API changes:


  * Removed NewTotalOrderPlainTableFactory because it is not used and implemented semantically incorrect.
