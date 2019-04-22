---
title: RocksDB 3.2 release
layout: post
author: leijin
category: blog
redirect_from:
  - /blog/647/rocksdb-3-2-release/
---

Check out new RocksDB release on [GitHub](https://github.com/facebook/rocksdb/releases/tag/rocksdb-3.2)!

New Features in RocksDB 3.2:

  * PlainTable now supports a new key encoding: for keys of the same prefix, the prefix is only written once. It can be enabled through encoding_type paramter of NewPlainTableFactory()


  * Add AdaptiveTableFactory, which is used to convert from a DB of PlainTable to BlockBasedTabe, or vise versa. It can be created using NewAdaptiveTableFactory()

<!--truncate-->

Public API changes:


  * We removed seek compaction as a concept from RocksDB


  * Add two paramters to NewHashLinkListRepFactory() for logging on too many entries in a hash bucket when flushing


  * Added new option BlockBasedTableOptions::hash_index_allow_collision. When enabled, prefix hash index for block-based table will not store prefix and allow hash collision, reducing memory consumption
