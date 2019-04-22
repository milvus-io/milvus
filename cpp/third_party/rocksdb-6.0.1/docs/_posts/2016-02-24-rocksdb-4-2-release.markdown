---
title: RocksDB 4.2 Release!
layout: post
author: sdong
category: blog
redirect_from:
  - /blog/3017/rocksdb-4-2-release/
---

New RocksDB release - 4.2!


**New Features**

  1. Introduce CreateLoggerFromOptions(), this function create a Logger for provided DBOptions.


  2. Add GetAggregatedIntProperty(), which returns the sum of the GetIntProperty of all the column families.


  3. Add MemoryUtil in rocksdb/utilities/memory.h. It currently offers a way to get the memory usage by type from a list rocksdb instances.


<!--truncate-->


**Public API changes**

  1. CompactionFilter::Context includes information of Column Family ID


  2. The need-compaction hint given by TablePropertiesCollector::NeedCompact() will be persistent and recoverable after DB recovery. This introduces a breaking format change. If you use this experimental feature, including NewCompactOnDeletionCollectorFactory() in the new version, you may not be able to directly downgrade the DB back to version 4.0 or lower.


  3. TablePropertiesCollectorFactory::CreateTablePropertiesCollector() now takes an option Context, containing the information of column family ID for the file being written.


  4. Remove DefaultCompactionFilterFactory.


[https://github.com/facebook/rocksdb/releases/tag/v4.2](https://github.com/facebook/rocksdb/releases/tag/v4.2)
