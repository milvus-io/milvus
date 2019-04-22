---
title: RocksDB 5.5.1 Released!
layout: post
author: lightmark
category: blog
---

### New Features
* FIFO compaction to support Intra L0 compaction too with CompactionOptionsFIFO.allow_compaction=true.
* Statistics::Reset() to reset user stats.
* ldb add option --try_load_options, which will open DB with its own option file.
* Introduce WriteBatch::PopSavePoint to pop the most recent save point explicitly.
* Support dynamically change `max_open_files` option via SetDBOptions()
* Added DB::CreateColumnFamilie() and DB::DropColumnFamilies() to bulk create/drop column families.
* Add debugging function `GetAllKeyVersions` to see internal versions of a range of keys.
* Support file ingestion with universal compaction style
* Support file ingestion behind with option `allow_ingest_behind`
* New option enable_pipelined_write which may improve write throughput in case writing from multiple threads and WAL enabled.

### Bug Fixes
* Fix the bug that Direct I/O uses direct reads for non-SST file
* Fix the bug that flush doesn't respond to fsync result
