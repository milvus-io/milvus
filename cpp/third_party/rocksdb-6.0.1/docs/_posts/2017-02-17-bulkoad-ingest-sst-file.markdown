---
title: Bulkloading by ingesting external SST files
layout: post
author: IslamAbdelRahman
category: blog
---

## Introduction

One of the basic operations of RocksDB is writing to RocksDB, Writes happen when user call (DB::Put, DB::Write, DB::Delete ... ), but what happens when you write to RocksDB ? .. this is a brief description of what happens.
- User insert a new key/value by calling DB::Put() (or DB::Write())
- We create a new entry for the new key/value in our in-memory structure (memtable / SkipList by default) and we assign it a new sequence number.
- When the memtable exceeds a specific size (64 MB for example), we convert this memtable to a SST file, and put this file in level 0 of our LSM-Tree
- Later, compaction will kick in and move data from level 0 to level 1, and then from level 1 to level 2 .. and so on 

But what if we can skip these steps and add data to the lowest possible level directly ? This is what bulk-loading does

## Bulkloading

- Write all of our keys and values into SST file outside of the DB
- Add the SST file into the LSM directly

This is bulk-loading, and in specific use-cases it allow users to achieve faster data loading and better write-amplification.

and doing it is as simple as 
```cpp
Options options;
SstFileWriter sst_file_writer(EnvOptions(), options, options.comparator);
Status s = sst_file_writer.Open(file_path);
assert(s.ok());

// Insert rows into the SST file, note that inserted keys must be 
// strictly increasing (based on options.comparator)
for (...) {
  s = sst_file_writer.Add(key, value);
  assert(s.ok());
}

// Ingest the external SST file into the DB
s = db_->IngestExternalFile({"/home/usr/file1.sst"}, IngestExternalFileOptions());
assert(s.ok());
```

You can find more details about how to generate SST files and ingesting them into RocksDB in this [wiki page](https://github.com/facebook/rocksdb/wiki/Creating-and-Ingesting-SST-files)

## Use cases
There are multiple use cases where bulkloading could be useful, for example
- Generating SST files in offline jobs in Hadoop, then downloading and ingesting the SST files into RocksDB
- Migrating shards between machines by dumping key-range in SST File and loading the file in a different machine
- Migrating from a different storage (InnoDB to RocksDB migration in MyRocks)
