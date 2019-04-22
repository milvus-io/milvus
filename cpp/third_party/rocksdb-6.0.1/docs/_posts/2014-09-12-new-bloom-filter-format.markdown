---
title: New Bloom Filter Format
layout: post
author: zagfox
category: blog
redirect_from:
  - /blog/1367/cuckoo/
---

## Introduction

In this post, we are introducing "full filter block" --- a new bloom filter format for [block based table](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format). This could bring about 40% of improvement for key query under in-memory (all data stored in memory, files stored in tmpfs/ramfs, an [example](https://github.com/facebook/rocksdb/wiki/RocksDB-In-Memory-Workload-Performance-Benchmarks) workload. The main idea behind is to generate a big filter that covers all the keys in SST file to avoid lots of unnecessary memory look ups.


<!--truncate-->

## What is Bloom Filter

In brief, [bloom filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter) is a bits array generated for a set of keys that could tell if an arbitrary key may exist in that set.

In RocksDB, we generate such a bloom filter for each SST file. When we conduct a query for a key, we first goes to the bloom filter block of SST file. If key may exist in filter, we goes into data block in SST file to search for the key. If not, we would return directly. So it could help speed up point look up operation a lot.

## Original Bloom Filter Format

Original bloom filter creates filters for each individual data block in SST file. It has complex structure (ref [here](https://github.com/facebook/rocksdb/wiki/Rocksdb-BlockBasedTable-Format#filter-meta-block)) which results in a lot of non-adjacent memory look ups.

Here's the work flow for checking original bloom filter in block based table:

1. Given the target key, we goes to the index block to get the "data block ID" where this key may reside.
1. Using the "data block ID", we goes to the filter block and get the correct "offset of filter".
1. Using the "offset of filter", we goes to the actual filter and do the checking.

## New Bloom Filter Format

New bloom filter creates filter for all keys in SST file and we name it "full filter". The data structure of full filter is very simple, there is just one big filter:

    [ full filter ]

In this way, the work flow of bloom filter checking is much simplified.

(1) Given the target key, we goes directly to the filter block and conduct the filter checking.

To be specific, there would be no checking for index block and no address jumping inside of filter block.

Though it is a big filter, the total filter size would be the same as the original filter.

One little draw back is that the new bloom filter introduces more memory consumption when building SST file because we need to buffer keys (or their hashes) before generating filter. Original filter just creates a bunch of small filters so it just buffer a small amount of keys. For full filter, we buffer hashes of all keys, which would take more memory when SST file size increases.


## Usage & Customization

You can refer to the document here for [usage](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#usage-of-new-bloom-filter) and [customization](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#customize-your-own-filterpolicy).
