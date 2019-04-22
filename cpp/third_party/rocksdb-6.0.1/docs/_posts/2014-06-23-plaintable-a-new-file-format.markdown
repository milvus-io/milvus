---
title: PlainTable — A New File Format
layout: post
author: sdong
category: blog
redirect_from:
  - /blog/599/plaintable-a-new-file-format/
---

In this post, we are introducing "PlainTable" -- a file format we designed for RocksDB, initially to satisfy a production use case at Facebook.

Design goals:

1. All data stored in memory, in files stored in tmpfs/ramfs. Support DBs larger than 100GB (may be sharded across multiple RocksDB instance).
1. Optimize for [prefix hashing](https://github.com/facebook/rocksdb/raw/gh-pages/talks/2014-03-27-RocksDB-Meetup-Siying-Prefix-Hash.pdf)
1. Less than or around 1 micro-second average latency for single Get() or Seek().
1. Minimize memory consumption.
1. Queries efficiently return empty results

<!--truncate-->

Notice that our priority was not to maximize query performance, but to strike a balance between query performance and memory consumption. PlainTable query performance is not as good as you would see with a nicely-designed hash table, but they are of the same order of magnitude, while keeping memory overhead to a minimum.

Since we are targeting micro-second latency, it is on the level of the number of CPU cache misses (if they cannot be parallellized, which are usually the case for index look-ups). On our target hardware with Intel CPUs of multiple sockets with NUMA, we can only allow 4-5 CPU cache misses (including costs of data TLB).

To meet our requirements, given that only hash prefix iterating is needed, we made two decisions:

1. to use a hash index, which is
1. directly addressed to rows, with no block structure.

Having addressed our latency goal, the next task was to design a very compact hash index to minimize memory consumption. Some tricks we used to meet this goal:

1. We only use 32-bit integers for data and index offsets.The first bit serves as a flag, so we can avoid using 8-byte pointers.
1. We never copy keys or parts of keys to index search structures. We store only offsets from which keys can be retrieved, to make comparisons with search keys.
1. Since our file is immutable, we can accurately estimate the number of hash buckets needed.

To make sure the format works efficiently with empty queries, we added a bloom filter check before the query. This adds only one cache miss for non-empty cases [1], but avoids multiple cache misses for most empty results queries. This is a good trade-off for use cases with a large percentage of empty results.

These are the design goals and basic ideas of PlainTable file format. For detailed information, see [this wiki page](https://github.com/facebook/rocksdb/wiki/PlainTable-Format).

[1] Bloom filter checks typically require multiple memory access. However, because they are independent, they usually do not make the CPU pipeline stale. In any case, we improved the bloom filter to improve data locality - we may cover this further in a future blog post.

### Comments

**[Siying Dong](siying.d@fb.com)**

Does [http://rocksdb.org/feed/](http://rocksdb.org/feed/) work?
