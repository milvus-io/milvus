---
title: Partitioned Index/Filters
layout: post
author: maysamyabandeh
category: blog
---

As DB/mem ratio gets larger, the memory footprint of filter/index blocks becomes non-trivial. Although `cache_index_and_filter_blocks` allows storing only a subset of them in block cache, their relatively large size negatively affects the performance by i) occupying the block cache space that could otherwise be used for caching data, ii) increasing the load on the disk storage by loading them into the cache after a miss. Here we illustrate these problems in more detail and explain how partitioning index/filters alleviates the overhead.

### How large are the index/filter blocks?

RocksDB has by default one index/filter block per SST file. The size of the index/filter varies based on the configuration but for a SST of size 256MB the index/filter block of size 0.5/5MB is typical, which is much larger than the typical data block size of 4-32KB. That is fine when all index/filters fit perfectly into memory and hence are read once per SST lifetime, not so much when they compete with data blocks for the block cache space and are also likely to be re-read many times from the disk.

### What is the big deal with large index/filter blocks?

When index/filter blocks are stored in block cache they are effectively competing with data blocks (as well as with each other) on this scarce resource. A filter of size 5MB is occupying the space that could otherwise be used to cache 1000s of data blocks (of size 4KB). This would result in more cache misses for data blocks. The large index/filters also kick each other out of the block cache more often and exacerbate their own cache miss rate too. This is while only a small part of the index/filter block might have been actually used during its lifetime in the cache.

After the cache miss of an index/filter, it has to be reloaded from the disk, and its large size is not helping in reducing the IO cost. While a simple point lookup might need at most a couple of data block reads (of size 4KB) one from each layer of LSM, it might end up also loading multiple megabytes of index/filter blocks. If that happens often then the disk is spending more time serving index/filters rather than the actual data blocks.

## What is partitioned index/filters?

With partitioning, the index/filter of a SST file is partitioned into smaller blocks with an additional top-level index on them. When reading an index/filter, only top-level index is loaded into memory. The partitioned index/filter then uses the top-level index to load on demand into the block cache the partitions that are required to perform the index/filter query. The top-level index, which has much smaller memory footprint, can be stored in heap or block cache depending on the `cache_index_and_filter_blocks` setting.

### Success stories

#### HDD, 100TB DB

In this example we have a DB of size 86G on HDD and emulate the small memory that is present to a node with 100TB of data by using direct IO (skipping OS file cache) and a very small block cache of size 60MB. Partitioning improves throughput by 11x from 5 op/s to 55 op/s.

#### SSD, Linkbench

In this example we have a DB of size 300G on SSD and emulate the small memory that would be available in presence of other DBs on the same node by by using direct IO (skipping OS file cache) and block cache of size 6G and 2G. Without partitioning the linkbench throughput drops from 38k tps to 23k when reducing block cache size from 6G to 2G. With partitioning the throughput drops from 38k to only 30k.

Learn more [here](https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters).
