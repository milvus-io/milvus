---
title: RocksDB 2015 H2 roadmap
layout: post
author: icanadi
category: blog
redirect_from:
  - /blog/2015/rocksdb-2015-h2-roadmap/
---

Every 6 months, RocksDB team gets together to prioritize the work ahead of us. We just went through this exercise and we wanted to share the results with the community. Here's what RocksDB team will be focusing on for the next 6 months:

<!--truncate-->

**MyRocks**

As you might know, we're working hard to integrate RocksDB as a storage engine for MySQL. This project is pretty important for us because we're heavy users of MySQL. We're already getting pretty good performance results, but there is more work to be done. We need to focus on both performance and stability. The most high priority items on are list are:




  1. Reduce CPU costs of RocksDB as a MySQL storage engine


  2. Implement pessimistic concurrency control to support repeatable read isolation level in MyRocks


  3. Reduce P99 read latency, which is high mostly because of lingering tombstones


  4. Port ZSTD compression


**MongoRocks**

Another database that we're working on is MongoDB. The project of integrating MongoDB with RocksDB storage engine is called MongoRocks. It's already running in production at Parse [1] and we're seeing surprisingly few issues. Our plans for the next half:




  1. Keep improving performance and stability, possibly reuse work done on MyRocks (workloads are pretty similar).


  2. Increase internal and external adoption.


  3. Support new MongoDB 3.2.


**RocksDB on cheaper storage media**

Up to now, our mission was to build the best key-value store “for fast storage” (flash and in-memory). However, there are some use-cases at Facebook that don't need expensive high-end storage. In the next six months, we plan to deploy RocksDB on cheaper storage media. We will optimize performance to RocksDB on either or both:




  1. Hard drive storage array.


  2. Tiered Storage.


**Quality of Service**

When talking to our customers, there are couple of issues that keep reoccurring. We need to fix them to make our customers happy. We will improve RocksDB to provide better assurance of performance and resource usage. Non-exhaustive list includes:




  1. Iterate P99 can be high due to the presence of tombstones.


  2. Write stalls can happen during high write loads.


  3. Better control of memory and disk usage.


  4. Service quality and performance of backup engine.


**Operation's user experience**

As we increase deployment of RocksDB, engineers are spending more time on debugging RocksDB issues. We plan to improve user experience when running RocksDB. The goal is to reduce TTD (time-to-debug). The work includes monitoring, visualizations and documentations.

[1]( http://blog.parse.com/announcements/mongodb-rocksdb-parse/](http://blog.parse.com/announcements/mongodb-rocksdb-parse/)


### Comments

**[Mike](allspace2012@outlook.com)**

What’s the status of this roadmap? “RocksDB on cheaper storage media”, has this been implemented?
