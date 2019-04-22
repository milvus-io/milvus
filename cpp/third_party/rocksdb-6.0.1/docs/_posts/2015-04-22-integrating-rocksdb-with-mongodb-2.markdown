---
title: Integrating RocksDB with MongoDB
layout: post
author: icanadi
category: blog
redirect_from:
  - /blog/1967/integrating-rocksdb-with-mongodb-2/
---

Over the last couple of years, we have been busy integrating RocksDB with various services here at Facebook that needed to store key-value pairs locally. We have also seen other companies using RocksDB as local storage components of their distributed systems.

<!--truncate-->

The next big challenge for us is to bring RocksDB storage engine to general purpose databases. Today we have an exciting milestone to share with our community! We're running MongoDB with RocksDB in production and seeing great results! You can read more about it here: [http://blog.parse.com/announcements/mongodb-rocksdb-parse/](http://blog.parse.com/announcements/mongodb-rocksdb-parse/)

Keep tuned for benchmarks and more stability and performance improvements.
