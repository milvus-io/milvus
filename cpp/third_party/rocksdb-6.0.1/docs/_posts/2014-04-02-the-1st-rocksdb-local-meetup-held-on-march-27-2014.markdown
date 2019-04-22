---
title: The 1st RocksDB Local Meetup Held on March 27, 2014
layout: post
author: xjin
category: blog
redirect_from:
  - /blog/323/the-1st-rocksdb-local-meetup-held-on-march-27-2014/
---

On Mar 27, 2014, RocksDB team @ Facebook held the 1st RocksDB local meetup in FB HQ (Menlo Park, California). We invited around 80 guests from 20+ local companies, including LinkedIn, Twitter, Dropbox, Square, Pinterest, MapR, Microsoft and IBM. Finally around 50 guests showed up, totaling around 60% show-up rate.

<!--truncate-->

[![Resize of 20140327_200754](/static/images/Resize-of-20140327_200754-300x225.jpg)](/static/images/Resize-of-20140327_200754-300x225.jpg)

RocksDB team @ Facebook gave four talks about the latest progress and experience on RocksDB:




  * [Supporting a 1PB In-Memory Workload](https://github.com/facebook/rocksdb/raw/gh-pages/talks/2014-03-27-RocksDB-Meetup-Haobo-RocksDB-In-Memory.pdf)




  * [Column Families in RocksDB](https://github.com/facebook/rocksdb/raw/gh-pages/talks/2014-03-27-RocksDB-Meetup-Igor-Column-Families.pdf)




  * ["Lockless" Get() in RocksDB?](https://github.com/facebook/rocksdb/raw/gh-pages/talks/2014-03-27-RocksDB-Meetup-Lei-Lockless-Get.pdf)




  * [Prefix Hashing in RocksDB](https://github.com/facebook/rocksdb/raw/gh-pages/talks/2014-03-27-RocksDB-Meetup-Siying-Prefix-Hash.pdf)


A very interesting question asked by a massive number of guests is: does RocksDB plan to provide replication functionality? Obviously, many applications need a resilient and distributed storage solution, not just single-node storage. We are considering how to approach this issue.

When will be the next meetup? We haven't decided yet. We will see whether the community is interested in it and how it can help RocksDB grow.

If you have any questions or feedback for the meetup or RocksDB, please let us know in [our Facebook group](https://www.facebook.com/groups/rocksdb.dev/).

### Comments

**[Rajiv](geetasen@gmail.com)**

Have any of these talks been recorded and if so will they be published?

**[Igor Canadi](icanadi@fb.com)**

Yes, I think we plan to publish them soon.
