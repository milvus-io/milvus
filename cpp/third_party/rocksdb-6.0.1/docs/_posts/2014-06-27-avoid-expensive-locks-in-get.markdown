---
title: Avoid Expensive Locks in Get()
layout: post
author: leijin
category: blog
redirect_from:
  - /blog/677/avoid-expensive-locks-in-get/
---

As promised in the previous [blog post](blog/2014/05/14/lock.html)!

RocksDB employs a multiversion concurrency control strategy. Before reading data, it needs to grab the current version, which is encapsulated in a data structure called [SuperVersion](https://reviews.facebook.net/rROCKSDB1fdb3f7dc60e96394e3e5b69a46ede5d67fb976c).

<!--truncate-->

At the beginning of `GetImpl()`, it used to do this:


    <span class="zw-portion">mutex_.Lock();
    </span>auto* s = super_version_->Ref();
    mutex_.Unlock();


The lock is necessary because pointer super_version_ may be updated, the corresponding SuperVersion may be deleted while Ref() is in progress.


`Ref()` simply increases the reference counter and returns “this” pointer. However, this simple operation posed big challenges for in-memory workload and stopped RocksDB from scaling read throughput beyond 8 cores. Running 32 read threads on a 32-core CPU leads to [70% system CPU usage](https://github.com/facebook/rocksdb/raw/gh-pages/talks/2014-03-27-RocksDB-Meetup-Lei-Lockless-Get.pdf). This is outrageous!




Luckily, we found a way to circumvent this problem by using [thread local storage](http://en.wikipedia.org/wiki/Thread-local_storage). Version change is a rare event comparable to millions of read requests. On the very first Get() request, each thread pays the mutex cost to acquire a reference to the new super version. Instead of releasing the reference after use, the reference is cached in thread’s local storage. An atomic variable is used to track global super version number. Subsequent reads simply compare the local super version number against the global super version number. If they are the same, the cached super version reference may be used directly, at no cost. If a version change is detected, mutex must be acquired to update the reference. The cost of mutex lock is amortized among millions of reads and becomes negligible.




The code looks something like this:





    SuperVersion* s = thread_local_->Get();
    if (s->version_number != super_version_number_.load()) {
      // slow path, cleanup of current super version is omitted
      mutex_.Lock();
      s = super_version_->Ref();
      mutex_.Unlock();
    }




The result is quite amazing. RocksDB can nicely [scale to 32 cores](https://github.com/facebook/rocksdb/raw/gh-pages/talks/2014-03-27-RocksDB-Meetup-Lei-Lockless-Get.pdf) and most CPU time is spent in user land.




Daryl Grove gives a pretty good [comparison between mutex and atomic](https://blogs.oracle.com/d/entry/the_cost_of_mutexes). However, the real cost difference lies beyond what is shown in the assembly code. Mutex can keep threads spinning on CPU or even trigger thread context switches in which all readers compete to access the critical area. Our approach prevents mutual competition by directing threads to check against a global version which does not change at high frequency, and is therefore much more cache-friendly.




The new approach entails one issue: a thread can visit GetImpl() once but can never come back again. SuperVersion is referenced and cached in its thread local storage. All resources (e.g., memtables, files) which belong to that version are frozen. A “supervisor” is required to visit each thread’s local storage and free its resources without incurring a lock. We designed a lockless sweep using CAS (compare and switch instruction). Here is how it works:




(1) A reader thread uses CAS to acquire SuperVersion from its local storage and to put in a special flag (SuperVersion::kSVInUse).




(2) Upon completion of GetImpl(), the reader thread tries to return SuperVersion to local storage by CAS, expecting the special flag (SuperVersion::kSVInUse) in its local storage. If it does not see SuperVersion::kSVInUse, that means a “sweep” was done and the reader thread is responsible for cleanup (this is expensive, but does not happen often on the hot path).




(3) After any flush/compaction, the background thread performs a sweep (CAS) across all threads’ local storage and frees encountered SuperVersion. A reader thread must re-acquire a new SuperVersion reference on its next visit.

### Comments

**[David Barbour](dmbarbour@gmail.com)**

Please post an example of reading the same rocksdb concurrently.

We are using the latest 3.0 rocksdb; however, when two separate processes
try and open the same rocksdb for reading, only one of the open requests
succeed. The other open always fails with “db/LOCK: Resource temporarily unavailable” So far we have not found an option that allows sharing the rocksdb for reads. An example would be most appreciated.
