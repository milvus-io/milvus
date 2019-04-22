---
title: WritePrepared Transactions
layout: post
author: maysamyabandeh
category: blog
---

RocksDB supports both optimistic and pessimistic concurrency controls. The pessimistic transactions make use of locks to provide isolation between the transactions. The default write policy in pessimistic transactions is _WriteCommitted_, which means that the data is written to the DB, i.e., the memtable, only after the transaction is committed. This policy simplified the implementation but came with some limitations in throughput, transaction size, and variety in supported isolation levels. In the below, we explain these in detail and present the other write policies, _WritePrepared_ and _WriteUnprepared_. We then dive into the design of _WritePrepared_ transactions.

### WriteCommitted, Pros and Cons

With _WriteCommitted_ write policy, the data is written to the memtable only after the transaction commits. This greatly simplifies the read path as any data that is read by other transactions can be assumed to be committed. This write policy, however, implies that the writes are buffered in memory in the meanwhile. This makes memory a bottleneck for large transactions. The delay of the commit phase in 2PC (two-phase commit) also becomes noticeable since most of the work, i.e., writing to memtable, is done at the commit phase. When the commit of multiple transactions are done in a serial fashion, such as in 2PC implementation of MySQL, the lengthy commit latency becomes a major contributor to lower throughput. Moreover this write policy cannot provide weaker isolation levels, such as READ UNCOMMITTED, that could potentially provide higher throughput for some applications.

### Alternatives: _WritePrepared_ and _WriteUnprepared_

To tackle the lengthy commit issue, we should do memtable writes at earlier phases of 2PC so that the commit phase become lightweight and fast. 2PC is composed of Write stage, where the transaction `::Put` is invoked, the prepare phase, where `::Prepare` is invoked (upon which the DB promises to commit the transaction if later is requested), and commit phase, where `::Commit` is invoked and the transaction writes become visible to all readers. To make the commit phase lightweight, the memtable write could be done at either `::Prepare` or `::Put` stages, resulting into _WritePrepared_ and _WriteUnprepared_ write policies respectively. The downside is that when another transaction is reading data, it would need a way to tell apart which data is committed, and if they are, whether they are committed before the transaction's start, i.e., in the read snapshot of the transaction. _WritePrepared_ would still have the issue of buffering the data, which makes the memory the bottleneck for large transactions. It however provides a good milestone for transitioning from _WriteCommitted_ to _WriteUnprepared_ write policy. Here we explain the design of _WritePrepared_ policy. We will cover the changes that make the design to also supported _WriteUnprepared_ in an upcoming post.

### _WritePrepared_ in a nutshell

These are the primary design questions that needs to be addressed:
1) How do we identify the key/values in the DB with transactions that wrote them?
2) How do we figure if a key/value written by transaction Txn_w is in the read snapshot of the reading transaction Txn_r?
3) How do we rollback the data written by aborted transactions?

With _WritePrepared_, a transaction still buffers the writes in a write batch object in memory. When 2PC `::Prepare` is called, it writes the in-memory write batch to the WAL (write-ahead log) as well as to the memtable(s) (one memtable per column family); We reuse the existing notion of sequence numbers in RocksDB to tag all the key/values in the same write batch with the same sequence number, `prepare_seq`, which is also used as the identifier for the transaction. At commit time, it writes a commit marker to the WAL, whose sequence number, `commit_seq`, will be used as the commit timestamp of the transaction. Before releasing the commit sequence number to the readers, it stores a mapping from `prepare_seq` to `commit_seq` in an in-memory data structure that we call _CommitCache_. When a transaction reading values from the DB (tagged with `prepare_seq`) it makes use of the _CommitCache_ to figure if `commit_seq` of the value is in its read snapshot. To rollback an aborted transaction, we apply the status before the transaction by making another write that cancels out the writes of the aborted transaction.

The _CommitCache_ is a lock-free data structure that caches the recent commit entries. Looking up the entries in the cache must be enough for almost all th transactions that commit in a timely manner. When evicting the older entries from the cache, it still maintains some other data structures to cover the corner cases for transactions that takes abnormally too long to finish. We will cover them in the design details below.

### Benchmark Results
Here we presents the improvements observed in MyRocks with sysbench and linkbench:
* benchmark...........tps.........p95 latency....cpu/query
* insert...................68%
* update-noindex...30%......38%
* update-index.......61%.......28%
* read-write............6%........3.5%
* read-only...........-1.2%.....-1.8%
* linkbench.............1.9%......+overall........0.6%

Here are also the detailed results for [In-Memory Sysbench](https://gist.github.com/maysamyabandeh/bdb868091b2929a6d938615fdcf58424) and [SSD Sysbench](https://gist.github.com/maysamyabandeh/ff94f378ab48925025c34c47eff99306) curtesy of [@mdcallag](https://github.com/mdcallag).

Learn more [here](https://github.com/facebook/rocksdb/wiki/WritePrepared-Transactions).
