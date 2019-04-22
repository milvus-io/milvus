---
title: 'WriteBatchWithIndex: Utility for Implementing Read-Your-Own-Writes'
layout: post
author: sdong
category: blog
redirect_from:
  - /blog/1901/write-batch-with-index/
---

RocksDB can be used as a storage engine of a higher level database. In fact, we are currently plugging RocksDB into MySQL and MongoDB as one of their storage engines. RocksDB can help with guaranteeing some of the ACID properties: durability is guaranteed by RocksDB by design; while consistency and isolation need to be enforced by concurrency controls on top of RocksDB; Atomicity can be implemented by committing a transaction's writes with one write batch to RocksDB in the end.

<!--truncate-->

However, if we enforce atomicity by only committing all writes in the end of the transaction in one batch, you cannot get the updated value from RocksDB previously written by the same transaction (read-your-own-write). To read the updated value, the databases on top of RocksDB need to maintain an internal buffer for all the written keys, and when a read happens they need to merge the result from RocksDB and from this buffer. This is a problem we faced when building the RocksDB storage engine in MongoDB. We solved it by creating a utility class, WriteBatchWithIndex (a write batch with a searchable index) and made it part of public API so that the community can also benefit from it.

Before talking about the index part, let me introduce write batch first. The write batch class, `WriteBatch`, is a RocksDB data structure for atomic writes of multiple keys. Users can buffer their updates to a `WriteBatch` by calling `write_batch.Put("key1", "value1")` or `write_batch.Delete("key2")`, similar as calling RocksDB's functions of the same names. In the end, they call `db->Write(write_batch)` to atomically update all those batched operations to the DB. It is how a database can guarantee atomicity, as shown above. Adding a searchable index to `WriteBatch`, we now have `WriteBatchWithIndex`. Users can put updates to WriteBatchIndex in the same way as to `WriteBatch`. In the end, users can get a `WriteBatch` object from it and issue `db->Write()`. Additionally, users can create an iterator of a WriteBatchWithIndex, seek to any key location and iterate from there.

To implement read-your-own-write using `WriteBatchWithIndex`, every time the user creates a transaction, we create a `WriteBatchWithIndex` attached to it. All the writes of the transaction go to the `WriteBatchWithIndex` first. When we commit the transaction, we atomically write the batch to RocksDB. When the user wants to call `Get()`, we first check if the value exists in the `WriteBatchWithIndex` and return the value if existing, by seeking and reading from an iterator of the write batch, before checking data in RocksDB. For example, here is the we implement it in MongoDB's RocksDB storage engine: [link](https://github.com/mongodb/mongo/blob/a31cc114a89a3645e97645805ba77db32c433dce/src/mongo/db/storage/rocks/rocks_recovery_unit.cpp#L245-L260). If a range query comes, we pass a DB's iterator to `WriteBatchWithIndex`, which creates a super iterator which combines the results from the DB iterator with the batch's iterator. Using this super iterator, we can iterate the DB with the transaction's own writes. Here is the iterator creation codes in MongoDB's RocksDB storage engine: [link](https://github.com/mongodb/mongo/blob/a31cc114a89a3645e97645805ba77db32c433dce/src/mongo/db/storage/rocks/rocks_recovery_unit.cpp#L266-L269). In this way, the database can solve the read-your-own-write problem by using RocksDB to handle a transaction's uncommitted writes.

Using `WriteBatchWithIndex`, we successfully implemented read-your-own-writes in the RocksDB storage engine of MongoDB. If you also have a read-your-own-write problem, `WriteBatchWithIndex` can help you implement it quickly and correctly.
