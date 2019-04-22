---
title: RocksDB Options File
layout: post
author: yhciang
category: blog
redirect_from:
  - /blog/3089/rocksdb-options-file/
---

In RocksDB 4.3, we added a new set of features that makes managing RocksDB options easier.  Specifically:

  * **Persisting Options Automatically**: Each RocksDB database will now automatically persist its current set of options into an INI file on every successful call of DB::Open(), SetOptions(), and CreateColumnFamily() / DropColumnFamily().



  * **Load Options from File**: We added [LoadLatestOptions() / LoadOptionsFromFile()](https://github.com/facebook/rocksdb/blob/4.3.fb/include/rocksdb/utilities/options_util.h#L48-L58) that enables developers to construct RocksDB options object from an options file.



  * **Sanity Check Options**: We added [CheckOptionsCompatibility](https://github.com/facebook/rocksdb/blob/4.3.fb/include/rocksdb/utilities/options_util.h#L64-L77) that performs compatibility check on two sets of RocksDB options.

<!--truncate-->

Want to know more about how to use this new features? Check out the [RocksDB Options File wiki page](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File) and start using this new feature today!
