---
title: Migrating from LevelDB to RocksDB
layout: post
author: lgalanis
category: blog
redirect_from:
  - /blog/1811/migrating-from-leveldb-to-rocksdb-2/
---

If you have an existing application that uses LevelDB and would like to migrate to using RocksDB, one problem you need to overcome is to map the options for LevelDB to proper options for RocksDB. As of release 3.9 this can be automatically done by using our option conversion utility found in rocksdb/utilities/leveldb_options.h. What is needed, is to first replace `leveldb::Options` with `rocksdb::LevelDBOptions`. Then, use `rocksdb::ConvertOptions( )` to convert the `LevelDBOptions` struct into appropriate RocksDB options. Here is an example:

<!--truncate-->

LevelDB code:

```c++
#include <string>
#include "leveldb/db.h"

using namespace leveldb;

int main(int argc, char** argv) {
  DB *db;

  Options opt;
  opt.create_if_missing = true;
  opt.max_open_files = 1000;
  opt.block_size = 4096;

  Status s = DB::Open(opt, "/tmp/mydb", &db);

  delete db;
}
```

RocksDB code:

```c++
#include <string>  
#include "rocksdb/db.h"  
#include "rocksdb/utilities/leveldb_options.h"  

using namespace rocksdb;  

int main(int argc, char** argv) {  
  DB *db;  

  LevelDBOptions opt;  
  opt.create_if_missing = true;  
  opt.max_open_files = 1000;  
  opt.block_size = 4096;  

  Options rocksdb_options = ConvertOptions(opt);  
  // add rocksdb specific options here  

  Status s = DB::Open(rocksdb_options, "/tmp/mydb_rocks", &db);

  delete db;  
}  
```

The difference is:

```diff
-#include "leveldb/db.h"
+#include "rocksdb/db.h"
+#include "rocksdb/utilities/leveldb_options.h"

-using namespace leveldb;
+using namespace rocksdb;

-  Options opt;
+  LevelDBOptions opt;

-  Status s = DB::Open(opt, "/tmp/mydb", &db);
+  Options rocksdb_options = ConvertOptions(opt);
+  // add rockdb specific options here
+
+  Status s = DB::Open(rocksdb_options, "/tmp/mydb_rocks", &db);
```

Once you get up and running with RocksDB you can then focus on tuning RocksDB further by modifying the converted options struct.

The reason why ConvertOptions is handy is because a lot of individual options in RocksDB have moved to other structures in different components. For example, block_size is not available in struct rocksdb::Options. It resides in struct rocksdb::BlockBasedTableOptions, which is used to create a TableFactory object that RocksDB uses internally to create the proper TableBuilder objects. If you were to write your application from scratch it would look like this:

RocksDB code from scratch:

```c++
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/table.h"

using namespace rocksdb;

int main(int argc, char** argv) {
  DB *db;

  Options opt;
  opt.create_if_missing = true;
  opt.max_open_files = 1000;

  BlockBasedTableOptions topt;
  topt.block_size = 4096;
  opt.table_factory.reset(NewBlockBasedTableFactory(topt));

  Status s = DB::Open(opt, "/tmp/mydb_rocks", &db);

  delete db;
}
```

The LevelDBOptions utility can ease migration to RocksDB from LevelDB and allows us to break down the various options across classes as it is needed.
