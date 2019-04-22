---
title: Reading RocksDB options from a file
layout: post
author: lgalanis
category: blog
redirect_from:
  - /blog/1883/reading-rocksdb-options-from-a-file/
---

RocksDB options can be provided using a file or any string to RocksDB. The format is straightforward: `write_buffer_size=1024;max_write_buffer_number=2`. Any whitespace around `=` and `;` is OK. Moreover, options can be nested as necessary. For example `BlockBasedTableOptions` can be nested as follows: `write_buffer_size=1024; max_write_buffer_number=2; block_based_table_factory={block_size=4k};`. Similarly any white space around `{` or `}` is ok. Here is what it looks like in code:

<!--truncate-->

```c++
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/convenience.h"

using namespace rocksdb;                                                                                           

int main(int argc, char** argv) {                                                                                  
  DB *db;

  Options opt;

  std::string options_string =                                                                                     
    "create_if_missing=true;max_open_files=1000;"                                                                  
    "block_based_table_factory={block_size=4096}";                                                                 

  Status s = GetDBOptionsFromString(opt, options_string, &opt);

  s = DB::Open(opt, "/tmp/mydb_rocks", &db);                                                                       

  // use db

  delete db;
}
```

Using `GetDBOptionsFromString` is a convenient way of changing options for your RocksDB application without needing to resort to recompilation or tedious command line parsing.
