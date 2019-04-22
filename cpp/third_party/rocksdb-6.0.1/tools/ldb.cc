//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "rocksdb/ldb_tool.h"

int main(int argc, char** argv) {
  rocksdb::LDBTool tool;
  tool.Run(argc, argv);
  return 0;
}
#else
#include <stdio.h>
int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "Not supported in lite mode.\n");
  return 1;
}
#endif  // ROCKSDB_LITE
