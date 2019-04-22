// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE
#include <string>
#include <vector>
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace rocksdb {

// An interface for converting a slice to a readable string
class SliceFormatter {
 public:
  virtual ~SliceFormatter() {}
  virtual std::string Format(const Slice& s) const = 0;
};

// Options for customizing ldb tool (beyond the DB Options)
struct LDBOptions {
  // Create LDBOptions with default values for all fields
  LDBOptions();

  // Key formatter that converts a slice to a readable string.
  // Default: Slice::ToString()
  std::shared_ptr<SliceFormatter> key_formatter;

  std::string print_help_header = "ldb - RocksDB Tool";
};

class LDBTool {
 public:
  void Run(
      int argc, char** argv, Options db_options = Options(),
      const LDBOptions& ldb_options = LDBOptions(),
      const std::vector<ColumnFamilyDescriptor>* column_families = nullptr);
};

} // namespace rocksdb

#endif  // ROCKSDB_LITE
