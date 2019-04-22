// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <string>

#include "rocksdb/slice.h"
#include "utilities/compaction_filters/remove_emptyvalue_compactionfilter.h"

namespace rocksdb {

const char* RemoveEmptyValueCompactionFilter::Name() const {
  return "RemoveEmptyValueCompactionFilter";
}

bool RemoveEmptyValueCompactionFilter::Filter(int /*level*/,
                                              const Slice& /*key*/,
                                              const Slice& existing_value,
                                              std::string* /*new_value*/,
                                              bool* /*value_changed*/) const {
  // remove kv pairs that have empty values
  return existing_value.empty();
}

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
