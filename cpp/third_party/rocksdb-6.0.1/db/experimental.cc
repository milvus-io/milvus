//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/experimental.h"

#include "db/db_impl.h"

namespace rocksdb {
namespace experimental {

#ifndef ROCKSDB_LITE

Status SuggestCompactRange(DB* db, ColumnFamilyHandle* column_family,
                           const Slice* begin, const Slice* end) {
  if (db == nullptr) {
    return Status::InvalidArgument("DB is empty");
  }

  return db->SuggestCompactRange(column_family, begin, end);
}

Status PromoteL0(DB* db, ColumnFamilyHandle* column_family, int target_level) {
  if (db == nullptr) {
    return Status::InvalidArgument("Didn't recognize DB object");
  }
  return db->PromoteL0(column_family, target_level);
}

#else  // ROCKSDB_LITE

Status SuggestCompactRange(DB* /*db*/, ColumnFamilyHandle* /*column_family*/,
                           const Slice* /*begin*/, const Slice* /*end*/) {
  return Status::NotSupported("Not supported in RocksDB LITE");
}

Status PromoteL0(DB* /*db*/, ColumnFamilyHandle* /*column_family*/,
                 int /*target_level*/) {
  return Status::NotSupported("Not supported in RocksDB LITE");
}

#endif  // ROCKSDB_LITE

Status SuggestCompactRange(DB* db, const Slice* begin, const Slice* end) {
  return SuggestCompactRange(db, db->DefaultColumnFamily(), begin, end);
}

}  // namespace experimental
}  // namespace rocksdb
