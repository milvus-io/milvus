// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "util/set_comparator.h"

namespace rocksdb {
// During recovery if the memtable is flushed we cannot rely on its help on
// duplicate key detection and as key insert will not be attempted. This class
// will be used as a emulator of memtable to tell if insertion of a key/seq
// would have resulted in duplication.
class DuplicateDetector {
 public:
  explicit DuplicateDetector(DBImpl* db) : db_(db) {}
  bool IsDuplicateKeySeq(uint32_t cf, const Slice& key, SequenceNumber seq) {
    assert(seq >= batch_seq_);
    if (batch_seq_ != seq) {  // it is a new batch
      keys_.clear();
    }
    batch_seq_ = seq;
    CFKeys& cf_keys = keys_[cf];
    if (cf_keys.size() == 0) {  // just inserted
      InitWithComp(cf);
    }
    auto it = cf_keys.insert(key);
    if (it.second == false) {  // second is false if a element already existed.
      keys_.clear();
      InitWithComp(cf);
      keys_[cf].insert(key);
      return true;
    }
    return false;
  }

 private:
  SequenceNumber batch_seq_ = 0;
  DBImpl* db_;
  using CFKeys = std::set<Slice, SetComparator>;
  std::map<uint32_t, CFKeys> keys_;
  void InitWithComp(const uint32_t cf) {
    auto h = db_->GetColumnFamilyHandle(cf);
    if (!h) {
      // TODO(myabandeh): This is not a concern in MyRocks as drop cf is not
      // implemented yet. When it does, we should return proper error instead
      // of throwing exception.
      ROCKS_LOG_FATAL(
          db_->immutable_db_options().info_log,
          "Recovering an entry from the dropped column family %" PRIu32
          ". WAL must must have been emptied before dropping the column "
          "family");
#ifndef ROCKSDB_LITE
      throw std::runtime_error(
          "Recovering an entry from the dropped column family %" PRIu32
          ". WAL must must have been flushed before dropping the column "
          "family");
#endif
      return;
    }
    auto cmp = h->GetComparator();
    keys_[cf] = CFKeys(SetComparator(cmp));
  }
};
}  // namespace rocksdb
