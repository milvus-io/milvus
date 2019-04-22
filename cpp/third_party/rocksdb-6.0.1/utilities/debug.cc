//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/debug.h"

#include "db/db_impl.h"

namespace rocksdb {

Status GetAllKeyVersions(DB* db, Slice begin_key, Slice end_key,
                         size_t max_num_ikeys,
                         std::vector<KeyVersion>* key_versions) {
  assert(key_versions != nullptr);
  key_versions->clear();

  DBImpl* idb = static_cast<DBImpl*>(db->GetRootDB());
  auto icmp = InternalKeyComparator(idb->GetOptions().comparator);
  ReadRangeDelAggregator range_del_agg(&icmp,
                                       kMaxSequenceNumber /* upper_bound */);
  Arena arena;
  ScopedArenaIterator iter(
      idb->NewInternalIterator(&arena, &range_del_agg, kMaxSequenceNumber));

  if (!begin_key.empty()) {
    InternalKey ikey;
    ikey.SetMinPossibleForUserKey(begin_key);
    iter->Seek(ikey.Encode());
  } else {
    iter->SeekToFirst();
  }

  size_t num_keys = 0;
  for (; iter->Valid(); iter->Next()) {
    ParsedInternalKey ikey;
    if (!ParseInternalKey(iter->key(), &ikey)) {
      return Status::Corruption("Internal Key [" + iter->key().ToString() +
                                "] parse error!");
    }

    if (!end_key.empty() &&
        icmp.user_comparator()->Compare(ikey.user_key, end_key) > 0) {
      break;
    }

    key_versions->emplace_back(ikey.user_key.ToString() /* _user_key */,
                               iter->value().ToString() /* _value */,
                               ikey.sequence /* _sequence */,
                               static_cast<int>(ikey.type) /* _type */);
    if (++num_keys >= max_num_ikeys) {
      break;
    }
  }
  return Status::OK();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
