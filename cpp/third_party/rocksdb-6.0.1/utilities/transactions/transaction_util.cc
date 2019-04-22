//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/transactions/transaction_util.h"

#include <inttypes.h>
#include <string>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "util/string_util.h"

namespace rocksdb {

Status TransactionUtil::CheckKeyForConflicts(
    DBImpl* db_impl, ColumnFamilyHandle* column_family, const std::string& key,
    SequenceNumber snap_seq, bool cache_only, ReadCallback* snap_checker,
    SequenceNumber min_uncommitted) {
  Status result;

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* sv = db_impl->GetAndRefSuperVersion(cfd);

  if (sv == nullptr) {
    result = Status::InvalidArgument("Could not access column family " +
                                     cfh->GetName());
  }

  if (result.ok()) {
    SequenceNumber earliest_seq =
        db_impl->GetEarliestMemTableSequenceNumber(sv, true);

    result = CheckKey(db_impl, sv, earliest_seq, snap_seq, key, cache_only,
                      snap_checker, min_uncommitted);

    db_impl->ReturnAndCleanupSuperVersion(cfd, sv);
  }

  return result;
}

Status TransactionUtil::CheckKey(DBImpl* db_impl, SuperVersion* sv,
                                 SequenceNumber earliest_seq,
                                 SequenceNumber snap_seq,
                                 const std::string& key, bool cache_only,
                                 ReadCallback* snap_checker,
                                 SequenceNumber min_uncommitted) {
  Status result;
  bool need_to_read_sst = false;

  // Since it would be too slow to check the SST files, we will only use
  // the memtables to check whether there have been any recent writes
  // to this key after it was accessed in this transaction.  But if the
  // Memtables do not contain a long enough history, we must fail the
  // transaction.
  if (earliest_seq == kMaxSequenceNumber) {
    // The age of this memtable is unknown.  Cannot rely on it to check
    // for recent writes.  This error shouldn't happen often in practice as
    // the Memtable should have a valid earliest sequence number except in some
    // corner cases (such as error cases during recovery).
    need_to_read_sst = true;

    if (cache_only) {
      result = Status::TryAgain(
          "Transaction ould not check for conflicts as the MemTable does not "
          "countain a long enough history to check write at SequenceNumber: ",
          ToString(snap_seq));
    }
  } else if (snap_seq < earliest_seq || min_uncommitted <= earliest_seq) {
    // Use <= for min_uncommitted since earliest_seq is actually the largest sec
    // before this memtable was created
    need_to_read_sst = true;

    if (cache_only) {
      // The age of this memtable is too new to use to check for recent
      // writes.
      char msg[300];
      snprintf(msg, sizeof(msg),
               "Transaction could not check for conflicts for operation at "
               "SequenceNumber %" PRIu64
               " as the MemTable only contains changes newer than "
               "SequenceNumber %" PRIu64
               ".  Increasing the value of the "
               "max_write_buffer_number_to_maintain option could reduce the "
               "frequency "
               "of this error.",
               snap_seq, earliest_seq);
      result = Status::TryAgain(msg);
    }
  }

  if (result.ok()) {
    SequenceNumber seq = kMaxSequenceNumber;
    bool found_record_for_key = false;

    Status s = db_impl->GetLatestSequenceForKey(sv, key, !need_to_read_sst,
                                                &seq, &found_record_for_key);

    if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
      result = s;
    } else if (found_record_for_key) {
      bool write_conflict = snap_checker == nullptr
                                ? snap_seq < seq
                                : !snap_checker->IsVisible(seq);
      if (write_conflict) {
        result = Status::Busy();
      }
    }
  }

  return result;
}

Status TransactionUtil::CheckKeysForConflicts(DBImpl* db_impl,
                                              const TransactionKeyMap& key_map,
                                              bool cache_only) {
  Status result;

  for (auto& key_map_iter : key_map) {
    uint32_t cf_id = key_map_iter.first;
    const auto& keys = key_map_iter.second;

    SuperVersion* sv = db_impl->GetAndRefSuperVersion(cf_id);
    if (sv == nullptr) {
      result = Status::InvalidArgument("Could not access column family " +
                                       ToString(cf_id));
      break;
    }

    SequenceNumber earliest_seq =
        db_impl->GetEarliestMemTableSequenceNumber(sv, true);

    // For each of the keys in this transaction, check to see if someone has
    // written to this key since the start of the transaction.
    for (const auto& key_iter : keys) {
      const auto& key = key_iter.first;
      const SequenceNumber key_seq = key_iter.second.seq;

      result = CheckKey(db_impl, sv, earliest_seq, key_seq, key, cache_only);

      if (!result.ok()) {
        break;
      }
    }

    db_impl->ReturnAndCleanupSuperVersion(cf_id, sv);

    if (!result.ok()) {
      break;
    }
  }

  return result;
}


}  // namespace rocksdb

#endif  // ROCKSDB_LITE
