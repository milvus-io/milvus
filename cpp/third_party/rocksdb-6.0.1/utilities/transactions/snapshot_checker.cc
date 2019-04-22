// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/snapshot_checker.h"

#ifdef ROCKSDB_LITE
#include <assert.h>
#endif  // ROCKSDB_LITE

#include "utilities/transactions/write_prepared_txn_db.h"

namespace rocksdb {

#ifdef ROCKSDB_LITE
WritePreparedSnapshotChecker::WritePreparedSnapshotChecker(
    WritePreparedTxnDB* /*txn_db*/) {}

SnapshotCheckerResult WritePreparedSnapshotChecker::CheckInSnapshot(
    SequenceNumber /*sequence*/, SequenceNumber /*snapshot_sequence*/) const {
  // Should never be called in LITE mode.
  assert(false);
  return SnapshotCheckerResult::kInSnapshot;
}

#else

WritePreparedSnapshotChecker::WritePreparedSnapshotChecker(
    WritePreparedTxnDB* txn_db)
    : txn_db_(txn_db){};

SnapshotCheckerResult WritePreparedSnapshotChecker::CheckInSnapshot(
    SequenceNumber sequence, SequenceNumber snapshot_sequence) const {
  bool snapshot_released = false;
  // TODO(myabandeh): set min_uncommitted
  bool in_snapshot = txn_db_->IsInSnapshot(
      sequence, snapshot_sequence, 0 /*min_uncommitted*/, &snapshot_released);
  if (snapshot_released) {
    return SnapshotCheckerResult::kSnapshotReleased;
  }
  return in_snapshot ? SnapshotCheckerResult::kInSnapshot
                     : SnapshotCheckerResult::kNotInSnapshot;
}

#endif  // ROCKSDB_LITE
DisableGCSnapshotChecker DisableGCSnapshotChecker::instance_;

}  // namespace rocksdb
