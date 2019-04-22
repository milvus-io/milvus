//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <vector>

#include "rocksdb/db.h"

namespace rocksdb {

class SnapshotList;

// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
class SnapshotImpl : public Snapshot {
 public:
  SequenceNumber number_;  // const after creation
  // It indicates the smallest uncommitted data at the time the snapshot was
  // taken. This is currently used by WritePrepared transactions to limit the
  // scope of queries to IsInSnpashot.
  SequenceNumber min_uncommitted_ = 0;

  virtual SequenceNumber GetSequenceNumber() const override { return number_; }

 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list
  SnapshotImpl* prev_;
  SnapshotImpl* next_;

  SnapshotList* list_;                 // just for sanity checks

  int64_t unix_time_;

  // Will this snapshot be used by a Transaction to do write-conflict checking?
  bool is_write_conflict_boundary_;
};

class SnapshotList {
 public:
  SnapshotList() {
    list_.prev_ = &list_;
    list_.next_ = &list_;
    list_.number_ = 0xFFFFFFFFL;      // placeholder marker, for debugging
    // Set all the variables to make UBSAN happy.
    list_.list_ = nullptr;
    list_.unix_time_ = 0;
    list_.is_write_conflict_boundary_ = false;
    count_ = 0;
  }

  // No copy-construct.
  SnapshotList(const SnapshotList&) = delete;

  bool empty() const { return list_.next_ == &list_; }
  SnapshotImpl* oldest() const { assert(!empty()); return list_.next_; }
  SnapshotImpl* newest() const { assert(!empty()); return list_.prev_; }

  SnapshotImpl* New(SnapshotImpl* s, SequenceNumber seq, uint64_t unix_time,
                    bool is_write_conflict_boundary) {
    s->number_ = seq;
    s->unix_time_ = unix_time;
    s->is_write_conflict_boundary_ = is_write_conflict_boundary;
    s->list_ = this;
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    count_++;
    return s;
  }

  // Do not responsible to free the object.
  void Delete(const SnapshotImpl* s) {
    assert(s->list_ == this);
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    count_--;
  }

  // retrieve all snapshot numbers up until max_seq. They are sorted in
  // ascending order (with no duplicates).
  std::vector<SequenceNumber> GetAll(
      SequenceNumber* oldest_write_conflict_snapshot = nullptr,
      const SequenceNumber& max_seq = kMaxSequenceNumber) const {
    std::vector<SequenceNumber> ret;

    if (oldest_write_conflict_snapshot != nullptr) {
      *oldest_write_conflict_snapshot = kMaxSequenceNumber;
    }

    if (empty()) {
      return ret;
    }
    const SnapshotImpl* s = &list_;
    while (s->next_ != &list_) {
      if (s->next_->number_ > max_seq) {
        break;
      }
      // Avoid duplicates
      if (ret.empty() || ret.back() != s->next_->number_) {
        ret.push_back(s->next_->number_);
      }

      if (oldest_write_conflict_snapshot != nullptr &&
          *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
          s->next_->is_write_conflict_boundary_) {
        // If this is the first write-conflict boundary snapshot in the list,
        // it is the oldest
        *oldest_write_conflict_snapshot = s->next_->number_;
      }

      s = s->next_;
    }
    return ret;
  }

  // get the sequence number of the most recent snapshot
  SequenceNumber GetNewest() {
    if (empty()) {
      return 0;
    }
    return newest()->number_;
  }

  int64_t GetOldestSnapshotTime() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->unix_time_;
    }
  }

  uint64_t count() const { return count_; }

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl list_;
  uint64_t count_;
};

}  // namespace rocksdb
