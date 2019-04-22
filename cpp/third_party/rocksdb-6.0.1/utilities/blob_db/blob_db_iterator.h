//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "monitoring/statistics.h"
#include "rocksdb/iterator.h"
#include "util/stop_watch.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {
namespace blob_db {

using rocksdb::ManagedSnapshot;

class BlobDBIterator : public Iterator {
 public:
  BlobDBIterator(ManagedSnapshot* snapshot, ArenaWrappedDBIter* iter,
                 BlobDBImpl* blob_db, Env* env, Statistics* statistics)
      : snapshot_(snapshot),
        iter_(iter),
        blob_db_(blob_db),
        env_(env),
        statistics_(statistics) {}

  virtual ~BlobDBIterator() = default;

  bool Valid() const override {
    if (!iter_->Valid()) {
      return false;
    }
    return status_.ok();
  }

  Status status() const override {
    if (!iter_->status().ok()) {
      return iter_->status();
    }
    return status_;
  }

  void SeekToFirst() override {
    StopWatch seek_sw(env_, statistics_, BLOB_DB_SEEK_MICROS);
    RecordTick(statistics_, BLOB_DB_NUM_SEEK);
    iter_->SeekToFirst();
    while (UpdateBlobValue()) {
      iter_->Next();
    }
  }

  void SeekToLast() override {
    StopWatch seek_sw(env_, statistics_, BLOB_DB_SEEK_MICROS);
    RecordTick(statistics_, BLOB_DB_NUM_SEEK);
    iter_->SeekToLast();
    while (UpdateBlobValue()) {
      iter_->Prev();
    }
  }

  void Seek(const Slice& target) override {
    StopWatch seek_sw(env_, statistics_, BLOB_DB_SEEK_MICROS);
    RecordTick(statistics_, BLOB_DB_NUM_SEEK);
    iter_->Seek(target);
    while (UpdateBlobValue()) {
      iter_->Next();
    }
  }

  void SeekForPrev(const Slice& target) override {
    StopWatch seek_sw(env_, statistics_, BLOB_DB_SEEK_MICROS);
    RecordTick(statistics_, BLOB_DB_NUM_SEEK);
    iter_->SeekForPrev(target);
    while (UpdateBlobValue()) {
      iter_->Prev();
    }
  }

  void Next() override {
    assert(Valid());
    StopWatch next_sw(env_, statistics_, BLOB_DB_NEXT_MICROS);
    RecordTick(statistics_, BLOB_DB_NUM_NEXT);
    iter_->Next();
    while (UpdateBlobValue()) {
      iter_->Next();
    }
  }

  void Prev() override {
    assert(Valid());
    StopWatch prev_sw(env_, statistics_, BLOB_DB_PREV_MICROS);
    RecordTick(statistics_, BLOB_DB_NUM_PREV);
    iter_->Prev();
    while (UpdateBlobValue()) {
      iter_->Prev();
    }
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice value() const override {
    assert(Valid());
    if (!iter_->IsBlob()) {
      return iter_->value();
    }
    return value_;
  }

  // Iterator::Refresh() not supported.

 private:
  // Return true if caller should continue to next value.
  bool UpdateBlobValue() {
    TEST_SYNC_POINT("BlobDBIterator::UpdateBlobValue:Start:1");
    TEST_SYNC_POINT("BlobDBIterator::UpdateBlobValue:Start:2");
    value_.Reset();
    status_ = Status::OK();
    if (iter_->Valid() && iter_->status().ok() && iter_->IsBlob()) {
      Status s = blob_db_->GetBlobValue(iter_->key(), iter_->value(), &value_);
      if (s.IsNotFound()) {
        return true;
      } else {
        if (!s.ok()) {
          status_ = s;
        }
        return false;
      }
    } else {
      return false;
    }
  }

  std::unique_ptr<ManagedSnapshot> snapshot_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;
  BlobDBImpl* blob_db_;
  Env* env_;
  Statistics* statistics_;
  Status status_;
  PinnableSlice value_;
};
}  // namespace blob_db
}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
