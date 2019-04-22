//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>
#include <string>
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "util/arena.h"
#include "util/autovector.h"

namespace rocksdb {

class Arena;
class DBIter;

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
extern Iterator* NewDBIterator(
    Env* env, const ReadOptions& read_options,
    const ImmutableCFOptions& cf_options,
    const MutableCFOptions& mutable_cf_options,
    const Comparator* user_key_comparator, InternalIterator* internal_iter,
    const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iterations,
    ReadCallback* read_callback, DBImpl* db_impl = nullptr,
    ColumnFamilyData* cfd = nullptr, bool allow_blob = false);

// A wrapper iterator which wraps DB Iterator and the arena, with which the DB
// iterator is supposed be allocated. This class is used as an entry point of
// a iterator hierarchy whose memory can be allocated inline. In that way,
// accessing the iterator tree can be more cache friendly. It is also faster
// to allocate.
class ArenaWrappedDBIter : public Iterator {
 public:
  virtual ~ArenaWrappedDBIter();

  // Get the arena to be used to allocate memory for DBIter to be wrapped,
  // as well as child iterators in it.
  virtual Arena* GetArena() { return &arena_; }
  virtual ReadRangeDelAggregator* GetRangeDelAggregator();

  // Set the internal iterator wrapped inside the DB Iterator. Usually it is
  // a merging iterator.
  virtual void SetIterUnderDBIter(InternalIterator* iter);
  virtual bool Valid() const override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Seek(const Slice& target) override;
  virtual void SeekForPrev(const Slice& target) override;
  virtual void Next() override;
  virtual void Prev() override;
  virtual Slice key() const override;
  virtual Slice value() const override;
  virtual Status status() const override;
  virtual Status Refresh() override;
  bool IsBlob() const;

  virtual Status GetProperty(std::string prop_name, std::string* prop) override;

  void Init(Env* env, const ReadOptions& read_options,
            const ImmutableCFOptions& cf_options,
            const MutableCFOptions& mutable_cf_options,
            const SequenceNumber& sequence,
            uint64_t max_sequential_skip_in_iterations, uint64_t version_number,
            ReadCallback* read_callback, DBImpl* db_impl, ColumnFamilyData* cfd,
            bool allow_blob, bool allow_refresh);

  void StoreRefreshInfo(const ReadOptions& read_options, DBImpl* db_impl,
                        ColumnFamilyData* cfd, ReadCallback* read_callback,
                        bool allow_blob) {
    read_options_ = read_options;
    db_impl_ = db_impl;
    cfd_ = cfd;
    read_callback_ = read_callback;
    allow_blob_ = allow_blob;
  }

 private:
  DBIter* db_iter_;
  Arena arena_;
  uint64_t sv_number_;
  ColumnFamilyData* cfd_ = nullptr;
  DBImpl* db_impl_ = nullptr;
  ReadOptions read_options_;
  ReadCallback* read_callback_;
  bool allow_blob_ = false;
  bool allow_refresh_ = true;
};

// Generate the arena wrapped iterator class.
// `db_impl` and `cfd` are used for reneweal. If left null, renewal will not
// be supported.
extern ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options,
    const ImmutableCFOptions& cf_options,
    const MutableCFOptions& mutable_cf_options, const SequenceNumber& sequence,
    uint64_t max_sequential_skip_in_iterations, uint64_t version_number,
    ReadCallback* read_callback, DBImpl* db_impl = nullptr,
    ColumnFamilyData* cfd = nullptr, bool allow_blob = false,
    bool allow_refresh = true);
}  // namespace rocksdb
