// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef ROCKSDB_LITE
#include "rocksdb/slice_transform.h"
#include "rocksdb/memtablerep.h"

namespace rocksdb {

class HashSkipListRepFactory : public MemTableRepFactory {
 public:
  explicit HashSkipListRepFactory(
    size_t bucket_count,
    int32_t skiplist_height,
    int32_t skiplist_branching_factor)
      : bucket_count_(bucket_count),
        skiplist_height_(skiplist_height),
        skiplist_branching_factor_(skiplist_branching_factor) { }

  virtual ~HashSkipListRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;
  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& compare, Allocator* allocator,
      const SliceTransform* transform, Logger* logger) override;

  virtual const char* Name() const override {
    return "HashSkipListRepFactory";
  }

 private:
  const size_t bucket_count_;
  const int32_t skiplist_height_;
  const int32_t skiplist_branching_factor_;
};

}
#endif  // ROCKSDB_LITE
