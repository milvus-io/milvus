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

class HashLinkListRepFactory : public MemTableRepFactory {
 public:
  explicit HashLinkListRepFactory(size_t bucket_count,
                                  uint32_t threshold_use_skiplist,
                                  size_t huge_page_tlb_size,
                                  int bucket_entries_logging_threshold,
                                  bool if_log_bucket_dist_when_flash)
      : bucket_count_(bucket_count),
        threshold_use_skiplist_(threshold_use_skiplist),
        huge_page_tlb_size_(huge_page_tlb_size),
        bucket_entries_logging_threshold_(bucket_entries_logging_threshold),
        if_log_bucket_dist_when_flash_(if_log_bucket_dist_when_flash) {}

  virtual ~HashLinkListRepFactory() {}

  using MemTableRepFactory::CreateMemTableRep;
  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& compare, Allocator* allocator,
      const SliceTransform* transform, Logger* logger) override;

  virtual const char* Name() const override {
    return "HashLinkListRepFactory";
  }

 private:
  const size_t bucket_count_;
  const uint32_t threshold_use_skiplist_;
  const size_t huge_page_tlb_size_;
  int bucket_entries_logging_threshold_;
  bool if_log_bucket_dist_when_flash_;
};

}
#endif  // ROCKSDB_LITE
