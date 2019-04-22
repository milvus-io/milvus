//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <memory>
#include <vector>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/filter_block.h"
#include "util/hash.h"

namespace rocksdb {


// A BlockBasedFilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to BlockBasedFilterBlockBuilder must match the regexp:
//      (StartBlock Add*)* Finish
class BlockBasedFilterBlockBuilder : public FilterBlockBuilder {
 public:
  BlockBasedFilterBlockBuilder(const SliceTransform* prefix_extractor,
      const BlockBasedTableOptions& table_opt);

  virtual bool IsBlockBased() override { return true; }
  virtual void StartBlock(uint64_t block_offset) override;
  virtual void Add(const Slice& key) override;
  virtual size_t NumAdded() const override { return num_added_; }
  virtual Slice Finish(const BlockHandle& tmp, Status* status) override;
  using FilterBlockBuilder::Finish;

 private:
  void AddKey(const Slice& key);
  void AddPrefix(const Slice& key);
  void GenerateFilter();

  // important: all of these might point to invalid addresses
  // at the time of destruction of this filter block. destructor
  // should NOT dereference them.
  const FilterPolicy* policy_;
  const SliceTransform* prefix_extractor_;
  bool whole_key_filtering_;

  size_t prev_prefix_start_;        // the position of the last appended prefix
                                    // to "entries_".
  size_t prev_prefix_size_;         // the length of the last appended prefix to
                                    // "entries_".
  std::string entries_;             // Flattened entry contents
  std::vector<size_t> start_;       // Starting index in entries_ of each entry
  std::string result_;              // Filter data computed so far
  std::vector<Slice> tmp_entries_;  // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;
  size_t num_added_;                // Number of keys added

  // No copying allowed
  BlockBasedFilterBlockBuilder(const BlockBasedFilterBlockBuilder&);
  void operator=(const BlockBasedFilterBlockBuilder&);
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
class BlockBasedFilterBlockReader : public FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  BlockBasedFilterBlockReader(const SliceTransform* prefix_extractor,
                              const BlockBasedTableOptions& table_opt,
                              bool whole_key_filtering,
                              BlockContents&& contents, Statistics* statistics);
  virtual bool IsBlockBased() override { return true; }

  virtual bool KeyMayMatch(
      const Slice& key, const SliceTransform* prefix_extractor,
      uint64_t block_offset = kNotValid, const bool no_io = false,
      const Slice* const const_ikey_ptr = nullptr) override;
  virtual bool PrefixMayMatch(
      const Slice& prefix, const SliceTransform* prefix_extractor,
      uint64_t block_offset = kNotValid, const bool no_io = false,
      const Slice* const const_ikey_ptr = nullptr) override;
  virtual size_t ApproximateMemoryUsage() const override;

  // convert this object to a human readable form
  std::string ToString() const override;

 private:
  const FilterPolicy* policy_;
  const SliceTransform* prefix_extractor_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
  BlockContents contents_;

  bool MayMatch(const Slice& entry, uint64_t block_offset);

  // No copying allowed
  BlockBasedFilterBlockReader(const BlockBasedFilterBlockReader&);
  void operator=(const BlockBasedFilterBlockReader&);
};
}  // namespace rocksdb
