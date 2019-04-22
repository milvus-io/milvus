//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "db/dbformat.h"
#include "util/hash.h"
#include "table/filter_block.h"

namespace rocksdb {

class FilterPolicy;
class FilterBitsBuilder;
class FilterBitsReader;

// A FullFilterBlockBuilder is used to construct a full filter for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
// The format of full filter block is:
// +----------------------------------------------------------------+
// |              full filter for all keys in sst file              |
// +----------------------------------------------------------------+
// The full filter can be very large. At the end of it, we put
// num_probes: how many hash functions are used in bloom filter
//
class FullFilterBlockBuilder : public FilterBlockBuilder {
 public:
  explicit FullFilterBlockBuilder(const SliceTransform* prefix_extractor,
                                  bool whole_key_filtering,
                                  FilterBitsBuilder* filter_bits_builder);
  // bits_builder is created in filter_policy, it should be passed in here
  // directly. and be deleted here
  ~FullFilterBlockBuilder() {}

  virtual bool IsBlockBased() override { return false; }
  virtual void StartBlock(uint64_t /*block_offset*/) override {}
  virtual void Add(const Slice& key) override;
  virtual size_t NumAdded() const override { return num_added_; }
  virtual Slice Finish(const BlockHandle& tmp, Status* status) override;
  using FilterBlockBuilder::Finish;

 protected:
  virtual void AddKey(const Slice& key);
  std::unique_ptr<FilterBitsBuilder> filter_bits_builder_;
  virtual void Reset();

 private:
  // important: all of these might point to invalid addresses
  // at the time of destruction of this filter block. destructor
  // should NOT dereference them.
  const SliceTransform* prefix_extractor_;
  bool whole_key_filtering_;
  bool last_whole_key_recorded_;
  std::string last_whole_key_str_;
  bool last_prefix_recorded_;
  std::string last_prefix_str_;

  uint32_t num_added_;
  std::unique_ptr<const char[]> filter_data_;

  void AddPrefix(const Slice& key);

  // No copying allowed
  FullFilterBlockBuilder(const FullFilterBlockBuilder&);
  void operator=(const FullFilterBlockBuilder&);
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
class FullFilterBlockReader : public FilterBlockReader {
 public:
  // REQUIRES: "contents" and filter_bits_reader must stay live
  // while *this is live.
  explicit FullFilterBlockReader(const SliceTransform* prefix_extractor,
                                 bool whole_key_filtering,
                                 const Slice& contents,
                                 FilterBitsReader* filter_bits_reader,
                                 Statistics* statistics);
  explicit FullFilterBlockReader(const SliceTransform* prefix_extractor,
                                 bool whole_key_filtering,
                                 BlockContents&& contents,
                                 FilterBitsReader* filter_bits_reader,
                                 Statistics* statistics);

  // bits_reader is created in filter_policy, it should be passed in here
  // directly. and be deleted here
  ~FullFilterBlockReader() {}

  virtual bool IsBlockBased() override { return false; }

  virtual bool KeyMayMatch(
      const Slice& key, const SliceTransform* prefix_extractor,
      uint64_t block_offset = kNotValid, const bool no_io = false,
      const Slice* const const_ikey_ptr = nullptr) override;

  virtual bool PrefixMayMatch(
      const Slice& prefix, const SliceTransform* prefix_extractor,
      uint64_t block_offset = kNotValid, const bool no_io = false,
      const Slice* const const_ikey_ptr = nullptr) override;
  virtual size_t ApproximateMemoryUsage() const override;
  virtual bool RangeMayExist(const Slice* iterate_upper_bound, const Slice& user_key,
                             const SliceTransform* prefix_extractor,
                             const Comparator* comparator,
                             const Slice* const const_ikey_ptr, bool* filter_checked,
                             bool need_upper_bound_check) override;
 private:
  const SliceTransform* prefix_extractor_;
  Slice contents_;
  std::unique_ptr<FilterBitsReader> filter_bits_reader_;
  BlockContents block_contents_;
  bool full_length_enabled_;
  size_t prefix_extractor_full_length_;

  // No copying allowed
  FullFilterBlockReader(const FullFilterBlockReader&);
  bool MayMatch(const Slice& entry);
  void operator=(const FullFilterBlockReader&);
  bool IsFilterCompatible(const Slice* iterate_upper_bound,
                          const Slice& prefix, const Comparator* comparator);

};

}  // namespace rocksdb
