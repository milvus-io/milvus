//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <list>
#include <string>
#include <unordered_map>
#include "db/dbformat.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "table/full_filter_block.h"
#include "table/index_builder.h"
#include "util/autovector.h"

namespace rocksdb {

class PartitionedFilterBlockBuilder : public FullFilterBlockBuilder {
 public:
  explicit PartitionedFilterBlockBuilder(
      const SliceTransform* prefix_extractor, bool whole_key_filtering,
      FilterBitsBuilder* filter_bits_builder, int index_block_restart_interval,
      const bool use_value_delta_encoding,
      PartitionedIndexBuilder* const p_index_builder,
      const uint32_t partition_size);

  virtual ~PartitionedFilterBlockBuilder();

  void AddKey(const Slice& key) override;

  size_t NumAdded() const override { return num_added_; }

  virtual Slice Finish(const BlockHandle& last_partition_block_handle,
                       Status* status) override;

 private:
  // Filter data
  BlockBuilder index_on_filter_block_builder_;  // top-level index builder
  BlockBuilder
      index_on_filter_block_builder_without_seq_;  // same for user keys
  struct FilterEntry {
    std::string key;
    Slice filter;
  };
  std::list<FilterEntry> filters;  // list of partitioned indexes and their keys
  std::unique_ptr<IndexBuilder> value;
  std::vector<std::unique_ptr<const char[]>> filter_gc;
  bool finishing_filters =
      false;  // true if Finish is called once but not complete yet.
  // The policy of when cut a filter block and Finish it
  void MaybeCutAFilterBlock();
  // Currently we keep the same number of partitions for filters and indexes.
  // This would allow for some potentioal optimizations in future. If such
  // optimizations did not realize we can use different number of partitions and
  // eliminate p_index_builder_
  PartitionedIndexBuilder* const p_index_builder_;
  // The desired number of filters per partition
  uint32_t filters_per_partition_;
  // The current number of filters in the last partition
  uint32_t filters_in_partition_;
  // Number of keys added
  size_t num_added_;
  BlockHandle last_encoded_handle_;
};

class PartitionedFilterBlockReader : public FilterBlockReader,
                                     public Cleanable {
 public:
  explicit PartitionedFilterBlockReader(
      const SliceTransform* prefix_extractor, bool whole_key_filtering,
      BlockContents&& contents, FilterBitsReader* filter_bits_reader,
      Statistics* stats, const InternalKeyComparator comparator,
      const BlockBasedTable* table, const bool index_key_includes_seq,
      const bool index_value_is_full);
  virtual ~PartitionedFilterBlockReader();

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

 private:
  BlockHandle GetFilterPartitionHandle(const Slice& entry);
  BlockBasedTable::CachableEntry<FilterBlockReader> GetFilterPartition(
      FilePrefetchBuffer* prefetch_buffer, BlockHandle& handle,
      const bool no_io, bool* cached,
      const SliceTransform* prefix_extractor = nullptr);
  virtual void CacheDependencies(
      bool bin, const SliceTransform* prefix_extractor) override;

  const SliceTransform* prefix_extractor_;
  std::unique_ptr<Block> idx_on_fltr_blk_;
  const InternalKeyComparator comparator_;
  const BlockBasedTable* table_;
  const bool index_key_includes_seq_;
  const bool index_value_is_full_;
  std::unordered_map<uint64_t,
                     BlockBasedTable::CachableEntry<FilterBlockReader>>
      filter_map_;
};

}  // namespace rocksdb
