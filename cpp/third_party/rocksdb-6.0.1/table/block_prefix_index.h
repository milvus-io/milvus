// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <stdint.h>
#include "rocksdb/status.h"

namespace rocksdb {

class Comparator;
class Iterator;
class Slice;
class SliceTransform;

// Build a hash-based index to speed up the lookup for "index block".
// BlockHashIndex accepts a key and, if found, returns its restart index within
// that index block.
class BlockPrefixIndex {
 public:

  // Maps a key to a list of data blocks that could potentially contain
  // the key, based on the prefix.
  // Returns the total number of relevant blocks, 0 means the key does
  // not exist.
  uint32_t GetBlocks(const Slice& key, uint32_t** blocks);

  size_t ApproximateMemoryUsage() const {
    return sizeof(BlockPrefixIndex) +
      (num_block_array_buffer_entries_ + num_buckets_) * sizeof(uint32_t);
  }

  // Create hash index by reading from the metadata blocks.
  // @params prefixes: a sequence of prefixes.
  // @params prefix_meta: contains the "metadata" to of the prefixes.
  static Status Create(const SliceTransform* hash_key_extractor,
                       const Slice& prefixes, const Slice& prefix_meta,
                       BlockPrefixIndex** prefix_index);

  ~BlockPrefixIndex() {
    delete[] buckets_;
    delete[] block_array_buffer_;
  }

 private:
  class Builder;
  friend Builder;

  BlockPrefixIndex(const SliceTransform* internal_prefix_extractor,
                   uint32_t num_buckets,
                   uint32_t* buckets,
                   uint32_t num_block_array_buffer_entries,
                   uint32_t* block_array_buffer)
      : internal_prefix_extractor_(internal_prefix_extractor),
        num_buckets_(num_buckets),
        num_block_array_buffer_entries_(num_block_array_buffer_entries),
        buckets_(buckets),
        block_array_buffer_(block_array_buffer) {}

  const SliceTransform* internal_prefix_extractor_;
  uint32_t num_buckets_;
  uint32_t num_block_array_buffer_entries_;
  uint32_t* buckets_;
  uint32_t* block_array_buffer_;
};

}  // namespace rocksdb
