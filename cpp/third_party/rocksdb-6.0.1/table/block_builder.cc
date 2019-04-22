//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <assert.h>
#include <algorithm>
#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "table/data_block_footer.h"
#include "util/coding.h"

namespace rocksdb {

BlockBuilder::BlockBuilder(
    int block_restart_interval, bool use_delta_encoding,
    bool use_value_delta_encoding,
    BlockBasedTableOptions::DataBlockIndexType index_type,
    double data_block_hash_table_util_ratio)
    : block_restart_interval_(block_restart_interval),
      use_delta_encoding_(use_delta_encoding),
      use_value_delta_encoding_(use_value_delta_encoding),
      restarts_(),
      counter_(0),
      finished_(false) {
  switch (index_type) {
    case BlockBasedTableOptions::kDataBlockBinarySearch:
      break;
    case BlockBasedTableOptions::kDataBlockBinaryAndHash:
      data_block_hash_index_builder_.Initialize(
          data_block_hash_table_util_ratio);
      break;
    default:
      assert(0);
  }
  assert(block_restart_interval_ >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
  if (data_block_hash_index_builder_.Valid()) {
    data_block_hash_index_builder_.Reset();
  }
}

size_t BlockBuilder::EstimateSizeAfterKV(const Slice& key, const Slice& value)
  const {
  size_t estimate = CurrentSizeEstimate();
  // Note: this is an imprecise estimate as it accounts for the whole key size
  // instead of non-shared key size.
  estimate += key.size();
  // In value delta encoding we estimate the value delta size as half the full
  // value size since only the size field of block handle is encoded.
  estimate +=
      !use_value_delta_encoding_ || (counter_ >= block_restart_interval_)
          ? value.size()
          : value.size() / 2;

  if (counter_ >= block_restart_interval_) {
    estimate += sizeof(uint32_t); // a new restart entry.
  }

  estimate += sizeof(int32_t); // varint for shared prefix length.
  // Note: this is an imprecise estimate as we will have to encoded size, one
  // for shared key and one for non-shared key.
  estimate += VarintLength(key.size()); // varint for key length.
  if (!use_value_delta_encoding_ || (counter_ >= block_restart_interval_)) {
    estimate += VarintLength(value.size());  // varint for value length.
  }

  return estimate;
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }

  uint32_t num_restarts = static_cast<uint32_t>(restarts_.size());
  BlockBasedTableOptions::DataBlockIndexType index_type =
      BlockBasedTableOptions::kDataBlockBinarySearch;
  if (data_block_hash_index_builder_.Valid() &&
      CurrentSizeEstimate() <= kMaxBlockSizeSupportedByHashIndex) {
    data_block_hash_index_builder_.Finish(buffer_);
    index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash;
  }

  // footer is a packed format of data_block_index_type and num_restarts
  uint32_t block_footer = PackIndexTypeAndNumRestarts(index_type, num_restarts);

  PutFixed32(&buffer_, block_footer);
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value,
                       const Slice* const delta_value) {
  assert(!finished_);
  assert(counter_ <= block_restart_interval_);
  assert(!use_value_delta_encoding_ || delta_value);
  size_t shared = 0;  // number of bytes shared with prev key
  if (counter_ >= block_restart_interval_) {
    // Restart compression
    restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
    estimate_ += sizeof(uint32_t);
    counter_ = 0;

    if (use_delta_encoding_) {
      // Update state
      last_key_.assign(key.data(), key.size());
    }
  } else if (use_delta_encoding_) {
    Slice last_key_piece(last_key_);
    // See how much sharing to do with previous string
    shared = key.difference_offset(last_key_piece);

    // Update state
    // We used to just copy the changed data here, but it appears to be
    // faster to just copy the whole thing.
    last_key_.assign(key.data(), key.size());
  }

  const size_t non_shared = key.size() - shared;
  const size_t curr_size = buffer_.size();

  if (use_value_delta_encoding_) {
    // Add "<shared><non_shared>" to buffer_
    PutVarint32Varint32(&buffer_, static_cast<uint32_t>(shared),
                        static_cast<uint32_t>(non_shared));
  } else {
    // Add "<shared><non_shared><value_size>" to buffer_
    PutVarint32Varint32Varint32(&buffer_, static_cast<uint32_t>(shared),
                                static_cast<uint32_t>(non_shared),
                                static_cast<uint32_t>(value.size()));
  }

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  // Use value delta encoding only when the key has shared bytes. This would
  // simplify the decoding, where it can figure which decoding to use simply by
  // looking at the shared bytes size.
  if (shared != 0 && use_value_delta_encoding_) {
    buffer_.append(delta_value->data(), delta_value->size());
  } else {
    buffer_.append(value.data(), value.size());
  }

  if (data_block_hash_index_builder_.Valid()) {
    data_block_hash_index_builder_.Add(ExtractUserKey(key),
                                       restarts_.size() - 1);
  }

  counter_++;
  estimate_ += buffer_.size() - curr_size;
}

}  // namespace rocksdb
