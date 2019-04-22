//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "data_block_footer.h"

#include "rocksdb/table.h"

namespace rocksdb {

const int kDataBlockIndexTypeBitShift = 31;

// 0x7FFFFFFF
const uint32_t kMaxNumRestarts = (1u << kDataBlockIndexTypeBitShift) - 1u;

// 0x7FFFFFFF
const uint32_t kNumRestartsMask = (1u << kDataBlockIndexTypeBitShift) - 1u;

uint32_t PackIndexTypeAndNumRestarts(
    BlockBasedTableOptions::DataBlockIndexType index_type,
    uint32_t num_restarts) {
  if (num_restarts > kMaxNumRestarts) {
    assert(0);  // mute travis "unused" warning
  }

  uint32_t block_footer = num_restarts;
  if (index_type == BlockBasedTableOptions::kDataBlockBinaryAndHash) {
    block_footer |= 1u << kDataBlockIndexTypeBitShift;
  } else if (index_type != BlockBasedTableOptions::kDataBlockBinarySearch) {
    assert(0);
  }

  return block_footer;
}

void UnPackIndexTypeAndNumRestarts(
    uint32_t block_footer,
    BlockBasedTableOptions::DataBlockIndexType* index_type,
    uint32_t* num_restarts) {
  if (index_type) {
    if (block_footer & 1u << kDataBlockIndexTypeBitShift) {
      *index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash;
    } else {
      *index_type = BlockBasedTableOptions::kDataBlockBinarySearch;
    }
  }

  if (num_restarts) {
    *num_restarts = block_footer & kNumRestartsMask;
    assert(*num_restarts <= kMaxNumRestarts);
  }
}

}  // namespace rocksdb
