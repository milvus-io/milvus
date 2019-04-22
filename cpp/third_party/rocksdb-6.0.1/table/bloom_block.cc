//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/bloom_block.h"

#include <string>
#include "rocksdb/slice.h"
#include "util/dynamic_bloom.h"

namespace rocksdb {

void BloomBlockBuilder::AddKeysHashes(const std::vector<uint32_t>& keys_hashes) {
  for (auto hash : keys_hashes) {
    bloom_.AddHash(hash);
  }
}

Slice BloomBlockBuilder::Finish() { return bloom_.GetRawData(); }

const std::string BloomBlockBuilder::kBloomBlock = "kBloomBlock";
}  // namespace rocksdb
