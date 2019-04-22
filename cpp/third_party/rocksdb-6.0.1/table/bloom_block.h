//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <vector>
#include <string>
#include "util/dynamic_bloom.h"

namespace rocksdb {
class Logger;

class BloomBlockBuilder {
 public:
  static const std::string kBloomBlock;

  explicit BloomBlockBuilder(uint32_t num_probes = 6) : bloom_(num_probes) {}

  void SetTotalBits(Allocator* allocator, uint32_t total_bits,
                    uint32_t locality, size_t huge_page_tlb_size,
                    Logger* logger) {
    bloom_.SetTotalBits(allocator, total_bits, locality, huge_page_tlb_size,
                        logger);
  }

  uint32_t GetNumBlocks() const { return bloom_.GetNumBlocks(); }

  void AddKeysHashes(const std::vector<uint32_t>& keys_hashes);

  Slice Finish();

 private:
  DynamicBloom bloom_;
};

};  // namespace rocksdb
