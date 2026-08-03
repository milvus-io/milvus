// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>

#include "common/OffsetMapping.h"

namespace milvus {

// Offset mapping for sealed storage: built once from a complete valid_data
// array, then immutable. Because nothing mutates after Build(), every read is
// lock-free.
//
// Each direction independently chooses one representation in Build():
//   - contiguous int32 storage, heap-backed or file-backed mmap
//   - hash map when valid rows are sparse and mmap was not requested
class SealedOffsetMapping final : public OffsetMapping {
 public:
    // Build the mapping from a complete valid_data array. Resets any state
    // from a previous Build(). A null pointer or zero count leaves the mapping
    // disabled.
    void
    Build(const bool* valid_data,
          int64_t total_count,
          const OffsetMappingBuildOptions& options = {});

    int64_t
    GetPhysicalOffset(int64_t logical_offset) const override;

    int64_t
    GetLogicalOffset(int64_t physical_offset) const override;

    int64_t
    GetValidCount() const override;

    // Sealed mappings are immutable once built, so this is a pure coordinate
    // conversion with no race; it exists so every caller can use one API.
    int64_t
    ValidCountBelow(int64_t logical_bound) const override;

    bool
    IsEnabled() const override;

    bool
    IsMmap() const override;

    int64_t
    GetTotalCount() const override;

    BitsetTransformStatus
    TransformBitset(const BitsetView& bitset,
                    TargetBitmap& result) const override;

    void
    TransformOffsets(std::vector<int64_t>& offsets) const override;

    void
    TransformLogicalOffsets(std::vector<int64_t>& offsets) const override;

    void
    FilterValidLogicalOffsets(
        const int64_t* logical_offsets,
        int64_t count,
        bool* valid_data,
        std::vector<int64_t>& physical_offsets) const override;

    bool
    IsUsingMap() const {
        return IsI2OUsingMap() || IsO2IUsingMap();
    }

    bool
    IsI2OUsingMap() const {
        return use_i2o_map_;
    }

    bool
    IsO2IUsingMap() const {
        return use_o2i_map_;
    }

    bool
    IsI2OMmap() const {
        return p2l_vec_.IsMmap();
    }

    bool
    IsO2IMmap() const {
        return l2p_vec_.IsMmap();
    }

 private:
    int64_t
    GetPhysicalOffsetInternal(int64_t logical_offset) const;

    int64_t
    GetLogicalOffsetInternal(int64_t physical_offset) const;

    bool enabled_{false};
    bool use_i2o_map_{false};
    bool use_o2i_map_{false};
    // Sealed vec mode storage (uses int32_t to save memory)
    OffsetMappingArray l2p_vec_;  // o2i: logical/original -> physical/index
    OffsetMappingArray p2l_vec_;  // i2o: physical/index -> logical/original

    // Sealed map mode storage (for sparse valid data)
    std::unordered_map<int32_t, int32_t> l2p_map_;  // logical -> physical
    std::unordered_map<int32_t, int32_t> p2l_map_;  // physical -> logical

    int64_t valid_count_{0};
    int64_t total_count_{0};  // total logical count (including nulls)
};

}  // namespace milvus
