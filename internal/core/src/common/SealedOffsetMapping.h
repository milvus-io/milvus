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
#include <vector>

#include "common/OffsetMapping.h"

namespace milvus {

// Offset mapping for sealed storage: built once from a complete valid_data
// array, then immutable. Because nothing mutates after Build(), every read is
// lock-free.
class SealedOffsetMapping final : public OffsetMapping {
 public:
    // Build the mapping from a complete valid_data array. Resets any state
    // from a previous Build(). A null pointer or zero count leaves the mapping
    // disabled.
    void
    Build(const bool* valid_data, int64_t total_count);

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

    int64_t
    GetTotalCount() const override;

    OffsetMappingIdView
    GetPhysicalToLogicalIds(int64_t physical_offset,
                            int64_t count) const override;

    void
    FilterValidLogicalOffsets(
        const int64_t* logical_offsets,
        int64_t count,
        bool* valid_data,
        std::vector<int64_t>& physical_offsets) const override;

 private:
    int64_t
    GetPhysicalOffsetInternal(int64_t logical_offset) const;

    int64_t
    GetLogicalOffsetInternal(int64_t physical_offset) const;

    bool enabled_{false};
    // Sealed mapping uses int32_t to save memory.
    std::vector<int32_t> l2p_vec_;  // logical/original -> physical/index
    std::vector<int32_t> p2l_vec_;  // physical/index -> logical/original

    int64_t valid_count_{0};
    int64_t total_count_{0};  // total logical count (including nulls)
};

}  // namespace milvus
