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
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "common/BitsetView.h"
#include "common/Types.h"

namespace milvus {

// Bidirectional offset mapping for nullable vector storage
// Maps between logical offsets (with nulls) and physical offsets (only valid
// data). This base class is a read-only no-op mapping; real storage is split
// between sealed and growing subclasses.
class OffsetMapping {
 public:
    enum class BitsetTransformStatus {
        NoFilter,
        AllFiltered,
        Transformed,
    };

    OffsetMapping() = default;
    virtual ~OffsetMapping() = default;

    // Get physical offset from logical offset. Returns -1 if null.
    virtual int64_t
    GetPhysicalOffset(int64_t logical_offset) const;

    // Get logical offset from physical offset. Returns -1 if not found.
    virtual int64_t
    GetLogicalOffset(int64_t physical_offset) const;

    // Check if a logical offset is valid (not null)
    virtual bool
    IsValid(int64_t logical_offset) const;

    // Get count of valid (non-null) elements
    virtual int64_t
    GetValidCount() const;

    // Check if mapping is enabled
    virtual bool
    IsEnabled() const;

    // Get total logical count (including nulls)
    virtual int64_t
    GetTotalCount() const;

    virtual BitsetTransformStatus
    TransformBitset(const BitsetView& bitset, TargetBitmap& result) const;

    virtual void
    TransformOffsets(std::vector<int64_t>& offsets) const;

    virtual void
    TransformLogicalOffsets(std::vector<int64_t>& offsets) const;

    virtual void
    FilterValidLogicalOffsets(const int64_t* logical_offsets,
                              int64_t count,
                              bool* valid_data,
                              std::vector<int64_t>& physical_offsets) const;
};

class SealedOffsetMapping final : public OffsetMapping {
 public:
    void
    Build(const bool* valid_data, int64_t total_count);

    int64_t
    GetPhysicalOffset(int64_t logical_offset) const override;

    int64_t
    GetLogicalOffset(int64_t physical_offset) const override;

    int64_t
    GetValidCount() const override;

    bool
    IsEnabled() const override;

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

 private:
    int64_t
    GetPhysicalOffsetInternal(int64_t logical_offset) const;

    int64_t
    GetLogicalOffsetInternal(int64_t physical_offset) const;

    bool enabled_{false};
    bool use_map_{false};
    // Sealed vec mode storage (uses int32_t to save memory)
    std::vector<int32_t> l2p_vec_;  // logical -> physical, -1 means null
    std::vector<int32_t> p2l_vec_;  // physical -> logical

    // Sealed map mode storage (for sparse valid data)
    std::unordered_map<int32_t, int32_t> l2p_map_;  // logical -> physical
    std::unordered_map<int32_t, int32_t> p2l_map_;  // physical -> logical

    int64_t valid_count_{0};
    int64_t total_count_{0};  // total logical count (including nulls)
};

class GrowingOffsetMapping final : public OffsetMapping {
 public:
    void
    Append(const bool* valid_data,
           int64_t count,
           int64_t start_logical = -1,
           int64_t start_physical = -1);

    int64_t
    GetPhysicalOffset(int64_t logical_offset) const override;

    int64_t
    GetLogicalOffset(int64_t physical_offset) const override;

    int64_t
    GetValidCount() const override;

    bool
    IsEnabled() const override;

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

 private:
    int64_t
    GetPhysicalOffsetInternal(int64_t logical_offset,
                              int64_t total_count) const;

    int64_t
    GetLogicalOffsetInternal(int64_t physical_offset,
                             int64_t valid_count) const;

    mutable std::shared_mutex mutex_;
    std::unordered_map<int32_t, int32_t> l2p_map_;  // logical -> physical
    std::unordered_map<int32_t, int32_t> p2l_map_;  // physical -> logical

    bool enabled_{false};
    int64_t valid_count_{0};
    int64_t total_count_{0};  // total logical count incl nulls
};

}  // namespace milvus
