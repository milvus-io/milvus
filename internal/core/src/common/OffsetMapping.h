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

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/BitsetView.h"
#include "common/Types.h"

namespace milvus {

struct OffsetMappingBuildOptions {
    // i2o: index/internal id -> original/logical offset.
    bool enable_mmap_i2o_map{false};
    // o2i: original/logical offset -> index/internal id.
    bool enable_mmap_o2i_map{false};
    std::string mmap_dir_path;
};

// Bidirectional offset mapping for nullable vector storage.
//
// A nullable vector field does not materialize its null rows, so the segment's
// logical offset space (every row, nulls included) and the storage/index
// physical offset space (valid rows only) drift apart. This interface is the
// single place that converts between them.
//
// It is a pure interface: every implementation must state its own behaviour.
// Three implementations exist, one per storage state:
//   - NoOpOffsetMapping     (below)          -- no mapping, identity
//   - SealedOffsetMapping   (SealedOffsetMapping.h)  -- immutable, built once
//   - GrowingOffsetMapping  (GrowingOffsetMapping.h) -- append-only, concurrent
//
// Deliberately NOT a base class with usable defaults: an implementation that
// forgets to override a conversion would silently inherit the identity, which
// on a mapping that has nulls means handing callers offsets past the end of
// physical storage. Making every conversion pure turns that into a compile
// error.
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
    GetPhysicalOffset(int64_t logical_offset) const = 0;

    // Get logical offset from physical offset. Returns -1 if not found.
    virtual int64_t
    GetLogicalOffset(int64_t physical_offset) const = 0;

    // Get count of valid (non-null) elements.
    virtual int64_t
    GetValidCount() const = 0;

    // Number of valid (non-null) rows whose logical offset is < logical_bound:
    // the physical scan bound that corresponds to a logical visibility bound.
    //
    // Callers that need a scan bound MUST use this and NOT GetValidCount().
    // On a growing mapping the two differ under concurrent inserts, and
    // GetValidCount() would admit rows the query must not see.
    //
    // PRECONDITION: physical offsets are claimed in ascending logical order,
    // so physical -> logical is monotonic and the visible rows form a physical
    // prefix. Nothing re-establishes that order after the fact -- the growing
    // implementation only asserts it. It holds because a growing segment
    // receives its inserts one at a time, in reserved logical order; see
    // GrowingOffsetMapping::Append.
    virtual int64_t
    ValidCountBelow(int64_t logical_bound) const = 0;

    // Whether a real mapping exists. When false, logical and physical spaces
    // coincide and the count accessors carry no information.
    virtual bool
    IsEnabled() const = 0;

    // Whether either direction is backed by mmap storage. Non-sealed
    // implementations use the default false capability.
    virtual bool
    IsMmap() const;

    // Get total logical count (including nulls).
    virtual int64_t
    GetTotalCount() const = 0;

    virtual BitsetTransformStatus
    TransformBitset(const BitsetView& bitset, TargetBitmap& result) const = 0;

    virtual void
    TransformOffsets(std::vector<int64_t>& offsets) const = 0;

    virtual void
    TransformLogicalOffsets(std::vector<int64_t>& offsets) const = 0;

    virtual void
    FilterValidLogicalOffsets(const int64_t* logical_offsets,
                              int64_t count,
                              bool* valid_data,
                              std::vector<int64_t>& physical_offsets) const = 0;

    // Check if a logical offset is valid (not null). Defined in terms of
    // GetPhysicalOffset, so implementations get it for free and cannot let the
    // two answers disagree.
    virtual bool
    IsValid(int64_t logical_offset) const;

 protected:
    // Shared TransformBitset fast paths: an empty / all-set / all-clear input
    // bitset needs no per-row conversion. Returns true when `status` has been
    // filled in and the caller should return it as-is.
    static bool
    ShouldSkipBitsetTransform(const BitsetView& bitset,
                              int64_t total_count,
                              TargetBitmap& result,
                              BitsetTransformStatus& status);
};

// Storage used by sealed mappings. Each direction can independently use a
// heap vector or a file-backed mmap region.
class OffsetMappingArray {
 public:
    OffsetMappingArray() = default;
    ~OffsetMappingArray();
    OffsetMappingArray(const OffsetMappingArray&) = delete;
    OffsetMappingArray&
    operator=(const OffsetMappingArray&) = delete;

    void
    Resize(size_t size, int32_t value, bool enable_mmap, std::string filepath);

    void
    Clear();

    size_t
    size() const {
        return size_;
    }

    bool
    IsMmap() const {
        return mmap_data_ != nullptr;
    }

    int32_t&
    operator[](size_t index) {
        return IsMmap() ? mmap_data_[index] : vec_[index];
    }

    const int32_t&
    operator[](size_t index) const {
        return IsMmap() ? mmap_data_[index] : vec_[index];
    }

 private:
    size_t size_{0};
    std::vector<int32_t> vec_;
    int32_t* mmap_data_{nullptr};
    size_t mmap_size_{0};
    std::string mmap_filepath_;
};

// The "no mapping" implementation, for fields that are not nullable.
//
// Exists so callers can hold a `const OffsetMapping&` unconditionally instead
// of branching on a null pointer; every conversion is the identity because
// logical and physical offsets are the same thing here.
//
// NOTE: GetValidCount() / GetTotalCount() return 0, not a row count -- this
// object has no idea how many rows the field holds. Callers that need a count
// must gate on IsEnabled() first and fall back to their own row count.
// ValidCountBelow() is deliberately NOT 0: a scan bound must survive the
// no-mapping case unchanged, and returning 0 there would silently scan nothing.
class NoOpOffsetMapping final : public OffsetMapping {
 public:
    int64_t
    GetPhysicalOffset(int64_t logical_offset) const override;

    int64_t
    GetLogicalOffset(int64_t physical_offset) const override;

    int64_t
    GetValidCount() const override;

    int64_t
    ValidCountBelow(int64_t logical_bound) const override;

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
};

}  // namespace milvus
