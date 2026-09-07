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
#include <vector>

namespace milvus {

struct OffsetMappingIdView {
    const int32_t* data = nullptr;
    int64_t count = 0;

    bool
    empty() const {
        return data == nullptr || count == 0;
    }
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

    // Get total logical count (including nulls).
    virtual int64_t
    GetTotalCount() const = 0;

    virtual OffsetMappingIdView
    GetPhysicalToLogicalIds(int64_t physical_offset, int64_t count) const = 0;

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

    OffsetMappingIdView
    GetPhysicalToLogicalIds(int64_t physical_offset,
                            int64_t count) const override;

    void
    FilterValidLogicalOffsets(
        const int64_t* logical_offsets,
        int64_t count,
        bool* valid_data,
        std::vector<int64_t>& physical_offsets) const override;
};

}  // namespace milvus
