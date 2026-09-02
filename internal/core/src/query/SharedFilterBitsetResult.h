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
#include <memory>
#include <string>

#include "common/ArrayOffsets.h"
#include "common/Vector.h"

namespace milvus::exec {
class QueryContext;
}

namespace milvus::query {

// One segment's shared filter bitset, reused by every branch of a hybrid
// search whose sub-requests carry the same predicate.
//
// Ownership crosses the cgo boundary: segcore produces one of these per
// segment, Go holds it for the duration of the branch fan-out and releases it
// with DeleteSharedFilterBitsetResult. It is written once, when the filter is
// evaluated, and is strictly read-only afterwards -- the branch searches run
// concurrently against the same object.
struct SharedFilterBitsetResult {
    // Post-MVCC bitset (bit set == row excluded) plus its validity bitmap.
    // Null when the segment had no active rows at the query timestamp.
    RowVectorPtr bitset;

    // Segment row count visible at the query timestamp when the filter ran.
    // A branch observing a different count is looking at another snapshot.
    int64_t active_count{0};

    int64_t segment_id{0};

    // ---- derived query state produced alongside the bitset ----
    //
    // Evaluating the filter subtree writes more than the bitset: MvccNode sets
    // all_rows_visible, and ElementFilterBitsNode sets the element-level
    // fields. Running it once means only the producing QueryContext receives
    // them, so they must be replayed onto every branch context or downstream
    // operators take the wrong path (PhyVectorSearchNode reads
    // all_rows_visible and bitset_is_element_level to pick between the empty
    // BitsetView fast path and the normal path).
    //
    // Anything a filter-subtree operator writes to the QueryContext belongs
    // here. Adding one without adding it here is a silent correctness bug.
    bool all_rows_visible{false};
    bool bitset_is_element_level{false};
    int64_t active_element_count{0};
    std::shared_ptr<const IArrayOffsets> array_offsets{nullptr};
    std::string struct_name;

    // Copy the derived state off the context that evaluated the filter.
    void
    CaptureFrom(const milvus::exec::QueryContext& ctx);

    // Replay it onto a branch's context.
    void
    ApplyTo(milvus::exec::QueryContext& ctx) const;
};

using SharedFilterBitsetResultPtr = std::unique_ptr<SharedFilterBitsetResult>;

}  // namespace milvus::query
