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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

#include "common/ArrayOffsets.h"
#include "common/QueryResult.h"
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
// concurrently against the same object. The one exception is the atomic
// claim flag on filter_storage_cost, documented below.
struct SharedFilterBitsetResult {
    // Post-MVCC bitset (bit set == row excluded) plus its validity bitmap.
    // Null when the segment had no active rows at the query timestamp.
    RowVectorPtr bitset;

    // Visible row count used when evaluating the filter. Every branch must use
    // the same row bound.
    int64_t active_count{0};

    int64_t segment_id{0};

    // Bytes the filter evaluation itself read. A plain search reports the
    // filter's bytes and the vector search's bytes on one result; here the
    // filter runs in phase 1 with its own OpContext, and each phase 2 branch
    // reports only its own vector search. The filter's share is handed to
    // exactly one branch through ClaimFilterStorageCost, so a group reports
    // it once -- the same total a plain search of any one branch would have
    // carried, not N times and not zero times. Which branch carries it is
    // whichever claims first; every consumer sums across branches, so that
    // is not observable.
    StorageCost filter_storage_cost;
    mutable std::atomic<bool> filter_storage_cost_claimed{false};

    // True for exactly one caller over the lifetime of this object.
    bool
    ClaimFilterStorageCost() const {
        return !filter_storage_cost_claimed.exchange(true);
    }

    // ---- derived query state produced alongside the bitset ----
    //
    // Evaluating the filter subtree writes more than the bitset:
    // ElementFilterBitsNode sets the element-level fields. Running it once
    // means only the producing QueryContext receives them, so they must be
    // replayed onto every branch context or downstream operators take the
    // wrong path (PhyVectorSearchNode reads bitset_is_element_level to pick
    // between the element-level and the row-level path).
    //
    // all_rows_visible is the one piece of query state deliberately left out.
    // MvccNode writes it only on its source-node sealed fast path, and in a
    // grouped search MvccNode is never the source: the proxy marks a
    // sub-request shareable only when it carries a predicate, and phase 1
    // asserts an extractable filter prefix, so FilterBitsNode feeds it. The
    // segcore API does accept a predicate-less plan, where the prefix is the
    // bare MvccNode and the flag is set; not carrying it costs that shape the
    // empty-BitsetView fast path and nothing else, because the branch then
    // builds a BitsetView over the all-visible bitmap it was handed.
    //
    // Anything else a filter-subtree operator writes to the QueryContext
    // belongs here. Adding one without adding it here is a silent correctness
    // bug.
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
