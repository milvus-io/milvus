// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "SearchOnIndex.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "CachedSearchIterator.h"
#include "Utils.h"
#include "common/Consts.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "exec/operator/Utils.h"
#include "index/VectorIndex.h"
#include "knowhere/dataset.h"
#include "query/helper.h"

namespace milvus::query {
void
SearchOnIndex(const dataset::SearchDataset& search_dataset,
              const index::VectorIndex& indexing,
              const SearchInfo& search_conf,
              const BitsetView& bitset,
              milvus::OpContext* op_context,
              SearchResult& search_result,
              bool is_sparse) {
    auto num_queries = search_dataset.num_queries;
    auto dim = search_dataset.dim;
    auto metric_type = search_dataset.metric_type;
    auto dataset =
        knowhere::GenDataSet(num_queries, dim, search_dataset.query_data);
    dataset->SetIsSparse(is_sparse);

    BitsetView search_bitset = bitset;
    const auto active_count = search_conf.active_count_;

    // A growing interim index can advance after the plan freezes its visible
    // logical prefix.  Knowhere treats an empty bitset as IDSelectorAll, so an
    // empty/no-filter fast path must still carry an explicit prefix length or
    // rows appended after active_count can occupy the ANN top-k before Reduce
    // gets a chance to validate their offsets.
    //
    // Indexed nullable mapping lives inside Knowhere IdMap, so the visible
    // prefix stays in logical row space.
    if (active_count >= 0 && active_count == 0) {
        FillEmptySearchResult(search_result, num_queries, search_conf.topk_);
        return;
    }

    auto pin_visible_prefix = [&](int64_t count) {
        TargetBitmap visible_prefix(count, false);
        search_bitset = search_result.PinBitset(std::move(visible_prefix));
    };

    if (active_count >= 0 && bitset.empty()) {
        pin_visible_prefix(active_count);
    } else if (active_count >= 0) {
        // The plan layer sizes the filter bitmap to the same frozen
        // active_count it stamps into SearchInfo (both come from one
        // QueryContext), and Knowhere filters every id >= bitset.size(), so
        // the incoming bitset normally already carries the bound.
        //
        // A caller without a plan node can still hand in a longer bitset --
        // FloatSegmentIndexSearch resolves active_count_ from the segment for
        // those. Narrow the view rather than rejecting the search: scanning
        // past the frozen bound is the failure this function exists to
        // prevent, and the subview aliases the caller's buffer, so this costs
        // nothing. A shorter bitset already bounds the scan more tightly than
        // active_count and is left alone.
        if (static_cast<int64_t>(bitset.size()) > active_count) {
            search_bitset = bitset.subview(0, active_count);
        }
    }

    if (milvus::exec::PrepareVectorIteratorsFromIndex(search_conf,
                                                      num_queries,
                                                      dataset,
                                                      search_result,
                                                      search_bitset,
                                                      indexing,
                                                      op_context)) {
        return;
    }

    if (search_conf.iterator_v2_info_.has_value()) {
        auto iter = CachedSearchIterator(
            indexing, dataset, search_conf, search_bitset, op_context);
        iter.NextBatch(search_conf, search_result);
        return;
    }

    indexing.Query(
        dataset, search_conf, search_bitset, op_context, search_result);
}

}  // namespace milvus::query
