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
#include "common/OffsetMapping.h"
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

    const auto& offset_mapping = indexing.GetOffsetMapping();
    TargetBitmap transformed_bitset;
    BitsetView search_bitset = bitset;
    const auto has_offset_mapping = offset_mapping.IsEnabled();
    if (has_offset_mapping) {
        if (offset_mapping.GetValidCount() == 0) {
            // All vectors are null, return empty result
            FillEmptySearchResult(
                search_result, num_queries, search_conf.topk_);
            return;
        }
        if (!bitset.empty()) {
            auto status =
                offset_mapping.TransformBitset(bitset, transformed_bitset);
            if (status == OffsetMapping::BitsetTransformStatus::AllFiltered) {
                FillEmptySearchResult(
                    search_result, num_queries, search_conf.topk_);
                return;
            }
            search_bitset =
                status == OffsetMapping::BitsetTransformStatus::NoFilter
                    ? BitsetView{}
                    : search_result.PinBitset(std::move(transformed_bitset));
        }
    }

    if (milvus::exec::PrepareVectorIteratorsFromIndex(search_conf,
                                                      num_queries,
                                                      dataset,
                                                      search_result,
                                                      search_bitset,
                                                      indexing)) {
        return;
    }

    if (search_conf.iterator_v2_info_.has_value()) {
        auto iter =
            CachedSearchIterator(indexing, dataset, search_conf, search_bitset);
        iter.NextBatch(search_conf, search_result);
        if (has_offset_mapping) {
            offset_mapping.TransformOffsets(search_result.seg_offsets_);
        }
        return;
    }

    indexing.Query(
        dataset, search_conf, search_bitset, op_context, search_result);
    if (has_offset_mapping) {
        offset_mapping.TransformOffsets(search_result.seg_offsets_);
    }
}

}  // namespace milvus::query
