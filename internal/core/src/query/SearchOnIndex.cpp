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
#include "Utils.h"
#include "exec/operator/Utils.h"
#include "CachedSearchIterator.h"

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
    if (offset_mapping.IsEnabled()) {
        transformed_bitset = TransformBitset(bitset, offset_mapping);
        search_bitset = BitsetView(transformed_bitset);
        if (offset_mapping.GetValidCount() == 0) {
            // All vectors are null, return empty result
            auto total_num = num_queries * search_conf.topk_;
            search_result.seg_offsets_.resize(total_num, INVALID_SEG_OFFSET);
            search_result.distances_.resize(total_num, 0.0f);
            search_result.total_nq_ = num_queries;
            search_result.unity_topK_ = search_conf.topk_;
            return;
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
        if (offset_mapping.IsEnabled()) {
            TransformOffset(search_result.seg_offsets_, offset_mapping);
        }
        return;
    }

    indexing.Query(
        dataset, search_conf, search_bitset, op_context, search_result);
    if (offset_mapping.IsEnabled()) {
        TransformOffset(search_result.seg_offsets_, offset_mapping);
    }
}

}  // namespace milvus::query
