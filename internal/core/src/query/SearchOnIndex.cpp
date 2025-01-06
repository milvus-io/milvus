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
#include "exec/operator/Utils.h"
#include "CachedSearchIterator.h"

namespace milvus::query {
void
SearchOnIndex(const dataset::SearchDataset& search_dataset,
              const index::VectorIndex& indexing,
              const SearchInfo& search_conf,
              const BitsetView& bitset,
              SearchResult& search_result,
              bool is_sparse) {
    auto num_queries = search_dataset.num_queries;
    auto dim = search_dataset.dim;
    auto metric_type = search_dataset.metric_type;
    auto dataset =
        knowhere::GenDataSet(num_queries, dim, search_dataset.query_data);
    dataset->SetIsSparse(is_sparse);
    if (milvus::exec::PrepareVectorIteratorsFromIndex(search_conf,
                                                      num_queries,
                                                      dataset,
                                                      search_result,
                                                      bitset,
                                                      indexing)) {
        return;
    }

    if (search_conf.iterator_v2_info_.has_value()) {
        auto iter =
            CachedSearchIterator(indexing, dataset, search_conf, bitset);
        iter.NextBatch(search_conf, search_result);
        return;
    }

    indexing.Query(dataset, search_conf, bitset, search_result);
}

}  // namespace milvus::query
