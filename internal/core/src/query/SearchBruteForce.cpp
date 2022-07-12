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

#include <string>
#include <vector>

#include "SearchBruteForce.h"
#include "knowhere/archive/BruteForce.h"

namespace milvus::query {

SubSearchResult
BruteForceSearch(const dataset::SearchDataset& dataset,
                 const void* chunk_data_raw,
                 int64_t chunk_rows,
                 const BitsetView& bitset) {
    SubSearchResult sub_result(dataset.num_queries, dataset.topk, dataset.metric_type, dataset.round_decimal);
    try {
        knowhere::BruteForceSearch(dataset.metric_type, chunk_data_raw, dataset.query_data, dataset.dim, chunk_rows,
                                   dataset.num_queries, dataset.topk, sub_result.get_seg_offsets(),
                                   sub_result.get_distances(), bitset);
    } catch (std::exception& e) {
        PanicInfo(e.what());
    }
    sub_result.round_values();
    return sub_result;
}

}  // namespace milvus::query
