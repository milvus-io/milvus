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

#include "SearchBruteForce.h"
#include "SubSearchResult.h"
#include "segcore/Utils.h"

#include "knowhere/common/Dataset.h"
#include "knowhere/index/vector_index/IndexBinaryIDMAP.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

namespace milvus::query {

SubSearchResult
SearchBruteForce(const dataset::SearchDataset& dataset,
                 const void* chunk_data_raw,
                 int64_t rows,
                 const BitsetView& bitset,
                 bool is_binary) {
    auto topk = dataset.topk;
    auto dim = dataset.dim;
    auto nq = dataset.num_queries;

    auto train_dataset = knowhere::GenDataset(rows, dim, chunk_data_raw);
    auto query_dataset = knowhere::GenDataset(nq, dim, dataset.query_data);

    auto conf = knowhere::Config{
        {knowhere::meta::METRIC_TYPE, dataset.metric_type},
        {knowhere::meta::DIM, dim},
        {knowhere::meta::TOPK, topk},
    };

    knowhere::DatasetPtr result;
    if (is_binary) {
        auto bin_idmap_index = std::make_shared<knowhere::BinaryIDMAP>();
        bin_idmap_index->Train(train_dataset, conf);
        bin_idmap_index->AddExWithoutIds(train_dataset, conf);
        result = bin_idmap_index->Query(query_dataset, conf, bitset);
    } else {
        auto idmap_index = std::make_shared<knowhere::IDMAP>();
        idmap_index->Train(train_dataset, conf);
        idmap_index->AddExWithoutIds(train_dataset, conf);
        result = idmap_index->Query(query_dataset, conf, bitset);
    }

    SubSearchResult sub_result(nq, topk, dataset.metric_type, dataset.round_decimal);
    std::copy_n(knowhere::GetDatasetIDs(result), topk * nq, sub_result.get_seg_offsets());
    std::copy_n(knowhere::GetDatasetDistance(result), topk * nq, sub_result.get_distances());
    sub_result.round_values();
    return sub_result;
}

}  // namespace milvus::query
