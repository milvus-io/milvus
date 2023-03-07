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

#include <cmath>

#include "common/QueryInfo.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "query/helper.h"

namespace milvus::query {

void
SearchOnSealedIndex(const Schema& schema,
                    const segcore::SealedIndexingRecord& record,
                    const SearchInfo& search_info,
                    const void* query_data,
                    int64_t num_queries,
                    const BitsetView& bitset,
                    SearchResult& result) {
    auto topk = search_info.topk_;
    auto round_decimal = search_info.round_decimal_;

    auto field_id = search_info.field_id_;
    auto& field = schema[field_id];
    // Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();

    AssertInfo(record.is_ready(field_id), "[SearchOnSealed]Record isn't ready");
    auto field_indexing = record.get_field_indexing(field_id);
    AssertInfo(field_indexing->metric_type_ == search_info.metric_type_,
               "Metric type of field index isn't the same with search info");

    auto final = [&] {
        auto ds = knowhere::GenDataSet(num_queries, dim, query_data);

        auto conf = search_info.search_params_;
        conf[knowhere::meta::TOPK] = search_info.topk_;
        conf[knowhere::meta::METRIC_TYPE] = field_indexing->metric_type_;
        auto vec_index =
            dynamic_cast<index::VectorIndex*>(field_indexing->indexing_.get());
        auto index_type = vec_index->GetIndexType();
        return vec_index->Query(ds, search_info, bitset);
    }();

    auto ids = final->seg_offsets_.data();
    float* distances = final->distances_.data();

    auto total_num = num_queries * topk;
    if (round_decimal != -1) {
        const float multiplier = pow(10.0, round_decimal);
        for (int i = 0; i < total_num; i++) {
            distances[i] = round(distances[i] * multiplier) / multiplier;
        }
    }
    result.seg_offsets_.resize(total_num);
    result.distances_.resize(total_num);
    result.total_nq_ = num_queries;
    result.unity_topK_ = topk;

    std::copy_n(ids, total_num, result.seg_offsets_.data());
    std::copy_n(distances, total_num, result.distances_.data());
}

void
SearchOnSealed(const Schema& schema,
               const void* vec_data,
               const SearchInfo& search_info,
               const void* query_data,
               int64_t num_queries,
               int64_t row_count,
               const BitsetView& bitset,
               SearchResult& result) {
    auto field_id = search_info.field_id_;
    auto& field = schema[field_id];

    query::dataset::SearchDataset dataset{search_info.metric_type_,
                                          num_queries,
                                          search_info.topk_,
                                          search_info.round_decimal_,
                                          field.get_dim(),
                                          query_data};

    CheckBruteForceSearchParam(field, search_info);
    auto sub_qr = BruteForceSearch(
        dataset, vec_data, row_count, search_info.search_params_, bitset);

    result.distances_ = std::move(sub_qr.mutable_distances());
    result.seg_offsets_ = std::move(sub_qr.mutable_seg_offsets());
    result.unity_topK_ = dataset.topk;
    result.total_nq_ = dataset.num_queries;
}

}  // namespace milvus::query
