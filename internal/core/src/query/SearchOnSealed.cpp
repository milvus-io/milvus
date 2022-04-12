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

#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/ConfAdapter.h"
#include "knowhere/index/vector_index/ConfAdapterMgr.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "query/SearchOnSealed.h"

namespace milvus::query {

void
SearchOnSealed(const Schema& schema,
               const segcore::SealedIndexingRecord& record,
               const SearchInfo& search_info,
               const void* query_data,
               int64_t num_queries,
               const BitsetView& bitset,
               SearchResult& result,
               int64_t segment_id) {
    auto topk = search_info.topk_;
    auto round_decimal = search_info.round_decimal_;

    auto field_offset = search_info.field_offset_;
    auto& field = schema[field_offset];
    // Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();

    AssertInfo(record.is_ready(field_offset), "[SearchOnSealed]Record isn't ready");
    auto field_indexing = record.get_field_indexing(field_offset);
    AssertInfo(field_indexing->metric_type_ == search_info.metric_type_,
               "Metric type of field index isn't the same with search info");

    auto final = [&] {
        auto ds = knowhere::GenDataset(num_queries, dim, query_data);

        auto conf = search_info.search_params_;
        conf[knowhere::meta::TOPK] = search_info.topk_;
        conf[knowhere::Metric::TYPE] = MetricTypeToName(field_indexing->metric_type_);
        auto index_type = field_indexing->indexing_->index_type();
        auto adapter = knowhere::AdapterMgr::GetInstance().GetAdapter(index_type);
        AssertInfo(adapter->CheckSearch(conf, index_type, field_indexing->indexing_->index_mode()),
                   "[SearchOnSealed]Search params check failed");
        return field_indexing->indexing_->Query(ds, conf, bitset);
    }();

    auto ids = final->Get<idx_t*>(knowhere::meta::IDS);
    auto distances = final->Get<float*>(knowhere::meta::DISTANCE);

    auto total_num = num_queries * topk;

    const float multiplier = pow(10.0, round_decimal);
    if (round_decimal != -1) {
        const float multiplier = pow(10.0, round_decimal);
        for (int i = 0; i < total_num; i++) {
            distances[i] = round(distances[i] * multiplier) / multiplier;
        }
    }
    result.ids_.resize(total_num);
    result.distances_.resize(total_num);
    result.num_queries_ = num_queries;
    result.topk_ = topk;

    std::copy_n(ids, total_num, result.ids_.data());
    std::copy_n(distances, total_num, result.distances_.data());
}
}  // namespace milvus::query
