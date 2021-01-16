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

//
// Created by mike on 12/26/20.
//

#include "query/SearchOnSealed.h"
#include <knowhere/index/vector_index/VecIndex.h>
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

namespace milvus::query {

// negate bitset, and merge them into one
aligned_vector<uint8_t>
AssembleNegBitmap(const BitmapSimple& bitmap_simple) {
    int64_t N = 0;

    for (auto& bitmap : bitmap_simple) {
        N += bitmap.size();
    }
    aligned_vector<uint8_t> result(upper_align(upper_div(N, 8), sizeof(BitmapChunk::block_type)));

    auto acc_byte_count = 0;
    for (auto& bitmap_raw : bitmap_simple) {
        auto bitmap = ~bitmap_raw;
        auto size = bitmap.size();
        Assert(size % 8 == 0);
        auto byte_count = size / 8;

        auto iter = reinterpret_cast<BitmapChunk::block_type*>(result.data() + acc_byte_count);
        boost::to_block_range(bitmap, iter);

        acc_byte_count += byte_count;
    }

    return result;
}

void
SearchOnSealed(const Schema& schema,
               const segcore::SealedIndexingRecord& record,
               const QueryInfo& query_info,
               const void* query_data,
               int64_t num_queries,
               const faiss::BitsetView& bitset,
               QueryResult& result) {
    auto topK = query_info.topK_;

    auto field_offset = query_info.field_offset_;
    auto& field = schema[field_offset];
    // Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();

    Assert(record.is_ready(field_offset));
    auto indexing_entry = record.get_entry(field_offset);
    std::cout << " SearchOnSealed, indexing_entry->metric:" << indexing_entry->metric_type_ << std::endl;
    std::cout << " SearchOnSealed, query_info.metric_type_:" << query_info.metric_type_ << std::endl;
    Assert(indexing_entry->metric_type_ == GetMetricType(query_info.metric_type_));

    auto final = [&] {
        auto ds = knowhere::GenDataset(num_queries, dim, query_data);

        auto conf = query_info.search_params_;
        conf[milvus::knowhere::meta::TOPK] = query_info.topK_;
        conf[milvus::knowhere::Metric::TYPE] = MetricTypeToName(indexing_entry->metric_type_);
        return indexing_entry->indexing_->Query(ds, conf, bitset);
    }();

    auto ids = final->Get<idx_t*>(knowhere::meta::IDS);
    auto distances = final->Get<float*>(knowhere::meta::DISTANCE);

    auto total_num = num_queries * topK;
    result.internal_seg_offsets_.resize(total_num);
    result.result_distances_.resize(total_num);
    result.num_queries_ = num_queries;
    result.topK_ = topK;

    std::copy_n(ids, total_num, result.internal_seg_offsets_.data());
    std::copy_n(distances, total_num, result.result_distances_.data());
}
}  // namespace milvus::query
