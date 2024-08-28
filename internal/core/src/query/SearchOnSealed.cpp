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

#include <algorithm>
#include <cmath>
#include <string>

#include "bitset/detail/element_wise.h"
#include "common/BitsetView.h"
#include "common/QueryInfo.h"
#include "common/Types.h"
#include "mmap/Column.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "query/helper.h"
#include "exec/operator/groupby/SearchGroupByOperator.h"

namespace milvus::query {

void
SearchOnSealedIndex(const Schema& schema,
                    const segcore::SealedIndexingRecord& record,
                    const SearchInfo& search_info,
                    const void* query_data,
                    int64_t num_queries,
                    const BitsetView& bitset,
                    SearchResult& search_result) {
    auto topK = search_info.topk_;
    auto round_decimal = search_info.round_decimal_;

    auto field_id = search_info.field_id_;
    auto& field = schema[field_id];
    auto is_sparse = field.get_data_type() == DataType::VECTOR_SPARSE_FLOAT;
    // TODO(SPARSE): see todo in PlanImpl.h::PlaceHolder.
    auto dim = is_sparse ? 0 : field.get_dim();

    AssertInfo(record.is_ready(field_id), "[SearchOnSealed]Record isn't ready");
    // Keep the field_indexing smart pointer, until all reference by raw dropped.
    auto field_indexing = record.get_field_indexing(field_id);
    AssertInfo(field_indexing->metric_type_ == search_info.metric_type_,
               "Metric type of field index isn't the same with search info,"
               "field index: {}, search info: {}",
               field_indexing->metric_type_,
               search_info.metric_type_);

    auto dataset = knowhere::GenDataSet(num_queries, dim, query_data);
    dataset->SetIsSparse(is_sparse);
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(field_indexing->indexing_.get());
    if (!milvus::exec::PrepareVectorIteratorsFromIndex(search_info,
                                                       num_queries,
                                                       dataset,
                                                       search_result,
                                                       bitset,
                                                       *vec_index)) {
        auto index_type = vec_index->GetIndexType();
        vec_index->Query(dataset, search_info, bitset, search_result);
        float* distances = search_result.distances_.data();
        auto total_num = num_queries * topK;
        if (round_decimal != -1) {
            const float multiplier = pow(10.0, round_decimal);
            for (int i = 0; i < total_num; i++) {
                distances[i] =
                    std::round(distances[i] * multiplier) / multiplier;
            }
        }
    }
    search_result.total_nq_ = num_queries;
    search_result.unity_topK_ = topK;
}

void
SearchOnSealed(const Schema& schema,
               std::shared_ptr<ChunkedColumnBase> column,
               const SearchInfo& search_info,
               const void* query_data,
               int64_t num_queries,
               int64_t row_count,
               const BitsetView& bitview,
               SearchResult& result) {
    auto field_id = search_info.field_id_;
    auto& field = schema[field_id];

    // TODO(SPARSE): see todo in PlanImpl.h::PlaceHolder.
    auto dim = field.get_data_type() == DataType::VECTOR_SPARSE_FLOAT
                   ? 0
                   : field.get_dim();

    query::dataset::SearchDataset dataset{search_info.metric_type_,
                                          num_queries,
                                          search_info.topk_,
                                          search_info.round_decimal_,
                                          dim,
                                          query_data};

    auto data_type = field.get_data_type();
    CheckBruteForceSearchParam(field, search_info);
    auto num_chunk = column->num_chunks();

    SubSearchResult final_qr(num_queries,
                             search_info.topk_,
                             search_info.metric_type_,
                             search_info.round_decimal_);

    auto offset = 0;
    for (int i = 0; i < num_chunk; ++i) {
        auto vec_data = column->Data(i);
        auto chunk_size = column->chunk_row_nums(i);
        char bitset_data[(chunk_size + 7) / 8];
        std::fill(bitset_data, bitset_data + sizeof(bitset_data), 0);
        bitset::detail::ElementWiseBitsetPolicy<char>::op_copy(
            reinterpret_cast<const char*>(bitview.data()),
            offset,
            bitset_data,
            0,
            chunk_size);
        offset += chunk_size;
        BitsetView bitset_view(reinterpret_cast<uint8_t*>(bitset_data),
                               column->chunk_row_nums(i));

        if (search_info.group_by_field_id_.has_value()) {
            auto sub_qr = BruteForceSearchIterators(dataset,
                                                    vec_data,
                                                    row_count,
                                                    search_info,
                                                    bitset_view,
                                                    data_type);
            final_qr.merge(sub_qr);
        } else {
            auto sub_qr = BruteForceSearch(dataset,
                                           vec_data,
                                           row_count,
                                           search_info,
                                           bitset_view,
                                           data_type);
            final_qr.merge(sub_qr);
        }
    }
    if (search_info.group_by_field_id_.has_value()) {
        result.AssembleChunkVectorIterators(
            num_queries, 1, -1, final_qr.chunk_iterators());
    } else {
        result.distances_ = std::move(final_qr.mutable_distances());
        result.seg_offsets_ = std::move(final_qr.mutable_seg_offsets());
    }
    result.unity_topK_ = dataset.topk;
    result.total_nq_ = dataset.num_queries;
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

    // TODO(SPARSE): see todo in PlanImpl.h::PlaceHolder.
    auto dim = field.get_data_type() == DataType::VECTOR_SPARSE_FLOAT
                   ? 0
                   : field.get_dim();

    query::dataset::SearchDataset dataset{search_info.metric_type_,
                                          num_queries,
                                          search_info.topk_,
                                          search_info.round_decimal_,
                                          dim,
                                          query_data};

    auto data_type = field.get_data_type();
    CheckBruteForceSearchParam(field, search_info);
    if (search_info.group_by_field_id_.has_value()) {
        auto sub_qr = BruteForceSearchIterators(
            dataset, vec_data, row_count, search_info, bitset, data_type);
        result.AssembleChunkVectorIterators(
            num_queries, 1, -1, sub_qr.chunk_iterators());
    } else {
        auto sub_qr = BruteForceSearch(
            dataset, vec_data, row_count, search_info, bitset, data_type);
        result.distances_ = std::move(sub_qr.mutable_distances());
        result.seg_offsets_ = std::move(sub_qr.mutable_seg_offsets());
    }
    result.unity_topK_ = dataset.topk;
    result.total_nq_ = dataset.num_queries;
}

}  // namespace milvus::query
