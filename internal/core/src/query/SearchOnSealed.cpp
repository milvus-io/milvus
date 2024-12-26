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
#include "query/CachedSearchIterator.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "query/helper.h"
#include "exec/operator/Utils.h"

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

    if (search_info.iterator_v2_info_.has_value()) {
        CachedSearchIterator cached_iter(
            *vec_index, dataset, search_info, bitset);
        cached_iter.NextBatch(search_info, search_result);
        return;
    }

    if (!milvus::exec::PrepareVectorIteratorsFromIndex(search_info,
                                                       num_queries,
                                                       dataset,
                                                       search_result,
                                                       bitset,
                                                       *vec_index)) {
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
               const std::map<std::string, std::string>& index_info,
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

    query::dataset::SearchDataset query_dataset{search_info.metric_type_,
                                                num_queries,
                                                search_info.topk_,
                                                search_info.round_decimal_,
                                                dim,
                                                query_data};

    auto data_type = field.get_data_type();
    CheckBruteForceSearchParam(field, search_info);

    if (search_info.iterator_v2_info_.has_value()) {
        CachedSearchIterator cached_iter(
            column, query_dataset, search_info, index_info, bitview, data_type);
        cached_iter.NextBatch(search_info, result);
        return;
    }

    auto num_chunk = column->num_chunks();

    SubSearchResult final_qr(num_queries,
                             search_info.topk_,
                             search_info.metric_type_,
                             search_info.round_decimal_);

    auto offset = 0;
    for (int i = 0; i < num_chunk; ++i) {
        auto vec_data = column->Data(i);
        auto chunk_size = column->chunk_row_nums(i);
        auto raw_dataset =
            query::dataset::RawDataset{offset, dim, chunk_size, vec_data};
        if (milvus::exec::UseVectorIterator(search_info)) {
            auto sub_qr =
                PackBruteForceSearchIteratorsIntoSubResult(query_dataset,
                                                           raw_dataset,
                                                           search_info,
                                                           index_info,
                                                           bitview,
                                                           data_type);
            final_qr.merge(sub_qr);
        } else {
            auto sub_qr = BruteForceSearch(query_dataset,
                                           raw_dataset,
                                           search_info,
                                           index_info,
                                           bitview,
                                           data_type);
            final_qr.merge(sub_qr);
        }
        offset += chunk_size;
    }
    if (milvus::exec::UseVectorIterator(search_info)) {
        result.AssembleChunkVectorIterators(num_queries,
                                            num_chunk,
                                            column->GetNumRowsUntilChunk(),
                                            final_qr.chunk_iterators());
    } else {
        result.distances_ = std::move(final_qr.mutable_distances());
        result.seg_offsets_ = std::move(final_qr.mutable_seg_offsets());
    }
    result.unity_topK_ = query_dataset.topk;
    result.total_nq_ = query_dataset.num_queries;
}

void
SearchOnSealed(const Schema& schema,
               const void* vec_data,
               const SearchInfo& search_info,
               const std::map<std::string, std::string>& index_info,
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

    query::dataset::SearchDataset query_dataset{search_info.metric_type_,
                                                num_queries,
                                                search_info.topk_,
                                                search_info.round_decimal_,
                                                dim,
                                                query_data};

    auto data_type = field.get_data_type();
    CheckBruteForceSearchParam(field, search_info);
    auto raw_dataset = query::dataset::RawDataset{0, dim, row_count, vec_data};
    if (milvus::exec::UseVectorIterator(search_info)) {
        auto sub_qr = PackBruteForceSearchIteratorsIntoSubResult(query_dataset,
                                                                 raw_dataset,
                                                                 search_info,
                                                                 index_info,
                                                                 bitset,
                                                                 data_type);
        result.AssembleChunkVectorIterators(
            num_queries, 1, {0}, sub_qr.chunk_iterators());
    } else if (search_info.iterator_v2_info_.has_value()) {
        CachedSearchIterator cached_iter(query_dataset,
                                         raw_dataset,
                                         search_info,
                                         index_info,
                                         bitset,
                                         data_type);
        cached_iter.NextBatch(search_info, result);
        return;
    } else {
        auto sub_qr = BruteForceSearch(query_dataset,
                                       raw_dataset,
                                       search_info,
                                       index_info,
                                       bitset,
                                       data_type);
        result.distances_ = std::move(sub_qr.mutable_distances());
        result.seg_offsets_ = std::move(sub_qr.mutable_seg_offsets());
    }
    result.unity_topK_ = query_dataset.topk;
    result.total_nq_ = query_dataset.num_queries;
}

}  // namespace milvus::query
