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

#include <folly/ExceptionWrapper.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Utils.h"
#include "common/ArrayOffsets.h"
#include "common/BitsetView.h"
#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/OffsetMapping.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "exec/operator/Utils.h"
#include "index/Index.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "mmap/ChunkedColumnInterface.h"
#include "query/CachedSearchIterator.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "query/SubSearchResult.h"
#include "query/Utils.h"
#include "query/helper.h"
#include "segcore/SealedIndexingRecord.h"

namespace milvus::query {

void
SearchOnSealedIndex(const Schema& schema,
                    const segcore::SealedIndexingRecord& record,
                    const SearchInfo& search_info,
                    const void* query_data,
                    const size_t* query_offsets,
                    int64_t num_queries,
                    const BitsetView& bitset,
                    milvus::OpContext* op_context,
                    SearchResult& search_result) {
    auto topK = search_info.topk_;
    auto round_decimal = search_info.round_decimal_;

    auto field_id = search_info.field_id_;
    auto& field = schema[field_id];
    auto is_sparse = field.get_data_type() == DataType::VECTOR_SPARSE_U32_F32;
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

    knowhere::DataSetPtr dataset;
    if (query_offsets == nullptr) {
        dataset = knowhere::GenDataSet(num_queries, dim, query_data);
    } else {
        // Rather than non-embedding list search where num_queries equals to the number of vectors,
        // in embedding list search, multiple vectors form an embedding list and the last element of query_offsets
        // stands for the total number of vectors.
        auto num_vectors = query_offsets[num_queries];
        dataset = knowhere::GenDataSet(num_vectors, dim, query_data);
        dataset->Set(knowhere::meta::EMB_LIST_OFFSET, query_offsets);
    }

    dataset->SetIsSparse(is_sparse);
    auto accessor =
        SemiInlineGet(field_indexing->indexing_->PinCells(nullptr, {0}));
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(accessor->get_cell_of(0));

    const auto& offset_mapping = vec_index->GetOffsetMapping();
    TargetBitmap transformed_bitset;
    BitsetView search_bitset = bitset;
    if (offset_mapping.IsEnabled()) {
        transformed_bitset = TransformBitset(bitset, offset_mapping);
        search_bitset = BitsetView(transformed_bitset);
        if (offset_mapping.GetValidCount() == 0) {
            auto total_num = num_queries * topK;
            search_result.seg_offsets_.resize(total_num, INVALID_SEG_OFFSET);
            search_result.distances_.resize(total_num, 0.0f);
            search_result.total_nq_ = num_queries;
            search_result.unity_topK_ = topK;
            return;
        }
    }

    if (search_info.iterator_v2_info_.has_value()) {
        CachedSearchIterator cached_iter(
            *vec_index, dataset, search_info, search_bitset);
        cached_iter.NextBatch(search_info, search_result);
        TransformOffset(search_result.seg_offsets_, offset_mapping);
        return;
    }

    bool use_iterator =
        milvus::exec::PrepareVectorIteratorsFromIndex(search_info,
                                                      num_queries,
                                                      dataset,
                                                      search_result,
                                                      search_bitset,
                                                      *vec_index);
    if (!use_iterator) {
        vec_index->Query(
            dataset, search_info, search_bitset, op_context, search_result);
        float* distances = search_result.distances_.data();
        auto total_num = num_queries * topK;
        if (round_decimal != -1) {
            const float multiplier = pow(10.0, round_decimal);
            for (int i = 0; i < total_num; i++) {
                distances[i] =
                    std::round(distances[i] * multiplier) / multiplier;
            }
        }

        // Handle element-level conversion if needed
        if (search_info.array_offsets_ != nullptr) {
            std::vector<int64_t> element_ids =
                std::move(search_result.seg_offsets_);
            search_result.seg_offsets_.resize(element_ids.size());
            search_result.element_indices_.resize(element_ids.size());

            for (size_t i = 0; i < element_ids.size(); i++) {
                if (element_ids[i] == INVALID_SEG_OFFSET) {
                    search_result.seg_offsets_[i] = INVALID_SEG_OFFSET;
                    search_result.element_indices_[i] = -1;
                } else {
                    auto [doc_id, elem_index] =
                        search_info.array_offsets_->ElementIDToRowID(
                            element_ids[i]);
                    search_result.seg_offsets_[i] = doc_id;
                    search_result.element_indices_[i] = elem_index;
                }
            }
            search_result.element_level_ = true;
        }
    }
    TransformOffset(search_result.seg_offsets_, offset_mapping);
    search_result.total_nq_ = num_queries;
    search_result.unity_topK_ = topK;
}

void
SearchOnSealedColumn(const Schema& schema,
                     ChunkedColumnInterface* column,
                     const SearchInfo& search_info,
                     const std::map<std::string, std::string>& index_info,
                     const void* query_data,
                     const size_t* query_offsets,
                     int64_t num_queries,
                     int64_t row_count,
                     const BitsetView& bitview,
                     milvus::OpContext* op_context,
                     SearchResult& result) {
    auto field_id = search_info.field_id_;
    auto& field = schema[field_id];

    auto data_type = field.get_data_type();
    auto element_type = field.get_element_type();
    // TODO(SPARSE): see todo in PlanImpl.h::PlaceHolder.
    auto dim =
        data_type == DataType::VECTOR_SPARSE_U32_F32 ? 0 : field.get_dim();

    query::dataset::SearchDataset query_dataset{search_info.metric_type_,
                                                num_queries,
                                                search_info.topk_,
                                                search_info.round_decimal_,
                                                dim,
                                                query_data,
                                                query_offsets};

    CheckBruteForceSearchParam(field, search_info);

    // Check for nullable vector field with all null values - must be done before creating iterators
    const auto& offset_mapping = column->GetOffsetMapping();
    TargetBitmap transformed_bitset;
    BitsetView search_bitview = bitview;
    if (offset_mapping.IsEnabled()) {
        transformed_bitset = TransformBitset(bitview, offset_mapping);
        search_bitview = BitsetView(transformed_bitset);
        if (offset_mapping.GetValidCount() == 0) {
            // All vectors are null, return empty result
            auto total_num = num_queries * search_info.topk_;
            result.seg_offsets_.resize(total_num, INVALID_SEG_OFFSET);
            result.distances_.resize(total_num, 0.0f);
            result.total_nq_ = num_queries;
            result.unity_topK_ = search_info.topk_;
            return;
        }
    }

    if (search_info.iterator_v2_info_.has_value()) {
        AssertInfo(data_type != DataType::VECTOR_ARRAY,
                   "vector array(embedding list) is not supported for "
                   "vector iterator");

        CachedSearchIterator cached_iter(column,
                                         query_dataset,
                                         search_info,
                                         index_info,
                                         search_bitview,
                                         data_type);
        cached_iter.NextBatch(search_info, result);
        return;
    }

    auto num_chunk = column->num_chunks();

    SubSearchResult final_qr(num_queries,
                             search_info.topk_,
                             search_info.metric_type_,
                             search_info.round_decimal_);

    // For element-level search (embedding-search-embedding), we need to use
    // element count instead of row count
    bool is_element_level_search =
        field.get_data_type() == DataType::VECTOR_ARRAY &&
        query_offsets == nullptr;

    if (is_element_level_search) {
        // embedding-search-embedding on embedding list pattern
        data_type = element_type;
    }

    auto offset = 0;
    auto vector_chunks = column->GetAllChunks(op_context);
    const auto& valid_count_per_chunk = column->GetValidCountPerChunk();
    for (int i = 0; i < num_chunk; ++i) {
        const auto& pw = vector_chunks[i];
        auto vec_data = pw.get()->Data();
        auto chunk_size = column->chunk_row_nums(i);
        if (offset_mapping.IsEnabled() && !valid_count_per_chunk.empty()) {
            chunk_size = valid_count_per_chunk[i];
        }

        // For element-level search, get element count from VectorArrayOffsets
        if (is_element_level_search) {
            auto elem_offsets_pw = column->VectorArrayOffsets(op_context, i);
            // offsets[row_count] gives total element count in this chunk
            chunk_size = elem_offsets_pw.get()[chunk_size];
        }

        auto raw_dataset =
            query::dataset::RawDataset{offset, dim, chunk_size, vec_data};

        PinWrapper<const size_t*> offsets_pw;
        if (data_type == DataType::VECTOR_ARRAY) {
            AssertInfo(
                query_offsets != nullptr,
                "query_offsets is nullptr, but data_type is vector array");

            offsets_pw = column->VectorArrayOffsets(op_context, i);
            raw_dataset.raw_data_offsets = offsets_pw.get();
        }

        if (milvus::exec::UseVectorIterator(search_info)) {
            AssertInfo(data_type != DataType::VECTOR_ARRAY,
                       "vector array(embedding list) is not supported for "
                       "vector iterator");
            auto sub_qr =
                PackBruteForceSearchIteratorsIntoSubResult(query_dataset,
                                                           raw_dataset,
                                                           search_info,
                                                           index_info,
                                                           search_bitview,
                                                           data_type);
            final_qr.merge(sub_qr);
        } else {
            auto sub_qr = BruteForceSearch(query_dataset,
                                           raw_dataset,
                                           search_info,
                                           index_info,
                                           search_bitview,
                                           data_type,
                                           element_type,
                                           op_context);
            final_qr.merge(sub_qr);
        }
        offset += chunk_size;
    }
    if (milvus::exec::UseVectorIterator(search_info)) {
        bool larger_is_closer = PositivelyRelated(search_info.metric_type_);
        result.AssembleChunkVectorIterators(num_queries,
                                            num_chunk,
                                            column->GetNumRowsUntilChunk(),
                                            final_qr.chunk_iterators(),
                                            offset_mapping,
                                            larger_is_closer);
    } else {
        if (search_info.array_offsets_ != nullptr) {
            auto [seg_offsets, elem_indicies] =
                final_qr.convert_to_element_offsets(
                    search_info.array_offsets_.get());
            result.seg_offsets_ = std::move(seg_offsets);
            result.element_indices_ = std::move(elem_indicies);
            result.element_level_ = true;
        } else {
            result.seg_offsets_ = std::move(final_qr.mutable_offsets());
        }
        result.distances_ = std::move(final_qr.mutable_distances());
        if (offset_mapping.IsEnabled()) {
            TransformOffset(result.seg_offsets_, offset_mapping);
        }
    }
    result.unity_topK_ = query_dataset.topk;
    result.total_nq_ = query_dataset.num_queries;
}

}  // namespace milvus::query
