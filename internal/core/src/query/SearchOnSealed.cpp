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
#include "cachinglayer/Utils.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/QueryInfo.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "query/CachedSearchIterator.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnSealed.h"
#include "query/Utils.h"
#include "query/helper.h"
#include "exec/operator/Utils.h"

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
        dataset->Set(knowhere::meta::NQ, num_queries);
    }

    dataset->SetIsSparse(is_sparse);
    auto accessor =
        SemiInlineGet(field_indexing->indexing_->PinCells(op_context, {0}));
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(accessor->get_cell_of(0));

    const auto& offset_mapping = vec_index->GetOffsetMapping();
    const bool is_element_level_search = search_info.array_offsets_ != nullptr;
    TargetBitmap transformed_bitset;
    BitsetView search_bitset = bitset;
    const auto has_offset_mapping = offset_mapping.IsEnabled();
    if (has_offset_mapping && !is_element_level_search) {
        if (offset_mapping.GetValidCount() == 0) {
            FillEmptySearchResult(search_result, num_queries, topK);
            return;
        }
        if (!bitset.empty()) {
            auto status =
                offset_mapping.TransformBitset(bitset, transformed_bitset);
            if (status == OffsetMapping::BitsetTransformStatus::AllFiltered) {
                FillEmptySearchResult(search_result, num_queries, topK);
                return;
            }
            search_bitset =
                status == OffsetMapping::BitsetTransformStatus::NoFilter
                    ? BitsetView{}
                    : search_result.PinBitset(std::move(transformed_bitset));
        }
    }

    if (search_info.iterator_v2_info_.has_value()) {
        CachedSearchIterator cached_iter(
            *vec_index, dataset, search_info, search_bitset);
        cached_iter.NextBatch(search_info, search_result);
        FinalizeVectorSearchOffsets(
            search_result, offset_mapping, search_info.array_offsets_.get());
        return;
    }

    AssertInfo(search_info.array_offsets_ == nullptr ||
                   !milvus::exec::UseVectorIterator(search_info),
               "element-level search is not supported for vector iterator");

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
    }
    FinalizeVectorSearchOffsets(
        search_result,
        offset_mapping,
        use_iterator ? nullptr : search_info.array_offsets_.get());
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

    if (column->IsNullable()) {
        column->BuildValidRowIds(op_context);
    }

    // Check for nullable vector field with all null values - must be done before creating iterators
    const auto& offset_mapping = column->GetOffsetMapping();
    // Element-level VECTOR_ARRAY search has already expanded the row bitset
    // to element IDs. OffsetMapping is row-level, so only use it for row-level
    // vector searches.
    bool is_element_level_search =
        field.get_data_type() == DataType::VECTOR_ARRAY &&
        search_info.array_offsets_ != nullptr;
    TargetBitmap transformed_bitset;
    BitsetView search_bitview = bitview;
    const auto has_offset_mapping = offset_mapping.IsEnabled();
    if (has_offset_mapping) {
        if (!is_element_level_search) {
            if (offset_mapping.GetValidCount() == 0) {
                // All vectors are null, return empty result
                FillEmptySearchResult(result, num_queries, search_info.topk_);
                return;
            }
            if (!bitview.empty()) {
                auto status =
                    offset_mapping.TransformBitset(bitview, transformed_bitset);
                if (status ==
                    OffsetMapping::BitsetTransformStatus::AllFiltered) {
                    FillEmptySearchResult(
                        result, num_queries, search_info.topk_);
                    return;
                }
                search_bitview =
                    status == OffsetMapping::BitsetTransformStatus::NoFilter
                        ? BitsetView{}
                        : result.PinBitset(std::move(transformed_bitset));
            }
        }
    }

    // For element-level search (embedding-search-embedding), the underlying
    // knowhere search is keyed by the scalar element type rather than
    // VECTOR_ARRAY, and per-chunk sizes must be counted in elements.
    if (is_element_level_search) {
        data_type = element_type;
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
        FinalizeVectorSearchOffsets(
            result, offset_mapping, search_info.array_offsets_.get());
        return;
    }

    const bool use_vector_iterator =
        milvus::exec::UseVectorIterator(search_info);
    auto num_chunk = column->num_chunks();

    SubSearchResult final_qr(num_queries,
                             search_info.topk_,
                             search_info.metric_type_,
                             search_info.round_decimal_);

    AssertInfo(!is_element_level_search ||
                   !milvus::exec::UseVectorIterator(search_info),
               "element-level search is not supported for vector iterator");

    int64_t offset = 0;
    auto vector_chunks = column->GetAllChunks(op_context);
    for (int i = 0; i < num_chunk; ++i) {
        auto pw = vector_chunks[i];
        auto vec_data = pw.get()->Data();
        auto row_count_in_chunk = column->chunk_row_nums(i);
        auto chunk_size = row_count_in_chunk;
        if (has_offset_mapping) {
            chunk_size = column->GetValidCountInChunk(i);
        }
        auto raw_dataset =
            query::dataset::RawDataset{offset, dim, chunk_size, vec_data};

        PinWrapper<const size_t*> offsets_pw;
        if (is_element_level_search) {
            offsets_pw = column->VectorArrayOffsets(op_context, i);
            auto elem_offsets = offsets_pw.get();
            chunk_size = elem_offsets[row_count_in_chunk];
            raw_dataset =
                query::dataset::RawDataset{offset, dim, chunk_size, vec_data};
        }

        if (data_type == DataType::VECTOR_ARRAY) {
            AssertInfo(
                query_offsets != nullptr,
                "query_offsets is nullptr, but data_type is vector array");

            offsets_pw = column->VectorArrayOffsets(op_context, i);
            raw_dataset.raw_data_offsets = offsets_pw.get();
        }

        if (use_vector_iterator) {
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
    if (use_vector_iterator) {
        bool larger_is_closer = PositivelyRelated(search_info.metric_type_);
        // Element-level search skips row-level mapping (element IDs are
        // not row-aligned); see ChunkMergeIterator ctor.
        const milvus::OffsetMapping* iter_offset_mapping =
            (search_info.array_offsets_ != nullptr || !has_offset_mapping)
                ? nullptr
                : &offset_mapping;
        result.AssembleChunkVectorIterators(num_queries,
                                            num_chunk,
                                            column->GetNumRowsUntilChunk(),
                                            final_qr.chunk_iterators(),
                                            iter_offset_mapping,
                                            larger_is_closer);
    } else {
        if (is_element_level_search) {
            auto [seg_offsets, element_indices] =
                final_qr.convert_to_element_offsets(
                    search_info.array_offsets_.get());
            result.seg_offsets_ = std::move(seg_offsets);
            result.element_indices_ = std::move(element_indices);
            result.element_level_ = true;
        } else {
            if (has_offset_mapping) {
                offset_mapping.TransformOffsets(final_qr.mutable_seg_offsets());
            }
            result.seg_offsets_ = std::move(final_qr.mutable_seg_offsets());
        }
        result.distances_ = std::move(final_qr.mutable_distances());
    }
    result.unity_topK_ = query_dataset.topk;
    result.total_nq_ = query_dataset.num_queries;
}

}  // namespace milvus::query
