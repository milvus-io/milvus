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
        dataset->Set(knowhere::meta::NQ, num_queries);
    }

    dataset->SetIsSparse(is_sparse);
    auto accessor =
        SemiInlineGet(field_indexing->indexing_->PinCells(op_context, {0}));
    auto vec_index =
        dynamic_cast<index::VectorIndex*>(accessor->get_cell_of(0));

    const bool is_element_level_search = search_info.array_offsets_ != nullptr;
    search_result.element_level_ = is_element_level_search;

    if (search_info.iterator_v2_info_.has_value()) {
        CachedSearchIterator cached_iter(
            *vec_index, dataset, search_info, bitset);
        cached_iter.NextBatch(search_info, search_result);
        FinalizeVectorSearchOffsets(search_result,
                                    search_info.array_offsets_.get());
        return;
    }

    bool use_iterator = milvus::exec::PrepareVectorIteratorsFromIndex(
        search_info, num_queries, dataset, search_result, bitset, *vec_index);
    if (!use_iterator) {
        vec_index->Query(
            dataset, search_info, bitset, op_context, search_result);
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

    column->BuildValidRowIds(op_context);

    bool is_element_level_search =
        field.get_data_type() == DataType::VECTOR_ARRAY &&
        search_info.array_offsets_ != nullptr;
    result.element_level_ = is_element_level_search;

    // For element-level search (embedding-search-embedding), the underlying
    // knowhere search is keyed by the scalar element type rather than
    // VECTOR_ARRAY, and per-chunk sizes must be counted in elements.
    if (is_element_level_search) {
        data_type = element_type;
    }

    if (search_info.iterator_v2_info_.has_value()) {
        // Element-level search has already replaced data_type with
        // element_type above. This assert still guards emb-list
        // (multi-search-multi) iterator, which proxy should have rejected
        // — keep it as a defense-in-depth check.
        AssertInfo(data_type != DataType::VECTOR_ARRAY,
                   "embedding list (multi-search-multi) iterator is not "
                   "supported on vector array fields");

        CachedSearchIterator cached_iter(
            column, query_dataset, search_info, index_info, bitview, data_type);
        cached_iter.NextBatch(search_info, result);
        FinalizeVectorSearchOffsets(result, search_info.array_offsets_.get());
        return;
    }

    const bool use_vector_iterator =
        milvus::exec::UseVectorIterator(search_info);
    auto num_chunk = column->num_chunks();

    SubSearchResult final_qr(num_queries,
                             search_info.topk_,
                             search_info.metric_type_,
                             search_info.round_decimal_);

    auto offset = 0;
    auto vector_chunks = column->GetAllChunks(op_context);
    for (int i = 0; i < num_chunk; ++i) {
        const auto& pw = vector_chunks[i];
        auto vec_data = pw.get()->Data();
        auto id_map =
            column->GetRawDataIdMapInChunk(i, !is_element_level_search);
        auto chunk_size = id_map.chunk_size;

        // For element-level search, get element count from VectorArrayOffsets
        if (is_element_level_search) {
            auto elem_offsets_pw = column->VectorArrayOffsets(op_context, i);
            // offsets[row_count] gives total element count in this chunk
            chunk_size = elem_offsets_pw.get()[chunk_size];
        }

        auto raw_dataset =
            query::dataset::RawDataset{id_map.has_id_map() ? 0 : offset,
                                       dim,
                                       chunk_size,
                                       vec_data,
                                       nullptr};
        auto raw_id_map =
            query::dataset::RawIdMapView{id_map.internal_to_external_ids,
                                         id_map.internal_to_external_ids_count};

        PinWrapper<const size_t*> offsets_pw;
        std::vector<size_t> compact_offsets;
        if (data_type == DataType::VECTOR_ARRAY) {
            AssertInfo(
                query_offsets != nullptr,
                "query_offsets is nullptr, but data_type is vector array");

            offsets_pw = column->VectorArrayOffsets(op_context, i);
            raw_dataset.raw_data_offsets = offsets_pw.get();
            if (!is_element_level_search && id_map.has_id_map()) {
                auto logical_offsets = offsets_pw.get();
                compact_offsets.reserve(id_map.internal_to_external_ids_count +
                                        1);
                compact_offsets.push_back(0);
                auto chunk_begin = column->GetNumRowsUntilChunk(i);
                for (size_t j = 0; j < id_map.internal_to_external_ids_count;
                     ++j) {
                    auto local_row =
                        id_map.internal_to_external_ids[j] - chunk_begin;
                    auto vector_count = logical_offsets[local_row + 1] -
                                        logical_offsets[local_row];
                    compact_offsets.push_back(compact_offsets.back() +
                                              vector_count);
                }
                raw_dataset.raw_data_offsets = compact_offsets.data();
            }
        }

        if (use_vector_iterator) {
            AssertInfo(data_type != DataType::VECTOR_ARRAY,
                       "vector array(embedding list) is not supported for "
                       "vector iterator");
            auto sub_qr =
                PackBruteForceSearchIteratorsIntoSubResult(query_dataset,
                                                           raw_dataset,
                                                           raw_id_map,
                                                           search_info,
                                                           index_info,
                                                           bitview,
                                                           data_type);
            final_qr.merge(sub_qr);
        } else {
            auto sub_qr = BruteForceSearch(query_dataset,
                                           raw_dataset,
                                           raw_id_map,
                                           search_info,
                                           index_info,
                                           bitview,
                                           data_type,
                                           element_type,
                                           op_context);
            final_qr.merge(sub_qr);
        }
        offset += chunk_size;
    }
    if (use_vector_iterator) {
        bool larger_is_closer = PositivelyRelated(search_info.metric_type_);
        result.AssembleChunkVectorIterators(num_queries,
                                            num_chunk,
                                            final_qr.chunk_iterators(),
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
    }
    result.unity_topK_ = query_dataset.topk;
    result.total_nq_ = query_dataset.num_queries;
}

}  // namespace milvus::query
