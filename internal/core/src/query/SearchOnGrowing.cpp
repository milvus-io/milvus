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

#include "SearchOnGrowing.h"
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include "segcore/Reduce.h"

#include <faiss/utils/distances.h>
#include "utils/tools.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnIndex.h"

namespace milvus::query {
Status
FloatSearch(const segcore::SegmentGrowingImpl& segment,
            const query::SearchInfo& info,
            const float* query_data,
            int64_t num_queries,
            int64_t ins_barrier,
            const BitsetView& bitset,
            SearchResult& results) {
    auto& schema = segment.get_schema();
    auto& indexing_record = segment.get_indexing_record();
    auto& record = segment.get_insert_record();
    // step 1: binary search to find the barrier of the snapshot
    // auto del_barrier = get_barrier(deleted_record_, timestamp);

#if 0
    auto bitmap_holder = get_deleted_bitmap(del_barrier, timestamp, ins_barrier);
    Assert(bitmap_holder);
    auto bitmap = bitmap_holder->bitmap_ptr;
#endif

    // step 2.1: get meta
    // step 2.2: get which vector field to search
    auto vecfield_offset = info.field_offset_;
    auto& field = schema[vecfield_offset];

    AssertInfo(field.get_data_type() == DataType::VECTOR_FLOAT, "[FloatSearch]Field data type isn't VECTOR_FLOAT");
    auto dim = field.get_dim();
    auto topk = info.topk_;
    auto total_count = topk * num_queries;
    auto metric_type = info.metric_type_;
    auto round_decimal = info.round_decimal_;
    // step 3: small indexing search
    // std::vector<int64_t> final_uids(total_count, -1);
    // std::vector<float> final_dis(total_count, std::numeric_limits<float>::max());
    SubSearchResult final_qr(num_queries, topk, metric_type, round_decimal);
    dataset::SearchDataset search_dataset{metric_type, num_queries, topk, round_decimal, dim, query_data};
    auto vec_ptr = record.get_field_data<FloatVector>(vecfield_offset);

    int current_chunk_id = 0;

    if (indexing_record.is_in(vecfield_offset)) {
        auto max_indexed_id = indexing_record.get_finished_ack();
        const auto& field_indexing = indexing_record.get_vec_field_indexing(vecfield_offset);
        auto search_conf = field_indexing.get_search_params(topk);
        AssertInfo(vec_ptr->get_size_per_chunk() == field_indexing.get_size_per_chunk(),
                   "[FloatSearch]Chunk size of vector not equal to chunk size of field index");

        for (int chunk_id = current_chunk_id; chunk_id < max_indexed_id; ++chunk_id) {
            auto size_per_chunk = field_indexing.get_size_per_chunk();
            auto indexing = field_indexing.get_chunk_indexing(chunk_id);

            auto sub_view = BitsetSubView(bitset, chunk_id * size_per_chunk, size_per_chunk);
            auto sub_qr = SearchOnIndex(search_dataset, *indexing, search_conf, sub_view);

            // convert chunk uid to segment uid
            for (auto& x : sub_qr.mutable_labels()) {
                if (x != -1) {
                    x += chunk_id * size_per_chunk;
                }
            }

            final_qr.merge(sub_qr);
        }
        current_chunk_id = max_indexed_id;
    }

    // step 4: brute force search where small indexing is unavailable
    auto vec_size_per_chunk = vec_ptr->get_size_per_chunk();
    auto max_chunk = upper_div(ins_barrier, vec_size_per_chunk);

    for (int chunk_id = current_chunk_id; chunk_id < max_chunk; ++chunk_id) {
        auto& chunk = vec_ptr->get_chunk(chunk_id);

        auto element_begin = chunk_id * vec_size_per_chunk;
        auto element_end = std::min(ins_barrier, (chunk_id + 1) * vec_size_per_chunk);
        auto size_per_chunk = element_end - element_begin;

        auto sub_view = BitsetSubView(bitset, element_begin, size_per_chunk);
        auto sub_qr = FloatSearchBruteForce(search_dataset, chunk.data(), size_per_chunk, sub_view);

        // convert chunk uid to segment uid
        for (auto& x : sub_qr.mutable_labels()) {
            if (x != -1) {
                x += chunk_id * vec_size_per_chunk;
            }
        }
        final_qr.merge(sub_qr);
    }
    current_chunk_id = max_chunk;
    results.result_distances_ = std::move(final_qr.mutable_values());
    results.internal_seg_offsets_ = std::move(final_qr.mutable_labels());
    results.topk_ = topk;
    results.num_queries_ = num_queries;

    return Status::OK();
}

Status
BinarySearch(const segcore::SegmentGrowingImpl& segment,
             const query::SearchInfo& info,
             const uint8_t* query_data,
             int64_t num_queries,
             int64_t ins_barrier,
             const faiss::BitsetView& bitset,
             SearchResult& results) {
    auto& schema = segment.get_schema();
    auto& indexing_record = segment.get_indexing_record();
    auto& record = segment.get_insert_record();
    // step 1: binary search to find the barrier of the snapshot
    // auto ins_barrier = get_barrier(record, timestamp);
    auto metric_type = info.metric_type_;
    // auto del_barrier = get_barrier(deleted_record_, timestamp);

#if 0
    auto bitmap_holder = get_deleted_bitmap(del_barrier, timestamp, ins_barrier);
    Assert(bitmap_holder);
    auto bitmap = bitmap_holder->bitmap_ptr;
#endif

    // step 2.1: get meta
    // step 2.2: get which vector field to search
    auto vecfield_offset = info.field_offset_;
    auto& field = schema[vecfield_offset];

    AssertInfo(field.get_data_type() == DataType::VECTOR_BINARY, "[BinarySearch]Field data type isn't VECTOR_BINARY");
    auto dim = field.get_dim();
    auto topk = info.topk_;
    auto total_count = topk * num_queries;
    auto round_decimal = info.round_decimal_;
    // step 3: small indexing search
    query::dataset::SearchDataset search_dataset{metric_type, num_queries, topk, round_decimal, dim, query_data};

    auto vec_ptr = record.get_field_data<BinaryVector>(vecfield_offset);

    auto max_indexed_id = 0;
    // step 4: brute force search where small indexing is unavailable

    auto vec_size_per_chunk = vec_ptr->get_size_per_chunk();
    auto max_chunk = upper_div(ins_barrier, vec_size_per_chunk);
    SubSearchResult final_result(num_queries, topk, metric_type, round_decimal);
    for (int chunk_id = max_indexed_id; chunk_id < max_chunk; ++chunk_id) {
        auto& chunk = vec_ptr->get_chunk(chunk_id);
        auto element_begin = chunk_id * vec_size_per_chunk;
        auto element_end = std::min(ins_barrier, (chunk_id + 1) * vec_size_per_chunk);
        auto nsize = element_end - element_begin;

        auto sub_view = BitsetSubView(bitset, element_begin, nsize);
        auto sub_result = BinarySearchBruteForce(search_dataset, chunk.data(), nsize, sub_view);

        // convert chunk uid to segment uid
        for (auto& x : sub_result.mutable_labels()) {
            if (x != -1) {
                x += chunk_id * vec_size_per_chunk;
            }
        }
        final_result.merge(sub_result);
    }

    final_result.round_values();
    results.result_distances_ = std::move(final_result.mutable_values());
    results.internal_seg_offsets_ = std::move(final_result.mutable_labels());
    results.topk_ = topk;
    results.num_queries_ = num_queries;

    return Status::OK();
}

// TODO: refactor and merge this into one
void
SearchOnGrowing(const segcore::SegmentGrowingImpl& segment,
                int64_t ins_barrier,
                const query::SearchInfo& info,
                const void* query_data,
                int64_t num_queries,
                const faiss::BitsetView& bitset,
                SearchResult& results) {
    // TODO: add data_type to info
    auto data_type = segment.get_schema()[info.field_offset_].get_data_type();
    AssertInfo(datatype_is_vector(data_type), "[SearchOnGrowing]Data type isn't vector type");
    if (data_type == DataType::VECTOR_FLOAT) {
        auto typed_data = reinterpret_cast<const float*>(query_data);
        FloatSearch(segment, info, typed_data, num_queries, ins_barrier, bitset, results);
    } else {
        auto typed_data = reinterpret_cast<const uint8_t*>(query_data);
        BinarySearch(segment, info, typed_data, num_queries, ins_barrier, bitset, results);
    }
}

}  // namespace milvus::query
