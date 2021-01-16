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

static faiss::ConcurrentBitsetPtr
create_bitmap_view(std::optional<const BitmapSimple*> bitmaps_opt, int64_t chunk_id) {
    if (!bitmaps_opt.has_value()) {
        return nullptr;
    }
    auto& bitmaps = *bitmaps_opt.value();
    auto src_vec = ~bitmaps.at(chunk_id);
    auto dst = std::make_shared<faiss::ConcurrentBitset>(src_vec.size());
    auto iter = reinterpret_cast<BitmapChunk::block_type*>(dst->mutable_data());
    boost::to_block_range(src_vec, iter);
    return dst;
}

Status
FloatSearch(const segcore::SegmentGrowingImpl& segment,
            const query::QueryInfo& info,
            const float* query_data,
            int64_t num_queries,
            Timestamp timestamp,
            const BitsetView& bitset,
            QueryResult& results) {
    auto& schema = segment.get_schema();
    auto& indexing_record = segment.get_indexing_record();
    auto& record = segment.get_insert_record();
    // step 1: binary search to find the barrier of the snapshot
    auto ins_barrier = get_barrier(record, timestamp);
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

    Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();
    auto topK = info.topK_;
    auto total_count = topK * num_queries;
    auto metric_type = GetMetricType(info.metric_type_);

    // step 3: small indexing search
    // std::vector<int64_t> final_uids(total_count, -1);
    // std::vector<float> final_dis(total_count, std::numeric_limits<float>::max());
    SubQueryResult final_qr(num_queries, topK, metric_type);
    dataset::FloatQueryDataset query_dataset{metric_type, num_queries, topK, dim, query_data};

    auto max_indexed_id = indexing_record.get_finished_ack();
    const auto& indexing_entry = indexing_record.get_vec_entry(vecfield_offset);
    auto search_conf = indexing_entry.get_search_conf(topK);

    for (int chunk_id = 0; chunk_id < max_indexed_id; ++chunk_id) {
        auto chunk_size = indexing_entry.get_chunk_size();
        auto indexing = indexing_entry.get_indexing(chunk_id);

        auto sub_view = BitsetSubView(bitset, chunk_id * chunk_size, chunk_size);
        auto sub_qr = SearchOnIndex(query_dataset, *indexing, search_conf, sub_view);

        // convert chunk uid to segment uid
        for (auto& x : sub_qr.mutable_labels()) {
            if (x != -1) {
                x += chunk_id * chunk_size;
            }
        }

        final_qr.merge(sub_qr);
    }
    auto vec_ptr = record.get_entity<FloatVector>(vecfield_offset);

    // step 4: brute force search where small indexing is unavailable
    auto vec_chunk_size = vec_ptr->get_chunk_size();
    Assert(vec_chunk_size == indexing_entry.get_chunk_size());
    auto max_chunk = upper_div(ins_barrier, vec_chunk_size);

    for (int chunk_id = max_indexed_id; chunk_id < max_chunk; ++chunk_id) {
        auto& chunk = vec_ptr->get_chunk(chunk_id);

        auto element_begin = chunk_id * vec_chunk_size;
        auto element_end = std::min(ins_barrier, (chunk_id + 1) * vec_chunk_size);
        auto chunk_size = element_end - element_begin;

        auto sub_view = BitsetSubView(bitset, element_begin, chunk_size);
        auto sub_qr = FloatSearchBruteForce(query_dataset, chunk.data(), chunk_size, sub_view);

        // convert chunk uid to segment uid
        for (auto& x : sub_qr.mutable_labels()) {
            if (x != -1) {
                x += chunk_id * vec_chunk_size;
            }
        }
        final_qr.merge(sub_qr);
    }

    results.result_distances_ = std::move(final_qr.mutable_values());
    results.internal_seg_offsets_ = std::move(final_qr.mutable_labels());
    results.topK_ = topK;
    results.num_queries_ = num_queries;

    return Status::OK();
}

Status
BinarySearch(const segcore::SegmentGrowingImpl& segment,
             const query::QueryInfo& info,
             const uint8_t* query_data,
             int64_t num_queries,
             Timestamp timestamp,
             const faiss::BitsetView& bitset,
             QueryResult& results) {
    auto& schema = segment.get_schema();
    auto& indexing_record = segment.get_indexing_record();
    auto& record = segment.get_insert_record();
    // step 1: binary search to find the barrier of the snapshot
    auto ins_barrier = get_barrier(record, timestamp);
    auto metric_type = GetMetricType(info.metric_type_);
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

    Assert(field.get_data_type() == DataType::VECTOR_BINARY);
    auto dim = field.get_dim();
    auto topK = info.topK_;
    auto total_count = topK * num_queries;

    // step 3: small indexing search
    query::dataset::BinaryQueryDataset query_dataset{metric_type, num_queries, topK, dim, query_data};

    auto vec_ptr = record.get_entity<BinaryVector>(vecfield_offset);

    auto max_indexed_id = 0;
    // step 4: brute force search where small indexing is unavailable

    auto vec_chunk_size = vec_ptr->get_chunk_size();
    auto max_chunk = upper_div(ins_barrier, vec_chunk_size);
    SubQueryResult final_result(num_queries, topK, metric_type);
    for (int chunk_id = max_indexed_id; chunk_id < max_chunk; ++chunk_id) {
        auto& chunk = vec_ptr->get_chunk(chunk_id);
        auto element_begin = chunk_id * vec_chunk_size;
        auto element_end = std::min(ins_barrier, (chunk_id + 1) * vec_chunk_size);
        auto nsize = element_end - element_begin;

        auto sub_view = BitsetSubView(bitset, element_begin, nsize);
        auto sub_result = BinarySearchBruteForce(query_dataset, chunk.data(), nsize, sub_view);

        // convert chunk uid to segment uid
        for (auto& x : sub_result.mutable_labels()) {
            if (x != -1) {
                x += chunk_id * vec_chunk_size;
            }
        }
        final_result.merge(sub_result);
    }

    results.result_distances_ = std::move(final_result.mutable_values());
    results.internal_seg_offsets_ = std::move(final_result.mutable_labels());
    results.topK_ = topK;
    results.num_queries_ = num_queries;

    return Status::OK();
}

// TODO: refactor and merge this into one
template <typename VectorType>
void
SearchOnGrowing(const segcore::SegmentGrowingImpl& segment,
                const query::QueryInfo& info,
                const EmbeddedType<VectorType>* query_data,
                int64_t num_queries,
                Timestamp timestamp,
                const faiss::BitsetView& bitset,
                QueryResult& results) {
    static_assert(IsVector<VectorType>);
    if constexpr (std::is_same_v<VectorType, FloatVector>) {
        FloatSearch(segment, info, query_data, num_queries, timestamp, bitset, results);
    } else {
        BinarySearch(segment, info, query_data, num_queries, timestamp, bitset, results);
    }
}
template void
SearchOnGrowing<FloatVector>(const segcore::SegmentGrowingImpl& segment,
                             const query::QueryInfo& info,
                             const EmbeddedType<FloatVector>* query_data,
                             int64_t num_queries,
                             Timestamp timestamp,
                             const faiss::BitsetView& bitset,
                             QueryResult& results);
template void
SearchOnGrowing<BinaryVector>(const segcore::SegmentGrowingImpl& segment,
                              const query::QueryInfo& info,
                              const EmbeddedType<BinaryVector>* query_data,
                              int64_t num_queries,
                              Timestamp timestamp,
                              const faiss::BitsetView& bitset,
                              QueryResult& results);

}  // namespace milvus::query
