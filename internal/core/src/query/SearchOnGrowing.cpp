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

#include <cstddef>
#include "common/BitsetView.h"
#include "common/QueryInfo.h"
#include "SearchOnGrowing.h"
#include "query/SearchBruteForce.h"
#include "query/SearchOnIndex.h"

namespace milvus::query {

void
FloatSegmentIndexSearch(const segcore::SegmentGrowingImpl& segment,
                        const SearchInfo& info,
                        const void* query_data,
                        int64_t num_queries,
                        int64_t ins_barrier,
                        const BitsetView& bitset,
                        SubSearchResult& results) {
    auto& schema = segment.get_schema();
    auto& indexing_record = segment.get_indexing_record();
    auto& record = segment.get_insert_record();

    auto vecfield_id = info.field_id_;
    auto& field = schema[vecfield_id];

    AssertInfo(field.get_data_type() == DataType::VECTOR_FLOAT,
               "[FloatSearch]Field data type isn't VECTOR_FLOAT");
    dataset::SearchDataset search_dataset{info.metric_type_,
                                          num_queries,
                                          info.topk_,
                                          info.round_decimal_,
                                          field.get_dim(),
                                          query_data};
    if (indexing_record.is_in(vecfield_id)) {
        const auto& field_indexing =
            indexing_record.get_vec_field_indexing(vecfield_id);

        auto indexing = field_indexing.get_segment_indexing();
        SearchInfo search_conf = field_indexing.get_search_params(info);
        auto vec_index = dynamic_cast<index::VectorIndex*>(indexing);
        auto result =
            SearchOnIndex(search_dataset, *vec_index, search_conf, bitset);
        results.merge(result);
    }
}

void
SearchOnGrowing(const segcore::SegmentGrowingImpl& segment,
                const SearchInfo& info,
                const void* query_data,
                int64_t num_queries,
                Timestamp timestamp,
                const BitsetView& bitset,
                SearchResult& results) {
    auto& schema = segment.get_schema();
    auto& record = segment.get_insert_record();
    auto active_count =
        std::min(int64_t(bitset.size()), segment.get_active_count(timestamp));

    // step 1.1: get meta
    // step 1.2: get which vector field to search
    auto vecfield_id = info.field_id_;
    auto& field = schema[vecfield_id];
    CheckBruteForceSearchParam(field, info);

    auto data_type = field.get_data_type();
    AssertInfo(datatype_is_vector(data_type),
               "[SearchOnGrowing]Data type isn't vector type");

    auto dim = field.get_dim();
    auto topk = info.topk_;
    auto metric_type = info.metric_type_;
    auto round_decimal = info.round_decimal_;

    // step 2: small indexing search
    SubSearchResult final_qr(num_queries, topk, metric_type, round_decimal);
    dataset::SearchDataset search_dataset{
        metric_type, num_queries, topk, round_decimal, dim, query_data};

    if (segment.get_indexing_record().SyncDataWithIndex(field.get_id())) {
        FloatSegmentIndexSearch(segment,
                                info,
                                query_data,
                                num_queries,
                                active_count,
                                bitset,
                                final_qr);
        results.distances_ = std::move(final_qr.mutable_distances());
        results.seg_offsets_ = std::move(final_qr.mutable_seg_offsets());
        results.unity_topK_ = topk;
        results.total_nq_ = num_queries;
    } else {
        std::shared_lock<std::shared_mutex> read_chunk_mutex(
            segment.get_chunk_mutex());
        int32_t current_chunk_id = 0;
        // step 3: brute force search where small indexing is unavailable
        auto vec_ptr = record.get_field_data_base(vecfield_id);
        auto vec_size_per_chunk = vec_ptr->get_size_per_chunk();
        auto max_chunk = upper_div(active_count, vec_size_per_chunk);

        for (int chunk_id = current_chunk_id; chunk_id < max_chunk;
             ++chunk_id) {
            auto chunk_data = vec_ptr->get_chunk_data(chunk_id);

            auto element_begin = chunk_id * vec_size_per_chunk;
            auto element_end =
                std::min(active_count, (chunk_id + 1) * vec_size_per_chunk);
            auto size_per_chunk = element_end - element_begin;

            auto sub_view = bitset.subview(element_begin, size_per_chunk);
            auto sub_qr = BruteForceSearch(search_dataset,
                                           chunk_data,
                                           size_per_chunk,
                                           info.search_params_,
                                           sub_view);

            // convert chunk uid to segment uid
            for (auto& x : sub_qr.mutable_seg_offsets()) {
                if (x != -1) {
                    x += chunk_id * vec_size_per_chunk;
                }
            }
            final_qr.merge(sub_qr);
        }
        results.distances_ = std::move(final_qr.mutable_distances());
        results.seg_offsets_ = std::move(final_qr.mutable_seg_offsets());
        results.unity_topK_ = topk;
        results.total_nq_ = num_queries;
    }
}

}  // namespace milvus::query
