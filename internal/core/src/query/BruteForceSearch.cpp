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

#include "BruteForceSearch.h"
#include <vector>
#include <common/Types.h>
#include <boost/dynamic_bitset.hpp>
#include <queue>

namespace milvus::query {

void
BinarySearchBruteForceNaive(MetricType metric_type,
                            int64_t code_size,
                            const uint8_t* binary_chunk,
                            int64_t chunk_size,
                            int64_t topk,
                            int64_t num_queries,
                            const uint8_t* query_data,
                            float* result_distances,
                            idx_t* result_labels,
                            faiss::ConcurrentBitsetPtr bitset) {
    // THIS IS A NAIVE IMPLEMENTATION, ready for optimize
    Assert(metric_type == faiss::METRIC_Jaccard);
    Assert(code_size % 4 == 0);

    using T = std::tuple<float, int>;

    for (int64_t q = 0; q < num_queries; ++q) {
        auto query_ptr = query_data + code_size * q;
        auto query = boost::dynamic_bitset(query_ptr, query_ptr + code_size);
        std::vector<T> max_heap(topk + 1, std::make_tuple(std::numeric_limits<float>::max(), -1));

        for (int64_t i = 0; i < chunk_size; ++i) {
            auto element_ptr = binary_chunk + code_size * i;
            auto element = boost::dynamic_bitset(element_ptr, element_ptr + code_size);
            auto the_and = (query & element).count();
            auto the_or = (query | element).count();
            auto distance = the_or ? (float)(the_or - the_and) / the_or : 0;
            if (distance < std::get<0>(max_heap[0])) {
                max_heap[topk] = std::make_tuple(distance, i);
                std::push_heap(max_heap.begin(), max_heap.end());
                std::pop_heap(max_heap.begin(), max_heap.end());
            }
        }
        std::sort(max_heap.begin(), max_heap.end());
        for (int k = 0; k < topk; ++k) {
            auto info = max_heap[k];
            result_distances[k + q * topk] = std::get<0>(info);
            result_labels[k + q * topk] = std::get<1>(info);
        }
    }
}

void
BinarySearchBruteForceFast(MetricType metric_type,
                           int64_t code_size,
                           const uint8_t* binary_chunk,
                           int64_t chunk_size,
                           int64_t topk,
                           int64_t num_queries,
                           const uint8_t* query_data,
                           float* result_distances,
                           idx_t* result_labels,
                           faiss::ConcurrentBitsetPtr bitset) {
    const idx_t block_size = chunk_size;
    bool use_heap = true;

    if (metric_type == faiss::METRIC_Jaccard || metric_type == faiss::METRIC_Tanimoto) {
        float* D = result_distances;
        for (idx_t query_base_index = 0; query_base_index < num_queries; query_base_index += block_size) {
            idx_t query_size = block_size;
            if (query_base_index + block_size > num_queries) {
                query_size = num_queries - query_base_index;
            }

            // We see the distances and labels as heaps.
            faiss::float_maxheap_array_t res = {size_t(query_size), size_t(topk),
                                                result_labels + query_base_index * topk, D + query_base_index * topk};

            binary_distence_knn_hc(metric_type, &res, query_data + query_base_index * code_size, binary_chunk,
                                   chunk_size, code_size,
                                   /* ordered = */ true, bitset);
        }
        if (metric_type == faiss::METRIC_Tanimoto) {
            for (int i = 0; i < topk * num_queries; i++) {
                D[i] = -log2(1 - D[i]);
            }
        }
    } else if (metric_type == faiss::METRIC_Substructure || metric_type == faiss::METRIC_Superstructure) {
        float* D = result_distances;
        for (idx_t s = 0; s < num_queries; s += block_size) {
            idx_t nn = block_size;
            if (s + block_size > num_queries) {
                nn = num_queries - s;
            }

            // only match ids will be chosed, not to use heap
            binary_distence_knn_mc(metric_type, query_data + s * code_size, binary_chunk, nn, chunk_size, topk,
                                   code_size, D + s * topk, result_labels + s * topk, bitset);
        }
    } else if (metric_type == faiss::METRIC_Hamming) {
        std::vector<int> int_distances(topk * num_queries);
        for (idx_t s = 0; s < num_queries; s += block_size) {
            idx_t nn = block_size;
            if (s + block_size > num_queries) {
                nn = num_queries - s;
            }
            if (use_heap) {
                // We see the distances and labels as heaps.
                faiss::int_maxheap_array_t res = {size_t(nn), size_t(topk), result_labels + s * topk,
                                                  int_distances.data() + s * topk};

                hammings_knn_hc(&res, query_data + s * code_size, binary_chunk, chunk_size, code_size,
                                /* ordered = */ true, bitset);
            } else {
                hammings_knn_mc(query_data + s * code_size, binary_chunk, nn, chunk_size, topk, code_size,
                                int_distances.data() + s * topk, result_labels + s * topk, bitset);
            }
        }
        for (int i = 0; i < num_queries; ++i) {
            result_distances[i] = static_cast<float>(int_distances[i]);
        }
    } else {
        PanicInfo("Unsupported metric type");
    }
}

void
BinarySearchBruteForce(const dataset::BinaryQueryDataset& query_dataset,
                       const uint8_t* binary_chunk,
                       int64_t chunk_size,
                       float* result_distances,
                       idx_t* result_labels,
                       faiss::ConcurrentBitsetPtr bitset) {
    // TODO: refactor the internal function
    BinarySearchBruteForceFast(query_dataset.metric_type, query_dataset.code_size, binary_chunk, chunk_size,
                               query_dataset.topk, query_dataset.num_queries, query_dataset.query_data,
                               result_distances, result_labels, bitset);
}
}  // namespace milvus::query
