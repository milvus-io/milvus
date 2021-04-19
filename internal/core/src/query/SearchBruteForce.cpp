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

#include "SearchBruteForce.h"
#include <vector>
#include <common/Types.h>
#include <boost/dynamic_bitset.hpp>
#include <queue>
#include "SubQueryResult.h"

namespace milvus::query {

SubQueryResult
BinarySearchBruteForceFast(MetricType metric_type,
                           int64_t code_size,
                           const uint8_t* binary_chunk,
                           int64_t chunk_size,
                           int64_t topk,
                           int64_t num_queries,
                           const uint8_t* query_data,
                           const faiss::BitsetView& bitset) {
    SubQueryResult sub_result(num_queries, topk, metric_type);
    float* result_distances = sub_result.get_values();
    idx_t* result_labels = sub_result.get_labels();

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
    return sub_result;
}

void
FloatSearchBruteForceFast(MetricType metric_type,
                          const float* chunk_data,
                          int64_t chunk_size,
                          float* result_distances,
                          idx_t* result_labels,
                          const faiss::BitsetView& bitset) {
    // TODO
}

SubQueryResult
BinarySearchBruteForce(const dataset::BinaryQueryDataset& query_dataset,
                       const uint8_t* binary_chunk,
                       int64_t chunk_size,
                       const faiss::BitsetView& bitset) {
    // TODO: refactor the internal function
    return BinarySearchBruteForceFast(query_dataset.metric_type, query_dataset.code_size, binary_chunk, chunk_size,
                                      query_dataset.topk, query_dataset.num_queries, query_dataset.query_data, bitset);
}
}  // namespace milvus::query
