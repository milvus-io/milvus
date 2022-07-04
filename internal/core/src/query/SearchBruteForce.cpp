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

#include <string>
#include <vector>

#include <faiss/utils/BinaryDistance.h>
#include <faiss/utils/distances.h>

#include "SearchBruteForce.h"
#include "SubSearchResult.h"
#include "common/Types.h"
#include "segcore/Utils.h"

namespace milvus::query {

// copy from faiss/IndexBinaryFlat.cpp::IndexBinaryFlat::search()
// disable lint to make further migration easier
static void
binary_search(MetricType metric_type,
              const uint8_t* xb,
              int64_t ntotal,
              int code_size,
              idx_t n,  // num_queries
              const uint8_t* x,
              idx_t k,  // topk
              float* D,
              idx_t* labels,
              const BitsetView bitset) {
    using namespace faiss;  // NOLINT
    if (metric_type == METRIC_Jaccard || metric_type == METRIC_Tanimoto) {
        float_maxheap_array_t res = {size_t(n), size_t(k), labels, D};
        binary_distance_knn_hc(METRIC_Jaccard, &res, x, xb, ntotal, code_size, bitset);

        if (metric_type == METRIC_Tanimoto) {
            for (int i = 0; i < k * n; i++) {
                D[i] = Jaccard_2_Tanimoto(D[i]);
            }
        }
    } else if (metric_type == METRIC_Hamming) {
        std::vector<int32_t> int_distances(n * k);
        int_maxheap_array_t res = {size_t(n), size_t(k), labels, int_distances.data()};
        binary_distance_knn_hc(METRIC_Hamming, &res, x, xb, ntotal, code_size, bitset);
        for (int i = 0; i < n * k; ++i) {
            D[i] = int_distances[i];
        }
    } else if (metric_type == METRIC_Substructure || metric_type == METRIC_Superstructure) {
        // only matched ids will be chosen, not to use heap
        binary_distance_knn_mc(metric_type, x, xb, n, ntotal, k, code_size, D, labels, bitset);
    } else {
        std::string msg =
            std::string("binary search not support metric type: ") + segcore::MetricTypeToString(metric_type);
        PanicInfo(msg);
    }
}

SubSearchResult
BinarySearchBruteForce(const dataset::SearchDataset& dataset,
                       const void* chunk_data_raw,
                       int64_t size_per_chunk,
                       const BitsetView& bitset) {
    // TODO: refactor the internal function
    auto metric_type = dataset.metric_type;
    auto num_queries = dataset.num_queries;
    auto topk = dataset.topk;
    auto dim = dataset.dim;
    auto round_decimal = dataset.round_decimal;
    SubSearchResult sub_result(num_queries, topk, metric_type, round_decimal);
    auto query_data = reinterpret_cast<const uint8_t*>(dataset.query_data);
    auto chunk_data = reinterpret_cast<const uint8_t*>(chunk_data_raw);

    int64_t code_size = dim / 8;
    binary_search(metric_type, chunk_data, size_per_chunk, code_size, num_queries, query_data, topk,
                  sub_result.get_distances(), sub_result.get_seg_offsets(), bitset);
    sub_result.round_values();
    return sub_result;
}

SubSearchResult
FloatSearchBruteForce(const dataset::SearchDataset& dataset,
                      const void* chunk_data_raw,
                      int64_t size_per_chunk,
                      const BitsetView& bitset) {
    auto metric_type = dataset.metric_type;
    auto num_queries = dataset.num_queries;
    auto topk = dataset.topk;
    auto dim = dataset.dim;
    auto round_decimal = dataset.round_decimal;
    SubSearchResult sub_qr(num_queries, topk, metric_type, round_decimal);
    auto query_data = reinterpret_cast<const float*>(dataset.query_data);
    auto chunk_data = reinterpret_cast<const float*>(chunk_data_raw);
    if (metric_type == MetricType::METRIC_L2) {
        faiss::float_maxheap_array_t buf{(size_t)num_queries, (size_t)topk, sub_qr.get_seg_offsets(),
                                         sub_qr.get_distances()};
        faiss::knn_L2sqr(query_data, chunk_data, dim, num_queries, size_per_chunk, &buf, nullptr, bitset);
    } else if (metric_type == MetricType::METRIC_INNER_PRODUCT) {
        faiss::float_minheap_array_t buf{(size_t)num_queries, (size_t)topk, sub_qr.get_seg_offsets(),
                                         sub_qr.get_distances()};
        faiss::knn_inner_product(query_data, chunk_data, dim, num_queries, size_per_chunk, &buf, bitset);
    } else {
        std::string msg = "search not support metric type: " + MetricTypeToName(metric_type);
        PanicInfo(msg);
    }
    sub_qr.round_values();
    return sub_qr;
}

}  // namespace milvus::query
