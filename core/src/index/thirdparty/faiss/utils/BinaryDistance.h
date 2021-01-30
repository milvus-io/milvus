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

#ifndef FAISS_BINARY_DISTANCE_H
#define FAISS_BINARY_DISTANCE_H

#include "faiss/MetricType.h"
#include <stdint.h>
#include <faiss/utils/BitsetView.h>
#include <faiss/utils/Heap.h>

/* The binary distance type */
typedef float tadis_t;

namespace faiss {
    extern int popcnt(
            const uint8_t* data1,
            const size_t n);

    extern int xor_popcnt(
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t n);

    extern int or_popcnt(
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t n);

    extern int and_popcnt(
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t n);

    extern float bvec_jaccard (
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t n);

    inline float Jaccard_2_Tanimoto (float jcd) {
        return -log2(1 - jcd);
    }

 /** Return the k matched distances for a set of binary query vectors,
  * using an array.
  * @param a       queries, size ha->nh * ncodes
  * @param b       database, size nb * ncodes
  * @param na      number of queries vectors
  * @param nb      number of database vectors
  * @param k       number of the matched vectors to return
  * @param ncodes  size of the binary codes (bytes)
 */
    void binary_distance_knn_mc (
            MetricType metric_type,
            const uint8_t * a,
            const uint8_t * b,
            size_t na,
            size_t nb,
            size_t k,
            size_t ncodes,
            float *distances,
            int64_t *labels,
            const BitsetView& bitset);

/** Return the k smallest distances for a set of binary query vectors,
 * using a heap.
 * @param a       queries, size ha->nh * ncodes
 * @param b       database, size nb * ncodes
 * @param nb      number of database vectors
 * @param ncodes  size of the binary codes (bytes)
 * @param ordered if != 0: order the results by decreasing distance
 *                (may be bottleneck for k/n > 0.01)
 */
    template <class C>
    void binary_distance_knn_hc (
            MetricType metric_type,
            HeapArray<C> * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            const BitsetView& bitset);


    extern template
    void binary_distance_knn_hc<CMax<int, int64_t>>(
            MetricType metric_type,
            int_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            const BitsetView& bitset);

    extern template
    void binary_distance_knn_hc<CMax<float, int64_t>>(
            MetricType metric_type,
            float_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            const BitsetView& bitset);

    template <class C>
    void binary_range_search(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        C::T radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView& bitset);

    extern template
    void binary_range_search<CMax<int, int64_t>>(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        int radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView& bitset);

    extern template
    void binary_range_search<CMax<float, int64_t>>(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        float radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView& bitset);

    extern template
    void binary_range_search<CMax<bool, int64_t>>(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        bool radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView& bitset);

} // namespace faiss



#endif // FAISS_BINARY_DISTANCE_H
