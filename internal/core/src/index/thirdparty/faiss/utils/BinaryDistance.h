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
#include <faiss/impl/AuxIndexStructures.h>

/* The binary distance type */
typedef float tadis_t;

namespace faiss {
    /**
     * Calculate the number of bit 1
     */
    extern int popcnt(
            const uint8_t* data,
            const size_t code_size);

    /**
     * Calculate the number of bit 1 after xor
     */
    extern int xor_popcnt(
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t code_size);

    /**
     * Calculate the number of bit 1 after or
     */
    extern int or_popcnt(
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t code_size);

    /**
     * Calculate the number of bit 1 after and
     */
    extern int and_popcnt(
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t code_size);

    /**
     * Judge whether data1 is a subset of data2
     */
    extern bool is_subset(
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t code_size);

    /**
     * Calculate Jaccard distance
     */
    extern float bvec_jaccard (
            const uint8_t* data1,
            const uint8_t* data2,
            const size_t code_size);

    /**
     * Distance conversion between Jaccard and Tanimoto
     */
    inline float Jaccard_2_Tanimoto (float jcd) {
        // To avoid a negative zero in C language
        return (jcd == 0.0) ? 0.0 : -log2(1 - jcd);
    }

    inline float Tanimoto_2_Jaccard (float tnmt) {
        return 1 - pow(2.0, -tnmt);
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
            const BitsetView bitset);

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
            const BitsetView bitset);


    extern template
    void binary_distance_knn_hc<CMax<int, int64_t>>(
            MetricType metric_type,
            int_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            const BitsetView bitset);

    extern template
    void binary_distance_knn_hc<CMax<float, int64_t>>(
            MetricType metric_type,
            float_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            const BitsetView bitset);

    template <class C, typename T>
    void binary_range_search(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        T radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView bitset);

    extern template
    void binary_range_search<CMax<int, int64_t>, int>(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        int radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView bitset);

    extern template
    void binary_range_search<CMax<float, int64_t>, float>(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        float radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView bitset);

    extern template
    void binary_range_search<CMin<bool, int64_t>, bool>(
        MetricType metric_type,
        const uint8_t * a,
        const uint8_t * b,
        size_t na,
        size_t nb,
        bool radius,
        size_t ncodes,
        std::vector<faiss::RangeSearchPartialResult*>& result,
        size_t buffer_size,
        const BitsetView bitset);

} // namespace faiss



#endif // FAISS_BINARY_DISTANCE_H
