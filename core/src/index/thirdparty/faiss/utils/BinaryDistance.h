#ifndef FAISS_BINARY_DISTANCE_H
#define FAISS_BINARY_DISTANCE_H

#include "faiss/Index.h"

#include <faiss/utils/hamming.h>

#include <stdint.h>



#include <faiss/utils/Heap.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/utils.h>
#include <faiss/FaissHook.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/distances_avx.h>
#include <faiss/utils/distances_avx512.h>
#include <faiss/utils/hamming.h>

#include <faiss/utils/jaccard-inl.h>
#include <faiss/utils/substructure-inl.h>
#include <faiss/utils/superstructure-inl.h>

/* The binary distance type */
typedef float tadis_t;

namespace faiss {



 /** Return the k matched distances for a set of binary query vectors,
  * using a max heap.
  * @param a       queries, size ha->nh * ncodes
  * @param b       database, size nb * ncodes
  * @param na      number of queries vectors
  * @param nb      number of database vectors
  * @param k       number of the matched vectors to return
  * @param ncodes  size of the binary codes (bytes)
 */
    void binary_distence_knn_mc (
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


    template <class T1>
    void binary_distence_knn_hc (
            MetricType metric_type,
            T1 * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            const BitsetView& bitset);


    extern template
    void binary_distence_knn_hc<int_maxheap_array_t>(
            MetricType metric_type,
            int_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            const BitsetView& bitset);

    extern template
    void binary_distence_knn_hc<float_maxheap_array_t>(
            MetricType metric_type,
            float_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            const BitsetView& bitset);

} // namespace faiss



#endif // FAISS_BINARY_DISTANCE_H
