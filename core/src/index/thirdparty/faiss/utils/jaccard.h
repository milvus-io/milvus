#ifndef FAISS_JACCARD_H
#define FAISS_JACCARD_H

#include <faiss/utils/hamming.h>

#include <stdint.h>

#include <faiss/utils/Heap.h>

/* The Jaccard distance type */
typedef float tadis_t;

namespace faiss {

    extern size_t jaccard_batch_size;

/** Return the k smallest Jaccard distances for a set of binary query vectors,
 * using a max heap.
 * @param a       queries, size ha->nh * ncodes
 * @param b       database, size nb * ncodes
 * @param nb      number of database vectors
 * @param ncodes  size of the binary codes (bytes)
 * @param ordered if != 0: order the results by decreasing distance
 *                (may be bottleneck for k/n > 0.01) */
    void jaccard_knn_hc (
            float_maxheap_array_t * ha,
            const uint8_t * a,
            const uint8_t * b,
            size_t nb,
            size_t ncodes,
            int ordered);

} //namespace faiss

#include <faiss/utils/jaccard-inl.h>

#endif //FAISS_JACCARD_H
