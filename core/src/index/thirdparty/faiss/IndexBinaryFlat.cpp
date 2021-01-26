/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/Index.h>
#include <faiss/IndexBinary.h>
#include <faiss/IndexBinaryFlat.h>

#include <cmath>
#include <cstring>
#include <faiss/utils/BinaryDistance.h>
#include <faiss/utils/hamming.h>
#include <faiss/utils/utils.h>
#include <faiss/utils/Heap.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>

namespace faiss {

IndexBinaryFlat::IndexBinaryFlat(idx_t d)
    : IndexBinary(d) {}

IndexBinaryFlat::IndexBinaryFlat(idx_t d, MetricType metric)
    : IndexBinary(d, metric) {}

void IndexBinaryFlat::add(idx_t n, const uint8_t *x) {
  xb.insert(xb.end(), x, x + n * code_size);
  ntotal += n;
}

void IndexBinaryFlat::reset() {
  xb.clear();
  ntotal = 0;
}

void IndexBinaryFlat::search(idx_t n, const uint8_t *x, idx_t k,
                             int32_t *distances, idx_t *labels,
                             const BitsetView& bitset) const {

    float *D = reinterpret_cast<float*>(distances);

    if (metric_type == METRIC_Jaccard || metric_type == METRIC_Tanimoto) {
        float_maxheap_array_t res = {
            size_t(n), size_t(k), labels, D
        };
        binary_distance_knn_hc(METRIC_Jaccard, &res, x, xb.data(), ntotal, code_size, bitset);

        if (metric_type == METRIC_Tanimoto) {
            for (int i = 0; i < k * n; i++) {
                D[i] = Jaccard_2_Tanimoto(D[i]);
            }
        }

    } else if (metric_type == METRIC_Hamming) {
        int_maxheap_array_t res = {
            size_t(n), size_t(k), labels, distances
        };
        binary_distance_knn_hc(METRIC_Hamming, &res, x, xb.data(), ntotal, code_size, bitset);

    } else if (metric_type == METRIC_Substructure || metric_type == METRIC_Superstructure) {
        const idx_t block_size = query_batch_size;
        for (idx_t s = 0; s < n; s += block_size) {
            idx_t nn = block_size;
            if (s + block_size > n) {
                nn = n - s;
            }

            // only match ids will be chosen, not to use heap
            binary_distance_knn_mc(metric_type, x + s * code_size, xb.data(), nn, ntotal, k, code_size,
                    D + s * k, labels + s * k, bitset);
        }

    } else {

    }
}

size_t IndexBinaryFlat::remove_ids(const IDSelector& sel) {
  idx_t j = 0;
  for (idx_t i = 0; i < ntotal; i++) {
    if (sel.is_member(i)) {
      // should be removed
    } else {
      if (i > j) {
        memmove(&xb[code_size * j], &xb[code_size * i], sizeof(xb[0]) * code_size);
      }
      j++;
    }
  }
  long nremove = ntotal - j;
  if (nremove > 0) {
    ntotal = j;
    xb.resize(ntotal * code_size);
  }
  return nremove;
}

void IndexBinaryFlat::reconstruct(idx_t key, uint8_t *recons) const {
  memcpy(recons, &(xb[code_size * key]), sizeof(*recons) * code_size);
}

void IndexBinaryFlat::range_search(idx_t n, const uint8_t *x, int radius,
                                   RangeSearchResult *result,
                                   const BitsetView& bitset) const
{
    FAISS_THROW_MSG("This interface is abandoned yet.");
}

void IndexBinaryFlat::range_search(faiss::IndexBinary::idx_t n,
                                   const uint8_t* x,
                                   int radius,
                                   std::vector<faiss::RangeSearchPartialResult*>& result,
                                   size_t buffer_size,
                                   const faiss::BitsetView& bitset)
{
    hamming_range_search (x, xb.data(), n, ntotal, radius, code_size, result, buffer_size, bitset);
}

}  // namespace faiss
