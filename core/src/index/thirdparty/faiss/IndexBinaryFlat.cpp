/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IndexBinaryFlat.h>

#include <cstring>
#include <faiss/utils/hamming.h>
#include <faiss/utils/utils.h>
#include <faiss/utils/Heap.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>

namespace faiss {

IndexBinaryFlat::IndexBinaryFlat(idx_t d)
    : IndexBinary(d) {}

void IndexBinaryFlat::add(idx_t n, const uint8_t *x) {
  xb.insert(xb.end(), x, x + n * code_size);
  ntotal += n;
}

void IndexBinaryFlat::reset() {
  xb.clear();
  ntotal = 0;
}

void IndexBinaryFlat::search(idx_t n, const uint8_t *x, idx_t k,
                             int32_t *distances, idx_t *labels) const {
  const idx_t block_size = query_batch_size;
  for (idx_t s = 0; s < n; s += block_size) {
    idx_t nn = block_size;
    if (s + block_size > n) {
      nn = n - s;
    }

    if (use_heap) {
      // We see the distances and labels as heaps.
      int_maxheap_array_t res = {
        size_t(nn), size_t(k), labels + s * k, distances + s * k
      };

      hammings_knn_hc(&res, x + s * code_size, xb.data(), ntotal, code_size,
                      /* ordered = */ true);
    } else {
      hammings_knn_mc(x + s * code_size, xb.data(), nn, ntotal, k, code_size,
                      distances + s * k, labels + s * k);
    }
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
                   RangeSearchResult *result) const
{
    hamming_range_search (x, xb.data(), n, ntotal, radius, code_size, result);
}

}  // namespace faiss
