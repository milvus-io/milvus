/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef INDEX_BINARY_FLAT_H
#define INDEX_BINARY_FLAT_H

#include <vector>

#include "IndexBinary.h"

namespace faiss {


/** Index that stores the full vectors and performs exhaustive search. */
struct IndexBinaryFlat : IndexBinary {
  /// database vectors, size ntotal * d / 8
  std::vector<uint8_t> xb;

  /** Select between using a heap or counting to select the k smallest values
   * when scanning inverted lists.
   */
  bool use_heap = true;

  size_t query_batch_size = 32;

  explicit IndexBinaryFlat(idx_t d);

  void add(idx_t n, const uint8_t *x) override;

  void reset() override;

  void search(idx_t n, const uint8_t *x, idx_t k,
              int32_t *distances, idx_t *labels) const override;

  void reconstruct(idx_t key, uint8_t *recons) const override;

  /** Remove some ids. Note that because of the indexing structure,
   * the semantics of this operation are different from the usual ones:
   * the new ids are shifted. */
  long remove_ids(const IDSelector& sel) override;

  IndexBinaryFlat() {}
};


}  // namespace faiss

#endif  // INDEX_BINARY_FLAT_H
