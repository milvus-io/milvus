/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_BINARY_FROM_FLOAT_H
#define FAISS_INDEX_BINARY_FROM_FLOAT_H

#include "IndexBinary.h"


namespace faiss {


struct Index;

/** IndexBinary backed by a float Index.
 *
 * Supports adding vertices and searching them.
 *
 * All queries are symmetric because there is no distinction between codes and
 * vectors.
 */
struct IndexBinaryFromFloat : IndexBinary {
  Index *index = nullptr;

  bool own_fields = false; ///< Whether object owns the index pointer.

  IndexBinaryFromFloat();

  explicit IndexBinaryFromFloat(Index *index);

  ~IndexBinaryFromFloat();

  void add(idx_t n, const uint8_t *x) override;

  void reset() override;

  void search(idx_t n, const uint8_t *x, idx_t k,
              int32_t *distances, idx_t *labels) const override;

  void train(idx_t n, const uint8_t *x) override;
};


}  // namespace faiss

#endif  // FAISS_INDEX_BINARY_FROM_FLOAT_H
