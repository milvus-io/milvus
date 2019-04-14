/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "IndexBinaryFromFloat.h"

#include <memory>
#include "utils.h"

namespace faiss {


IndexBinaryFromFloat::IndexBinaryFromFloat() {}

IndexBinaryFromFloat::IndexBinaryFromFloat(Index *index)
    : IndexBinary(index->d),
      index(index),
      own_fields(false) {
  is_trained = index->is_trained;
  ntotal = index->ntotal;
}

IndexBinaryFromFloat::~IndexBinaryFromFloat() {
  if (own_fields) {
    delete index;
  }
}

void IndexBinaryFromFloat::add(idx_t n, const uint8_t *x) {
  constexpr idx_t bs = 32768;
  std::unique_ptr<float[]> xf(new float[bs * d]);

  for (idx_t b = 0; b < n; b += bs) {
    idx_t bn = std::min(bs, n - b);
    binary_to_real(bn * d, x + b * code_size, xf.get());

    index->add(bn, xf.get());
  }
  ntotal = index->ntotal;
}

void IndexBinaryFromFloat::reset() {
  index->reset();
  ntotal = index->ntotal;
}

void IndexBinaryFromFloat::search(idx_t n, const uint8_t *x, idx_t k,
                                  int32_t *distances, idx_t *labels) const {
  constexpr idx_t bs = 32768;
  std::unique_ptr<float[]> xf(new float[bs * d]);
  std::unique_ptr<float[]> df(new float[bs * k]);

  for (idx_t b = 0; b < n; b += bs) {
    idx_t bn = std::min(bs, n - b);
    binary_to_real(bn * d, x + b * code_size, xf.get());

    index->search(bn, xf.get(), k, df.get(), labels + b * k);
    for (int i = 0; i < bn * k; ++i) {
      distances[b * k + i] = int32_t(std::round(df[i] / 4.0));
    }
  }
}

void IndexBinaryFromFloat::train(idx_t n, const uint8_t *x) {
  std::unique_ptr<float[]> xf(new float[n * d]);
  binary_to_real(n * d, x, xf.get());

  index->train(n, xf.get());
  is_trained = true;
  ntotal = index->ntotal;
}

}  // namespace faiss
