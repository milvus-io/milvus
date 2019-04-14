/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#pragma once

#include "HNSW.h"
#include "IndexBinaryFlat.h"
#include "utils.h"


namespace faiss {


/** The HNSW index is a normal random-access index with a HNSW
 * link structure built on top */

struct IndexBinaryHNSW : IndexBinary {
  typedef HNSW::storage_idx_t storage_idx_t;

  // the link strcuture
  HNSW hnsw;

  // the sequential storage
  bool own_fields;
  IndexBinary *storage;

  explicit IndexBinaryHNSW();
  explicit IndexBinaryHNSW(int d, int M = 32);
  explicit IndexBinaryHNSW(IndexBinary *storage, int M = 32);

  ~IndexBinaryHNSW() override;

  DistanceComputer *get_distance_computer() const;

  void add(idx_t n, const uint8_t *x) override;

  /// Trains the storage if needed
  void train(idx_t n, const uint8_t* x) override;

  /// entry point for search
  void search(idx_t n, const uint8_t *x, idx_t k,
              int32_t *distances, idx_t *labels) const override;

  void reconstruct(idx_t key, uint8_t* recons) const override;

  void reset() override;
};


}  // namespace faiss
