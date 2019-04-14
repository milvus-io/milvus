/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "../Index.h"
#include "utils/WorkerThread.h"
#include <memory>
#include <vector>

namespace faiss { namespace gpu {

/// Takes individual faiss::Index instances, and splits queries for
/// sending to each Index instance, and joins the results together
/// when done.
/// Each index is managed by a separate CPU thread.
class IndexProxy : public faiss::Index {
 public:
  IndexProxy();
  ~IndexProxy() override;

  /// Adds an index that is managed by ourselves.
  /// WARNING: once an index is added to this proxy, it becomes unsafe
  /// to touch it from any other thread than that on which is managing
  /// it, until we are shut down. Use runOnIndex to perform work on it
  /// instead.
  void addIndex(faiss::Index* index);

  /// Remove an index that is managed by ourselves.
  /// This will flush all pending work on that index, and then shut
  /// down its managing thread, and will remove the index.
  void removeIndex(faiss::Index* index);

  /// Run a function on all indices, in the thread that the index is
  /// managed in.
  void runOnIndex(std::function<void(faiss::Index*)> f);

  /// faiss::Index API
  /// All indices receive the same call
  void reset() override;

  /// faiss::Index API
  /// All indices receive the same call
  void train(Index::idx_t n, const float* x) override;

  /// faiss::Index API
  /// All indices receive the same call
  void add(Index::idx_t n, const float* x) override;

  /// faiss::Index API
  /// Query is partitioned into a slice for each sub-index
  /// split by ceil(n / #indices) for our sub-indices
  void search(faiss::Index::idx_t n,
              const float* x,
              faiss::Index::idx_t k,
              float* distances,
              faiss::Index::idx_t* labels) const override;

  /// reconstructs from the first index
  void reconstruct(idx_t, float *v) const override;

  bool own_fields;

  int count() const {return indices_.size(); }

  faiss::Index* at(int i) {return indices_[i].first; }
  const faiss::Index* at(int i) const {return indices_[i].first; }


 private:
  /// Collection of Index instances, with their managing worker thread
  mutable std::vector<std::pair<faiss::Index*,
                                std::unique_ptr<WorkerThread> > > indices_;
};



/** Clustering on GPU (is here because uses Proxy with ngpu > 1
 *
 * @param ngpu nb of GPUs to use
 * @param d dimension of the data
 * @param n nb of training vectors
 * @param k nb of output centroids
 * @param x training set (size n * d)
 * @param centroids output centroids (size k * d)
 * @return final quantization error
 */
float kmeans_clustering_gpu (int ngpu, size_t d, size_t n, size_t k,
                             const float *x,
                             float *centroids,
                             bool useFloat16,
                             bool storeTransposed);



} } // namespace
