/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "Index.h"
#include "IndexBinary.h"
#include "WorkerThread.h"
#include <memory>
#include <vector>

namespace faiss {

/// Takes individual faiss::Index instances, and splits queries for
/// sending to each Index instance, and joins the results together
/// when done.
/// Each index is managed by a separate CPU thread.
template<class IndexClass>
class IndexReplicasTemplate : public IndexClass {
 public:
  using idx_t = typename IndexClass::idx_t;
  using component_t = typename IndexClass::component_t;
  using distance_t = typename IndexClass::distance_t;

  IndexReplicasTemplate();
  ~IndexReplicasTemplate() override;

  /// Adds an index that is managed by ourselves.
  /// WARNING: once an index is added to this proxy, it becomes unsafe
  /// to touch it from any other thread than that on which is managing
  /// it, until we are shut down. Use runOnIndex to perform work on it
  /// instead.
  void addIndex(IndexClass* index);

  /// Remove an index that is managed by ourselves.
  /// This will flush all pending work on that index, and then shut
  /// down its managing thread, and will remove the index.
  void removeIndex(IndexClass* index);

  /// Run a function on all indices, in the thread that the index is
  /// managed in.
  void runOnIndex(std::function<void(IndexClass*)> f);

  /// faiss::Index API
  /// All indices receive the same call
  void reset() override;

  /// faiss::Index API
  /// All indices receive the same call
  virtual void train(idx_t n, const component_t* x) override;

  /// faiss::Index API
  /// All indices receive the same call
  virtual void add(idx_t n, const component_t* x) override;

  /// faiss::Index API
  /// Query is partitioned into a slice for each sub-index
  /// split by ceil(n / #indices) for our sub-indices
  virtual void search(idx_t n,
              const component_t* x,
              idx_t k,
              distance_t* distances,
              idx_t* labels) const override;

  /// reconstructs from the first index
  virtual void reconstruct(idx_t, component_t *v) const override;

  bool own_fields;

  int count() const {return indices_.size(); }

  IndexClass* at(int i) {return indices_[i].first; }
  const IndexClass* at(int i) const {return indices_[i].first; }

 private:
  /// Collection of Index instances, with their managing worker thread
  mutable std::vector<std::pair<IndexClass*,
                                std::unique_ptr<WorkerThread> > > indices_;
};

using IndexReplicas = IndexReplicasTemplate<Index>;
using IndexBinaryReplicas = IndexReplicasTemplate<IndexBinary>;


} // namespace
