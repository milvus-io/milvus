/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/Index.h>
#include <faiss/IndexBinary.h>
#include <faiss/impl/ThreadedIndex.h>

namespace faiss {

/// Takes individual faiss::Index instances, and splits queries for
/// sending to each Index instance, and joins the results together
/// when done.
/// Each index is managed by a separate CPU thread.
template <typename IndexT>
class IndexReplicasTemplate : public ThreadedIndex<IndexT> {
 public:
  using idx_t = typename IndexT::idx_t;
  using component_t = typename IndexT::component_t;
  using distance_t = typename IndexT::distance_t;

  /// The dimension that all sub-indices must share will be the dimension of the
  /// first sub-index added
  /// @param threaded do we use one thread per sub-index or do queries
  /// sequentially?
  explicit IndexReplicasTemplate(bool threaded = true);

  /// @param d the dimension that all sub-indices must share
  /// @param threaded do we use one thread per sub index or do queries
  /// sequentially?
  explicit IndexReplicasTemplate(idx_t d, bool threaded = true);

  /// int version due to the implicit bool conversion ambiguity of int as
  /// dimension
  explicit IndexReplicasTemplate(int d, bool threaded = true);

  /// Alias for addIndex()
  void add_replica(IndexT* index) { this->addIndex(index); }

  /// Alias for removeIndex()
  void remove_replica(IndexT* index) { this->removeIndex(index); }

  /// faiss::Index API
  /// All indices receive the same call
  void train(idx_t n, const component_t* x) override;

  /// faiss::Index API
  /// All indices receive the same call
  void add(idx_t n, const component_t* x) override;

  /// faiss::Index API
  /// Query is partitioned into a slice for each sub-index
  /// split by ceil(n / #indices) for our sub-indices
  void search(idx_t n,
              const component_t* x,
              idx_t k,
              distance_t* distances,
              idx_t* labels) const override;

  /// reconstructs from the first index
  void reconstruct(idx_t, component_t *v) const override;

 protected:
  /// Called just after an index is added
  void onAfterAddIndex(IndexT* index) override;
};

using IndexReplicas = IndexReplicasTemplate<Index>;
using IndexBinaryReplicas = IndexReplicasTemplate<IndexBinary>;

} // namespace
