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

/**
 * Index that concatenates the results from several sub-indexes
 */
template <typename IndexT>
struct IndexShardsTemplate : public ThreadedIndex<IndexT> {
  using idx_t = typename IndexT::idx_t;
  using component_t = typename IndexT::component_t;
  using distance_t = typename IndexT::distance_t;

  /**
   * The dimension that all sub-indices must share will be the dimension of the
   * first sub-index added
   *
   * @param threaded     do we use one thread per sub_index or do
   *                     queries sequentially?
   * @param successive_ids should we shift the returned ids by
   *                     the size of each sub-index or return them
   *                     as they are?
   */
  explicit IndexShardsTemplate(bool threaded = false,
                               bool successive_ids = true);

  /**
   * @param threaded     do we use one thread per sub_index or do
   *                     queries sequentially?
   * @param successive_ids should we shift the returned ids by
   *                     the size of each sub-index or return them
   *                     as they are?
   */
  explicit IndexShardsTemplate(idx_t d,
                               bool threaded = false,
                               bool successive_ids = true);

  /// int version due to the implicit bool conversion ambiguity of int as
  /// dimension
  explicit IndexShardsTemplate(int d,
                               bool threaded = false,
                               bool successive_ids = true);

  /// Alias for addIndex()
  void add_shard(IndexT* index) { this->addIndex(index); }

  /// Alias for removeIndex()
  void remove_shard(IndexT* index) { this->removeIndex(index); }

  /// supported only for sub-indices that implement add_with_ids
  void add(idx_t n, const component_t* x) override;

  /**
   * Cases (successive_ids, xids):
   * - true, non-NULL       ERROR: it makes no sense to pass in ids and
   *                        request them to be shifted
   * - true, NULL           OK, but should be called only once (calls add()
   *                        on sub-indexes).
   * - false, non-NULL      OK: will call add_with_ids with passed in xids
   *                        distributed evenly over shards
   * - false, NULL          OK: will call add_with_ids on each sub-index,
   *                        starting at ntotal
   */
  void add_with_ids(idx_t n, const component_t* x, const idx_t* xids) override;

  void search(idx_t n, const component_t* x, idx_t k,
              distance_t* distances, idx_t* labels) const override;

  void train(idx_t n, const component_t* x) override;

  // update metric_type and ntotal. Call if you changes something in
  // the shard indexes.
  void sync_with_shard_indexes();

  bool successive_ids;

 protected:
  /// Called just after an index is added
  void onAfterAddIndex(IndexT* index) override;

  /// Called just after an index is removed
  void onAfterRemoveIndex(IndexT* index) override;
};

using IndexShards = IndexShardsTemplate<Index>;
using IndexBinaryShards = IndexShardsTemplate<IndexBinary>;


} // namespace faiss
