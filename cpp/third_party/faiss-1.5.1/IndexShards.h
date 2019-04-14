/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-
#pragma once

#include <vector>

#include "Index.h"
#include "IndexBinary.h"

namespace faiss {

/** Index that concatenates the results from several sub-indexes
 *
 */
template<class IndexClass>
struct IndexShardsTemplate : IndexClass {

    using idx_t = typename IndexClass::idx_t;
    using component_t = typename IndexClass::component_t;
    using distance_t = typename IndexClass::distance_t;

    std::vector<IndexClass*> shard_indexes;
    bool own_fields;      /// should the sub-indexes be deleted along with this?
    bool threaded;
    bool successive_ids;

    /**
     * @param threaded     do we use one thread per sub_index or do
     *                     queries sequentially?
     * @param successive_ids should we shift the returned ids by
     *                     the size of each sub-index or return them
     *                     as they are?
     */
    explicit IndexShardsTemplate (idx_t d, bool threaded = false,
                                  bool successive_ids = true);

    void add_shard (IndexClass *);

    // update metric_type and ntotal. Call if you changes something in
    // the shard indexes.
    void sync_with_shard_indexes ();

    IndexClass *at(int i) {return shard_indexes[i]; }

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

    void search(
        idx_t n, const component_t* x, idx_t k,
        distance_t* distances, idx_t* labels) const override;

    void train(idx_t n, const component_t* x) override;

    void reset() override;

    ~IndexShardsTemplate() override;
};

using IndexShards = IndexShardsTemplate<Index>;
using IndexBinaryShards = IndexShardsTemplate<IndexBinary>;


} // namespace faiss
