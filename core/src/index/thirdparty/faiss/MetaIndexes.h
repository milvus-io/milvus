/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef META_INDEXES_H
#define META_INDEXES_H

#include <vector>
#include <unordered_map>
#include <faiss/Index.h>
#include <faiss/IndexShards.h>
#include <faiss/IndexReplicas.h>

namespace faiss {

/** Index that translates search results to ids */
template <typename IndexT>
struct IndexIDMapTemplate : IndexT {
    using idx_t = typename IndexT::idx_t;
    using component_t = typename IndexT::component_t;
    using distance_t = typename IndexT::distance_t;

    IndexT * index;           ///! the sub-index
    bool own_fields;          ///! whether pointers are deleted in destructo
    std::vector<idx_t> id_map;

    explicit IndexIDMapTemplate (IndexT *index);

    /// @param xids if non-null, ids to store for the vectors (size n)
    void add_with_ids(idx_t n, const component_t* x, const idx_t* xids) override;

    /// this will fail. Use add_with_ids
    void add(idx_t n, const component_t* x) override;

    void search(
        idx_t n, const component_t* x, idx_t k,
        distance_t* distances,
        idx_t* labels) const override;

    void train(idx_t n, const component_t* x) override;

    void reset() override;

    /// remove ids adapted to IndexFlat
    size_t remove_ids(const IDSelector& sel) override;

    void range_search (idx_t n, const component_t *x, distance_t radius,
                       RangeSearchResult *result) const override;

    ~IndexIDMapTemplate () override;
    IndexIDMapTemplate () {own_fields=false; index=nullptr; }
};

using IndexIDMap = IndexIDMapTemplate<Index>;
using IndexBinaryIDMap = IndexIDMapTemplate<IndexBinary>;


/** same as IndexIDMap but also provides an efficient reconstruction
 *  implementation via a 2-way index */
template <typename IndexT>
struct IndexIDMap2Template : IndexIDMapTemplate<IndexT> {
    using idx_t = typename IndexT::idx_t;
    using component_t = typename IndexT::component_t;
    using distance_t = typename IndexT::distance_t;

    std::unordered_map<idx_t, idx_t> rev_map;

    explicit IndexIDMap2Template (IndexT *index);

    /// make the rev_map from scratch
    void construct_rev_map ();

    void add_with_ids(idx_t n, const component_t* x, const idx_t* xids) override;

    size_t remove_ids(const IDSelector& sel) override;

    void reconstruct (idx_t key, component_t * recons) const override;

    ~IndexIDMap2Template() override {}
    IndexIDMap2Template () {}
};

using IndexIDMap2 = IndexIDMap2Template<Index>;
using IndexBinaryIDMap2 = IndexIDMap2Template<IndexBinary>;


/** splits input vectors in segments and assigns each segment to a sub-index
 * used to distribute a MultiIndexQuantizer
 */
struct IndexSplitVectors: Index {
    bool own_fields;
    bool threaded;
    std::vector<Index*> sub_indexes;
    idx_t sum_d;  /// sum of dimensions seen so far

    explicit IndexSplitVectors (idx_t d, bool threaded = false);

    void add_sub_index (Index *);
    void sync_with_sub_indexes ();

    void add(idx_t n, const float* x) override;

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    void train(idx_t n, const float* x) override;

    void reset() override;

    ~IndexSplitVectors() override;
};


} // namespace faiss


#endif
