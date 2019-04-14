/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef META_INDEXES_H
#define META_INDEXES_H

#include <vector>
#include <unordered_map>
#include "Index.h"
#include "IndexShards.h"

namespace faiss {

/** Index that translates search results to ids */
struct IndexIDMap : Index {
    Index * index;            ///! the sub-index
    bool own_fields;          ///! whether pointers are deleted in destructo
    std::vector<long> id_map;

    explicit IndexIDMap (Index *index);

    /// Same as add_core, but stores xids instead of sequential ids
    /// @param xids if non-null, ids to store for the vectors (size n)
    void add_with_ids(idx_t n, const float* x, const long* xids) override;

    /// this will fail. Use add_with_ids
    void add(idx_t n, const float* x) override;

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    void train(idx_t n, const float* x) override;

    void reset() override;

    /// remove ids adapted to IndexFlat
    long remove_ids(const IDSelector& sel) override;

    void range_search (idx_t n, const float *x, float radius,
                       RangeSearchResult *result) const override;

    ~IndexIDMap() override;
    IndexIDMap () {own_fields=false; index=nullptr; }
};

/** same as IndexIDMap but also provides an efficient reconstruction
    implementation via a 2-way index */
struct IndexIDMap2 : IndexIDMap {

    std::unordered_map<idx_t, idx_t> rev_map;

    explicit IndexIDMap2 (Index *index);

    /// make the rev_map from scratch
    void construct_rev_map ();

    void add_with_ids(idx_t n, const float* x, const long* xids) override;

    long remove_ids(const IDSelector& sel) override;

    void reconstruct (idx_t key, float * recons) const override;

    ~IndexIDMap2() override {}
    IndexIDMap2 () {}
};

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
