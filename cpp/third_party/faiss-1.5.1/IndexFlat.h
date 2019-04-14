/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef INDEX_FLAT_H
#define INDEX_FLAT_H

#include <vector>

#include "Index.h"


namespace faiss {

/** Index that stores the full vectors and performs exhaustive search */
struct IndexFlat: Index {
    /// database vectors, size ntotal * d
    std::vector<float> xb;

    explicit IndexFlat (idx_t d, MetricType metric = METRIC_L2);

    void add(idx_t n, const float* x) override;

    void reset() override;

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    void range_search(
        idx_t n,
        const float* x,
        float radius,
        RangeSearchResult* result) const override;

    void reconstruct(idx_t key, float* recons) const override;

    /** compute distance with a subset of vectors
     *
     * @param x       query vectors, size n * d
     * @param labels  indices of the vectors that should be compared
     *                for each query vector, size n * k
     * @param distances
     *                corresponding output distances, size n * k
     */
    void compute_distance_subset (
            idx_t n,
            const float *x,
            idx_t k,
            float *distances,
            const idx_t *labels) const;

    /** remove some ids. NB that Because of the structure of the
     * indexing structre, the semantics of this operation are
     * different from the usual ones: the new ids are shifted */
    long remove_ids(const IDSelector& sel) override;

    IndexFlat () {}
};



struct IndexFlatIP:IndexFlat {
    explicit IndexFlatIP (idx_t d): IndexFlat (d, METRIC_INNER_PRODUCT) {}
    IndexFlatIP () {}
};


struct IndexFlatL2:IndexFlat {
    explicit IndexFlatL2 (idx_t d): IndexFlat (d, METRIC_L2) {}
    IndexFlatL2 () {}
};


// same as an IndexFlatL2 but a value is subtracted from each distance
struct IndexFlatL2BaseShift: IndexFlatL2 {
    std::vector<float> shift;

    IndexFlatL2BaseShift (idx_t d, size_t nshift, const float *shift);

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;
};


/** Index that queries in a base_index (a fast one) and refines the
 *  results with an exact search, hopefully improving the results.
 */
struct IndexRefineFlat: Index {

    /// storage for full vectors
    IndexFlat refine_index;

    /// faster index to pre-select the vectors that should be filtered
    Index *base_index;
    bool own_fields;  ///< should the base index be deallocated?

    /// factor between k requested in search and the k requested from
    /// the base_index (should be >= 1)
    float k_factor;

    explicit IndexRefineFlat (Index *base_index);

    IndexRefineFlat ();

    void train(idx_t n, const float* x) override;

    void add(idx_t n, const float* x) override;

    void reset() override;

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    ~IndexRefineFlat() override;
};


/// optimized version for 1D "vectors"
struct IndexFlat1D:IndexFlatL2 {
    bool continuous_update; ///< is the permutation updated continuously?

    std::vector<idx_t> perm; ///< sorted database indices

    explicit IndexFlat1D (bool continuous_update=true);

    /// if not continuous_update, call this between the last add and
    /// the first search
    void update_permutation ();

    void add(idx_t n, const float* x) override;

    void reset() override;

    /// Warn: the distances returned are L1 not L2
    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;
};


}

#endif
