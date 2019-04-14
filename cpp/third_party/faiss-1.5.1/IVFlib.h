/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_IVFLIB_H
#define FAISS_IVFLIB_H

/** Since IVF (inverted file) indexes are of so much use for
 * large-scale use cases, we group a few functions related to them in
 * this small library. Most functions work both on IndexIVFs and
 * IndexIVFs embedded within an IndexPreTransform.
 */

#include <vector>
#include "IndexIVF.h"

namespace faiss { namespace ivflib {


/** check if two indexes have the same parameters and are trained in
 * the same way, otherwise throw. */
void check_compatible_for_merge (const Index * index1,
                                 const Index * index2);

/** get an IndexIVF from an index. The index may be an IndexIVF or
 * some wrapper class that encloses an IndexIVF
 *
 * throws an exception if this is not the case.
 */
const IndexIVF * extract_index_ivf (const Index * index);
IndexIVF * extract_index_ivf (Index * index);

/** Merge index1 into index0. Works on IndexIVF's and IndexIVF's
 *  embedded in a IndexPreTransform. On output, the index1 is empty.
 *
 * @param shift_ids: translate the ids from index1 to index0->prev_ntotal
 */
void merge_into(Index *index0, Index *index1, bool shift_ids);

typedef Index::idx_t idx_t;

/* Returns the cluster the embeddings belong to.
 *
 * @param index      Index, which should be an IVF index
 *                   (otherwise there are no clusters)
 * @param embeddings object descriptors for which the centroids should be found,
 *                   size num_objects * d
 * @param centroid_ids
 *                   cluster id each object belongs to, size num_objects
 */
void search_centroid(Index *index,
                     const float* x, int n,
                     idx_t* centroid_ids);

/* Returns the cluster the embeddings belong to.
 *
 * @param index      Index, which should be an IVF index
 *                   (otherwise there are no clusters)
 * @param query_centroid_ids
 *                   centroid ids corresponding to the query vectors (size n)
 * @param result_centroid_ids
 *                   centroid ids corresponding to the results (size n * k)
 * other arguments are the same as the standard search function
 */
void search_and_return_centroids(Index *index,
                                 size_t n,
                                 const float* xin,
                                 long k,
                                 float *distances,
                                 idx_t* labels,
                                 idx_t* query_centroid_ids,
                                 idx_t* result_centroid_ids);


/** A set of IndexIVFs concatenated together in a FIFO fashion.
 * at each "step", the oldest index slice is removed and a new index is added.
 */
struct SlidingIndexWindow {
    /// common index that contains the sliding window
    Index * index;

    /// InvertedLists of index
    ArrayInvertedLists *ils;

    /// number of slices currently in index
    int n_slice;

    /// same as index->nlist
    size_t nlist;

    /// cumulative list sizes at each slice
    std::vector<std::vector<size_t> > sizes;

    /// index should be initially empty and trained
    SlidingIndexWindow (Index *index);

    /** Add one index to the current index and remove the oldest one.
     *
     * @param sub_index        slice to swap in (can be NULL)
     * @param remove_oldest    if true, remove the oldest slices */
    void step(const Index *sub_index, bool remove_oldest);

};


/// Get a subset of inverted lists [i0, i1)
ArrayInvertedLists * get_invlist_range (const Index *index,
                                        long i0, long i1);

/// Set a subset of inverted lists
void set_invlist_range (Index *index, long i0, long i1,
                        ArrayInvertedLists * src);


// search an IndexIVF, possibly  embedded in an IndexPreTransform
// with given parameters
void search_with_parameters (const Index *index,
                             idx_t n, const float *x, idx_t k,
                             float *distances, idx_t *labels,
                             IVFSearchParameters *params);

} } // namespace faiss::ivflib

#endif
