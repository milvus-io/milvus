/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved
// -*- c -*-

#ifndef FAISS_INDEX_FLAT_C_H
#define FAISS_INDEX_FLAT_C_H

#include "Index_c.h"
#include "faiss_c.h"

#ifdef __cplusplus
extern "C" {
#endif

// forward declaration
typedef enum FaissMetricType FaissMetricType;

/** Opaque type for IndexFlat */
FAISS_DECLARE_CLASS_INHERITED(IndexFlat, Index)

int faiss_IndexFlat_new(FaissIndexFlat** p_index);

int faiss_IndexFlat_new_with(FaissIndexFlat** p_index, idx_t d, FaissMetricType metric);

/** get a pointer to the index's internal data (the `xb` field). The outputs
 * become invalid after any data addition or removal operation.
 * 
 * @param index   opaque pointer to index object
 * @param p_xb    output, the pointer to the beginning of `xb`.
 * @param p_size  output, the current size of `sb` in number of float values.
 */
void faiss_IndexFlat_xb(FaissIndexFlat* index, float** p_xb, size_t* p_size);

/** attempt a dynamic cast to a flat index, thus checking
 * check whether the underlying index type is `IndexFlat`.
 * 
 * @param index opaque pointer to index object
 * @return the same pointer if the index is a flat index, NULL otherwise
 */
FAISS_DECLARE_INDEX_DOWNCAST(IndexFlat)

FAISS_DECLARE_DESTRUCTOR(IndexFlat)

/** compute distance with a subset of vectors
 *
 * @param index   opaque pointer to index object
 * @param x       query vectors, size n * d
 * @param labels  indices of the vectors that should be compared
 *                for each query vector, size n * k
 * @param distances
 *                corresponding output distances, size n * k
 */
int faiss_IndexFlat_compute_distance_subset(
    FaissIndex *index,
    idx_t n,
    const float *x,
    idx_t k,
    float *distances,
    const idx_t *labels);

/** Opaque type for IndexFlatIP */
FAISS_DECLARE_CLASS_INHERITED(IndexFlatIP, Index)

int faiss_IndexFlatIP_new(FaissIndexFlatIP** p_index);

int faiss_IndexFlatIP_new_with(FaissIndexFlatIP** p_index, idx_t d);

/** Opaque type for IndexFlatL2 */
FAISS_DECLARE_CLASS_INHERITED(IndexFlatL2, Index)

int faiss_IndexFlatL2_new(FaissIndexFlatL2** p_index);

int faiss_IndexFlatL2_new_with(FaissIndexFlatL2** p_index, idx_t d);

/** Opaque type for IndexFlatL2BaseShift
 *
 * same as an IndexFlatL2 but a value is subtracted from each distance
 */
FAISS_DECLARE_CLASS_INHERITED(IndexFlatL2BaseShift, Index)

int faiss_IndexFlatL2BaseShift_new(FaissIndexFlatL2BaseShift** p_index, idx_t d, size_t nshift, const float *shift);

/** Opaque type for IndexRefineFlat
 *
 * Index that queries in a base_index (a fast one) and refines the
 * results with an exact search, hopefully improving the results.
 */
FAISS_DECLARE_CLASS_INHERITED(IndexRefineFlat, Index)

int faiss_IndexRefineFlat_new(FaissIndexRefineFlat** p_index, FaissIndex* base_index);

FAISS_DECLARE_DESTRUCTOR(IndexRefineFlat)

/** Opaque type for IndexFlat1D
 * 
 * optimized version for 1D "vectors"
 */
FAISS_DECLARE_CLASS_INHERITED(IndexFlat1D, Index)

int faiss_IndexFlat1D_new(FaissIndexFlat1D** p_index);
int faiss_IndexFlat1D_new_with(FaissIndexFlat1D** p_index, int continuous_update);

int faiss_IndexFlat1D_update_permutation(FaissIndexFlat1D* index);

#ifdef __cplusplus
}
#endif

#endif
