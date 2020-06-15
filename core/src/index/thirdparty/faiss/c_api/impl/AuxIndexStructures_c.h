/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#ifndef FAISS_AUX_INDEX_STRUCTURES_C_H
#define FAISS_AUX_INDEX_STRUCTURES_C_H

#include "../Index_c.h"
#include "../faiss_c.h"

#ifdef __cplusplus
extern "C" {
#endif

FAISS_DECLARE_CLASS(RangeSearchResult)

FAISS_DECLARE_GETTER(RangeSearchResult, size_t, nq)

int faiss_RangeSearchResult_new(FaissRangeSearchResult** p_rsr, idx_t nq);

int faiss_RangeSearchResult_new_with(FaissRangeSearchResult** p_rsr, idx_t nq, int alloc_lims);

/// called when lims contains the nb of elements result entries
/// for each query
int faiss_RangeSearchResult_do_allocation(FaissRangeSearchResult* rsr);

FAISS_DECLARE_DESTRUCTOR(RangeSearchResult)

/// getter for buffer_size
FAISS_DECLARE_GETTER(RangeSearchResult, size_t, buffer_size)

/// getter for lims: size (nq + 1)
void faiss_RangeSearchResult_lims(
    FaissRangeSearchResult* rsr, size_t** lims);

/// getter for labels and respective distances (not sorted):
/// result for query i is labels[lims[i]:lims[i+1]]
void faiss_RangeSearchResult_labels(
    FaissRangeSearchResult* rsr, idx_t** labels, float** distances);


/** Encapsulates a set of ids to remove. */
FAISS_DECLARE_CLASS(IDSelector)
FAISS_DECLARE_DESTRUCTOR(IDSelector)

int faiss_IDSelector_is_member(const FaissIDSelector* sel, idx_t id);

/** remove ids between [imni, imax) */
FAISS_DECLARE_CLASS(IDSelectorRange)
FAISS_DECLARE_DESTRUCTOR(IDSelectorRange)

FAISS_DECLARE_GETTER(IDSelectorRange, idx_t, imin)
FAISS_DECLARE_GETTER(IDSelectorRange, idx_t, imax)

int faiss_IDSelectorRange_new(FaissIDSelectorRange** p_sel, idx_t imin, idx_t imax);

/** Remove ids from a set. Repetitions of ids in the indices set
 * passed to the constructor does not hurt performance. The hash
 * function used for the bloom filter and GCC's implementation of
 * unordered_set are just the least significant bits of the id. This
 * works fine for random ids or ids in sequences but will produce many
 * hash collisions if lsb's are always the same */
FAISS_DECLARE_CLASS(IDSelectorBatch)

FAISS_DECLARE_GETTER(IDSelectorBatch, int, nbits)
FAISS_DECLARE_GETTER(IDSelectorBatch, idx_t, mask)

int faiss_IDSelectorBatch_new(FaissIDSelectorBatch** p_sel, size_t n, const idx_t* indices);

// Below are structures used only by Index implementations

/** List of temporary buffers used to store results before they are
 *  copied to the RangeSearchResult object. */
FAISS_DECLARE_CLASS(BufferList)
FAISS_DECLARE_DESTRUCTOR(BufferList)

FAISS_DECLARE_GETTER(BufferList, size_t, buffer_size)
FAISS_DECLARE_GETTER(BufferList, size_t, wp)

typedef struct FaissBuffer {
    idx_t *ids;
    float *dis;
} FaissBuffer;

int faiss_BufferList_append_buffer(FaissBufferList* bl);

int faiss_BufferList_new(FaissBufferList** p_bl, size_t buffer_size);

int faiss_BufferList_add(FaissBufferList* bl, idx_t id, float dis);

/// copy elemnts ofs:ofs+n-1 seen as linear data in the buffers to
/// tables dest_ids, dest_dis
int faiss_BufferList_copy_range(
    FaissBufferList* bl, size_t ofs, size_t n, idx_t *dest_ids, float *dest_dis);

/// the entries in the buffers are split per query
FAISS_DECLARE_CLASS(RangeSearchPartialResult)

/// result structure for a single query
FAISS_DECLARE_CLASS(RangeQueryResult)
FAISS_DECLARE_GETTER(RangeQueryResult, idx_t, qno)
FAISS_DECLARE_GETTER(RangeQueryResult, size_t, nres)
FAISS_DECLARE_GETTER(RangeQueryResult, FaissRangeSearchPartialResult*, pres)

int faiss_RangeQueryResult_add(FaissRangeQueryResult* qr, float dis, idx_t id);


FAISS_DECLARE_GETTER(RangeSearchPartialResult, FaissRangeSearchResult*, res)

int faiss_RangeSearchPartialResult_new(
    FaissRangeSearchPartialResult** p_res, FaissRangeSearchResult* res_in);

int faiss_RangeSearchPartialResult_finalize(
    FaissRangeSearchPartialResult* res);

/// called by range_search before do_allocation
int faiss_RangeSearchPartialResult_set_lims(
    FaissRangeSearchPartialResult* res);

int faiss_RangeSearchPartialResult_new_result(
    FaissRangeSearchPartialResult* res, idx_t qno, FaissRangeQueryResult** qr);


FAISS_DECLARE_CLASS(DistanceComputer)
/// called before computing distances
int faiss_DistanceComputer_set_query(FaissDistanceComputer *dc, const float *x);

/**
 * Compute distance of vector i to current query.
 * This function corresponds to the function call operator: DistanceComputer::operator()
 */
int faiss_DistanceComputer_vector_to_query_dis( FaissDistanceComputer *dc, idx_t i, float *qd);
/// compute distance between two stored vectors
int faiss_DistanceComputer_symmetric_dis(FaissDistanceComputer *dc, idx_t i, idx_t j, float *vd);

FAISS_DECLARE_DESTRUCTOR(DistanceComputer)


#ifdef __cplusplus
}
#endif

#endif
