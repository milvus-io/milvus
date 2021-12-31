/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "AuxIndexStructures_c.h"
#include "../../impl/AuxIndexStructures.h"
#include "../macros_impl.h"
#include <iostream>

using faiss::BufferList;
using faiss::IDSelector;
using faiss::IDSelectorBatch;
using faiss::IDSelectorRange;
using faiss::RangeSearchResult;
using faiss::RangeSearchPartialResult;
using faiss::RangeQueryResult;
using faiss::DistanceComputer;

DEFINE_GETTER(RangeSearchResult, size_t, nq)

int faiss_RangeSearchResult_new(FaissRangeSearchResult** p_rsr, idx_t nq) {
    try {
        *p_rsr = reinterpret_cast<FaissRangeSearchResult*>(
            new RangeSearchResult(nq));
        return 0;
    } CATCH_AND_HANDLE
}

int faiss_RangeSearchResult_new_with(FaissRangeSearchResult** p_rsr, idx_t nq, int alloc_lims) {
    try {
        *p_rsr = reinterpret_cast<FaissRangeSearchResult*>(
            new RangeSearchResult(nq, static_cast<bool>(alloc_lims)));
        return 0;
    } CATCH_AND_HANDLE
}

/// called when lims contains the nb of elements result entries
/// for each query
int faiss_RangeSearchResult_do_allocation(FaissRangeSearchResult* rsr) {
    try {
        reinterpret_cast<RangeSearchResult*>(rsr)->do_allocation();
        return 0;
    } CATCH_AND_HANDLE
}

DEFINE_DESTRUCTOR(RangeSearchResult)

/// getter for buffer_size
DEFINE_GETTER(RangeSearchResult, size_t, buffer_size)

/// getter for lims: size (nq + 1)
void faiss_RangeSearchResult_lims(FaissRangeSearchResult* rsr, size_t** lims) {
    *lims = reinterpret_cast<RangeSearchResult*>(rsr)->lims;
}

/// getter for labels and respective distances (not sorted):
/// result for query i is labels[lims[i]:lims[i+1]]
void faiss_RangeSearchResult_labels(FaissRangeSearchResult* rsr, idx_t** labels, float** distances) {
    auto sr = reinterpret_cast<RangeSearchResult*>(rsr);
    *labels = sr->labels;
    *distances = sr->distances;
}

DEFINE_DESTRUCTOR(IDSelector)

int faiss_IDSelector_is_member(const FaissIDSelector* sel, idx_t id)  {
    return reinterpret_cast<const IDSelector*>(sel)->is_member(id);
}

DEFINE_DESTRUCTOR(IDSelectorRange)

DEFINE_GETTER(IDSelectorRange, idx_t, imin)
DEFINE_GETTER(IDSelectorRange, idx_t, imax)

int faiss_IDSelectorRange_new(FaissIDSelectorRange** p_sel, idx_t imin, idx_t imax) {
    try {
        *p_sel = reinterpret_cast<FaissIDSelectorRange*>(
            new IDSelectorRange(imin, imax)
        );
        return 0;
    } CATCH_AND_HANDLE
}

DEFINE_GETTER(IDSelectorBatch, int, nbits)
DEFINE_GETTER(IDSelectorBatch, idx_t, mask)

int faiss_IDSelectorBatch_new(FaissIDSelectorBatch** p_sel, size_t n, const idx_t* indices) {
    try {
        *p_sel = reinterpret_cast<FaissIDSelectorBatch*>(
            new IDSelectorBatch(n, indices)
        );
        return 0;
    } CATCH_AND_HANDLE
}

// Below are structures used only by Index implementations

DEFINE_DESTRUCTOR(BufferList)

DEFINE_GETTER(BufferList, size_t, buffer_size)
DEFINE_GETTER(BufferList, size_t, wp)

int faiss_BufferList_append_buffer(FaissBufferList* bl) {
    try {
        reinterpret_cast<BufferList*>(bl)->append_buffer();
        return 0;
    } CATCH_AND_HANDLE
}

int faiss_BufferList_new(FaissBufferList** p_bl, size_t buffer_size) {
    try {
        *p_bl = reinterpret_cast<FaissBufferList*>(
            new BufferList(buffer_size)
        );
        return 0;
    } CATCH_AND_HANDLE
}

int faiss_BufferList_add(FaissBufferList* bl, idx_t id, float dis) {
    try {
        reinterpret_cast<BufferList*>(bl)->add(id, dis);
        return 0;
    } CATCH_AND_HANDLE
}

/// copy elemnts ofs:ofs+n-1 seen as linear data in the buffers to
/// tables dest_ids, dest_dis
int faiss_BufferList_copy_range(
    FaissBufferList* bl, size_t ofs, size_t n, idx_t *dest_ids, float *dest_dis) {
    try {
        reinterpret_cast<BufferList*>(bl)->copy_range(ofs, n, dest_ids, dest_dis);
        return 0;
    } CATCH_AND_HANDLE
}

DEFINE_GETTER(RangeQueryResult, idx_t, qno)
DEFINE_GETTER(RangeQueryResult, size_t, nres)
DEFINE_GETTER_PERMISSIVE(RangeQueryResult, FaissRangeSearchPartialResult*, pres)

int faiss_RangeQueryResult_add(FaissRangeQueryResult* qr, float dis, idx_t id) {
    try {
        reinterpret_cast<RangeQueryResult*>(qr)->add(dis, id);
        return 0;
    } CATCH_AND_HANDLE
}

DEFINE_GETTER_PERMISSIVE(RangeSearchPartialResult, FaissRangeSearchResult*, res)

int faiss_RangeSearchPartialResult_new(
    FaissRangeSearchPartialResult** p_res, FaissRangeSearchResult* res_in) {
    try {
        *p_res = reinterpret_cast<FaissRangeSearchPartialResult*>(
            new RangeSearchPartialResult(
                reinterpret_cast<RangeSearchResult*>(res_in))
        );
        return 0;
    } CATCH_AND_HANDLE
}

int faiss_RangeSearchPartialResult_finalize(
    FaissRangeSearchPartialResult* res) {
    try {
        reinterpret_cast<RangeSearchPartialResult*>(res)->finalize();
        return 0;
    } CATCH_AND_HANDLE
}

/// called by range_search before do_allocation
int faiss_RangeSearchPartialResult_set_lims(
    FaissRangeSearchPartialResult* res) {
    try {
        reinterpret_cast<RangeSearchPartialResult*>(res)->set_lims();
        return 0;
    } CATCH_AND_HANDLE
}

int faiss_RangeSearchPartialResult_new_result(
    FaissRangeSearchPartialResult* res, idx_t qno, FaissRangeQueryResult** qr) {

    try {
        auto q = 
            &reinterpret_cast<RangeSearchPartialResult*>(res)->new_result(qno);
        if (qr) {
            *qr = reinterpret_cast<FaissRangeQueryResult*>(&q);
        }
        return 0;
    } CATCH_AND_HANDLE
}

DEFINE_DESTRUCTOR(DistanceComputer)

int faiss_DistanceComputer_set_query(FaissDistanceComputer *dc, const float *x) {
    try {
        reinterpret_cast<DistanceComputer*>(dc)->set_query(x);
        return 0;
    }
    CATCH_AND_HANDLE
}

int faiss_DistanceComputer_vector_to_query_dis(FaissDistanceComputer *dc, idx_t i, float *qd) {
    try {
        *qd = reinterpret_cast<DistanceComputer*>(dc)->operator()(i);
        return 0;
    }
    CATCH_AND_HANDLE
}

int faiss_DistanceComputer_symmetric_dis(FaissDistanceComputer *dc, idx_t i, idx_t j, float *vd) {
    try {
        *vd = reinterpret_cast<DistanceComputer*>(dc)->symmetric_dis(i, j);
        return 0;
    }
    CATCH_AND_HANDLE
}