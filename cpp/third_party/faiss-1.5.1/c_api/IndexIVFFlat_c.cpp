/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "Index_c.h"
#include "Clustering_c.h"
#include "IndexIVFFlat_c.h"
#include "IndexIVFFlat.h"
#include "macros_impl.h"

using faiss::Index;
using faiss::IndexIVFFlat;
using faiss::MetricType;

DEFINE_DESTRUCTOR(IndexIVFFlat)
DEFINE_INDEX_DOWNCAST(IndexIVFFlat)

int faiss_IndexIVFFlat_new(FaissIndexIVFFlat** p_index) {
    try {
        *p_index = reinterpret_cast<FaissIndexIVFFlat*>(new IndexIVFFlat());
    } CATCH_AND_HANDLE
}

int faiss_IndexIVFFlat_new_with(FaissIndexIVFFlat** p_index,
    FaissIndex* quantizer, size_t d, size_t nlist)
{
    try {
        auto q = reinterpret_cast<Index*>(quantizer);
        *p_index = reinterpret_cast<FaissIndexIVFFlat*>(new IndexIVFFlat(q, d, nlist));
    } CATCH_AND_HANDLE
}

int faiss_IndexIVFFlat_new_with_metric(
    FaissIndexIVFFlat** p_index, FaissIndex* quantizer, size_t d, size_t nlist,
    FaissMetricType metric)
{
    try {
        auto q = reinterpret_cast<Index*>(quantizer);
        auto m = static_cast<MetricType>(metric);
        *p_index = reinterpret_cast<FaissIndexIVFFlat*>(new IndexIVFFlat(q, d, nlist, m));
    } CATCH_AND_HANDLE
}

int faiss_IndexIVFFlat_add_core(FaissIndexIVFFlat* index, idx_t n, 
    const float * x, const long *xids, const long *precomputed_idx)
{
    try {
        reinterpret_cast<IndexIVFFlat*>(index)->add_core(n, x, xids, precomputed_idx);
    } CATCH_AND_HANDLE
}

int faiss_IndexIVFFlat_update_vectors(FaissIndexIVFFlat* index, int nv,
    idx_t *idx, const float *v)
{
    try {
        reinterpret_cast<IndexIVFFlat*>(index)->update_vectors(nv, idx, v);
    } CATCH_AND_HANDLE
}
