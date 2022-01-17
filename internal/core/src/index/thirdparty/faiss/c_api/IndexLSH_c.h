/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#ifndef INDEX_LSH_C_H
#define INDEX_LSH_C_H

#include "faiss_c.h"
#include "Index_c.h"
#include "Clustering_c.h"

#ifdef __cplusplus
extern "C" {
#endif

/** The sign of each vector component is put in a binary signature */
FAISS_DECLARE_CLASS_INHERITED(IndexLSH, Index)
FAISS_DECLARE_DESTRUCTOR(IndexLSH)
FAISS_DECLARE_INDEX_DOWNCAST(IndexLSH)

FAISS_DECLARE_GETTER(IndexLSH, int, nbits)
FAISS_DECLARE_GETTER(IndexLSH, int, bytes_per_vec)
FAISS_DECLARE_GETTER(IndexLSH, int, rotate_data)
FAISS_DECLARE_GETTER(IndexLSH, int, train_thresholds)

int faiss_IndexLSH_new(FaissIndexLSH** p_index, idx_t d, int nbits);

int faiss_IndexLSH_new_with_options(FaissIndexLSH** p_index, idx_t d, int nbits, int rotate_data, int train_thresholds);

#ifdef __cplusplus
}
#endif

#endif
