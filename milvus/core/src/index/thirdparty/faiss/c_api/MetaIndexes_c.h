/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#ifndef METAINDEXES_C_H
#define METAINDEXES_C_H

#include "faiss_c.h"
#include "Index_c.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Index that translates search results to ids */
FAISS_DECLARE_CLASS_INHERITED(IndexIDMap, Index)

FAISS_DECLARE_GETTER_SETTER(IndexIDMap, int, own_fields)

int faiss_IndexIDMap_new(FaissIndexIDMap** p_index, FaissIndex* index);

/** get a pointer to the index map's internal ID vector (the `id_map` field). The
 * outputs of this function become invalid after any operation that can modify the index.
 * 
 * @param index   opaque pointer to index object
 * @param p_id_map    output, the pointer to the beginning of `id_map`.
 * @param p_size  output, the current length of `id_map`.
 */
void faiss_IndexIDMap_id_map(FaissIndexIDMap* index, idx_t** p_id_map, size_t* p_size);

/** same as IndexIDMap but also provides an efficient reconstruction
    implementation via a 2-way index */
FAISS_DECLARE_CLASS_INHERITED(IndexIDMap2, IndexIDMap)

int faiss_IndexIDMap2_new(FaissIndexIDMap2** p_index, FaissIndex* index);

/// make the rev_map from scratch
int faiss_IndexIDMap2_construct_rev_map(FaissIndexIDMap2* index);

#ifdef __cplusplus
}
#endif
#endif
