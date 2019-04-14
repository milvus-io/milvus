/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
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
void faiss_IndexIDMap_id_map(FaissIndexIDMap* index, long** p_id_map, size_t* p_size);

/** same as IndexIDMap but also provides an efficient reconstruction
    implementation via a 2-way index */
FAISS_DECLARE_CLASS_INHERITED(IndexIDMap2, IndexIDMap)

int faiss_IndexIDMap2_new(FaissIndexIDMap2** p_index, FaissIndex* index);

/// make the rev_map from scratch
int faiss_IndexIDMap2_construct_rev_map(FaissIndexIDMap2* index);

/** Index that concatenates the results from several sub-indexes
 */
FAISS_DECLARE_CLASS_INHERITED(IndexShards, Index)

FAISS_DECLARE_GETTER_SETTER(IndexShards, int, own_fields)
FAISS_DECLARE_GETTER_SETTER(IndexShards, int, threaded)
FAISS_DECLARE_GETTER_SETTER(IndexShards, int, successive_ids)

int faiss_IndexShards_new(FaissIndexShards** p_index, idx_t d);

int faiss_IndexShards_new_with_options(FaissIndexShards** p_index, idx_t d, int threaded, int successive_ids);

/** get a pointer to the index' shards (the `shard_indexes` field). The
 * outputs of this function become invalid after any operation that can modify the index.
 * 
 * @param index   opaque pointer to index object
 * @param p_shard_indexes    output, the pointer to the beginning of `shard_indexes`.
 * @param p_size  output, the current length of `shard_indexes`.
 */
void faiss_IndexShards_shard_indexes(FaissIndexShards* index, FaissIndex** p_shard_indexes, size_t* p_size);

int faiss_IndexShards_add_shard(FaissIndexShards* index, FaissIndex* shard);

/// update metric_type and ntotal
int faiss_IndexShards_sync_with_shard_indexes(FaissIndexShards* index);

FaissIndex* faiss_IndexShards_at(FaissIndexShards* index, int i);

#ifdef __cplusplus
}
#endif
#endif
