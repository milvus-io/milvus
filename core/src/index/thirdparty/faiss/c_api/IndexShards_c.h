/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#ifndef INDEXSHARDS_C_H
#define INDEXSHARDS_C_H

#include "faiss_c.h"
#include "Index_c.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Index that concatenates the results from several sub-indexes
 */
FAISS_DECLARE_CLASS_INHERITED(IndexShards, Index)

FAISS_DECLARE_GETTER_SETTER(IndexShards, int, own_fields)
FAISS_DECLARE_GETTER_SETTER(IndexShards, int, successive_ids)

int faiss_IndexShards_new(FaissIndexShards** p_index, idx_t d);

int faiss_IndexShards_new_with_options(FaissIndexShards** p_index, idx_t d, int threaded, int successive_ids);

int faiss_IndexShards_add_shard(FaissIndexShards* index, FaissIndex* shard);

/// update metric_type and ntotal
int faiss_IndexShards_sync_with_shard_indexes(FaissIndexShards* index);

FaissIndex* faiss_IndexShards_at(FaissIndexShards* index, int i);

#ifdef __cplusplus
}
#endif
#endif