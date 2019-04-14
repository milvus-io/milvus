/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "MetaIndexes_c.h"
#include "MetaIndexes.h"
#include "macros_impl.h"

using faiss::Index;
using faiss::IndexIDMap;
using faiss::IndexIDMap2;
using faiss::IndexShards;

DEFINE_GETTER(IndexIDMap, int, own_fields)
DEFINE_SETTER(IndexIDMap, int, own_fields)

int faiss_IndexIDMap_new(FaissIndexIDMap** p_index, FaissIndex* index) {
    try {
        auto out = new IndexIDMap(reinterpret_cast<Index*>(index));
        *p_index = reinterpret_cast<FaissIndexIDMap*>(out);
    } CATCH_AND_HANDLE
}

void faiss_IndexIDMap_id_map(FaissIndexIDMap* index, long** p_id_map, size_t* p_size) {
    auto idx = reinterpret_cast<IndexIDMap*>(index);
    if (p_id_map)
        *p_id_map = idx->id_map.data();
    if (p_size)
        *p_size = idx->id_map.size();
}

int faiss_IndexIDMap2_new(FaissIndexIDMap2** p_index, FaissIndex* index) {
    try {
        auto out = new IndexIDMap2(reinterpret_cast<Index*>(index));
        *p_index = reinterpret_cast<FaissIndexIDMap2*>(out);
    } CATCH_AND_HANDLE
}

int faiss_IndexIDMap2_construct_rev_map(FaissIndexIDMap2* index) {
    try {
        reinterpret_cast<IndexIDMap2*>(index)->construct_rev_map();
    } CATCH_AND_HANDLE
}

DEFINE_GETTER(IndexShards, int, own_fields)
DEFINE_SETTER(IndexShards, int, own_fields)

DEFINE_GETTER(IndexShards, int, threaded)
DEFINE_SETTER(IndexShards, int, threaded)

DEFINE_GETTER(IndexShards, int, successive_ids)
DEFINE_SETTER(IndexShards, int, successive_ids)

int faiss_IndexShards_new(FaissIndexShards** p_index, idx_t d) {
    try {
        auto out = new IndexShards(d);
        *p_index = reinterpret_cast<FaissIndexShards*>(out);
    } CATCH_AND_HANDLE
}

int faiss_IndexShards_new_with_options(FaissIndexShards** p_index, idx_t d, int threaded, int successive_ids) {
    try {
        auto out = new IndexShards(d, static_cast<bool>(threaded), static_cast<bool>(successive_ids));
        *p_index = reinterpret_cast<FaissIndexShards*>(out);
    } CATCH_AND_HANDLE
}

/** get a pointer to the index' shards (the `shard_indexes` field). The
 * outputs of this function become invalid after any operation that can modify the index.
 * 
 * @param index   opaque pointer to index object
 * @param p_shard_indexes    output, the pointer to the beginning of `shard_indexes`.
 * @param p_size  output, the current length of `shard_indexes`.
 */
void faiss_IndexShards_shard_indexes(FaissIndexShards* index, FaissIndex** p_shard_indexes, size_t* p_size) {
    auto idx = reinterpret_cast<IndexShards*>(index);
    if (p_shard_indexes)
        *p_shard_indexes = reinterpret_cast<FaissIndex*>(idx->shard_indexes.data());
    if (p_size)
        *p_size = idx->shard_indexes.size();
}

int faiss_IndexShards_add_shard(FaissIndexShards* index, FaissIndex* shard) {
    try {
        reinterpret_cast<IndexShards*>(index)->add_shard(
            reinterpret_cast<Index*>(shard));
    } CATCH_AND_HANDLE
}

int faiss_IndexShards_sync_with_shard_indexes(FaissIndexShards* index) {
    try {
        reinterpret_cast<IndexShards*>(index)->sync_with_shard_indexes();
    } CATCH_AND_HANDLE
}

FaissIndex* faiss_IndexShards_at(FaissIndexShards* index, int i) {
    auto shard = reinterpret_cast<IndexShards*>(index)->at(i);
    return reinterpret_cast<FaissIndex*>(shard);
}
