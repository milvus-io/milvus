/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#ifndef FAISS_GPU_CLONER_OPTIONS_C_H
#define FAISS_GPU_CLONER_OPTIONS_C_H

#include "faiss_c.h"
#include "GpuIndicesOptions_c.h"

#ifdef __cplusplus
extern "C" {
#endif

FAISS_DECLARE_CLASS(GpuClonerOptions)

FAISS_DECLARE_DESTRUCTOR(GpuClonerOptions)

/// Default constructor for GpuClonerOptions
int faiss_GpuClonerOptions_new(FaissGpuClonerOptions**);

/// how should indices be stored on index types that support indices
/// (anything but GpuIndexFlat*)?
FAISS_DECLARE_GETTER_SETTER(GpuClonerOptions, FaissIndicesOptions, indicesOptions)

/// (boolean) is the coarse quantizer in float16?
FAISS_DECLARE_GETTER_SETTER(GpuClonerOptions, int, useFloat16CoarseQuantizer)

/// (boolean) for GpuIndexIVFFlat, is storage in float16?
/// for GpuIndexIVFPQ, are intermediate calculations in float16?
FAISS_DECLARE_GETTER_SETTER(GpuClonerOptions, int, useFloat16)

/// (boolean) use precomputed tables?
FAISS_DECLARE_GETTER_SETTER(GpuClonerOptions, int, usePrecomputed)

/// reserve vectors in the invfiles?
FAISS_DECLARE_GETTER_SETTER(GpuClonerOptions, long, reserveVecs)

/// (boolean) For GpuIndexFlat, store data in transposed layout?
FAISS_DECLARE_GETTER_SETTER(GpuClonerOptions, int, storeTransposed)

/// (boolean) Set verbose options on the index
FAISS_DECLARE_GETTER_SETTER(GpuClonerOptions, int, verbose)

FAISS_DECLARE_CLASS_INHERITED(GpuMultipleClonerOptions, GpuClonerOptions)

FAISS_DECLARE_DESTRUCTOR(GpuMultipleClonerOptions)

/// Default constructor for GpuMultipleClonerOptions
int faiss_GpuMultipleClonerOptions_new(FaissGpuMultipleClonerOptions**);

/// (boolean) Whether to shard the index across GPUs, versus replication
/// across GPUs
FAISS_DECLARE_GETTER_SETTER(GpuMultipleClonerOptions, int, shard)

/// IndexIVF::copy_subset_to subset type
FAISS_DECLARE_GETTER_SETTER(GpuMultipleClonerOptions, int, shard_type)

#ifdef __cplusplus
}
#endif
#endif