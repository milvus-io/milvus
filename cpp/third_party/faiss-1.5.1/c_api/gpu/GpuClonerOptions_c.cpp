/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "GpuClonerOptions_c.h"
#include "gpu/GpuClonerOptions.h"
#include "macros_impl.h"

using faiss::gpu::IndicesOptions;
using faiss::gpu::GpuClonerOptions;
using faiss::gpu::GpuMultipleClonerOptions;

int faiss_GpuClonerOptions_new(FaissGpuClonerOptions** p) {
    try {
        *p = reinterpret_cast<FaissGpuClonerOptions*>(new GpuClonerOptions());
    } CATCH_AND_HANDLE
}

int faiss_GpuMultipleClonerOptions_new(FaissGpuMultipleClonerOptions** p) {
    try {
        *p = reinterpret_cast<FaissGpuMultipleClonerOptions*>(new GpuMultipleClonerOptions());
    } CATCH_AND_HANDLE
}

DEFINE_DESTRUCTOR(GpuClonerOptions)
DEFINE_DESTRUCTOR(GpuMultipleClonerOptions)

DEFINE_GETTER(GpuClonerOptions, FaissIndicesOptions, indicesOptions)
DEFINE_GETTER(GpuClonerOptions, int, useFloat16CoarseQuantizer)
DEFINE_GETTER(GpuClonerOptions, int, useFloat16)
DEFINE_GETTER(GpuClonerOptions, int, usePrecomputed)
DEFINE_GETTER(GpuClonerOptions, long, reserveVecs)
DEFINE_GETTER(GpuClonerOptions, int, storeTransposed)
DEFINE_GETTER(GpuClonerOptions, int, verbose)
DEFINE_GETTER(GpuMultipleClonerOptions, int, shard)
DEFINE_GETTER(GpuMultipleClonerOptions, int, shard_type)

DEFINE_SETTER_STATIC(GpuClonerOptions, IndicesOptions, FaissIndicesOptions, indicesOptions)
DEFINE_SETTER_STATIC(GpuClonerOptions, bool, int, useFloat16CoarseQuantizer)
DEFINE_SETTER_STATIC(GpuClonerOptions, bool, int, useFloat16)
DEFINE_SETTER_STATIC(GpuClonerOptions, bool, int, usePrecomputed)
DEFINE_SETTER(GpuClonerOptions, long, reserveVecs)
DEFINE_SETTER_STATIC(GpuClonerOptions, bool, int, storeTransposed)
DEFINE_SETTER_STATIC(GpuClonerOptions, bool, int, verbose)
DEFINE_SETTER_STATIC(GpuMultipleClonerOptions, bool, int, shard)
DEFINE_SETTER(GpuMultipleClonerOptions, int, shard_type)
