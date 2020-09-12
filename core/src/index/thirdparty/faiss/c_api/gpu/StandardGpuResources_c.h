/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#ifndef FAISS_STANDARD_GPURESOURCES_C_H
#define FAISS_STANDARD_GPURESOURCES_C_H

#include <cuda_runtime_api.h>
#include "faiss_c.h"
#include "gpu/GpuResources_c.h"

#ifdef __cplusplus
extern "C" {
#endif

/// Default implementation of GpuResources that allocates a cuBLAS
/// stream and 2 streams for use, as well as temporary memory
FAISS_DECLARE_CLASS_INHERITED(StandardGpuResources, GpuResources)

FAISS_DECLARE_DESTRUCTOR(StandardGpuResources)

/// Default constructor for StandardGpuResources
int faiss_StandardGpuResources_new(FaissStandardGpuResources**);

/// Disable allocation of temporary memory; all temporary memory
/// requests will call cudaMalloc / cudaFree at the point of use
int faiss_StandardGpuResources_noTempMemory(FaissStandardGpuResources*);

/// Specify that we wish to use a certain fixed size of memory on
/// all devices as temporary memory
int faiss_StandardGpuResources_setTempMemory(FaissStandardGpuResources*, size_t size);

/// Set amount of pinned memory to allocate, for async GPU <-> CPU
/// transfers
int faiss_StandardGpuResources_setPinnedMemory(FaissStandardGpuResources*, size_t size);

/// Called to change the stream for work ordering
int faiss_StandardGpuResources_setDefaultStream(FaissStandardGpuResources*, int device, cudaStream_t stream);

/// Called to change the work ordering streams to the null stream
/// for all devices
int faiss_StandardGpuResources_setDefaultNullStreamAllDevices(FaissStandardGpuResources*);

#ifdef __cplusplus
}
#endif
#endif