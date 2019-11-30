/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#ifndef FAISS_GPU_RESOURCES_C_H
#define FAISS_GPU_RESOURCES_C_H

#include <cuda_runtime_api.h>
#include <cublas.h>
#include "faiss_c.h"

#ifdef __cplusplus
extern "C" {
#endif

/// Base class of GPU-side resource provider; hides provision of
/// cuBLAS handles, CUDA streams and a temporary memory manager
FAISS_DECLARE_CLASS(GpuResources)

FAISS_DECLARE_DESTRUCTOR(GpuResources)

/// Call to pre-allocate resources for a particular device. If this is
/// not called, then resources will be allocated at the first time
/// of demand
int faiss_GpuResources_initializeForDevice(FaissGpuResources*, int);

/// Returns the cuBLAS handle that we use for the given device
int faiss_GpuResources_getBlasHandle(FaissGpuResources*, int, cublasHandle_t*);

/// Returns the stream that we order all computation on for the
/// given device
int faiss_GpuResources_getDefaultStream(FaissGpuResources*, int, cudaStream_t*);

/// Returns the available CPU pinned memory buffer
int faiss_GpuResources_getPinnedMemory(FaissGpuResources*, void**, size_t*);

/// Returns the stream on which we perform async CPU <-> GPU copies
int faiss_GpuResources_getAsyncCopyStream(FaissGpuResources*, int, cudaStream_t*);

/// Calls getBlasHandle with the current device
int faiss_GpuResources_getBlasHandleCurrentDevice(FaissGpuResources*, cublasHandle_t*);

/// Calls getDefaultStream with the current device
int faiss_GpuResources_getDefaultStreamCurrentDevice(FaissGpuResources*, cudaStream_t*);

/// Synchronizes the CPU with respect to the default stream for the
/// given device
// equivalent to cudaDeviceSynchronize(getDefaultStream(device))
int faiss_GpuResources_syncDefaultStream(FaissGpuResources*, int);

/// Calls syncDefaultStream for the current device
int faiss_GpuResources_syncDefaultStreamCurrentDevice(FaissGpuResources*);

/// Calls getAsyncCopyStream for the current device
int faiss_GpuResources_getAsyncCopyStreamCurrentDevice(FaissGpuResources*, cudaStream_t*);

#ifdef __cplusplus
}
#endif
#endif