/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda.h>

#if CUDA_VERSION >= 8000
// Whether or not we enable usage of CUDA Unified Memory
#define FAISS_UNIFIED_MEM 1
#endif

namespace faiss { namespace gpu {

enum MemorySpace {
  /// Managed using cudaMalloc/cudaFree
  Device = 1,
  /// Managed using cudaMallocManaged/cudaFree
  Unified = 2,
  /// Managed using cudaHostAlloc/cudaFreeHost
  HostPinned = 3,
};

/// All memory allocations and de-allocations come through these functions

/// Allocates CUDA memory for a given memory space (void pointer)
/// Throws a FaissException if we are unable to allocate the memory
void allocMemorySpaceV(MemorySpace space, void** p, size_t size);

template <typename T>
inline void allocMemorySpace(MemorySpace space, T** p, size_t size) {
  allocMemorySpaceV(space, (void**)(void*) p, size);
}

/// Frees CUDA memory for a given memory space
/// Asserts if we are unable to free the region
void freeMemorySpace(MemorySpace space, void* p);

} }
