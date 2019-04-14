/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "MemorySpace.h"
#include "../../FaissAssert.h"
#include <cuda_runtime.h>

namespace faiss { namespace gpu {

/// Allocates CUDA memory for a given memory space
void allocMemorySpaceV(MemorySpace space, void** p, size_t size) {
  switch (space) {
    case MemorySpace::Device:
    {
      auto err = cudaMalloc(p, size);

      // Throw if we fail to allocate
      FAISS_THROW_IF_NOT_FMT(
        err == cudaSuccess,
        "failed to cudaMalloc %zu bytes (error %d %s)",
        size, (int) err, cudaGetErrorString(err));
    }
    break;
    case MemorySpace::Unified:
    {
#ifdef FAISS_UNIFIED_MEM
      auto err = cudaMallocManaged(p, size);

      // Throw if we fail to allocate
      FAISS_THROW_IF_NOT_FMT(
        err == cudaSuccess,
        "failed to cudaMallocManaged %zu bytes (error %d %s)",
        size, (int) err, cudaGetErrorString(err));
#else
      FAISS_THROW_MSG("Attempting to allocate via cudaMallocManaged "
                      "without CUDA 8+ support");
#endif
    }
    break;
    case MemorySpace::HostPinned:
    {
      auto err = cudaHostAlloc(p, size, cudaHostAllocDefault);

      // Throw if we fail to allocate
      FAISS_THROW_IF_NOT_FMT(
        err == cudaSuccess,
        "failed to cudaHostAlloc %zu bytes (error %d %s)",
        size, (int) err, cudaGetErrorString(err));
    }
    break;
    default:
      FAISS_ASSERT_FMT(false, "unknown MemorySpace %d", (int) space);
      break;
  }
}

// We'll allow allocation to fail, but free should always succeed and be a
// fatal error if it doesn't free
void freeMemorySpace(MemorySpace space, void* p) {
  switch (space) {
    case MemorySpace::Device:
    case MemorySpace::Unified:
    {
      auto err = cudaFree(p);
      FAISS_ASSERT_FMT(err == cudaSuccess,
                       "Failed to cudaFree pointer %p (error %d %s)",
                       p, (int) err, cudaGetErrorString(err));
    }
    break;
    case MemorySpace::HostPinned:
    {
      auto err = cudaFreeHost(p);
      FAISS_ASSERT_FMT(err == cudaSuccess,
                       "Failed to cudaFreeHost pointer %p (error %d %s)",
                       p, (int) err, cudaGetErrorString(err));
    }
    break;
    default:
      FAISS_ASSERT_FMT(false, "unknown MemorySpace %d", (int) space);
      break;
  }
}

} }
