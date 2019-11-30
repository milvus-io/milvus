/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda.h>

namespace faiss { namespace gpu {

#ifdef __CUDA_ARCH__
#if __CUDA_ARCH__ <= 750
constexpr int kWarpSize = 32;
#else
#error Unknown __CUDA_ARCH__; please define parameters for compute capability
#endif // __CUDA_ARCH__ types
#endif // __CUDA_ARCH__

#ifndef __CUDA_ARCH__
// dummy value for host compiler
constexpr int kWarpSize = 32;
#endif // !__CUDA_ARCH__

// This is a memory barrier for intra-warp writes to shared memory.
__forceinline__ __device__ void warpFence() {

#if CUDA_VERSION >= 9000
  __syncwarp();
#else
  // For the time being, assume synchronicity.
  //  __threadfence_block();
#endif
}

#if CUDA_VERSION > 9000
// Based on the CUDA version (we assume what version of nvcc/ptxas we were
// compiled with), the register allocation algorithm is much better, so only
// enable the 2048 selection code if we are above 9.0 (9.2 seems to be ok)
#define GPU_MAX_SELECTION_K 2048
#else
#define GPU_MAX_SELECTION_K 1024
#endif

} } // namespace
