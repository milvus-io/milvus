/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "../WarpSelectKernel.cuh"
#include "../Limits.cuh"

#define WARP_SELECT_DECL(TYPE, DIR, WARP_Q)                             \
  extern void runWarpSelect_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _(   \
    Tensor<TYPE, 2, true>& in,                                          \
    Tensor<TYPE, 2, true>& outK,                                        \
    Tensor<int, 2, true>& outV,                                         \
    bool dir,                                                           \
    int k,                                                              \
    cudaStream_t stream)

#define WARP_SELECT_IMPL(TYPE, DIR, WARP_Q, THREAD_Q)                   \
  void runWarpSelect_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _(          \
    Tensor<TYPE, 2, true>& in,                                          \
    Tensor<TYPE, 2, true>& outK,                                        \
    Tensor<int, 2, true>& outV,                                         \
    bool dir,                                                           \
    int k,                                                              \
    cudaStream_t stream) {                                              \
                                                                        \
    constexpr int kWarpSelectNumThreads = 128;                          \
    auto grid = dim3(utils::divUp(in.getSize(0),                        \
                                  (kWarpSelectNumThreads / kWarpSize))); \
    auto block = dim3(kWarpSelectNumThreads);                           \
                                                                        \
    FAISS_ASSERT(k <= WARP_Q);                                          \
    FAISS_ASSERT(dir == DIR);                                           \
                                                                        \
    auto kInit = dir ? Limits<TYPE>::getMin() : Limits<TYPE>::getMax(); \
    auto vInit = -1;                                                    \
                                                                        \
    warpSelect<TYPE, int, DIR, WARP_Q, THREAD_Q, kWarpSelectNumThreads> \
      <<<grid, block, 0, stream>>>(in, outK, outV, kInit, vInit, k);    \
    CUDA_TEST_ERROR();                                                  \
  }

#define WARP_SELECT_CALL(TYPE, DIR, WARP_Q)                     \
  runWarpSelect_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _(       \
    in, outK, outV, dir, k, stream)
