/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "../BlockSelectKernel.cuh"
#include "../Limits.cuh"

#define BLOCK_SELECT_DECL(TYPE, DIR, WARP_Q)                            \
  extern void runBlockSelect_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _(  \
    Tensor<TYPE, 2, true>& in,                                          \
    Tensor<TYPE, 2, true>& outK,                                        \
    Tensor<int, 2, true>& outV,                                         \
    bool dir,                                                           \
    int k,                                                              \
    cudaStream_t stream);                                               \
                                                                        \
  extern void runBlockSelectPair_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _( \
    Tensor<TYPE, 2, true>& inK,                                         \
    Tensor<int, 2, true>& inV,                                          \
    Tensor<TYPE, 2, true>& outK,                                        \
    Tensor<int, 2, true>& outV,                                         \
    bool dir,                                                           \
    int k,                                                              \
    cudaStream_t stream)

#define BLOCK_SELECT_IMPL(TYPE, DIR, WARP_Q, THREAD_Q)                  \
  void runBlockSelect_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _(         \
    Tensor<TYPE, 2, true>& in,                                          \
    Tensor<TYPE, 2, true>& outK,                                        \
    Tensor<int, 2, true>& outV,                                         \
    bool dir,                                                           \
    int k,                                                              \
    cudaStream_t stream) {                                              \
    FAISS_ASSERT(in.getSize(0) == outK.getSize(0));                     \
    FAISS_ASSERT(in.getSize(0) == outV.getSize(0));                     \
    FAISS_ASSERT(outK.getSize(1) == k);                                 \
    FAISS_ASSERT(outV.getSize(1) == k);                                 \
                                                                        \
    auto grid = dim3(in.getSize(0));                                    \
                                                                        \
    constexpr int kBlockSelectNumThreads = (WARP_Q <= 1024) ? 128 : 64; \
    auto block = dim3(kBlockSelectNumThreads);                          \
                                                                        \
    FAISS_ASSERT(k <= WARP_Q);                                          \
    FAISS_ASSERT(dir == DIR);                                           \
                                                                        \
    auto kInit = dir ? Limits<TYPE>::getMin() : Limits<TYPE>::getMax(); \
    auto vInit = -1;                                                    \
                                                                        \
    blockSelect<TYPE, int, DIR, WARP_Q, THREAD_Q, kBlockSelectNumThreads> \
      <<<grid, block, 0, stream>>>(in, outK, outV, kInit, vInit, k);    \
    CUDA_TEST_ERROR();                                                  \
  }                                                                     \
                                                                        \
  void runBlockSelectPair_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _(     \
    Tensor<TYPE, 2, true>& inK,                                         \
    Tensor<int, 2, true>& inV,                                          \
    Tensor<TYPE, 2, true>& outK,                                        \
    Tensor<int, 2, true>& outV,                                         \
    bool dir,                                                           \
    int k,                                                              \
    cudaStream_t stream) {                                              \
    FAISS_ASSERT(inK.isSameSize(inV));                                  \
    FAISS_ASSERT(outK.isSameSize(outV));                                \
                                                                        \
    auto grid = dim3(inK.getSize(0));                                   \
                                                                        \
    constexpr int kBlockSelectNumThreads = (WARP_Q <= 1024) ? 128 : 64; \
    auto block = dim3(kBlockSelectNumThreads);                          \
                                                                        \
    FAISS_ASSERT(k <= WARP_Q);                                          \
    FAISS_ASSERT(dir == DIR);                                           \
                                                                        \
    auto kInit = dir ? Limits<TYPE>::getMin() : Limits<TYPE>::getMax(); \
    auto vInit = -1;                                                    \
                                                                        \
    blockSelectPair<TYPE, int, DIR, WARP_Q, THREAD_Q, kBlockSelectNumThreads> \
      <<<grid, block, 0, stream>>>(inK, inV, outK, outV, kInit, vInit, k); \
    CUDA_TEST_ERROR();                                                  \
  }


#define BLOCK_SELECT_CALL(TYPE, DIR, WARP_Q)                    \
  runBlockSelect_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _(      \
    in, outK, outV, dir, k, stream)

#define BLOCK_SELECT_PAIR_CALL(TYPE, DIR, WARP_Q)               \
  runBlockSelectPair_ ## TYPE ## _ ## DIR ## _ ## WARP_Q ## _(  \
    inK, inV, outK, outV, dir, k, stream)
