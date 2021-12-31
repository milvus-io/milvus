/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/MergeNetworkUtils.cuh>
#include <faiss/gpu/utils/PtxUtils.cuh>
#include <faiss/gpu/utils/StaticUtils.h>
#include <faiss/gpu/utils/WarpShuffles.cuh>
#include <faiss/impl/FaissAssert.h>
#include <cuda.h>

namespace faiss { namespace gpu {

// Merge pairs of lists smaller than blockDim.x (NumThreads)
template <int NumThreads,
          typename K,
          typename V,
          int N,
          int L,
          bool AllThreads,
          bool Dir,
          typename Comp,
          bool FullMerge>
inline __device__ void blockMergeSmall(K* listK, V* listV) {
  static_assert(utils::isPowerOf2(L), "L must be a power-of-2");
  static_assert(utils::isPowerOf2(NumThreads),
                "NumThreads must be a power-of-2");
  static_assert(L <= NumThreads, "merge list size must be <= NumThreads");

  // Which pair of lists we are merging
  int mergeId = threadIdx.x / L;

  // Which thread we are within the merge
  int tid = threadIdx.x % L;

  // listK points to a region of size N * 2 * L
  listK += 2 * L * mergeId;
  listV += 2 * L * mergeId;

  // It's not a bitonic merge, both lists are in the same direction,
  // so handle the first swap assuming the second list is reversed
  int pos = L - 1 - tid;
  int stride = 2 * tid + 1;

  if (AllThreads || (threadIdx.x < N * L)) {
    K ka = listK[pos];
    K kb = listK[pos + stride];

    bool swap = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
    listK[pos] = swap ? kb : ka;
    listK[pos + stride] = swap ? ka : kb;

    V va = listV[pos];
    V vb = listV[pos + stride];
    listV[pos] = swap ? vb : va;
    listV[pos + stride] = swap ? va : vb;

    // FIXME: is this a CUDA 9 compiler bug?
    // K& ka = listK[pos];
    // K& kb = listK[pos + stride];

    // bool s = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
    // swap(s, ka, kb);

    // V& va = listV[pos];
    // V& vb = listV[pos + stride];
    // swap(s, va, vb);
  }

  __syncthreads();

#pragma unroll
  for (int stride = L / 2; stride > 0; stride /= 2) {
    int pos = 2 * tid - (tid & (stride - 1));

    if (AllThreads || (threadIdx.x < N * L)) {
      K ka = listK[pos];
      K kb = listK[pos + stride];

      bool swap = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
      listK[pos] = swap ? kb : ka;
      listK[pos + stride] = swap ? ka : kb;

      V va = listV[pos];
      V vb = listV[pos + stride];
      listV[pos] = swap ? vb : va;
      listV[pos + stride] = swap ? va : vb;

      // FIXME: is this a CUDA 9 compiler bug?
      // K& ka = listK[pos];
      // K& kb = listK[pos + stride];

      // bool s = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
      // swap(s, ka, kb);

      // V& va = listV[pos];
      // V& vb = listV[pos + stride];
      // swap(s, va, vb);
    }

    __syncthreads();
  }
}

// Merge pairs of sorted lists larger than blockDim.x (NumThreads)
template <int NumThreads,
          typename K,
          typename V,
          int L,
          bool Dir,
          typename Comp,
          bool FullMerge>
inline __device__ void blockMergeLarge(K* listK, V* listV) {
  static_assert(utils::isPowerOf2(L), "L must be a power-of-2");
  static_assert(L >= kWarpSize, "merge list size must be >= 32");
  static_assert(utils::isPowerOf2(NumThreads),
                "NumThreads must be a power-of-2");
  static_assert(L >= NumThreads, "merge list size must be >= NumThreads");

  // For L > NumThreads, each thread has to perform more work
  // per each stride.
  constexpr int kLoopPerThread = L / NumThreads;

  // It's not a bitonic merge, both lists are in the same direction,
  // so handle the first swap assuming the second list is reversed
#pragma unroll
  for (int loop = 0; loop < kLoopPerThread; ++loop) {
    int tid = loop * NumThreads + threadIdx.x;
    int pos = L - 1 - tid;
    int stride = 2 * tid + 1;

   K ka = listK[pos];
    K kb = listK[pos + stride];

    bool swap = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
    listK[pos] = swap ? kb : ka;
    listK[pos + stride] = swap ? ka : kb;

    V va = listV[pos];
    V vb = listV[pos + stride];
    listV[pos] = swap ? vb : va;
    listV[pos + stride] = swap ? va : vb;

    // FIXME: is this a CUDA 9 compiler bug?
    // K& ka = listK[pos];
    // K& kb = listK[pos + stride];

    // bool s = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
    // swap(s, ka, kb);

    // V& va = listV[pos];
    // V& vb = listV[pos + stride];
    // swap(s, va, vb);
  }

  __syncthreads();

  constexpr int kSecondLoopPerThread =
    FullMerge ? kLoopPerThread : kLoopPerThread / 2;

#pragma unroll
  for (int stride = L / 2; stride > 0; stride /= 2) {
#pragma unroll
    for (int loop = 0; loop < kSecondLoopPerThread; ++loop) {
      int tid = loop * NumThreads + threadIdx.x;
      int pos = 2 * tid - (tid & (stride - 1));

      K ka = listK[pos];
      K kb = listK[pos + stride];

      bool swap = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
      listK[pos] = swap ? kb : ka;
      listK[pos + stride] = swap ? ka : kb;

      V va = listV[pos];
      V vb = listV[pos + stride];
      listV[pos] = swap ? vb : va;
      listV[pos + stride] = swap ? va : vb;

      // FIXME: is this a CUDA 9 compiler bug?
      // K& ka = listK[pos];
      // K& kb = listK[pos + stride];

      // bool s = Dir ? Comp::gt(ka, kb) : Comp::lt(ka, kb);
      // swap(s, ka, kb);

      // V& va = listV[pos];
      // V& vb = listV[pos + stride];
      // swap(s, va, vb);
    }

    __syncthreads();
  }
}

/// Class template to prevent static_assert from firing for
/// mixing smaller/larger than block cases
template <int NumThreads,
          typename K,
          typename V,
          int N,
          int L,
          bool Dir,
          typename Comp,
          bool SmallerThanBlock,
          bool FullMerge>
struct BlockMerge {
};

/// Merging lists smaller than a block
template <int NumThreads,
          typename K,
          typename V,
          int N,
          int L,
          bool Dir,
          typename Comp,
          bool FullMerge>
struct BlockMerge<NumThreads, K, V, N, L, Dir, Comp, true, FullMerge> {
  static inline __device__ void merge(K* listK, V* listV) {
    constexpr int kNumParallelMerges = NumThreads / L;
    constexpr int kNumIterations = N / kNumParallelMerges;

    static_assert(L <= NumThreads, "list must be <= NumThreads");
    static_assert((N < kNumParallelMerges) ||
                  (kNumIterations * kNumParallelMerges == N),
                  "improper selection of N and L");

    if (N < kNumParallelMerges) {
      // We only need L threads per each list to perform the merge
      blockMergeSmall<NumThreads, K, V, N, L, false, Dir, Comp, FullMerge>(
        listK, listV);
    } else {
      // All threads participate
#pragma unroll
      for (int i = 0; i < kNumIterations; ++i) {
        int start = i * kNumParallelMerges * 2 * L;

        blockMergeSmall<NumThreads, K, V, N, L, true, Dir, Comp, FullMerge>(
          listK + start, listV + start);
      }
    }
  }
};

/// Merging lists larger than a block
template <int NumThreads,
          typename K,
          typename V,
          int N,
          int L,
          bool Dir,
          typename Comp,
          bool FullMerge>
struct BlockMerge<NumThreads, K, V, N, L, Dir, Comp, false, FullMerge> {
  static inline __device__ void merge(K* listK, V* listV) {
    // Each pair of lists is merged sequentially
#pragma unroll
    for (int i = 0; i < N; ++i) {
      int start = i * 2 * L;

      blockMergeLarge<NumThreads, K, V, L, Dir, Comp, FullMerge>(
        listK + start, listV + start);
    }
  }
};

template <int NumThreads,
          typename K,
          typename V,
          int N,
          int L,
          bool Dir,
          typename Comp,
          bool FullMerge = true>
inline __device__ void blockMerge(K* listK, V* listV) {
  constexpr bool kSmallerThanBlock = (L <= NumThreads);

  BlockMerge<NumThreads, K, V, N, L, Dir, Comp, kSmallerThanBlock, FullMerge>::
    merge(listK, listV);
}

} } // namespace
