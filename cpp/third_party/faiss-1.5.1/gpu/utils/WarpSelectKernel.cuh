/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "Float16.cuh"
#include "Select.cuh"

namespace faiss { namespace gpu {

template <typename K,
          typename IndexType,
          bool Dir,
          int NumWarpQ,
          int NumThreadQ,
          int ThreadsPerBlock>
__global__ void warpSelect(Tensor<K, 2, true> in,
                           Tensor<K, 2, true> outK,
                           Tensor<IndexType, 2, true> outV,
                           K initK,
                           IndexType initV,
                           int k) {
  constexpr int kNumWarps = ThreadsPerBlock / kWarpSize;

  WarpSelect<K, IndexType, Dir, Comparator<K>,
                NumWarpQ, NumThreadQ, ThreadsPerBlock>
    heap(initK, initV, k);

  int warpId = threadIdx.x / kWarpSize;
  int row = blockIdx.x * kNumWarps + warpId;

  if (row >= in.getSize(0)) {
    return;
  }

  int i = getLaneId();
  K* inStart = in[row][i].data();

  // Whole warps must participate in the selection
  int limit = utils::roundDown(in.getSize(1), kWarpSize);

  for (; i < limit; i += kWarpSize) {
    heap.add(*inStart, (IndexType) i);
    inStart += kWarpSize;
  }

  // Handle non-warp multiple remainder
  if (i < in.getSize(1)) {
    heap.addThreadQ(*inStart, (IndexType) i);
  }

  heap.reduce();
  heap.writeOut(outK[row].data(),
                outV[row].data(), k);
}

void runWarpSelect(Tensor<float, 2, true>& in,
                      Tensor<float, 2, true>& outKeys,
                      Tensor<int, 2, true>& outIndices,
                      bool dir, int k, cudaStream_t stream);

#ifdef FAISS_USE_FLOAT16
void runWarpSelect(Tensor<half, 2, true>& in,
                      Tensor<half, 2, true>& outKeys,
                      Tensor<int, 2, true>& outIndices,
                      bool dir, int k, cudaStream_t stream);
#endif

} } // namespace
