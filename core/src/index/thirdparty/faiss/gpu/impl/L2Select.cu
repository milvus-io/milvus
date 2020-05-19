/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/impl/L2Select.cuh>
#include <faiss/impl/FaissAssert.h>

#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/MathOperators.cuh>
#include <faiss/gpu/utils/Pair.cuh>
#include <faiss/gpu/utils/Reductions.cuh>
#include <faiss/gpu/utils/Select.cuh>
#include <faiss/gpu/utils/Tensor.cuh>
#include <faiss/gpu/utils/StaticUtils.h>

namespace faiss { namespace gpu {

// L2 + select kernel for k == 1, implements re-use of ||c||^2
template <typename T, int kRowsPerBlock, int kBlockSize>
__global__ void l2SelectMin1(Tensor<T, 2, true> productDistances,
                             Tensor<T, 1, true> centroidDistances,
                             Tensor<T, 2, true> outDistances,
                             Tensor<int, 2, true> outIndices) {
  // Each block handles kRowsPerBlock rows of the distances (results)
  Pair<T, int> threadMin[kRowsPerBlock];
  __shared__ Pair<T, int> blockMin[kRowsPerBlock * (kBlockSize / kWarpSize)];

  T distance[kRowsPerBlock];

#pragma unroll
  for (int i = 0; i < kRowsPerBlock; ++i) {
    threadMin[i].k = Limits<T>::getMax();
    threadMin[i].v = -1;
  }

  // blockIdx.x: which chunk of rows we are responsible for updating
  int rowStart = blockIdx.x * kRowsPerBlock;

  // FIXME: if we have exact multiples, don't need this
  bool endRow = (blockIdx.x == gridDim.x - 1);

  if (endRow) {
    if (productDistances.getSize(0) % kRowsPerBlock == 0) {
      endRow = false;
    }
  }

  if (endRow) {
    for (int row = rowStart; row < productDistances.getSize(0); ++row) {
      for (int col = threadIdx.x; col < productDistances.getSize(1);
           col += blockDim.x) {
        distance[0] = Math<T>::add(centroidDistances[col],
                                   productDistances[row][col]);

        if (Math<T>::lt(distance[0], threadMin[0].k)) {
          threadMin[0].k = distance[0];
          threadMin[0].v = col;
        }
      }

      // Reduce within the block
      threadMin[0] =
        blockReduceAll<Pair<T, int>, Min<Pair<T, int>>, false, false>(
        threadMin[0], Min<Pair<T, int>>(), blockMin);

      if (threadIdx.x == 0) {
        outDistances[row][0] = threadMin[0].k;
        outIndices[row][0] = threadMin[0].v;
      }

      // so we can use the shared memory again
      __syncthreads();

      threadMin[0].k = Limits<T>::getMax();
      threadMin[0].v = -1;
    }
  } else {
    for (int col = threadIdx.x; col < productDistances.getSize(1);
         col += blockDim.x) {
      T centroidDistance = centroidDistances[col];

#pragma unroll
      for (int row = 0; row < kRowsPerBlock; ++row) {
        distance[row] = productDistances[rowStart + row][col];
      }

#pragma unroll
      for (int row = 0; row < kRowsPerBlock; ++row) {
        distance[row] = Math<T>::add(distance[row], centroidDistance);
      }

#pragma unroll
      for (int row = 0; row < kRowsPerBlock; ++row) {
        if (Math<T>::lt(distance[row], threadMin[row].k)) {
          threadMin[row].k = distance[row];
          threadMin[row].v = col;
        }
      }
    }

    // Reduce within the block
    blockReduceAll<kRowsPerBlock,
                   Pair<T, int>,
                   Min<Pair<T, int> >,
                   false,
                   false>(threadMin,
                          Min<Pair<T, int> >(),
                          blockMin);

    if (threadIdx.x == 0) {
#pragma unroll
      for (int row = 0; row < kRowsPerBlock; ++row) {
        outDistances[rowStart + row][0] = threadMin[row].k;
        outIndices[rowStart + row][0] = threadMin[row].v;
      }
    }
  }
}

// L2 + select kernel for k > 1, no re-use of ||c||^2
template <typename T, int NumWarpQ, int NumThreadQ, int ThreadsPerBlock>
__global__ void l2SelectMinK(Tensor<T, 2, true> productDistances,
                             Tensor<T, 1, true> centroidDistances,
                             Tensor<T, 2, true> outDistances,
                             Tensor<int, 2, true> outIndices,
                             int k, T initK) {
  // Each block handles a single row of the distances (results)
  constexpr int kNumWarps = ThreadsPerBlock / kWarpSize;

  __shared__ T smemK[kNumWarps * NumWarpQ];
  __shared__ int smemV[kNumWarps * NumWarpQ];

  BlockSelect<T, int, false, Comparator<T>,
              NumWarpQ, NumThreadQ, ThreadsPerBlock>
    heap(initK, -1, smemK, smemV, k);

  int row = blockIdx.x;

  // Whole warps must participate in the selection
  int limit = utils::roundDown(productDistances.getSize(1), kWarpSize);
  int i = threadIdx.x;

  for (; i < limit; i += blockDim.x) {
    T v = Math<T>::add(centroidDistances[i],
                       productDistances[row][i]);
    heap.add(v, i);
  }

  if (i < productDistances.getSize(1)) {
    T v = Math<T>::add(centroidDistances[i],
                       productDistances[row][i]);
    heap.addThreadQ(v, i);
  }

  heap.reduce();
  for (int i = threadIdx.x; i < k; i += blockDim.x) {
    outDistances[row][i] = smemK[i];
    outIndices[row][i] = smemV[i];
  }
}

template <typename T>
void runL2SelectMin(Tensor<T, 2, true>& productDistances,
                    Tensor<T, 1, true>& centroidDistances,
                    Tensor<T, 2, true>& outDistances,
                    Tensor<int, 2, true>& outIndices,
                    int k,
                    cudaStream_t stream) {
  FAISS_ASSERT(productDistances.getSize(0) == outDistances.getSize(0));
  FAISS_ASSERT(productDistances.getSize(0) == outIndices.getSize(0));
  FAISS_ASSERT(centroidDistances.getSize(0) == productDistances.getSize(1));
  FAISS_ASSERT(outDistances.getSize(1) == k);
  FAISS_ASSERT(outIndices.getSize(1) == k);
  FAISS_ASSERT(k <= GPU_MAX_SELECTION_K);

  if (k == 1) {
    constexpr int kThreadsPerBlock = 256;
    constexpr int kRowsPerBlock = 8;

    auto block = dim3(kThreadsPerBlock);
    auto grid = dim3(utils::divUp(outDistances.getSize(0), kRowsPerBlock));

    l2SelectMin1<T, kRowsPerBlock, kThreadsPerBlock>
      <<<grid, block, 0, stream>>>(productDistances, centroidDistances,
                                   outDistances, outIndices);
  } else {
    auto grid = dim3(outDistances.getSize(0));

#define RUN_L2_SELECT(BLOCK, NUM_WARP_Q, NUM_THREAD_Q)                  \
    do {                                                                \
      l2SelectMinK<T, NUM_WARP_Q, NUM_THREAD_Q, BLOCK>                  \
        <<<grid, BLOCK, 0, stream>>>(productDistances, centroidDistances, \
                                     outDistances, outIndices,          \
                                     k, Limits<T>::getMax());           \
    } while (0)

    // block size 128 for everything <= 1024
    if (k <= 32) {
      RUN_L2_SELECT(128, 32, 2);
    } else if (k <= 64) {
      RUN_L2_SELECT(128, 64, 3);
    } else if (k <= 128) {
      RUN_L2_SELECT(128, 128, 3);
    } else if (k <= 256) {
      RUN_L2_SELECT(128, 256, 4);
    } else if (k <= 512) {
      RUN_L2_SELECT(128, 512, 8);
    } else if (k <= 1024) {
      RUN_L2_SELECT(128, 1024, 8);

#if GPU_MAX_SELECTION_K >= 2048
    } else if (k <= 2048) {
      // smaller block for less shared memory
      RUN_L2_SELECT(64, 2048, 8);
#endif

    } else {
      FAISS_ASSERT(false);
    }
  }

  CUDA_TEST_ERROR();
}

void runL2SelectMin(Tensor<float, 2, true>& productDistances,
                    Tensor<float, 1, true>& centroidDistances,
                    Tensor<float, 2, true>& outDistances,
                    Tensor<int, 2, true>& outIndices,
                    int k,
                    cudaStream_t stream) {
  runL2SelectMin<float>(productDistances,
                        centroidDistances,
                        outDistances,
                        outIndices,
                        k,
                        stream);
}

} } // namespace
