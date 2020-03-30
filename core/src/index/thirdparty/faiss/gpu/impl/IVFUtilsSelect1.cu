/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/impl/IVFUtils.cuh>
#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/Limits.cuh>
#include <faiss/gpu/utils/Select.cuh>
#include <faiss/gpu/utils/StaticUtils.h>
#include <faiss/gpu/utils/Tensor.cuh>

//
// This kernel is split into a separate compilation unit to cut down
// on compile time
//

namespace faiss { namespace gpu {

template <int ThreadsPerBlock, int NumWarpQ, int NumThreadQ, bool Dir>
__global__ void
pass1SelectLists(Tensor<int, 2, true> prefixSumOffsets,
                 Tensor<uint8_t, 1, true> bitset,
                 Tensor<float, 1, true> distance,
                 int nprobe,
                 int k,
                 Tensor<float, 3, true> heapDistances,
                 Tensor<int, 3, true> heapIndices) {
  constexpr int kNumWarps = ThreadsPerBlock / kWarpSize;

  __shared__ float smemK[kNumWarps * NumWarpQ];
  __shared__ int smemV[kNumWarps * NumWarpQ];

  constexpr auto kInit = Dir ? kFloatMin : kFloatMax;
  BlockSelect<float, int, Dir, Comparator<float>,
              NumWarpQ, NumThreadQ, ThreadsPerBlock>
    heap(kInit, -1, smemK, smemV, k);

  auto queryId = blockIdx.y;
  auto sliceId = blockIdx.x;
  auto numSlices = gridDim.x;

  int sliceSize = (nprobe / numSlices);
  int sliceStart = sliceSize * sliceId;
  int sliceEnd = sliceId == (numSlices - 1) ? nprobe :
    sliceStart + sliceSize;
  auto offsets = prefixSumOffsets[queryId].data();

  // We ensure that before the array (at offset -1), there is a 0 value
  int start = *(&offsets[sliceStart] - 1);
  int end = offsets[sliceEnd - 1];

  int num = end - start;
  int limit = utils::roundDown(num, kWarpSize);

  int i = threadIdx.x;
  auto distanceStart = distance[start].data();
  bool bitsetEmpty = (bitset.getSize(0) == 0);
  int blockId, threadId;
  int idx;

  // BlockSelect add cannot be used in a warp divergent circumstance; we
  // handle the remainder warp below
  for (; i < limit; i += blockDim.x) {
    /* 2D-grid, 1D-block */
    //blockId = blockIdx.x + blockIdx.y * gridDim.x + blockIdx.z * gridDim.x * gridDim.y;
    //threadId = blockId * (blockDim.x * blockDim.y * blockDim.z) + (threadIdx.z * (blockDim.x * blockDim.y))
    //           + (threadIdx.y * blockDim.x) + threadIdx.x;
    blockId = blockIdx.y * gridDim.x + blockIdx.x;
    threadId = blockId * blockDim.x + i;
    idx = start + i;
//      printf("CYD(%s:%d) - gridDim(%d, %d, %d), blockDim(%d, %d, %d), blockIdx(%d, %d, %d), threadIdx(%d, %d),"
//             "start = %d, i = %d, blockId = %d, threadId = %d, bitset %s\n",
//             __FUNCTION__, __LINE__,
//             gridDim.x, gridDim.y, gridDim.z,
//             blockDim.x, blockDim.y, blockDim.z,
//             blockIdx.x, blockIdx.y, blockIdx.z,
//             threadIdx.x, threadIdx.y,
//             start, i, blockId, threadId, ((bitset[threadId >> 3] & (0x1 << (threadId & 0x7))))? "TRUE ===========" : "FALSE");
    if (bitsetEmpty || (!(bitset[threadId >> 3] & (0x1 << (threadId & 0x7))))) {
      heap.add(distanceStart[i], start + i);
//      printf("CYD - add %f %d\n", distanceStart[i], start + i);
    } else {
      heap.add((1.0 / 0.0), start + i);
//      printf("CYD - add %f %d\n", (1.0/0.0), start + i);
    }
  }

  // Handle warp divergence separately
  if (i < num) {
    blockId = blockIdx.y * gridDim.x + blockIdx.x;
    threadId = blockId * blockDim.x + i;
    if (bitsetEmpty || (!(bitset[threadId >> 3] & (0x1 << (threadId & 0x7))))) {
      heap.addThreadQ(distanceStart[i], start + i);
    } else {
      heap.addThreadQ((1.0 / 0.0), start + i);
//      printf("CYD(%s:%d) - blockDim(%d, %d), blockIdx(%d, %d), threadIdx(%d, %d), i = %d, blockId = %d, threadId = %d, bitset %s\n",
//             __FUNCTION__, __LINE__, blockDim.x, blockDim.y, blockIdx.x, blockIdx.y, threadIdx.x, threadIdx.y,
//             i, blockId, threadId, ((bitset[threadId >> 3] & (0x1 << (threadId & 0x7))))? "TRUE ===========" : "FALSE");
    }
  }

  // Merge all final results
  heap.reduce();

  // Write out the final k-selected values; they should be all
  // together
  for (int i = threadIdx.x; i < k; i += blockDim.x) {
    printf("XY - i = %d, %d\n", i, smemV[i]);
    heapDistances[queryId][sliceId][i] = smemK[i];
    heapIndices[queryId][sliceId][i] = smemV[i];
  }
}

void
runPass1SelectLists(Tensor<int, 2, true>& prefixSumOffsets,
                    Tensor<uint8_t, 1, true>& bitset,
                    Tensor<float, 1, true>& distance,
                    int nprobe,
                    int k,
                    bool chooseLargest,
                    Tensor<float, 3, true>& heapDistances,
                    Tensor<int, 3, true>& heapIndices,
                    cudaStream_t stream) {
  // This is caught at a higher level
  FAISS_ASSERT(k <= GPU_MAX_SELECTION_K);

  auto grid = dim3(heapDistances.getSize(1), prefixSumOffsets.getSize(0));

#define RUN_PASS(BLOCK, NUM_WARP_Q, NUM_THREAD_Q, DIR)                  \
  do {                                                                  \
    pass1SelectLists<BLOCK, NUM_WARP_Q, NUM_THREAD_Q, DIR>              \
      <<<grid, BLOCK, 0, stream>>>(prefixSumOffsets,                    \
                                   bitset,                              \
                                   distance,                            \
                                   nprobe,                              \
                                   k,                                   \
                                   heapDistances,                       \
                                   heapIndices);                        \
    CUDA_TEST_ERROR();                                                  \
    return; /* success */                                               \
  } while (0)

#if GPU_MAX_SELECTION_K >= 2048

  // block size 128 for k <= 1024, 64 for k = 2048
#define RUN_PASS_DIR(DIR)                                 \
  do {                                                    \
    if (k == 1) {                                         \
      RUN_PASS(128, 1, 1, DIR);                           \
    } else if (k <= 32) {                                 \
      RUN_PASS(128, 32, 2, DIR);                          \
    } else if (k <= 64) {                                 \
      RUN_PASS(128, 64, 3, DIR);                          \
    } else if (k <= 128) {                                \
      RUN_PASS(128, 128, 3, DIR);                         \
    } else if (k <= 256) {                                \
      RUN_PASS(128, 256, 4, DIR);                         \
    } else if (k <= 512) {                                \
      RUN_PASS(128, 512, 8, DIR);                         \
    } else if (k <= 1024) {                               \
      RUN_PASS(128, 1024, 8, DIR);                        \
    } else if (k <= 2048) {                               \
      RUN_PASS(64, 2048, 8, DIR);                         \
    }                                                     \
  } while (0)

#else

#define RUN_PASS_DIR(DIR)                                 \
  do {                                                    \
    if (k == 1) {                                         \
      RUN_PASS(128, 1, 1, DIR);                           \
    } else if (k <= 32) {                                 \
      RUN_PASS(128, 32, 2, DIR);                          \
    } else if (k <= 64) {                                 \
      RUN_PASS(128, 64, 3, DIR);                          \
    } else if (k <= 128) {                                \
      RUN_PASS(128, 128, 3, DIR);                         \
    } else if (k <= 256) {                                \
      RUN_PASS(128, 256, 4, DIR);                         \
    } else if (k <= 512) {                                \
      RUN_PASS(128, 512, 8, DIR);                         \
    } else if (k <= 1024) {                               \
      RUN_PASS(128, 1024, 8, DIR);                        \
    }                                                     \
  } while (0)

#endif // GPU_MAX_SELECTION_K

  if (chooseLargest) {
    RUN_PASS_DIR(true);
  } else {
    RUN_PASS_DIR(false);
  }

#undef RUN_PASS_DIR
#undef RUN_PASS
}

} } // namespace
