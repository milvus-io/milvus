/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "IVFUtils.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/StaticUtils.h"
#include "../utils/Tensor.cuh"
#include "../utils/ThrustAllocator.cuh"
#include <thrust/scan.h>
#include <thrust/execution_policy.h>

namespace faiss { namespace gpu {

// Calculates the total number of intermediate distances to consider
// for all queries
__global__ void
getResultLengths(Tensor<int, 2, true> topQueryToCentroid,
                 int* listLengths,
                 int totalSize,
                 Tensor<int, 2, true> length) {
  int linearThreadId = blockIdx.x * blockDim.x + threadIdx.x;
  if (linearThreadId >= totalSize) {
    return;
  }

  int nprobe = topQueryToCentroid.getSize(1);
  int queryId = linearThreadId / nprobe;
  int listId = linearThreadId % nprobe;

  int centroidId = topQueryToCentroid[queryId][listId];

  // Safety guard in case NaNs in input cause no list ID to be generated
  length[queryId][listId] = (centroidId != -1) ? listLengths[centroidId] : 0;
}

void runCalcListOffsets(Tensor<int, 2, true>& topQueryToCentroid,
                        thrust::device_vector<int>& listLengths,
                        Tensor<int, 2, true>& prefixSumOffsets,
                        Tensor<char, 1, true>& thrustMem,
                        cudaStream_t stream) {
  FAISS_ASSERT(topQueryToCentroid.getSize(0) == prefixSumOffsets.getSize(0));
  FAISS_ASSERT(topQueryToCentroid.getSize(1) == prefixSumOffsets.getSize(1));

  int totalSize = topQueryToCentroid.numElements();

  int numThreads = std::min(totalSize, getMaxThreadsCurrentDevice());
  int numBlocks = utils::divUp(totalSize, numThreads);

  auto grid = dim3(numBlocks);
  auto block = dim3(numThreads);

  getResultLengths<<<grid, block, 0, stream>>>(
    topQueryToCentroid,
    listLengths.data().get(),
    totalSize,
    prefixSumOffsets);
  CUDA_TEST_ERROR();

  // Prefix sum of the indices, so we know where the intermediate
  // results should be maintained
  // Thrust wants a place for its temporary allocations, so provide
  // one, so it won't call cudaMalloc/Free
  GpuResourcesThrustAllocator alloc(thrustMem.data(),
                                    thrustMem.getSizeInBytes());

  thrust::inclusive_scan(thrust::cuda::par(alloc).on(stream),
                         prefixSumOffsets.data(),
                         prefixSumOffsets.data() + totalSize,
                         prefixSumOffsets.data());
  CUDA_TEST_ERROR();
}

} } // namespace
