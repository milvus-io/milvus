/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "VectorResidual.cuh"
#include "../../FaissAssert.h"
#include "../utils/ConversionOperators.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/Tensor.cuh"
#include "../utils/StaticUtils.h"
#include <math_constants.h> // in CUDA SDK, for CUDART_NAN_F

namespace faiss { namespace gpu {

template <typename CentroidT, bool LargeDim>
__global__ void calcResidual(Tensor<float, 2, true> vecs,
                             Tensor<CentroidT, 2, true> centroids,
                             Tensor<int, 1, true> vecToCentroid,
                             Tensor<float, 2, true> residuals) {
  auto vec = vecs[blockIdx.x];
  auto residual = residuals[blockIdx.x];

  int centroidId = vecToCentroid[blockIdx.x];
  // Vector could be invalid (containing NaNs), so -1 was the
  // classified centroid
  if (centroidId == -1) {
    if (LargeDim) {
      for (int i = threadIdx.x; i < vecs.getSize(1); i += blockDim.x) {
        residual[i] = CUDART_NAN_F;
      }
    } else {
      residual[threadIdx.x] = CUDART_NAN_F;
    }

    return;
  }

  auto centroid = centroids[centroidId];

  if (LargeDim) {
    for (int i = threadIdx.x; i < vecs.getSize(1); i += blockDim.x) {
      residual[i] = vec[i] - ConvertTo<float>::to(centroid[i]);
    }
  } else {
    residual[threadIdx.x] = vec[threadIdx.x] -
      ConvertTo<float>::to(centroid[threadIdx.x]);
  }
}

template <typename CentroidT>
void calcResidual(Tensor<float, 2, true>& vecs,
                  Tensor<CentroidT, 2, true>& centroids,
                  Tensor<int, 1, true>& vecToCentroid,
                  Tensor<float, 2, true>& residuals,
                  cudaStream_t stream) {
  FAISS_ASSERT(vecs.getSize(1) == centroids.getSize(1));
  FAISS_ASSERT(vecs.getSize(1) == residuals.getSize(1));
  FAISS_ASSERT(vecs.getSize(0) == vecToCentroid.getSize(0));
  FAISS_ASSERT(vecs.getSize(0) == residuals.getSize(0));

  dim3 grid(vecs.getSize(0));

  int maxThreads = getMaxThreadsCurrentDevice();
  bool largeDim = vecs.getSize(1) > maxThreads;
  dim3 block(std::min(vecs.getSize(1), maxThreads));

  if (largeDim) {
    calcResidual<CentroidT, true><<<grid, block, 0, stream>>>(
      vecs, centroids, vecToCentroid, residuals);
  } else {
    calcResidual<CentroidT, false><<<grid, block, 0, stream>>>(
      vecs, centroids, vecToCentroid, residuals);
  }

  CUDA_TEST_ERROR();
}

void runCalcResidual(Tensor<float, 2, true>& vecs,
                     Tensor<float, 2, true>& centroids,
                     Tensor<int, 1, true>& vecToCentroid,
                     Tensor<float, 2, true>& residuals,
                     cudaStream_t stream) {
  calcResidual<float>(vecs, centroids, vecToCentroid, residuals, stream);
}

#ifdef FAISS_USE_FLOAT16
void runCalcResidual(Tensor<float, 2, true>& vecs,
                     Tensor<half, 2, true>& centroids,
                     Tensor<int, 1, true>& vecToCentroid,
                     Tensor<float, 2, true>& residuals,
                     cudaStream_t stream) {
  calcResidual<half>(vecs, centroids, vecToCentroid, residuals, stream);
}
#endif

} } // namespace
