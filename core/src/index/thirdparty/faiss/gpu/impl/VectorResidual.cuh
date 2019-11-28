/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/utils/Tensor.cuh>

namespace faiss { namespace gpu {

// Calculates residual v_i - c_j for all v_i in vecs where j = vecToCentroid[i]
void runCalcResidual(Tensor<float, 2, true>& vecs,
                     Tensor<float, 2, true>& centroids,
                     Tensor<int, 1, true>& vecToCentroid,
                     Tensor<float, 2, true>& residuals,
                     cudaStream_t stream);

void runCalcResidual(Tensor<float, 2, true>& vecs,
                     Tensor<half, 2, true>& centroids,
                     Tensor<int, 1, true>& vecToCentroid,
                     Tensor<float, 2, true>& residuals,
                     cudaStream_t stream);

// Gather vectors
void runReconstruct(Tensor<int, 1, true>& listIds,
                    Tensor<float, 2, true>& vecs,
                    Tensor<float, 2, true>& out,
                    cudaStream_t stream);

void runReconstruct(Tensor<int, 1, true>& listIds,
                    Tensor<half, 2, true>& vecs,
                    Tensor<float, 2, true>& out,
                    cudaStream_t stream);

} } // namespace
