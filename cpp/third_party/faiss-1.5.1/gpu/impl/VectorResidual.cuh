/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "../utils/Tensor.cuh"
#include "../utils/Float16.cuh"

namespace faiss { namespace gpu {

// Calculates residual v_i - c_j for all v_i in vecs where j = vecToCentroid[i]
void runCalcResidual(Tensor<float, 2, true>& vecs,
                     Tensor<float, 2, true>& centroids,
                     Tensor<int, 1, true>& vecToCentroid,
                     Tensor<float, 2, true>& residuals,
                     cudaStream_t stream);

#ifdef FAISS_USE_FLOAT16
void runCalcResidual(Tensor<float, 2, true>& vecs,
                     Tensor<half, 2, true>& centroids,
                     Tensor<int, 1, true>& vecToCentroid,
                     Tensor<float, 2, true>& residuals,
                     cudaStream_t stream);
#endif

} } // namespace
