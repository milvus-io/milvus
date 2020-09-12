/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/utils/Tensor.cuh>

namespace faiss { namespace gpu {

void runL2SelectMin(Tensor<float, 2, true>& productDistances,
                    Tensor<float, 1, true>& centroidDistances,
                    Tensor<uint8_t, 1, true>& bitset,
                    Tensor<float, 2, true>& outDistances,
                    Tensor<int, 2, true>& outIndices,
                    int k,
                    cudaStream_t stream);
<<<<<<< HEAD
=======
                    
void runL2SelectMin(float* outDis_h,
                    int* outInd_h,
                    int startPos,
                    int curQuerySize,
                    int i,
                    int nprobe,
                    Tensor<float, 2, true>& productDistances,
                    Tensor<float, 1, true>& centroidDistances,
                    Tensor<float, 2, true>& outDistances,
                    Tensor<int, 2, true>& outIndices,
                    int k,
                    cudaStream_t stream);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda

} } // namespace
