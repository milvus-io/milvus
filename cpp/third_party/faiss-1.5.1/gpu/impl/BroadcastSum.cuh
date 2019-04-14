/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "../utils/Float16.cuh"
#include "../utils/Tensor.cuh"

namespace faiss { namespace gpu {

// output[x][i] += input[i] for all x
void runSumAlongColumns(Tensor<float, 1, true>& input,
                        Tensor<float, 2, true>& output,
                        cudaStream_t stream);

#ifdef FAISS_USE_FLOAT16
void runSumAlongColumns(Tensor<half, 1, true>& input,
                        Tensor<half, 2, true>& output,
                        cudaStream_t stream);
#endif

// output[x][i] = input[i] for all x
void runAssignAlongColumns(Tensor<float, 1, true>& input,
                           Tensor<float, 2, true>& output,
                           cudaStream_t stream);

#ifdef FAISS_USE_FLOAT16
void runAssignAlongColumns(Tensor<half, 1, true>& input,
                           Tensor<half, 2, true>& output,
                           cudaStream_t stream);
#endif

// output[i][x] += input[i] for all x
// If zeroClamp, output[i][x] = max(output[i][x] + input[i], 0) for all x
void runSumAlongRows(Tensor<float, 1, true>& input,
                     Tensor<float, 2, true>& output,
                     bool zeroClamp,
                     cudaStream_t stream);

#ifdef FAISS_USE_FLOAT16
void runSumAlongRows(Tensor<half, 1, true>& input,
                     Tensor<half, 2, true>& output,
                     bool zeroClamp,
                     cudaStream_t stream);
#endif

} } // namespace
