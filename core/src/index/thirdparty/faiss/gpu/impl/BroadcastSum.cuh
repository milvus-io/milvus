/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/utils/Tensor.cuh>

namespace faiss { namespace gpu {

// output[x][i] += input[i] for all x
void runSumAlongColumns(Tensor<float, 1, true>& input,
                        Tensor<float, 2, true>& output,
                        cudaStream_t stream);

void runSumAlongColumns(Tensor<half, 1, true>& input,
                        Tensor<half, 2, true>& output,
                        cudaStream_t stream);

// output[x][i] = input[i] for all x
void runAssignAlongColumns(Tensor<float, 1, true>& input,
                           Tensor<float, 2, true>& output,
                           cudaStream_t stream);

void runAssignAlongColumns(Tensor<half, 1, true>& input,
                           Tensor<half, 2, true>& output,
                           cudaStream_t stream);

// output[i][x] += input[i] for all x
// If zeroClamp, output[i][x] = max(output[i][x] + input[i], 0) for all x
void runSumAlongRows(Tensor<float, 1, true>& input,
                     Tensor<float, 2, true>& output,
                     bool zeroClamp,
                     cudaStream_t stream);

void runSumAlongRows(Tensor<half, 1, true>& input,
                     Tensor<half, 2, true>& output,
                     bool zeroClamp,
                     cudaStream_t stream);

} } // namespace
