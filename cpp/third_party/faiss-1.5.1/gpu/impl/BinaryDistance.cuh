/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "../utils/DeviceTensor.cuh"

namespace faiss { namespace gpu {

// Performs brute-force k-NN comparison between `vecs` and `query`, where they
// are encoded as binary vectors
void runBinaryDistance(Tensor<unsigned char, 2, true>& vecs,
                       Tensor<unsigned char, 2, true>& query,
                       Tensor<int, 2, true>& outK,
                       Tensor<int, 2, true>& outV,
                       int k, cudaStream_t stream);

} } // namespace
