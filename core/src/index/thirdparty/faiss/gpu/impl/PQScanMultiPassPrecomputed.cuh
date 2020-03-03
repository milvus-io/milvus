/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/GpuIndicesOptions.h>
#include <faiss/gpu/utils/Tensor.cuh>
#include <faiss/gpu/utils/NoTypeTensor.cuh>
#include <thrust/device_vector.h>

namespace faiss { namespace gpu {

class GpuResources;

void runPQScanMultiPassPrecomputed(Tensor<float, 2, true>& queries,
                                   Tensor<float, 2, true>& precompTerm1,
                                   NoTypeTensor<3, true>& precompTerm2,
                                   NoTypeTensor<3, true>& precompTerm3,
                                   Tensor<int, 2, true>& topQueryToCentroid,
                                   bool useFloat16Lookup,
                                   int bytesPerCode,
                                   int numSubQuantizers,
                                   int numSubQuantizerCodes,
                                   thrust::device_vector<void*>& listCodes,
                                   thrust::device_vector<void*>& listIndices,
                                   IndicesOptions indicesOptions,
                                   thrust::device_vector<int>& listLengths,
                                   int maxListLength,
                                   int k,
                                   // output
                                   Tensor<float, 2, true>& outDistances,
                                   // output
                                   Tensor<long, 2, true>& outIndices,
                                   GpuResources* res);

} } // namespace
