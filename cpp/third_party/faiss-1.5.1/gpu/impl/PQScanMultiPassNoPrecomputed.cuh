/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "../GpuIndicesOptions.h"
#include "../utils/Tensor.cuh"
#include <thrust/device_vector.h>

namespace faiss { namespace gpu {

class GpuResources;

/// For no precomputed codes, is this a supported number of dimensions
/// per subquantizer?
bool isSupportedNoPrecomputedSubDimSize(int dims);

void runPQScanMultiPassNoPrecomputed(Tensor<float, 2, true>& queries,
                                     Tensor<float, 2, true>& centroids,
                                     Tensor<float, 3, true>& pqCentroidsInnermostCode,
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
