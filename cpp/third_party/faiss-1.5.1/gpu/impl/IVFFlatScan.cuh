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

void runIVFFlatScan(Tensor<float, 2, true>& queries,
                    Tensor<int, 2, true>& listIds,
                    thrust::device_vector<void*>& listData,
                    thrust::device_vector<void*>& listIndices,
                    IndicesOptions indicesOptions,
                    thrust::device_vector<int>& listLengths,
                    int maxListLength,
                    int k,
                    bool l2Distance,
                    bool useFloat16,
                    // output
                    Tensor<float, 2, true>& outDistances,
                    // output
                    Tensor<long, 2, true>& outIndices,
                    GpuResources* res);

} } // namespace
