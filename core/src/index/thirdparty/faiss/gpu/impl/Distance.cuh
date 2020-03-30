/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/utils/DeviceTensor.cuh>
#include <faiss/gpu/utils/Float16.cuh>
#include <faiss/utils/ConcurrentBitset.h>

namespace faiss { namespace gpu {

class GpuResources;

/// Calculates brute-force L2 distance between `vectors` and
/// `queries`, returning the k closest results seen
void runL2Distance(GpuResources* resources,
                   Tensor<float, 2, true>& vectors,
                   bool vectorsRowMajor,
                   // can be optionally pre-computed; nullptr if we
                   // have to compute it upon the call
                   Tensor<float, 1, true>* vectorNorms,
                   Tensor<float, 2, true>& queries,
                   bool queriesRowMajor,
                   Tensor<uint8_t, 1, true>& bitset,
                   int k,
                   Tensor<float, 2, true>& outDistances,
                   Tensor<int, 2, true>& outIndices,
                   // Do we care about `outDistances`? If not, we can
                   // take shortcuts.
                   bool ignoreOutDistances = false);

/// Calculates brute-force inner product distance between `vectors`
/// and `queries`, returning the k closest results seen
void runIPDistance(GpuResources* resources,
                   Tensor<float, 2, true>& vectors,
                   bool vectorsRowMajor,
                   Tensor<float, 2, true>& queries,
                   bool queriesRowMajor,
                   Tensor<uint8_t, 1, true>& bitset,
                   int k,
                   Tensor<float, 2, true>& outDistances,
                   Tensor<int, 2, true>& outIndices);

void runIPDistance(GpuResources* resources,
                   Tensor<half, 2, true>& vectors,
                   bool vectorsRowMajor,
                   Tensor<half, 2, true>& queries,
                   bool queriesRowMajor,
                   Tensor<uint8_t, 1, true>& bitset,
                   int k,
                   Tensor<half, 2, true>& outDistances,
                   Tensor<int, 2, true>& outIndices,
                   bool useHgemm);

void runL2Distance(GpuResources* resources,
                   Tensor<half, 2, true>& vectors,
                   bool vectorsRowMajor,
                   Tensor<half, 1, true>* vectorNorms,
                   Tensor<half, 2, true>& queries,
                   bool queriesRowMajor,
                   Tensor<uint8_t, 1, true>& bitset,
                   int k,
                   Tensor<half, 2, true>& outDistances,
                   Tensor<int, 2, true>& outIndices,
                   bool useHgemm,
                   bool ignoreOutDistances = false);


} } // namespace
