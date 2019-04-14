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

/// Update device-side list pointers in a batch
void runUpdateListPointers(Tensor<int, 1, true>& listIds,
                           Tensor<int, 1, true>& newListLength,
                           Tensor<void*, 1, true>& newCodePointers,
                           Tensor<void*, 1, true>& newIndexPointers,
                           thrust::device_vector<int>& listLengths,
                           thrust::device_vector<void*>& listCodes,
                           thrust::device_vector<void*>& listIndices,
                           cudaStream_t stream);

/// Actually append the new codes / vector indices to the individual lists

/// IVFPQ
void runIVFPQInvertedListAppend(Tensor<int, 1, true>& listIds,
                                Tensor<int, 1, true>& listOffset,
                                Tensor<int, 2, true>& encodings,
                                Tensor<long, 1, true>& indices,
                                thrust::device_vector<void*>& listCodes,
                                thrust::device_vector<void*>& listIndices,
                                IndicesOptions indicesOptions,
                                cudaStream_t stream);

/// IVF flat storage
void runIVFFlatInvertedListAppend(Tensor<int, 1, true>& listIds,
                                  Tensor<int, 1, true>& listOffset,
                                  Tensor<float, 2, true>& vecs,
                                  Tensor<long, 1, true>& indices,
                                  bool useFloat16,
                                  thrust::device_vector<void*>& listData,
                                  thrust::device_vector<void*>& listIndices,
                                  IndicesOptions indicesOptions,
                                  cudaStream_t stream);

} } // namespace
