/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "../Index.h"

namespace faiss { namespace gpu {

class GpuResources;

/// A wrapper for gpu/impl/Distance.cuh to expose direct brute-force k-nearest
/// neighbor searches on an externally-provided region of memory (e.g., from a
/// pytorch tensor).
/// The data (vectors, queries, outDistances, outIndices) can be resident on the
/// GPU or the CPU, but all calculations are performed on the GPU. If the result
/// buffers are on the CPU, results will be copied back when done.
///
/// All GPU computation is performed on the current CUDA device, and ordered
/// with respect to resources->getDefaultStreamCurrentDevice().
///
/// For each vector in `queries`, searches all of `vectors` to find its k
/// nearest neighbors with respect to the given metric
void bruteForceKnn(GpuResources* resources,
                   faiss::MetricType metric,
                   // A region of memory size numVectors x dims, with dims
                   // innermost
                   const float* vectors,
                   int numVectors,
                   // A region of memory size numQueries x dims, with dims
                   // innermost
                   const float* queries,
                   int numQueries,
                   int dims,
                   int k,
                   // A region of memory size numQueries x k, with k
                   // innermost
                   float* outDistances,
                   // A region of memory size numQueries x k, with k
                   // innermost
                   faiss::Index::idx_t* outIndices);

} } // namespace
