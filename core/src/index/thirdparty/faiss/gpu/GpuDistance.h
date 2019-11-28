/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/Index.h>

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
                   // If vectorsRowMajor is true, this is
                   // numVectors x dims, with dims innermost; otherwise,
                   // dims x numVectors, with numVectors innermost
                   const float* vectors,
                   bool vectorsRowMajor,
                   int numVectors,
                   // If queriesRowMajor is true, this is
                   // numQueries x dims, with dims innermost; otherwise,
                   // dims x numQueries, with numQueries innermost
                   const float* queries,
                   bool queriesRowMajor,
                   int numQueries,
                   int dims,
                   int k,
                   // A region of memory size numQueries x k, with k
                   // innermost (row major)
                   float* outDistances,
                   // A region of memory size numQueries x k, with k
                   // innermost (row major)
                   faiss::Index::idx_t* outIndices);

} } // namespace
