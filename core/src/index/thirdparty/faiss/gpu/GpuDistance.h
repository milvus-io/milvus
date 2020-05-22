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

// Scalar type of the vector data
enum class DistanceDataType {
  F32 = 1,
  F16,
};

/// Arguments to brute-force GPU k-nearest neighbor searching
struct GpuDistanceParams {
  GpuDistanceParams()
      : metric(faiss::MetricType::METRIC_L2),
        metricArg(0),
        k(0),
        dims(0),
        vectors(nullptr),
        vectorType(DistanceDataType::F32),
        vectorsRowMajor(true),
        numVectors(0),
        vectorNorms(nullptr),
        queries(nullptr),
        queryType(DistanceDataType::F32),
        queriesRowMajor(true),
        numQueries(0),
        outDistances(nullptr),
        ignoreOutDistances(false),
        outIndices(nullptr) {
  }

  //
  // Search parameters
  //

  // Search parameter: distance metric
  faiss::MetricType metric;

  // Search parameter: distance metric argument (if applicable)
  // For metric == METRIC_Lp, this is the p-value
  float metricArg;

  // Search parameter: return k nearest neighbors
  int k;

  // Vector dimensionality
  int dims;

  //
  // Vectors being queried
  //

  // If vectorsRowMajor is true, this is
  // numVectors x dims, with dims innermost; otherwise,
  // dims x numVectors, with numVectors innermost
  const void* vectors;
  DistanceDataType vectorType;
  bool vectorsRowMajor;
  int numVectors;

  // Precomputed L2 norms for each vector in `vectors`, which can be optionally
  // provided in advance to speed computation for METRIC_L2
  const float* vectorNorms;

  //
  // The query vectors (i.e., find k-nearest neighbors in `vectors` for each of
  // the `queries`
  //

  // If queriesRowMajor is true, this is
  // numQueries x dims, with dims innermost; otherwise,
  // dims x numQueries, with numQueries innermost
  const void* queries;
  DistanceDataType queryType;
  bool queriesRowMajor;
  int numQueries;

  //
  // Output results
  //

  // A region of memory size numQueries x k, with k
  // innermost (row major)
  float* outDistances;

  // Do we only care abouty the indices reported, rather than the output
  // distances?
  bool ignoreOutDistances;

  // A region of memory size numQueries x k, with k
  // innermost (row major)
  faiss::Index::idx_t* outIndices;
};

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
void bfKnn(GpuResources* resources, const GpuDistanceParams& args);

/// Deprecated legacy implementation
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
