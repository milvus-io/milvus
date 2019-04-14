/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "GpuDistance.h"
#include "../FaissAssert.h"
#include "GpuResources.h"
#include "impl/Distance.cuh"
#include "utils/ConversionOperators.cuh"
#include "utils/CopyUtils.cuh"
#include "utils/DeviceUtils.h"
#include "utils/DeviceTensor.cuh"

#include <thrust/execution_policy.h>
#include <thrust/transform.h>

namespace faiss { namespace gpu {

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
                   faiss::Index::idx_t* outIndices) {
  auto device = getCurrentDevice();
  auto stream = resources->getDefaultStreamCurrentDevice();
  auto& mem = resources->getMemoryManagerCurrentDevice();

  auto tVectors = toDevice<float, 2>(resources,
                                     device,
                                     const_cast<float*>(vectors),
                                     stream,
                                     {numVectors, dims});
  auto tQueries = toDevice<float, 2>(resources,
                                     device,
                                     const_cast<float*>(queries),
                                     stream,
                                     {numQueries, dims});

  auto tOutDistances = toDevice<float, 2>(resources,
                                          device,
                                          outDistances,
                                          stream,
                                          {numQueries, k});

  // FlatIndex only supports an interface returning int indices, allocate
  // temporary memory for it
  DeviceTensor<int, 2, true> tOutIntIndices(mem, {numQueries, k}, stream);

  // Do the work
  if (metric == faiss::MetricType::METRIC_L2) {
    runL2Distance(resources,
                  tVectors,
                  nullptr,
                  nullptr, // compute norms in temp memory
                  tQueries,
                  k,
                  tOutDistances,
                  tOutIntIndices);
  } else if (metric == faiss::MetricType::METRIC_INNER_PRODUCT) {
    runIPDistance(resources,
                  tVectors,
                  nullptr,
                  tQueries,
                  k,
                  tOutDistances,
                  tOutIntIndices);
  } else {
    FAISS_THROW_MSG("metric should be METRIC_L2 or METRIC_INNER_PRODUCT");
  }

  // Convert and copy int indices out
  auto tOutIndices = toDevice<faiss::Index::idx_t, 2>(resources,
                                                      device,
                                                      outIndices,
                                                      stream,
                                                      {numQueries, k});

  // Convert int to idx_t
  thrust::transform(thrust::cuda::par.on(stream),
                    tOutIntIndices.data(),
                    tOutIntIndices.end(),
                    tOutIndices.data(),
                    IntToIdxType());

  // Copy back if necessary
  fromDevice<float, 2>(tOutDistances, outDistances, stream);
  fromDevice<faiss::Index::idx_t, 2>(tOutIndices, outIndices, stream);
}

} } // namespace
