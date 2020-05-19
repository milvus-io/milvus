/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/GpuDistance.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/impl/Distance.cuh>
#include <faiss/gpu/utils/ConversionOperators.cuh>
#include <faiss/gpu/utils/CopyUtils.cuh>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/DeviceTensor.cuh>

namespace faiss { namespace gpu {

void bruteForceKnn(GpuResources* resources,
                   faiss::MetricType metric,
                   // A region of memory size numVectors x dims, with dims
                   // innermost
                   const float* vectors,
                   bool vectorsRowMajor,
                   int numVectors,
                   // A region of memory size numQueries x dims, with dims
                   // innermost
                   const float* queries,
                   bool queriesRowMajor,
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
                                     {vectorsRowMajor ? numVectors : dims,
                                      vectorsRowMajor ? dims : numVectors});
  auto tQueries = toDevice<float, 2>(resources,
                                     device,
                                     const_cast<float*>(queries),
                                     stream,
                                     {queriesRowMajor ? numQueries : dims,
                                      queriesRowMajor ? dims : numQueries});

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
                  vectorsRowMajor,
                  nullptr, // compute norms in temp memory
                  tQueries,
                  queriesRowMajor,
                  k,
                  tOutDistances,
                  tOutIntIndices);
  } else if (metric == faiss::MetricType::METRIC_INNER_PRODUCT) {
    runIPDistance(resources,
                  tVectors,
                  vectorsRowMajor,
                  tQueries,
                  queriesRowMajor,
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
  convertTensor<int, faiss::Index::idx_t, 2>(stream,
                                             tOutIntIndices,
                                             tOutIndices);

  // Copy back if necessary
  fromDevice<float, 2>(tOutDistances, outDistances, stream);
  fromDevice<faiss::Index::idx_t, 2>(tOutIndices, outIndices, stream);
}

} } // namespace
