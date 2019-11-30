/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/IndexFlat.h>
#include <faiss/gpu/GpuDistance.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/utils/CopyUtils.cuh>
#include <faiss/gpu/utils/Transpose.cuh>
#include <faiss/gpu/test/TestUtils.h>
#include <gtest/gtest.h>
#include <sstream>
#include <vector>

void testTransposition(bool colMajorVecs,
                       bool colMajorQueries,
                       faiss::MetricType metric) {
  int device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  int dim = faiss::gpu::randVal(20, 150);
  int numVecs = faiss::gpu::randVal(10, 30000);
  int numQuery = faiss::gpu::randVal(1, 1024);
  int k = faiss::gpu::randVal(20, 70);

  // Input data for CPU
  std::vector<float> vecs = faiss::gpu::randVecs(numVecs, dim);
  std::vector<float> queries = faiss::gpu::randVecs(numQuery, dim);

  // The CPU index is our reference for the results
  faiss::IndexFlatL2 cpuIndexL2(dim);
  cpuIndexL2.add(numVecs, vecs.data());

  std::vector<float> cpuDistanceL2(numQuery * k, 0);
  std::vector<faiss::Index::idx_t> cpuIndicesL2(numQuery * k, -1);

  cpuIndexL2.search(numQuery, queries.data(), k,
                    cpuDistanceL2.data(), cpuIndicesL2.data());

  faiss::IndexFlatIP cpuIndexIP(dim);
  cpuIndexIP.add(numVecs, vecs.data());

  std::vector<float> cpuDistanceIP(numQuery * k, 0);
  std::vector<faiss::Index::idx_t> cpuIndicesIP(numQuery * k, -1);

  cpuIndexIP.search(numQuery, queries.data(), k,
                    cpuDistanceIP.data(), cpuIndicesIP.data());

  // The transpose and distance code assumes the desired device is already set
  faiss::gpu::DeviceScope scope(device);
  auto stream = res.getDefaultStream(device);

  // Copy input data to GPU, and pre-transpose both vectors and queries for
  // passing
  auto gpuVecs = faiss::gpu::toDevice<float, 2>(
    nullptr, device, vecs.data(), stream, {numVecs, dim});
  auto gpuQueries = faiss::gpu::toDevice<float, 2>(
    nullptr, device, queries.data(), stream, {numQuery, dim});

  faiss::gpu::DeviceTensor<float, 2, true> vecsT({dim, numVecs});
  faiss::gpu::runTransposeAny(gpuVecs, 0, 1, vecsT, stream);

  faiss::gpu::DeviceTensor<float, 2, true> queriesT({dim, numQuery});
  faiss::gpu::runTransposeAny(gpuQueries, 0, 1, queriesT, stream);

  std::vector<float> gpuDistance(numQuery * k, 0);
  std::vector<faiss::Index::idx_t> gpuIndices(numQuery * k, -1);

  faiss::gpu::bruteForceKnn(
    &res,
    metric,
    colMajorVecs ? vecsT.data() : gpuVecs.data(),
    !colMajorVecs,
    numVecs,
    colMajorQueries ? queriesT.data() : gpuQueries.data(),
    !colMajorQueries,
    numQuery,
    dim,
    k,
    gpuDistance.data(),
    gpuIndices.data());

  std::stringstream str;
  str << "metric " << metric
      << " colMajorVecs " << colMajorVecs
      << " colMajorQueries " << colMajorQueries;

  faiss::gpu::compareLists(metric == faiss::MetricType::METRIC_L2 ?
                           cpuDistanceL2.data() : cpuDistanceIP.data(),
                           metric == faiss::MetricType::METRIC_L2 ?
                           cpuIndicesL2.data() : cpuIndicesIP.data(),
                           gpuDistance.data(),
                           gpuIndices.data(),
                           numQuery, k,
                           str.str(),
                           false, false, true,
                           6e-3f, 0.1f, 0.015f);
}

// Test different memory layouts for brute-force k-NN
TEST(TestGpuDistance, Transposition_RR) {
  testTransposition(false, false, faiss::MetricType::METRIC_L2);
//  testTransposition(false, false, faiss::MetricType::METRIC_INNER_PRODUCT);
}

TEST(TestGpuDistance, Transposition_RC) {
  testTransposition(false, true, faiss::MetricType::METRIC_L2);
//  testTransposition(false, true, faiss::MetricType::METRIC_INNER_PRODUCT);
}

TEST(TestGpuDistance, Transposition_CR) {
  testTransposition(true, false, faiss::MetricType::METRIC_L2);
//  testTransposition(true, false, faiss::MetricType::METRIC_INNER_PRODUCT);
}

TEST(TestGpuDistance, Transposition_CC) {
  testTransposition(true, true, faiss::MetricType::METRIC_L2);
//  testTransposition(true, true, faiss::MetricType::METRIC_INNER_PRODUCT);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  // just run with a fixed test seed
  faiss::gpu::setTestSeed(100);

  return RUN_ALL_TESTS();
}
