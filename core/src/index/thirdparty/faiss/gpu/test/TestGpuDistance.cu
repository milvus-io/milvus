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
                       faiss::MetricType metric,
                       float metricArg = 0) {
  int device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  int dim = faiss::gpu::randVal(20, 150);
  int numVecs = faiss::gpu::randVal(10, 30000);
  int numQuery = faiss::gpu::randVal(1, 1024);
  int k = std::min(numVecs, faiss::gpu::randVal(20, 70));

  // Input data for CPU
  std::vector<float> vecs = faiss::gpu::randVecs(numVecs, dim);
  std::vector<float> queries = faiss::gpu::randVecs(numQuery, dim);

  if (metric == faiss::MetricType::METRIC_JensenShannon) {
    // make values positive
    for (auto& v : vecs) {
      v = std::abs(v);
      if (v == 0) {
        v = 1e-6;
      }
    }

    for (auto& q : queries) {
      q = std::abs(q);
      if (q == 0) {
        q = 1e-6;
      }
    }
  }

  // The CPU index is our reference for the results
  faiss::IndexFlat cpuIndex(dim, metric);
  cpuIndex.metric_arg = metricArg;
  cpuIndex.add(numVecs, vecs.data());

  std::vector<float> cpuDistance(numQuery * k, 0);
  std::vector<faiss::Index::idx_t> cpuIndices(numQuery * k, -1);

  cpuIndex.search(numQuery, queries.data(), k,
                    cpuDistance.data(), cpuIndices.data());

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

  faiss::gpu::GpuDistanceParams args;
  args.metric = metric;
  args.metricArg = metricArg;
  args.k = k;
  args.dims = dim;
  args.vectors = colMajorVecs ? vecsT.data() : gpuVecs.data();
  args.vectorsRowMajor = !colMajorVecs;
  args.numVectors = numVecs;
  args.queries = colMajorQueries ? queriesT.data() : gpuQueries.data();
  args.queriesRowMajor = !colMajorQueries;
  args.numQueries = numQuery;
  args.outDistances = gpuDistance.data();
  args.outIndices = gpuIndices.data();

  faiss::gpu::bfKnn(&res, args);

  std::stringstream str;
  str << "metric " << metric
      << " colMajorVecs " << colMajorVecs
      << " colMajorQueries " << colMajorQueries;

  faiss::gpu::compareLists(cpuDistance.data(),
                           cpuIndices.data(),
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
  testTransposition(false, false, faiss::MetricType::METRIC_INNER_PRODUCT);
}

TEST(TestGpuDistance, Transposition_RC) {
  testTransposition(false, true, faiss::MetricType::METRIC_L2);
}

TEST(TestGpuDistance, Transposition_CR) {
  testTransposition(true, false, faiss::MetricType::METRIC_L2);
}

TEST(TestGpuDistance, Transposition_CC) {
  testTransposition(true, true, faiss::MetricType::METRIC_L2);
}

TEST(TestGpuDistance, L1) {
  testTransposition(false, false, faiss::MetricType::METRIC_L1);
}

// Test other transpositions with the general distance kernel
TEST(TestGpuDistance, L1_RC) {
  testTransposition(false, true, faiss::MetricType::METRIC_L1);
}

TEST(TestGpuDistance, L1_CR) {
  testTransposition(true, false, faiss::MetricType::METRIC_L1);
}

TEST(TestGpuDistance, L1_CC) {
  testTransposition(true, true, faiss::MetricType::METRIC_L1);
}

// Test remainder of metric types
TEST(TestGpuDistance, Linf) {
  testTransposition(false, false, faiss::MetricType::METRIC_Linf);
}

TEST(TestGpuDistance, Lp) {
  testTransposition(false, false, faiss::MetricType::METRIC_Lp, 3);
}

TEST(TestGpuDistance, Canberra) {
  testTransposition(false, false, faiss::MetricType::METRIC_Canberra);
}

TEST(TestGpuDistance, BrayCurtis) {
  testTransposition(false, false, faiss::MetricType::METRIC_BrayCurtis);
}

TEST(TestGpuDistance, JensenShannon) {
  testTransposition(false, false, faiss::MetricType::METRIC_JensenShannon);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  // just run with a fixed test seed
  faiss::gpu::setTestSeed(100);

  return RUN_ALL_TESTS();
}
