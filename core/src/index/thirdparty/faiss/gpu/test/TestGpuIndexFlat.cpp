/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/IndexFlat.h>
#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/test/TestUtils.h>
#include <gtest/gtest.h>
#include <sstream>
#include <vector>

// FIXME: figure out a better way to test fp16
constexpr float kF16MaxRelErr = 0.07f;
constexpr float kF32MaxRelErr = 6e-3f;

struct TestFlatOptions {
  TestFlatOptions()
      : metric(faiss::MetricType::METRIC_L2),
        metricArg(0),
        useFloat16(false),
        useTransposed(false),
        numVecsOverride(-1),
        numQueriesOverride(-1),
        kOverride(-1),
        dimOverride(-1) {
  }

  faiss::MetricType metric;
  float metricArg;

  bool useFloat16;
  bool useTransposed;
  int numVecsOverride;
  int numQueriesOverride;
  int kOverride;
  int dimOverride;
};

void testFlat(const TestFlatOptions& opt) {
  int numVecs = opt.numVecsOverride > 0 ?
    opt.numVecsOverride : faiss::gpu::randVal(1000, 5000);
  int dim = opt.dimOverride > 0 ?
    opt.dimOverride : faiss::gpu::randVal(50, 800);
  int numQuery = opt.numQueriesOverride > 0 ?
    opt.numQueriesOverride : faiss::gpu::randVal(1, 512);

  // Due to loss of precision in a float16 accumulator, for large k,
  // the number of differences is pretty huge. Restrict ourselves to a
  // fairly small `k` for float16
  int k = opt.useFloat16 ?
    std::min(faiss::gpu::randVal(1, 50), numVecs) :
    std::min(faiss::gpu::randVal(1, faiss::gpu::getMaxKSelection()), numVecs);
  if (opt.kOverride > 0) {
    k = opt.kOverride;
  }

  faiss::IndexFlat cpuIndex(dim, opt.metric);
  cpuIndex.metric_arg = opt.metricArg;

  // Construct on a random device to test multi-device, if we have
  // multiple devices
  int device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexFlatConfig config;
  config.device = device;
  config.useFloat16 = opt.useFloat16;
  config.storeTransposed = opt.useTransposed;

  faiss::gpu::GpuIndexFlat gpuIndex(&res, dim, opt.metric, config);
  gpuIndex.metric_arg = opt.metricArg;

  std::vector<float> vecs = faiss::gpu::randVecs(numVecs, dim);
  cpuIndex.add(numVecs, vecs.data());
  gpuIndex.add(numVecs, vecs.data());

  std::stringstream str;
  str << "metric " << opt.metric
      << " marg " << opt.metricArg
      << " numVecs " << numVecs
      << " dim " << dim
      << " useFloat16 " << opt.useFloat16
      << " transposed " << opt.useTransposed
      << " numQuery " << numQuery
      << " k " << k;

  // To some extent, we depend upon the relative error for the test
  // for float16
  faiss::gpu::compareIndices(cpuIndex, gpuIndex, numQuery, dim, k, str.str(),
                             opt.useFloat16 ? kF16MaxRelErr : kF32MaxRelErr,
                             // FIXME: the fp16 bounds are
                             // useless when math (the accumulator) is
                             // in fp16. Figure out another way to test
                             opt.useFloat16 ? 0.99f : 0.1f,
                             opt.useFloat16 ? 0.65f : 0.015f);
}

TEST(TestGpuIndexFlat, IP_Float32) {
  for (int tries = 0; tries < 3; ++tries) {
    TestFlatOptions opt;
    opt.metric = faiss::MetricType::METRIC_INNER_PRODUCT;
    opt.useFloat16 = false;
    opt.useTransposed = false;

    testFlat(opt);

    opt.useTransposed = true;
    testFlat(opt);
  }
}

TEST(TestGpuIndexFlat, L1_Float32) {
  TestFlatOptions opt;
  opt.metric = faiss::MetricType::METRIC_L1;
  opt.useFloat16 = false;
  opt.useTransposed = false;

  testFlat(opt);

  opt.useTransposed = true;
  testFlat(opt);
}

TEST(TestGpuIndexFlat, Lp_Float32) {
  TestFlatOptions opt;
  opt.metric = faiss::MetricType::METRIC_Lp;
  opt.metricArg = 5;
  opt.useFloat16 = false;
  opt.useTransposed = false;

  testFlat(opt);

  // Don't bother testing the transposed version, the L1 test should be good
  // enough for that
}

TEST(TestGpuIndexFlat, L2_Float32) {
  for (int tries = 0; tries < 3; ++tries) {
    TestFlatOptions opt;
    opt.metric = faiss::MetricType::METRIC_L2;

    opt.useFloat16 = false;
    opt.useTransposed = false;

    testFlat(opt);

    opt.useTransposed = true;
    testFlat(opt);
  }
}

// test specialized k == 1 codepath
TEST(TestGpuIndexFlat, L2_Float32_K1) {
  for (int tries = 0; tries < 3; ++tries) {
    TestFlatOptions opt;
    opt.metric = faiss::MetricType::METRIC_L2;
    opt.useFloat16 = false;
    opt.useTransposed = false;
    opt.kOverride = 1;

    testFlat(opt);
  }
}

TEST(TestGpuIndexFlat, IP_Float16) {
  for (int tries = 0; tries < 3; ++tries) {
    TestFlatOptions opt;
    opt.metric = faiss::MetricType::METRIC_INNER_PRODUCT;
    opt.useFloat16 = true;
    opt.useTransposed = false;

    testFlat(opt);

    opt.useTransposed = true;
    testFlat(opt);
  }
}

TEST(TestGpuIndexFlat, L2_Float16) {
  for (int tries = 0; tries < 3; ++tries) {
    TestFlatOptions opt;
    opt.metric = faiss::MetricType::METRIC_L2;
    opt.useFloat16 = true;
    opt.useTransposed = false;

    testFlat(opt);

    opt.useTransposed = true;
    testFlat(opt);
  }
}

// test specialized k == 1 codepath
TEST(TestGpuIndexFlat, L2_Float16_K1) {
  for (int tries = 0; tries < 3; ++tries) {
    TestFlatOptions opt;
    opt.metric = faiss::MetricType::METRIC_L2;
    opt.useFloat16 = true;
    opt.useTransposed = false;
    opt.kOverride = 1;

    testFlat(opt);
  }
}

// test tiling along a huge vector set
TEST(TestGpuIndexFlat, L2_Tiling) {
  for (int tries = 0; tries < 2; ++tries) {
    TestFlatOptions opt;
    opt.metric = faiss::MetricType::METRIC_L2;
    opt.useFloat16 = false;
    opt.useTransposed = false;
    opt.numVecsOverride = 1000000;

    // keep the rest of the problem reasonably small
    opt.numQueriesOverride = 4;
    opt.dimOverride = 64;
    opt.kOverride = 64;

    testFlat(opt);
  }
}

TEST(TestGpuIndexFlat, QueryEmpty) {
  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexFlatConfig config;
  config.device = 0;
  config.useFloat16 = false;
  config.storeTransposed = false;

  int dim = 128;
  faiss::gpu::GpuIndexFlatL2 gpuIndex(&res, dim, config);

  // Querying an empty index should not blow up, and just return
  // (FLT_MAX, -1)
  int numQuery = 10;
  int k = 50;
  std::vector<float> queries(numQuery * dim, 1.0f);

  std::vector<float> dist(numQuery * k, 0);
  std::vector<faiss::Index::idx_t> ind(numQuery * k);

  gpuIndex.search(numQuery, queries.data(), k, dist.data(), ind.data());

  for (auto d : dist) {
    EXPECT_EQ(d, std::numeric_limits<float>::max());
  }

  for (auto i : ind) {
    EXPECT_EQ(i, -1);
  }
}

TEST(TestGpuIndexFlat, CopyFrom) {
  int numVecs = faiss::gpu::randVal(100, 200);
  int dim = faiss::gpu::randVal(1, 1000);

  faiss::IndexFlatL2 cpuIndex(dim);

  std::vector<float> vecs = faiss::gpu::randVecs(numVecs, dim);
  cpuIndex.add(numVecs, vecs.data());

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  // Fill with garbage values
  int device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);

  faiss::gpu::GpuIndexFlatConfig config;
  config.device = 0;
  config.useFloat16 = false;
  config.storeTransposed = false;

  faiss::gpu::GpuIndexFlatL2 gpuIndex(&res, 2000, config);
  gpuIndex.copyFrom(&cpuIndex);

  EXPECT_EQ(cpuIndex.ntotal, gpuIndex.ntotal);
  EXPECT_EQ(gpuIndex.ntotal, numVecs);

  EXPECT_EQ(cpuIndex.d, gpuIndex.d);
  EXPECT_EQ(cpuIndex.d, dim);

  int idx = faiss::gpu::randVal(0, numVecs - 1);

  std::vector<float> gpuVals(dim);
  gpuIndex.reconstruct(idx, gpuVals.data());

  std::vector<float> cpuVals(dim);
  cpuIndex.reconstruct(idx, cpuVals.data());

  EXPECT_EQ(gpuVals, cpuVals);
}

TEST(TestGpuIndexFlat, CopyTo) {
  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  int numVecs = faiss::gpu::randVal(100, 200);
  int dim = faiss::gpu::randVal(1, 1000);

  int device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);

  faiss::gpu::GpuIndexFlatConfig config;
  config.device = device;
  config.useFloat16 = false;
  config.storeTransposed = false;

  faiss::gpu::GpuIndexFlatL2 gpuIndex(&res, dim, config);

  std::vector<float> vecs = faiss::gpu::randVecs(numVecs, dim);
  gpuIndex.add(numVecs, vecs.data());

  // Fill with garbage values
  faiss::IndexFlatL2 cpuIndex(2000);
  gpuIndex.copyTo(&cpuIndex);

  EXPECT_EQ(cpuIndex.ntotal, gpuIndex.ntotal);
  EXPECT_EQ(gpuIndex.ntotal, numVecs);

  EXPECT_EQ(cpuIndex.d, gpuIndex.d);
  EXPECT_EQ(cpuIndex.d, dim);

  int idx = faiss::gpu::randVal(0, numVecs - 1);

  std::vector<float> gpuVals(dim);
  gpuIndex.reconstruct(idx, gpuVals.data());

  std::vector<float> cpuVals(dim);
  cpuIndex.reconstruct(idx, cpuVals.data());

  EXPECT_EQ(gpuVals, cpuVals);
}

TEST(TestGpuIndexFlat, UnifiedMemory) {
  // Construct on a random device to test multi-device, if we have
  // multiple devices
  int device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);

  if (!faiss::gpu::getFullUnifiedMemSupport(device)) {
    return;
  }

  int dim = 256;

  // FIXME: GpuIndexFlat doesn't support > 2^31 (vecs * dims) due to
  // kernel indexing, so we can't test unified memory for memory
  // oversubscription.
  size_t numVecs = 50000;
  int numQuery = 10;
  int k = 10;

  faiss::IndexFlatL2 cpuIndexL2(dim);

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexFlatConfig config;
  config.device = device;
  config.memorySpace = faiss::gpu::MemorySpace::Unified;

  faiss::gpu::GpuIndexFlatL2 gpuIndexL2(&res, dim, config);

  std::vector<float> vecs = faiss::gpu::randVecs(numVecs, dim);
  cpuIndexL2.add(numVecs, vecs.data());
  gpuIndexL2.add(numVecs, vecs.data());

  // To some extent, we depend upon the relative error for the test
  // for float16
  faiss::gpu::compareIndices(cpuIndexL2, gpuIndexL2,
                             numQuery, dim, k, "Unified Memory",
                             kF32MaxRelErr,
                             0.1f,
                             0.015f);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  // just run with a fixed test seed
  faiss::gpu::setTestSeed(100);

  return RUN_ALL_TESTS();
}
