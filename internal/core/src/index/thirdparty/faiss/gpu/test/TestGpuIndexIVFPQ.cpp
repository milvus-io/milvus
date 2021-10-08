/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/IndexFlat.h>
#include <faiss/IndexIVFPQ.h>
#include <faiss/gpu/GpuIndexIVFPQ.h>
#include <faiss/gpu/StandardGpuResources.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <faiss/gpu/test/TestUtils.h>
#include <cmath>
#include <gtest/gtest.h>
#include <sstream>
#include <vector>


void pickEncoding(int& codes, int& dim) {
  std::vector<int> codeSizes{
    3, 4, 8, 12, 16, 20, 24,
      28, 32, 40, 48, 56, 64, 96
  };

  // Above 32 doesn't work with no precomputed codes
  std::vector<int> dimSizes{4, 8, 10, 12, 16, 20, 24, 28, 32};

  while (true) {
    codes = codeSizes[faiss::gpu::randVal(0, codeSizes.size() - 1)];
    dim = codes * dimSizes[faiss::gpu::randVal(0, dimSizes.size() - 1)];

    // for such a small test, super-low or high dim is more likely to
    // generate comparison errors
    if (dim < 256 && dim >= 64) {
      return;
    }
  }
}

struct Options {
  Options() {
    numAdd = faiss::gpu::randVal(2000, 5000);
    numCentroids = std::sqrt((float) numAdd);
    numTrain = numCentroids * 40;

    pickEncoding(codes, dim);

    // TODO: Change back to `faiss::gpu::randVal(3, 7)` when we officially
    //   support non-multiple of 8 subcodes for IVFPQ.
    bitsPerCode = 8;
    nprobe = std::min(faiss::gpu::randVal(40, 1000), numCentroids);
    numQuery = faiss::gpu::randVal(1, 8);

    // Due to the approximate nature of the query and of floating point
    // differences between GPU and CPU, to stay within our error bounds, only
    // use a small k
    k = std::min(faiss::gpu::randVal(5, 20), numAdd / 40);
    usePrecomputed = faiss::gpu::randBool();
    indicesOpt = faiss::gpu::randSelect({
        faiss::gpu::INDICES_CPU,
          faiss::gpu::INDICES_32_BIT,
          faiss::gpu::INDICES_64_BIT});
    if (codes > 48) {
      // large codes can only fit using float16
      useFloat16 = true;
    } else {
      useFloat16 = faiss::gpu::randBool();
    }

    device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);
  }

  std::string toString() const {
    std::stringstream str;
    str << "IVFPQ device " << device
        << " numVecs " << numAdd
        << " dim " << dim
        << " numCentroids " << numCentroids
        << " codes " << codes
        << " bitsPerCode " << bitsPerCode
        << " nprobe " << nprobe
        << " numQuery " << numQuery
        << " k " << k
        << " usePrecomputed " << usePrecomputed
        << " indicesOpt " << indicesOpt
        << " useFloat16 " << useFloat16;

    return str.str();
  }

  float getCompareEpsilon() const {
    return 0.03f;
  }

  float getPctMaxDiff1() const {
    return useFloat16 ? 0.30f : 0.10f;
  }

  float getPctMaxDiffN() const {
    return useFloat16 ? 0.05f : 0.02f;
  }

  int numAdd;
  int numCentroids;
  int numTrain;
  int codes;
  int dim;
  int bitsPerCode;
  int nprobe;
  int numQuery;
  int k;
  bool usePrecomputed;
  faiss::gpu::IndicesOptions indicesOpt;
  bool useFloat16;
  int device;
};

TEST(TestGpuIndexIVFPQ, Query_L2) {
  for (int tries = 0; tries < 2; ++tries) {
    Options opt;

    std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
    std::vector<float> addVecs = faiss::gpu::randVecs(opt.numAdd, opt.dim);

    faiss::IndexFlatL2 coarseQuantizer(opt.dim);
    faiss::IndexIVFPQ cpuIndex(&coarseQuantizer, opt.dim, opt.numCentroids,
                               opt.codes, opt.bitsPerCode);
    cpuIndex.nprobe = opt.nprobe;
    cpuIndex.train(opt.numTrain, trainVecs.data());
    cpuIndex.add(opt.numAdd, addVecs.data());

    faiss::gpu::StandardGpuResources res;
    res.noTempMemory();

    faiss::gpu::GpuIndexIVFPQConfig config;
    config.device = opt.device;
    config.usePrecomputedTables = opt.usePrecomputed;
    config.indicesOptions = opt.indicesOpt;
    config.useFloat16LookupTables = opt.useFloat16;

    faiss::gpu::GpuIndexIVFPQ gpuIndex(&res, &cpuIndex, config);
    gpuIndex.setNumProbes(opt.nprobe);

    faiss::gpu::compareIndices(cpuIndex, gpuIndex,
                               opt.numQuery, opt.dim, opt.k, opt.toString(),
                               opt.getCompareEpsilon(),
                               opt.getPctMaxDiff1(),
                               opt.getPctMaxDiffN());
  }
}

TEST(TestGpuIndexIVFPQ, Query_IP) {
  for (int tries = 0; tries < 2; ++tries) {
    Options opt;

    std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
    std::vector<float> addVecs = faiss::gpu::randVecs(opt.numAdd, opt.dim);

    faiss::IndexFlatIP coarseQuantizer(opt.dim);
    faiss::IndexIVFPQ cpuIndex(&coarseQuantizer, opt.dim, opt.numCentroids,
                               opt.codes, opt.bitsPerCode);
    cpuIndex.metric_type = faiss::MetricType::METRIC_INNER_PRODUCT;

    cpuIndex.nprobe = opt.nprobe;
    cpuIndex.train(opt.numTrain, trainVecs.data());
    cpuIndex.add(opt.numAdd, addVecs.data());

    faiss::gpu::StandardGpuResources res;
    res.noTempMemory();

    faiss::gpu::GpuIndexIVFPQConfig config;
    config.device = opt.device;
    config.usePrecomputedTables = false; // not supported/required for IP
    config.indicesOptions = opt.indicesOpt;
    config.useFloat16LookupTables = opt.useFloat16;

    faiss::gpu::GpuIndexIVFPQ gpuIndex(&res, &cpuIndex, config);
    gpuIndex.setNumProbes(opt.nprobe);

    faiss::gpu::compareIndices(cpuIndex, gpuIndex,
                               opt.numQuery, opt.dim, opt.k, opt.toString(),
                               opt.getCompareEpsilon(),
                               opt.getPctMaxDiff1(),
                               opt.getPctMaxDiffN());
  }
}

TEST(TestGpuIndexIVFPQ, Float16Coarse) {
  Options opt;

  std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
  std::vector<float> addVecs = faiss::gpu::randVecs(opt.numAdd, opt.dim);

  faiss::IndexFlatL2 coarseQuantizer(opt.dim);
  faiss::IndexIVFPQ cpuIndex(&coarseQuantizer, opt.dim, opt.numCentroids,
                             opt.codes, opt.bitsPerCode);
  cpuIndex.nprobe = opt.nprobe;
  cpuIndex.train(opt.numTrain, trainVecs.data());

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexIVFPQConfig config;
  config.device = opt.device;
  config.flatConfig.useFloat16 = true;
  config.usePrecomputedTables = opt.usePrecomputed;
  config.indicesOptions = opt.indicesOpt;
  config.useFloat16LookupTables = opt.useFloat16;

  faiss::gpu::GpuIndexIVFPQ gpuIndex(&res, &cpuIndex, config);
  gpuIndex.setNumProbes(opt.nprobe);

  gpuIndex.add(opt.numAdd, addVecs.data());
  cpuIndex.add(opt.numAdd, addVecs.data());

  faiss::gpu::compareIndices(cpuIndex, gpuIndex,
                             opt.numQuery, opt.dim, opt.k, opt.toString(),
                             opt.getCompareEpsilon(),
                             opt.getPctMaxDiff1(),
                             opt.getPctMaxDiffN());
}

TEST(TestGpuIndexIVFPQ, Add_L2) {
  for (int tries = 0; tries < 2; ++tries) {
    Options opt;

    std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
    std::vector<float> addVecs = faiss::gpu::randVecs(opt.numAdd, opt.dim);

    faiss::IndexFlatL2 coarseQuantizer(opt.dim);
    faiss::IndexIVFPQ cpuIndex(&coarseQuantizer, opt.dim, opt.numCentroids,
                               opt.codes, opt.bitsPerCode);
    cpuIndex.nprobe = opt.nprobe;
    cpuIndex.train(opt.numTrain, trainVecs.data());

    faiss::gpu::StandardGpuResources res;
    res.noTempMemory();

    faiss::gpu::GpuIndexIVFPQConfig config;
    config.device = opt.device;
    config.usePrecomputedTables = opt.usePrecomputed;
    config.indicesOptions = opt.indicesOpt;
    config.useFloat16LookupTables = opt.useFloat16;

    faiss::gpu::GpuIndexIVFPQ gpuIndex(&res, &cpuIndex, config);
    gpuIndex.setNumProbes(opt.nprobe);

    gpuIndex.add(opt.numAdd, addVecs.data());
    cpuIndex.add(opt.numAdd, addVecs.data());

    faiss::gpu::compareIndices(cpuIndex, gpuIndex,
                               opt.numQuery, opt.dim, opt.k, opt.toString(),
                               opt.getCompareEpsilon(),
                               opt.getPctMaxDiff1(),
                               opt.getPctMaxDiffN());
  }
}

TEST(TestGpuIndexIVFPQ, Add_IP) {
  for (int tries = 0; tries < 2; ++tries) {
    Options opt;

    std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
    std::vector<float> addVecs = faiss::gpu::randVecs(opt.numAdd, opt.dim);

    faiss::IndexFlatIP coarseQuantizer(opt.dim);
    faiss::IndexIVFPQ cpuIndex(&coarseQuantizer, opt.dim, opt.numCentroids,
                               opt.codes, opt.bitsPerCode);
    cpuIndex.metric_type = faiss::MetricType::METRIC_INNER_PRODUCT;
    cpuIndex.nprobe = opt.nprobe;
    cpuIndex.train(opt.numTrain, trainVecs.data());

    faiss::gpu::StandardGpuResources res;
    res.noTempMemory();

    faiss::gpu::GpuIndexIVFPQConfig config;
    config.device = opt.device;
    config.usePrecomputedTables = opt.usePrecomputed;
    config.indicesOptions = opt.indicesOpt;
    config.useFloat16LookupTables = opt.useFloat16;

    faiss::gpu::GpuIndexIVFPQ gpuIndex(&res, &cpuIndex, config);
    gpuIndex.setNumProbes(opt.nprobe);

    gpuIndex.add(opt.numAdd, addVecs.data());
    cpuIndex.add(opt.numAdd, addVecs.data());

    faiss::gpu::compareIndices(cpuIndex, gpuIndex,
                               opt.numQuery, opt.dim, opt.k, opt.toString(),
                               opt.getCompareEpsilon(),
                               opt.getPctMaxDiff1(),
                               opt.getPctMaxDiffN());
  }
}

TEST(TestGpuIndexIVFPQ, CopyTo) {
  Options opt;
  std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
  std::vector<float> addVecs = faiss::gpu::randVecs(opt.numAdd, opt.dim);

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexIVFPQConfig config;
  config.device = opt.device;
  config.usePrecomputedTables = opt.usePrecomputed;
  config.indicesOptions = opt.indicesOpt;
  config.useFloat16LookupTables = opt.useFloat16;

  faiss::gpu::GpuIndexIVFPQ gpuIndex(&res,
                                     opt.dim,
                                     opt.numCentroids,
                                     opt.codes,
                                     opt.bitsPerCode,
                                     faiss::METRIC_L2,
                                     config);
  gpuIndex.setNumProbes(opt.nprobe);
  gpuIndex.train(opt.numTrain, trainVecs.data());
  gpuIndex.add(opt.numAdd, addVecs.data());

  // Use garbage values to see if we overwrite them
  faiss::IndexFlatL2 cpuQuantizer(1);
  faiss::IndexIVFPQ cpuIndex(&cpuQuantizer, 1, 1, 1, 1);

  gpuIndex.copyTo(&cpuIndex);

  EXPECT_EQ(cpuIndex.ntotal, gpuIndex.ntotal);
  EXPECT_EQ(gpuIndex.ntotal, opt.numAdd);

  EXPECT_EQ(cpuIndex.d, gpuIndex.d);
  EXPECT_EQ(cpuIndex.d, opt.dim);
  EXPECT_EQ(cpuIndex.nlist, gpuIndex.getNumLists());
  EXPECT_EQ(cpuIndex.nprobe, gpuIndex.getNumProbes());
  EXPECT_EQ(cpuIndex.pq.M, gpuIndex.getNumSubQuantizers());
  EXPECT_EQ(gpuIndex.getNumSubQuantizers(), opt.codes);
  EXPECT_EQ(cpuIndex.pq.nbits, gpuIndex.getBitsPerCode());
  EXPECT_EQ(gpuIndex.getBitsPerCode(), opt.bitsPerCode);

  // Query both objects; results should be equivalent
  faiss::gpu::compareIndices(cpuIndex, gpuIndex,
                             opt.numQuery, opt.dim, opt.k, opt.toString(),
                             opt.getCompareEpsilon(),
                             opt.getPctMaxDiff1(),
                             opt.getPctMaxDiffN());
}

TEST(TestGpuIndexIVFPQ, CopyFrom) {
  Options opt;
  std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
  std::vector<float> addVecs = faiss::gpu::randVecs(opt.numAdd, opt.dim);

  faiss::IndexFlatL2 coarseQuantizer(opt.dim);
  faiss::IndexIVFPQ cpuIndex(&coarseQuantizer, opt.dim, opt.numCentroids,
                             opt.codes, opt.bitsPerCode);
  cpuIndex.nprobe = opt.nprobe;
  cpuIndex.train(opt.numTrain, trainVecs.data());
  cpuIndex.add(opt.numAdd, addVecs.data());

  // Use garbage values to see if we overwrite them
  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexIVFPQConfig config;
  config.device = opt.device;
  config.usePrecomputedTables = opt.usePrecomputed;
  config.indicesOptions = opt.indicesOpt;
  config.useFloat16LookupTables = opt.useFloat16;

  faiss::gpu::GpuIndexIVFPQ
    gpuIndex(&res, 1, 1, 1, 1, faiss::METRIC_L2, config);
  gpuIndex.setNumProbes(1);

  gpuIndex.copyFrom(&cpuIndex);

  // Make sure we are equivalent
  EXPECT_EQ(cpuIndex.ntotal, gpuIndex.ntotal);
  EXPECT_EQ(gpuIndex.ntotal, opt.numAdd);

  EXPECT_EQ(cpuIndex.d, gpuIndex.d);
  EXPECT_EQ(cpuIndex.d, opt.dim);
  EXPECT_EQ(cpuIndex.nlist, gpuIndex.getNumLists());
  EXPECT_EQ(cpuIndex.nprobe, gpuIndex.getNumProbes());
  EXPECT_EQ(cpuIndex.pq.M, gpuIndex.getNumSubQuantizers());
  EXPECT_EQ(gpuIndex.getNumSubQuantizers(), opt.codes);
  EXPECT_EQ(cpuIndex.pq.nbits, gpuIndex.getBitsPerCode());
  EXPECT_EQ(gpuIndex.getBitsPerCode(), opt.bitsPerCode);

  // Query both objects; results should be equivalent
  faiss::gpu::compareIndices(cpuIndex, gpuIndex,
                             opt.numQuery, opt.dim, opt.k, opt.toString(),
                             opt.getCompareEpsilon(),
                             opt.getPctMaxDiff1(),
                             opt.getPctMaxDiffN());
}

TEST(TestGpuIndexIVFPQ, QueryNaN) {
  Options opt;

  std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
  std::vector<float> addVecs = faiss::gpu::randVecs(opt.numAdd, opt.dim);

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexIVFPQConfig config;
  config.device = opt.device;
  config.usePrecomputedTables = opt.usePrecomputed;
  config.indicesOptions = opt.indicesOpt;
  config.useFloat16LookupTables = opt.useFloat16;

  faiss::gpu::GpuIndexIVFPQ gpuIndex(&res,
                                     opt.dim,
                                     opt.numCentroids,
                                     opt.codes,
                                     opt.bitsPerCode,
                                     faiss::METRIC_L2,
                                     config);

  gpuIndex.setNumProbes(opt.nprobe);

  gpuIndex.train(opt.numTrain, trainVecs.data());
  gpuIndex.add(opt.numAdd, addVecs.data());

  int numQuery = 5;
  std::vector<float> nans(numQuery * opt.dim,
                          std::numeric_limits<float>::quiet_NaN());

  std::vector<float> distances(numQuery * opt.k, 0);
  std::vector<faiss::Index::idx_t> indices(numQuery * opt.k, 0);

  gpuIndex.search(numQuery,
                  nans.data(),
                  opt.k,
                  distances.data(),
                  indices.data());

  for (int q = 0; q < numQuery; ++q) {
    for (int k = 0; k < opt.k; ++k) {
      EXPECT_EQ(indices[q * opt.k + k], -1);
      EXPECT_EQ(distances[q * opt.k + k], std::numeric_limits<float>::max());
    }
  }
}

TEST(TestGpuIndexIVFPQ, AddNaN) {
  Options opt;

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexIVFPQConfig config;
  config.device = opt.device;
  config.usePrecomputedTables = opt.usePrecomputed;
  config.indicesOptions = opt.indicesOpt;
  config.useFloat16LookupTables = opt.useFloat16;

  faiss::gpu::GpuIndexIVFPQ gpuIndex(&res,
                                     opt.dim,
                                     opt.numCentroids,
                                     opt.codes,
                                     opt.bitsPerCode,
                                     faiss::METRIC_L2,
                                     config);

  gpuIndex.setNumProbes(opt.nprobe);

  int numNans = 10;
  std::vector<float> nans(numNans * opt.dim,
                          std::numeric_limits<float>::quiet_NaN());

  // Make one vector valid, which should actually add
  for (int i = 0; i < opt.dim; ++i) {
    nans[i] = 0.0f;
  }

  std::vector<float> trainVecs = faiss::gpu::randVecs(opt.numTrain, opt.dim);
  gpuIndex.train(opt.numTrain, trainVecs.data());

  // should not crash
  EXPECT_EQ(gpuIndex.ntotal, 0);
  gpuIndex.add(numNans, nans.data());

  std::vector<float> queryVecs = faiss::gpu::randVecs(opt.numQuery, opt.dim);
  std::vector<float> distance(opt.numQuery * opt.k, 0);
  std::vector<faiss::Index::idx_t> indices(opt.numQuery * opt.k, 0);

  // should not crash
  gpuIndex.search(opt.numQuery, queryVecs.data(), opt.k,
                  distance.data(), indices.data());
}

TEST(TestGpuIndexIVFPQ, UnifiedMemory) {
  // Construct on a random device to test multi-device, if we have
  // multiple devices
  int device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);

  if (!faiss::gpu::getFullUnifiedMemSupport(device)) {
    return;
  }

  int dim = 128;

  int numCentroids = 256;
  // Unfortunately it would take forever to add 24 GB in IVFPQ data,
  // so just perform a small test with data allocated in the unified
  // memory address space
  size_t numAdd = 10000;
  size_t numTrain = numCentroids * 40;
  int numQuery = 10;
  int k = 10;
  int nprobe = 8;
  int codes = 8;
  int bitsPerCode = 8;

  std::vector<float> trainVecs = faiss::gpu::randVecs(numTrain, dim);
  std::vector<float> addVecs = faiss::gpu::randVecs(numAdd, dim);

  faiss::IndexFlatL2 quantizer(dim);
  faiss::IndexIVFPQ cpuIndex(&quantizer, dim, numCentroids, codes, bitsPerCode);

  cpuIndex.train(numTrain, trainVecs.data());
  cpuIndex.add(numAdd, addVecs.data());
  cpuIndex.nprobe = nprobe;

  faiss::gpu::StandardGpuResources res;
  res.noTempMemory();

  faiss::gpu::GpuIndexIVFPQConfig config;
  config.device = device;
  config.memorySpace = faiss::gpu::MemorySpace::Unified;

  faiss::gpu::GpuIndexIVFPQ gpuIndex(&res,
                                     dim,
                                     numCentroids,
                                     codes,
                                     bitsPerCode,
                                     faiss::METRIC_L2,
                                     config);
  gpuIndex.copyFrom(&cpuIndex);
  gpuIndex.setNumProbes(nprobe);

  faiss::gpu::compareIndices(cpuIndex, gpuIndex,
                             numQuery, dim, k, "Unified Memory",
                             0.015f,
                             0.1f,
                             0.015f);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  // just run with a fixed test seed
  faiss::gpu::setTestSeed(100);

  return RUN_ALL_TESTS();
}
