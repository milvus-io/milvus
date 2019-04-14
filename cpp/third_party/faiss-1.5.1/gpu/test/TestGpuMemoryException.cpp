/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "../../IndexFlat.h"
#include "../GpuIndexFlat.h"
#include "../StandardGpuResources.h"
#include "../utils/DeviceUtils.h"
#include "../test/TestUtils.h"
#include <gtest/gtest.h>

// Test to see if we can recover after attempting to allocate too much GPU
// memory
TEST(TestGpuMemoryException, AddException) {
  size_t numBrokenAdd = std::numeric_limits<int>::max();
  size_t numRealAdd = 10000;
  size_t devFree = 0;
  size_t devTotal = 0;

  CUDA_VERIFY(cudaMemGetInfo(&devFree, &devTotal));

  // Figure out the dimensionality needed to get at least greater than devTotal
  size_t dims = ((devTotal / sizeof(float)) / numBrokenAdd) + 1;

  faiss::gpu::StandardGpuResources res;

  faiss::gpu::GpuIndexFlatConfig config;
  config.device = faiss::gpu::randVal(0, faiss::gpu::getNumDevices() - 1);

  faiss::gpu::GpuIndexFlatL2 gpuIndexL2(&res, (int) dims, config);
  faiss::IndexFlatL2 cpuIndex((int) dims);

  // Should throw on attempting to allocate too much data
  {
    // allocate memory without initialization
    auto vecs = std::unique_ptr<float[]>(new float[numBrokenAdd * dims]);
    EXPECT_THROW(gpuIndexL2.add(numBrokenAdd, vecs.get()),
                 faiss::FaissException);
  }

  // Should be able to add a smaller set of data now
  {
    auto vecs = faiss::gpu::randVecs(numRealAdd, dims);
    EXPECT_NO_THROW(gpuIndexL2.add(numRealAdd, vecs.data()));
    cpuIndex.add(numRealAdd, vecs.data());
  }

  // Should throw on attempting to allocate too much data
  {
    // allocate memory without initialization
    auto vecs = std::unique_ptr<float[]>(new float[numBrokenAdd * dims]);
    EXPECT_THROW(gpuIndexL2.add(numBrokenAdd, vecs.get()),
                 faiss::FaissException);
  }

  // Should be able to query results from what we had before
  {
    size_t numQuery = 10;
    auto vecs = faiss::gpu::randVecs(numQuery, dims);
    EXPECT_NO_THROW(compareIndices(vecs, cpuIndex, gpuIndexL2,
                                   numQuery, dims, 50, "",
                                   6e-3f, 0.1f, 0.015f));
  }
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);

  // just run with a fixed test seed
  faiss::gpu::setTestSeed(100);

  return RUN_ALL_TESTS();
}
