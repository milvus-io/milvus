/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "../../IndexBinaryFlat.h"
#include "../../utils.h"
#include "../GpuIndexBinaryFlat.h"
#include "../StandardGpuResources.h"
#include "../test/TestUtils.h"
#include "../utils/DeviceTensor.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/HostTensor.cuh"
#include "../utils/Timer.h"
#include <gflags/gflags.h>
#include <map>
#include <memory>
#include <vector>

#include <cuda_profiler_api.h>

DEFINE_int32(k, 3, "final number of closest results returned");
DEFINE_int32(num, 128, "# of vecs");
DEFINE_int32(dim, 128, "# of dimensions");
DEFINE_int32(num_queries, 3, "number of query vectors");
DEFINE_int64(seed, -1, "specify random seed");
DEFINE_int64(pinned_mem, 0, "pinned memory allocation to use");
DEFINE_bool(cpu, true, "run the CPU code for timing and comparison");
DEFINE_bool(use_unified_mem, false, "use Pascal unified memory for the index");

using namespace faiss::gpu;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  cudaProfilerStop();

  auto seed = FLAGS_seed != -1L ? FLAGS_seed : time(nullptr);
  printf("using seed %ld\n", seed);

  auto numQueries = FLAGS_num_queries;

  auto index = std::unique_ptr<faiss::IndexBinaryFlat>(
    new faiss::IndexBinaryFlat(FLAGS_dim));

  HostTensor<unsigned char, 2, true> vecs({FLAGS_num, FLAGS_dim / 8});
  faiss::byte_rand(vecs.data(), vecs.numElements(), seed);

  index->add(FLAGS_num, vecs.data());

  printf("Database: dim %d num vecs %d\n", FLAGS_dim, FLAGS_num);
  printf("Hamming lookup: %d queries, total k %d\n",
         numQueries, FLAGS_k);

  // Convert to GPU index
  printf("Copying index to GPU...\n");

  GpuIndexBinaryFlatConfig config;
  config.memorySpace = FLAGS_use_unified_mem ?
    MemorySpace::Unified : MemorySpace::Device;

  faiss::gpu::StandardGpuResources res;

  faiss::gpu::GpuIndexBinaryFlat gpuIndex(&res,
                                          index.get(),
                                          config);
  printf("copy done\n");

  // Build query vectors
  HostTensor<unsigned char, 2, true> cpuQuery({numQueries, FLAGS_dim / 8});
  faiss::byte_rand(cpuQuery.data(), cpuQuery.numElements(), seed);

  // Time faiss CPU
  HostTensor<int, 2, true>
    cpuDistances({numQueries, FLAGS_k});
  HostTensor<faiss::IndexBinary::idx_t, 2, true>
    cpuIndices({numQueries, FLAGS_k});

  if (FLAGS_cpu) {
    float cpuTime = 0.0f;

    CpuTimer timer;
    index->search(numQueries,
                  cpuQuery.data(),
                  FLAGS_k,
                  cpuDistances.data(),
                  cpuIndices.data());

    cpuTime = timer.elapsedMilliseconds();
    printf("CPU time %.3f ms\n", cpuTime);
  }

  HostTensor<int, 2, true> gpuDistances({numQueries, FLAGS_k});
  HostTensor<faiss::Index::idx_t, 2, true> gpuIndices({numQueries, FLAGS_k});

  CUDA_VERIFY(cudaProfilerStart());
  faiss::gpu::synchronizeAllDevices();

  float gpuTime = 0.0f;

  // Time GPU
  {
    CpuTimer timer;

    gpuIndex.search(cpuQuery.getSize(0),
                    cpuQuery.data(),
                    FLAGS_k,
                    gpuDistances.data(),
                    gpuIndices.data());

    // There is a device -> host copy above, so no need to time
    // additional synchronization with the GPU
    gpuTime = timer.elapsedMilliseconds();
  }

  CUDA_VERIFY(cudaProfilerStop());
  printf("GPU time %.3f ms\n", gpuTime);

  CUDA_VERIFY(cudaDeviceSynchronize());

  return 0;
}
