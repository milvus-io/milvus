/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "../../IndexFlat.h"
#include "../../utils.h"
#include "../GpuIndexFlat.h"
#include "IndexWrapper.h"
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

DEFINE_bool(l2, true, "L2 or inner product");
DEFINE_int32(k, 3, "final number of closest results returned");
DEFINE_int32(num, 128, "# of vecs");
DEFINE_int32(dim, 128, "# of dimensions");
DEFINE_int32(num_queries, 3, "number of query vectors");
DEFINE_bool(diff, true, "show exact distance + index output discrepancies");
DEFINE_bool(use_float16, false, "use encodings in float16");
DEFINE_bool(use_float16_math, false, "perform math in float16");
DEFINE_bool(transposed, false, "store vectors transposed");
DEFINE_int64(seed, -1, "specify random seed");
DEFINE_int32(num_gpus, 1, "number of gpus to use");
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

  auto index = std::unique_ptr<faiss::IndexFlat>(
    new faiss::IndexFlat(FLAGS_dim, FLAGS_l2 ?
                         faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT));

  HostTensor<float, 2, true> vecs({FLAGS_num, FLAGS_dim});
  faiss::float_rand(vecs.data(), vecs.numElements(), seed);

  index->add(FLAGS_num, vecs.data());

  printf("Database: dim %d num vecs %d\n", FLAGS_dim, FLAGS_num);
  printf("%s lookup: %d queries, total k %d\n",
         FLAGS_l2 ? "L2" : "IP",
         numQueries, FLAGS_k);
  printf("float16 encoding %s\n", FLAGS_use_float16 ? "enabled" : "disabled");
  printf("transposed storage %s\n", FLAGS_transposed ? "enabled" : "disabled");

  // Convert to GPU index
  printf("Copying index to %d GPU(s)...\n", FLAGS_num_gpus);

  auto initFn = [&index](faiss::gpu::GpuResources* res, int dev) ->
    std::unique_ptr<faiss::gpu::GpuIndexFlat> {
    ((faiss::gpu::StandardGpuResources*) res)->setPinnedMemory(
      FLAGS_pinned_mem);

    GpuIndexFlatConfig config;
    config.device = dev;
    config.useFloat16 = FLAGS_use_float16;
    config.useFloat16Accumulator = FLAGS_use_float16_math;
    config.storeTransposed = FLAGS_transposed;
    config.memorySpace = FLAGS_use_unified_mem ?
    MemorySpace::Unified : MemorySpace::Device;

    auto p = std::unique_ptr<faiss::gpu::GpuIndexFlat>(
      new faiss::gpu::GpuIndexFlat(res, index.get(), config));
    return p;
  };

  IndexWrapper<faiss::gpu::GpuIndexFlat> gpuIndex(FLAGS_num_gpus, initFn);
  printf("copy done\n");

  // Build query vectors
  HostTensor<float, 2, true> cpuQuery({numQueries, FLAGS_dim});
  faiss::float_rand(cpuQuery.data(), cpuQuery.numElements(), seed);

  // Time faiss CPU
  HostTensor<float, 2, true> cpuDistances({numQueries, FLAGS_k});
  HostTensor<faiss::Index::idx_t, 2, true> cpuIndices({numQueries, FLAGS_k});

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

  HostTensor<float, 2, true> gpuDistances({numQueries, FLAGS_k});
  HostTensor<faiss::Index::idx_t, 2, true> gpuIndices({numQueries, FLAGS_k});

  CUDA_VERIFY(cudaProfilerStart());
  faiss::gpu::synchronizeAllDevices();

  float gpuTime = 0.0f;

  // Time GPU
  {
    CpuTimer timer;

    gpuIndex.getIndex()->search(cpuQuery.getSize(0),
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

  if (FLAGS_cpu) {
    compareLists(cpuDistances.data(), cpuIndices.data(),
                 gpuDistances.data(), gpuIndices.data(),
                 numQueries, FLAGS_k,
                 "", true, FLAGS_diff, false);
  }

  CUDA_VERIFY(cudaDeviceSynchronize());

  return 0;
}
