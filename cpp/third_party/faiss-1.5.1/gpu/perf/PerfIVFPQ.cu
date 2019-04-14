/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "../../IndexIVFPQ.h"
#include "../../index_io.h"
#include "../../utils.h"

#include "../GpuIndexIVFPQ.h"
#include "IndexWrapper.h"
#include "../test/TestUtils.h"
#include "../utils/DeviceTensor.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/HostTensor.cuh"
#include "../utils/Timer.h"

#include <cuda_profiler_api.h>
#include <gflags/gflags.h>
#include <map>
#include <memory>
#include <vector>

DEFINE_int32(nprobe, 5, "number of coarse centroids to probe");
DEFINE_int32(k, 3, "final number of closest results returned");
DEFINE_int32(num_queries, 3, "number of query vectors");
DEFINE_string(in, "/home/jhj/local/index.out", "index file for input");
DEFINE_bool(diff, true, "show exact distance + index output discrepancies");
DEFINE_bool(use_precomputed, true, "enable or disable precomputed codes");
DEFINE_bool(float16_lookup, false, "use float16 residual distance tables");
DEFINE_int64(seed, -1, "specify random seed");
DEFINE_int32(num_gpus, 1, "number of gpus to use");
DEFINE_int32(index, 2, "0 = no indices on GPU; 1 = 32 bit, 2 = 64 bit on GPU");

using namespace faiss::gpu;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  CUDA_VERIFY(cudaProfilerStop());

  auto seed = FLAGS_seed != -1L ? FLAGS_seed : time(nullptr);
  printf("using seed %ld\n", seed);

  auto numQueries = FLAGS_num_queries;

  auto index = std::unique_ptr<faiss::IndexIVFPQ>(
    dynamic_cast<faiss::IndexIVFPQ*>(faiss::read_index(FLAGS_in.c_str())));
  FAISS_ASSERT((bool) index);
  index->nprobe = FLAGS_nprobe;

  if (!FLAGS_use_precomputed) {
    index->use_precomputed_table = 0;
  }

  auto dim = index->d;
  auto codes = index->pq.M;
  auto bitsPerCode = index->pq.nbits;

  printf("Database: dim %d num vecs %ld\n", dim, index->ntotal);
  printf("Coarse centroids: %ld\n", index->quantizer->ntotal);
  printf("PQ centroids: codes %ld bits per code %ld\n", codes, bitsPerCode);
  printf("L2 lookup: %d queries, nprobe %d, total k %d, "
         "precomputed codes %d\n\n",
         numQueries, FLAGS_nprobe, FLAGS_k,
         FLAGS_use_precomputed);

  // Convert to GPU index
  printf("Copying index to %d GPU(s)...\n", FLAGS_num_gpus);

  auto precomp = FLAGS_use_precomputed;
  auto indicesOpt = (faiss::gpu::IndicesOptions) FLAGS_index;
  auto useFloat16Lookup = FLAGS_float16_lookup;

  auto initFn = [precomp, indicesOpt, useFloat16Lookup, &index]
    (faiss::gpu::GpuResources* res, int dev) ->
    std::unique_ptr<faiss::gpu::GpuIndexIVFPQ> {

    faiss::gpu::GpuIndexIVFPQConfig config;
    config.device = dev;
    config.usePrecomputedTables = precomp;
    config.indicesOptions = indicesOpt;
    config.useFloat16LookupTables = useFloat16Lookup;

    auto p = std::unique_ptr<faiss::gpu::GpuIndexIVFPQ>(
      new faiss::gpu::GpuIndexIVFPQ(res, index.get(), config));

    return p;
  };

  IndexWrapper<faiss::gpu::GpuIndexIVFPQ> gpuIndex(FLAGS_num_gpus, initFn);
  gpuIndex.setNumProbes(FLAGS_nprobe);
  printf("copy done\n");

  // Build query vectors
  HostTensor<float, 2, true> cpuQuery({numQueries, dim});
  faiss::float_rand(cpuQuery.data(), cpuQuery.numElements(), seed);

  // Time faiss CPU
  HostTensor<float, 2, true> cpuDistances({numQueries, FLAGS_k});
  HostTensor<faiss::Index::idx_t, 2, true> cpuIndices({numQueries, FLAGS_k});

  float cpuTime = 0.0f;

  {
    CpuTimer timer;
    index->search(numQueries,
                  cpuQuery.data(),
                  FLAGS_k,
                  cpuDistances.data(),
                  cpuIndices.data());

    cpuTime = timer.elapsedMilliseconds();
  }

  printf("CPU time %.3f ms\n", cpuTime);

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

  compareLists(cpuDistances.data(), cpuIndices.data(),
               gpuDistances.data(), gpuIndices.data(),
               numQueries, FLAGS_k,
               "", true, FLAGS_diff, false);

  CUDA_VERIFY(cudaDeviceSynchronize());
  // printf("\ncudaMalloc usage %zd\n",
  //        resources.getMemoryManager().getHighWaterCudaMalloc());

  return 0;
}
