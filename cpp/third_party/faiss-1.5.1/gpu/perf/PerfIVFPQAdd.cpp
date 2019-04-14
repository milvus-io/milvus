/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */



#include <cuda_profiler_api.h>
#include "../../IndexFlat.h"
#include "../../IndexIVFPQ.h"
#include "../GpuIndexIVFPQ.h"
#include "../StandardGpuResources.h"
#include "../test/TestUtils.h"
#include "../utils/DeviceUtils.h"
#include "../utils/Timer.h"
#include <gflags/gflags.h>
#include <map>
#include <vector>

DEFINE_int32(batches, 10, "number of batches of vectors to add");
DEFINE_int32(batch_size, 10000, "number of vectors in each batch");
DEFINE_int32(dim, 256, "dimension of vectors");
DEFINE_int32(centroids, 4096, "num coarse centroids to use");
DEFINE_int32(bytes_per_vec, 32, "bytes per encoded vector");
DEFINE_int32(bits_per_code, 8, "bits per PQ code");
DEFINE_int32(index, 2, "0 = no indices on GPU; 1 = 32 bit, 2 = 64 bit on GPU");
DEFINE_bool(time_gpu, true, "time add to GPU");
DEFINE_bool(time_cpu, false, "time add to CPU");
DEFINE_bool(per_batch_time, false, "print per-batch times");
DEFINE_bool(reserve_memory, false, "whether or not to pre-reserve memory");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  cudaProfilerStop();

  int dim = FLAGS_dim;
  int numCentroids = FLAGS_centroids;
  int bytesPerVec = FLAGS_bytes_per_vec;
  int bitsPerCode = FLAGS_bits_per_code;

  faiss::gpu::StandardGpuResources res;

  // IndexIVFPQ will complain, but just give us enough to get through this
  int numTrain = 4 * numCentroids;
  std::vector<float> trainVecs = faiss::gpu::randVecs(numTrain, dim);

  faiss::IndexFlatL2 coarseQuantizer(dim);
  faiss::IndexIVFPQ cpuIndex(&coarseQuantizer, dim, numCentroids,
                             bytesPerVec, bitsPerCode);
  if (FLAGS_time_cpu) {
    cpuIndex.train(numTrain, trainVecs.data());
  }

  faiss::gpu::GpuIndexIVFPQConfig config;
  config.device = 0;
  config.indicesOptions = (faiss::gpu::IndicesOptions) FLAGS_index;

  faiss::gpu::GpuIndexIVFPQ gpuIndex(
    &res, dim, numCentroids, bytesPerVec, bitsPerCode,
    faiss::METRIC_L2, config);

  if (FLAGS_time_gpu) {
    gpuIndex.train(numTrain, trainVecs.data());
    if (FLAGS_reserve_memory) {
      size_t numVecs = (size_t) FLAGS_batches * (size_t) FLAGS_batch_size;
      gpuIndex.reserveMemory(numVecs);
    }
  }

  cudaDeviceSynchronize();
  CUDA_VERIFY(cudaProfilerStart());

  float totalGpuTime = 0.0f;
  float totalCpuTime = 0.0f;

  for (int i = 0; i < FLAGS_batches; ++i) {
    if (!FLAGS_per_batch_time) {
      if (i % 10 == 0) {
        printf("Adding batch %d\n", i + 1);
      }
    }

    auto addVecs = faiss::gpu::randVecs(FLAGS_batch_size, dim);

    if (FLAGS_time_gpu) {
      faiss::gpu::CpuTimer timer;
      gpuIndex.add(FLAGS_batch_size, addVecs.data());
      CUDA_VERIFY(cudaDeviceSynchronize());
      auto time = timer.elapsedMilliseconds();

      totalGpuTime += time;

      if (FLAGS_per_batch_time) {
      printf("Batch %d | GPU time to add %d vecs: %.3f ms (%.5f ms per)\n",
             i + 1, FLAGS_batch_size, time, time / (float) FLAGS_batch_size);
      }
    }

    if (FLAGS_time_cpu) {
      faiss::gpu::CpuTimer timer;
      cpuIndex.add(FLAGS_batch_size, addVecs.data());
      auto time = timer.elapsedMilliseconds();

      totalCpuTime += time;

      if (FLAGS_per_batch_time) {
        printf("Batch %d | CPU time to add %d vecs: %.3f ms (%.5f ms per)\n",
               i + 1, FLAGS_batch_size, time, time / (float) FLAGS_batch_size);
      }
    }
  }

  CUDA_VERIFY(cudaProfilerStop());

  int total = FLAGS_batch_size * FLAGS_batches;

  if (FLAGS_time_gpu) {
    printf("%d dim, %d centroids, %d x %d encoding\n"
           "GPU time to add %d vectors (%d batches, %d per batch): "
           "%.3f ms (%.3f us per)\n",
           dim, numCentroids, bytesPerVec, bitsPerCode,
           total, FLAGS_batches, FLAGS_batch_size,
           totalGpuTime, totalGpuTime * 1000.0f / (float) total);
  }

  if (FLAGS_time_cpu) {
    printf("%d dim, %d centroids, %d x %d encoding\n"
           "CPU time to add %d vectors (%d batches, %d per batch): "
           "%.3f ms (%.3f us per)\n",
           dim, numCentroids, bytesPerVec, bitsPerCode,
           total, FLAGS_batches, FLAGS_batch_size,
           totalCpuTime, totalCpuTime * 1000.0f / (float) total);
  }

  return 0;
}
