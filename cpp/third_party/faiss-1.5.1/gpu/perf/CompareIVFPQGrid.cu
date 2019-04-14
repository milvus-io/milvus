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
#include <gflags/gflags.h>
#include <memory>
#include <vector>

DEFINE_int32(k, 10, "final number of closest results returned");
DEFINE_string(in, "/home/jhj/local/ivfpq_index.out", "index file for input");
DEFINE_bool(use_precomputed, true, "enable or disable precomputed codes");
DEFINE_bool(float16_lookup, false, "use float16 residual distance tables");
DEFINE_int32(num_gpus, 1, "number of gpus to use");
DEFINE_int32(index, 2, "0 = no indices on GPU; 1 = 32 bit, 2 = 64 bit on GPU");

using namespace faiss::gpu;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto seed = time(nullptr);
  auto k = FLAGS_k;

  auto index = std::unique_ptr<faiss::IndexIVFPQ>(
    dynamic_cast<faiss::IndexIVFPQ*>(faiss::read_index(FLAGS_in.c_str())));
  FAISS_ASSERT((bool) index);

  auto dim = index->d;
  auto codes = index->pq.M;
  auto bitsPerCode = index->pq.nbits;

  printf("Database: dim %d num vecs %ld\n", dim, index->ntotal);
  printf("Coarse centroids: %ld\n", index->quantizer->ntotal);
  printf("PQ centroids: codes %ld bits per code %ld\n", codes, bitsPerCode);
  printf("L2 lookup: total k %d, precomputed codes %d\n\n",
         k, FLAGS_use_precomputed);

  // Convert to GPU index
  printf("Copying index to %d GPU(s)...\n", FLAGS_num_gpus);

  bool precomp = FLAGS_use_precomputed;
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
  printf("copy done\n");

  auto querySizes = std::vector<int>{1, 4, 16, 64, 256, 1024, 4096, 16384};
  auto nprobeSizes = std::vector<int>{1, 4, 8, 16, 32, 64, 128, 256};

  HostTensor<float, 2, true> cpuTimePerVector(
    {(int) querySizes.size(), (int) nprobeSizes.size()});
  HostTensor<float, 2, true> gpuTimePerVector(
    {(int) querySizes.size(), (int) nprobeSizes.size()});

  printf("GPU relative speedup over CPU (x):\n");

  for (auto q = 0; q < querySizes.size(); ++q) {
    auto numQueries = querySizes[q];
    bool first = true;

    for (auto p = 0; p < nprobeSizes.size(); ++p) {
      auto nprobe = nprobeSizes[q];

      HostTensor<float, 2, true> cpuQuery{numQueries, dim};
      faiss::float_rand(cpuQuery.data(), cpuQuery.numElements(), seed);

      HostTensor<faiss::Index::idx_t, 2, true> resultIndices{numQueries, k};
      HostTensor<float, 2, true> resultDistances{numQueries, k};

      index->nprobe = nprobe;

      float cpuTime = 0.0f;
      {
        CpuTimer timer;
        if (!FLAGS_use_precomputed) {
          index->use_precomputed_table = 0;
        }
        index->search(numQueries, cpuQuery.data(),
                      k, resultDistances.data(), resultIndices.data());
        cpuTime = timer.elapsedMilliseconds();
        cpuTimePerVector[q][p] = cpuTime / (float) numQueries;
      }

      gpuIndex.setNumProbes(nprobe);

      HostTensor<float, 2, true>
        gpuHostDistances({numQueries, k});
      HostTensor<faiss::Index::idx_t, 2, true>
        gpuHostIndices({numQueries, k});

      DeviceTensor<float, 2, true> gpuQuery(cpuQuery, 0);
      DeviceTensor<float, 2, true> gpuDistances({numQueries, k});
      DeviceTensor<long, 2, true> gpuIndices({numQueries, k});
      CUDA_VERIFY(cudaDeviceSynchronize());

      float gpuTime = 0.0f;

      {
        CpuTimer timer;

        gpuIndex.getIndex()->search(cpuQuery.getSize(0),
                                    cpuQuery.data(),
                                    FLAGS_k,
                                    gpuHostDistances.data(),
                                    gpuHostIndices.data());

        CUDA_VERIFY(cudaDeviceSynchronize());
        gpuTime = timer.elapsedMilliseconds();
      }

      gpuTimePerVector[q][p] = gpuTime / (float) numQueries;

      if (!first) {
        printf(", ");
      }
      first = false;

      printf("%.2f", cpuTime / gpuTime);
    }

    printf("\n");
  }

  printf("\n");
  printf("CPU time per query vector (us):\n");

  for (int q = 0; q < cpuTimePerVector.getSize(0); ++q) {
    bool first = true;

    for (int p = 0; p < cpuTimePerVector.getSize(1); ++p) {
      if (!first) {
        printf(", ");
      }
      first = false;

      printf("%.1f", (float) cpuTimePerVector[q][p] * 1000.0f);
    }

    printf("\n");
  }

  printf("\n");
  printf("GPU time per query vector (us):\n");

  for (int q = 0; q < gpuTimePerVector.getSize(0); ++q) {
    bool first = true;

    for (int p = 0; p < gpuTimePerVector.getSize(1); ++p) {
      if (!first) {
        printf(", ");
      }
      first = false;

      printf("%.1f", (float) gpuTimePerVector[q][p] * 1000.0f);
    }

    printf("\n");
  }

  return 0;
}
