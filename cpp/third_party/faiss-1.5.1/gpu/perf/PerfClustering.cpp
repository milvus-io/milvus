/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "../../utils.h"
#include "../../Clustering.h"
#include "../GpuIndexFlat.h"
#include "../StandardGpuResources.h"
#include "IndexWrapper.h"
#include "../utils/DeviceUtils.h"
#include "../utils/Timer.h"
#include <gflags/gflags.h>
#include <memory>
#include <vector>

#include <cuda_profiler_api.h>

DEFINE_int32(num, 10000, "# of vecs");
DEFINE_int32(k, 100, "# of clusters");
DEFINE_int32(dim, 128, "# of dimensions");
DEFINE_int32(niter, 10, "# of iterations");
DEFINE_bool(L2_metric, true, "If true, use L2 metric. If false, use IP metric");
DEFINE_bool(use_float16, false, "use float16 vectors and math");
DEFINE_bool(transposed, false, "transposed vector storage");
DEFINE_bool(verbose, false, "turn on clustering logging");
DEFINE_int64(seed, -1, "specify random seed");
DEFINE_int32(num_gpus, 1, "number of gpus to use");
DEFINE_int64(min_paging_size, -1, "minimum size to use CPU -> GPU paged copies");
DEFINE_int64(pinned_mem, -1, "pinned memory allocation to use");
DEFINE_int32(max_points, -1, "max points per centroid");

using namespace faiss::gpu;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  cudaProfilerStop();

  auto seed = FLAGS_seed != -1L ? FLAGS_seed : time(nullptr);
  printf("using seed %ld\n", seed);

  std::vector<float> vecs((size_t) FLAGS_num * FLAGS_dim);
  faiss::float_rand(vecs.data(), vecs.size(), seed);

  printf("K-means metric %s dim %d centroids %d num train %d niter %d\n",
         FLAGS_L2_metric ? "L2" : "IP",
         FLAGS_dim, FLAGS_k, FLAGS_num, FLAGS_niter);
  printf("float16 math %s\n", FLAGS_use_float16 ? "enabled" : "disabled");
  printf("transposed storage %s\n", FLAGS_transposed ? "enabled" : "disabled");
  printf("verbose %s\n", FLAGS_verbose ? "enabled" : "disabled");

  auto initFn = [](faiss::gpu::GpuResources* res, int dev) ->
    std::unique_ptr<faiss::gpu::GpuIndexFlat> {
    if (FLAGS_pinned_mem >= 0) {
      ((faiss::gpu::StandardGpuResources*) res)->setPinnedMemory(
        FLAGS_pinned_mem);
    }

    GpuIndexFlatConfig config;
    config.device = dev;
    config.useFloat16 = FLAGS_use_float16;
    config.storeTransposed = FLAGS_transposed;

    auto p = std::unique_ptr<faiss::gpu::GpuIndexFlat>(
      FLAGS_L2_metric ?
      (faiss::gpu::GpuIndexFlat*)
      new faiss::gpu::GpuIndexFlatL2(res, FLAGS_dim, config) :
      (faiss::gpu::GpuIndexFlat*)
      new faiss::gpu::GpuIndexFlatIP(res, FLAGS_dim, config));

    if (FLAGS_min_paging_size >= 0) {
      p->setMinPagingSize(FLAGS_min_paging_size);
    }
    return p;
  };

  IndexWrapper<faiss::gpu::GpuIndexFlat> gpuIndex(FLAGS_num_gpus, initFn);

  CUDA_VERIFY(cudaProfilerStart());
  faiss::gpu::synchronizeAllDevices();

  float gpuTime = 0.0f;

  faiss::ClusteringParameters cp;
  cp.niter = FLAGS_niter;
  cp.verbose = FLAGS_verbose;

  if (FLAGS_max_points > 0) {
    cp.max_points_per_centroid = FLAGS_max_points;
  }

  faiss::Clustering kmeans(FLAGS_dim, FLAGS_k, cp);

  // Time k-means
  {
    CpuTimer timer;

    kmeans.train(FLAGS_num, vecs.data(), *(gpuIndex.getIndex()));

    // There is a device -> host copy above, so no need to time
    // additional synchronization with the GPU
    gpuTime = timer.elapsedMilliseconds();
  }

  CUDA_VERIFY(cudaProfilerStop());
  printf("k-means time %.3f ms\n", gpuTime);

  CUDA_VERIFY(cudaDeviceSynchronize());

  return 0;
}
