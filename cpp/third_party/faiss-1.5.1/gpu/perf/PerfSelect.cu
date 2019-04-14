/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "../utils/DeviceDefs.cuh"
#include "../utils/DeviceUtils.h"
#include "../utils/BlockSelectKernel.cuh"
#include "../utils/WarpSelectKernel.cuh"
#include "../utils/HostTensor.cuh"
#include "../utils/DeviceTensor.cuh"
#include "../test/TestUtils.h"
#include <algorithm>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <sstream>
#include <unordered_map>
#include <vector>

DEFINE_int32(rows, 10000, "rows in matrix");
DEFINE_int32(cols, 40000, "cols in matrix");
DEFINE_int32(k, 100, "k");
DEFINE_bool(dir, false, "direction of sort");
DEFINE_bool(warp, false, "warp select");
DEFINE_int32(iter, 5, "iterations to run");
DEFINE_bool(k_powers, false, "test k powers of 2 from 1 -> max k");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::vector<float> v = faiss::gpu::randVecs(FLAGS_rows, FLAGS_cols);
  faiss::gpu::HostTensor<float, 2, true> hostVal({FLAGS_rows, FLAGS_cols});

  for (int r = 0; r < FLAGS_rows; ++r) {
    for (int c = 0; c < FLAGS_cols; ++c) {
      hostVal[r][c] = v[r * FLAGS_cols + c];
    }
  }

  // Select top-k on GPU
  faiss::gpu::DeviceTensor<float, 2, true> gpuVal(hostVal, 0);

  int startK = FLAGS_k;
  int limitK = FLAGS_k;

  if (FLAGS_k_powers) {
    startK = 1;
    limitK = GPU_MAX_SELECTION_K;
  }

  for (int k = startK; k <= limitK; k *= 2) {
    faiss::gpu::DeviceTensor<float, 2, true> gpuOutVal({FLAGS_rows, k});
    faiss::gpu::DeviceTensor<int, 2, true> gpuOutInd({FLAGS_rows, k});

    for (int i = 0; i < FLAGS_iter; ++i) {
      if (FLAGS_warp) {
        faiss::gpu::runWarpSelect(gpuVal, gpuOutVal, gpuOutInd,
                                  FLAGS_dir, k, 0);
      } else {
        faiss::gpu::runBlockSelect(gpuVal, gpuOutVal, gpuOutInd,
                                   FLAGS_dir, k, 0);
      }
    }
  }

  cudaDeviceSynchronize();
}
