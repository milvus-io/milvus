/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/gpu/utils/warpselect/WarpSelectImpl.cuh>
#include <faiss/gpu/utils/DeviceDefs.cuh>

namespace faiss { namespace gpu {

// warp Q to thread Q:
// 1, 1
// 32, 2
// 64, 3
// 128, 3
// 256, 4
// 512, 8
// 1024, 8
// 2048, 8

WARP_SELECT_DECL(half, true, 1);
WARP_SELECT_DECL(half, true, 32);
WARP_SELECT_DECL(half, true, 64);
WARP_SELECT_DECL(half, true, 128);
WARP_SELECT_DECL(half, true, 256);
WARP_SELECT_DECL(half, true, 512);
WARP_SELECT_DECL(half, true, 1024);
#if GPU_MAX_SELECTION_K >= 2048
WARP_SELECT_DECL(half, true, 2048);
#endif

WARP_SELECT_DECL(half, false, 1);
WARP_SELECT_DECL(half, false, 32);
WARP_SELECT_DECL(half, false, 64);
WARP_SELECT_DECL(half, false, 128);
WARP_SELECT_DECL(half, false, 256);
WARP_SELECT_DECL(half, false, 512);
WARP_SELECT_DECL(half, false, 1024);
#if GPU_MAX_SELECTION_K >= 2048
WARP_SELECT_DECL(half, false, 2048);
#endif

void runWarpSelect(Tensor<half, 2, true>& in,
                      Tensor<half, 2, true>& outK,
                      Tensor<int, 2, true>& outV,
                      bool dir, int k, cudaStream_t stream) {
  FAISS_ASSERT(k <= 1024);

  if (dir) {
    if (k == 1) {
      WARP_SELECT_CALL(half, true, 1);
    } else if (k <= 32) {
      WARP_SELECT_CALL(half, true, 32);
    } else if (k <= 64) {
      WARP_SELECT_CALL(half, true, 64);
    } else if (k <= 128) {
      WARP_SELECT_CALL(half, true, 128);
    } else if (k <= 256) {
      WARP_SELECT_CALL(half, true, 256);
    } else if (k <= 512) {
      WARP_SELECT_CALL(half, true, 512);
    } else if (k <= 1024) {
      WARP_SELECT_CALL(half, true, 1024);
#if GPU_MAX_SELECTION_K >= 2048
    } else if (k <= 2048) {
      WARP_SELECT_CALL(half, true, 2048);
#endif
    }
  } else {
    if (k == 1) {
      WARP_SELECT_CALL(half, false, 1);
    } else if (k <= 32) {
      WARP_SELECT_CALL(half, false, 32);
    } else if (k <= 64) {
      WARP_SELECT_CALL(half, false, 64);
    } else if (k <= 128) {
      WARP_SELECT_CALL(half, false, 128);
    } else if (k <= 256) {
      WARP_SELECT_CALL(half, false, 256);
    } else if (k <= 512) {
      WARP_SELECT_CALL(half, false, 512);
    } else if (k <= 1024) {
      WARP_SELECT_CALL(half, false, 1024);
#if GPU_MAX_SELECTION_K >= 2048
    } else if (k <= 2048) {
      WARP_SELECT_CALL(half, false, 2048);
#endif
    }
  }
}

} } // namespace
