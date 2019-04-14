/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "blockselect/BlockSelectImpl.cuh"
#include "DeviceDefs.cuh"

namespace faiss { namespace gpu {

#ifdef FAISS_USE_FLOAT16

// warp Q to thread Q:
// 1, 1
// 32, 2
// 64, 3
// 128, 3
// 256, 4
// 512, 8
// 1024, 8
// 2048, 8

BLOCK_SELECT_DECL(half, true, 1);
BLOCK_SELECT_DECL(half, true, 32);
BLOCK_SELECT_DECL(half, true, 64);
BLOCK_SELECT_DECL(half, true, 128);
BLOCK_SELECT_DECL(half, true, 256);
BLOCK_SELECT_DECL(half, true, 512);
BLOCK_SELECT_DECL(half, true, 1024);
#if GPU_MAX_SELECTION_K >= 2048
BLOCK_SELECT_DECL(half, true, 2048);
#endif

BLOCK_SELECT_DECL(half, false, 1);
BLOCK_SELECT_DECL(half, false, 32);
BLOCK_SELECT_DECL(half, false, 64);
BLOCK_SELECT_DECL(half, false, 128);
BLOCK_SELECT_DECL(half, false, 256);
BLOCK_SELECT_DECL(half, false, 512);
BLOCK_SELECT_DECL(half, false, 1024);
#if GPU_MAX_SELECTION_K >= 2048
BLOCK_SELECT_DECL(half, false, 2048);
#endif

void runBlockSelect(Tensor<half, 2, true>& in,
                    Tensor<half, 2, true>& outK,
                    Tensor<int, 2, true>& outV,
                    bool dir, int k, cudaStream_t stream) {
  FAISS_ASSERT(k <= GPU_MAX_SELECTION_K);

  if (dir) {
    if (k == 1) {
      BLOCK_SELECT_CALL(half, true, 1);
    } else if (k <= 32) {
      BLOCK_SELECT_CALL(half, true, 32);
    } else if (k <= 64) {
      BLOCK_SELECT_CALL(half, true, 64);
    } else if (k <= 128) {
      BLOCK_SELECT_CALL(half, true, 128);
    } else if (k <= 256) {
      BLOCK_SELECT_CALL(half, true, 256);
    } else if (k <= 512) {
      BLOCK_SELECT_CALL(half, true, 512);
    } else if (k <= 1024) {
      BLOCK_SELECT_CALL(half, true, 1024);
#if GPU_MAX_SELECTION_K >= 2048
    } else if (k <= 2048) {
      BLOCK_SELECT_CALL(half, true, 2048);
#endif
    }
  } else {
    if (k == 1) {
      BLOCK_SELECT_CALL(half, false, 1);
    } else if (k <= 32) {
      BLOCK_SELECT_CALL(half, false, 32);
    } else if (k <= 64) {
      BLOCK_SELECT_CALL(half, false, 64);
    } else if (k <= 128) {
      BLOCK_SELECT_CALL(half, false, 128);
    } else if (k <= 256) {
      BLOCK_SELECT_CALL(half, false, 256);
    } else if (k <= 512) {
      BLOCK_SELECT_CALL(half, false, 512);
    } else if (k <= 1024) {
      BLOCK_SELECT_CALL(half, false, 1024);
#if GPU_MAX_SELECTION_K >= 2048
    } else if (k <= 2048) {
      BLOCK_SELECT_CALL(half, false, 2048);
#endif
    }
  }
}

void runBlockSelectPair(Tensor<half, 2, true>& inK,
                        Tensor<int, 2, true>& inV,
                        Tensor<half, 2, true>& outK,
                        Tensor<int, 2, true>& outV,
                        bool dir, int k, cudaStream_t stream) {
  FAISS_ASSERT(k <= GPU_MAX_SELECTION_K);

  if (dir) {
    if (k == 1) {
      BLOCK_SELECT_PAIR_CALL(half, true, 1);
    } else if (k <= 32) {
      BLOCK_SELECT_PAIR_CALL(half, true, 32);
    } else if (k <= 64) {
      BLOCK_SELECT_PAIR_CALL(half, true, 64);
    } else if (k <= 128) {
      BLOCK_SELECT_PAIR_CALL(half, true, 128);
    } else if (k <= 256) {
      BLOCK_SELECT_PAIR_CALL(half, true, 256);
    } else if (k <= 512) {
      BLOCK_SELECT_PAIR_CALL(half, true, 512);
    } else if (k <= 1024) {
      BLOCK_SELECT_PAIR_CALL(half, true, 1024);
#if GPU_MAX_SELECTION_K >= 2048
    } else if (k <= 2048) {
      BLOCK_SELECT_PAIR_CALL(half, true, 2048);
#endif
    }
  } else {
    if (k == 1) {
      BLOCK_SELECT_PAIR_CALL(half, false, 1);
    } else if (k <= 32) {
      BLOCK_SELECT_PAIR_CALL(half, false, 32);
    } else if (k <= 64) {
      BLOCK_SELECT_PAIR_CALL(half, false, 64);
    } else if (k <= 128) {
      BLOCK_SELECT_PAIR_CALL(half, false, 128);
    } else if (k <= 256) {
      BLOCK_SELECT_PAIR_CALL(half, false, 256);
    } else if (k <= 512) {
      BLOCK_SELECT_PAIR_CALL(half, false, 512);
    } else if (k <= 1024) {
      BLOCK_SELECT_PAIR_CALL(half, false, 1024);
#if GPU_MAX_SELECTION_K >= 2048
    } else if (k <= 2048) {
      BLOCK_SELECT_PAIR_CALL(half, false, 2048);
#endif
    }
  }
}

#endif

} } // namespace
