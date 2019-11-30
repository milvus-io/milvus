/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/utils/DeviceDefs.cuh>
#include <faiss/gpu/utils/PtxUtils.cuh>
#include <faiss/gpu/utils/ReductionOperators.cuh>
#include <faiss/gpu/utils/StaticUtils.h>
#include <faiss/gpu/utils/WarpShuffles.cuh>
#include <cuda.h>

namespace faiss { namespace gpu {

template <typename T, typename Op, int ReduceWidth = kWarpSize>
__device__ inline T warpReduceAll(T val, Op op) {
#pragma unroll
  for (int mask = ReduceWidth / 2; mask > 0; mask >>= 1) {
    val = op(val, shfl_xor(val, mask));
  }

  return val;
}

/// Sums a register value across all warp threads
template <typename T, int ReduceWidth = kWarpSize>
__device__ inline T warpReduceAllSum(T val) {
  return warpReduceAll<T, Sum<T>, ReduceWidth>(val, Sum<T>());
}

/// Performs a block-wide reduction
template <typename T, typename Op, bool BroadcastAll, bool KillWARDependency>
__device__ inline T blockReduceAll(T val, Op op, T* smem) {
  int laneId = getLaneId();
  int warpId = threadIdx.x / kWarpSize;

  val = warpReduceAll<T, Op>(val, op);
  if (laneId == 0) {
    smem[warpId] = val;
  }
  __syncthreads();

  if (warpId == 0) {
    val = laneId < utils::divUp(blockDim.x, kWarpSize) ? smem[laneId] :
      op.identity();
    val = warpReduceAll<T, Op>(val, op);

    if (BroadcastAll) {
      __threadfence_block();

      if (laneId == 0) {
        smem[0] = val;
      }
    }
  }

  if (BroadcastAll) {
    __syncthreads();
    val = smem[0];
  }

  if (KillWARDependency) {
    __syncthreads();
  }

  return val;
}

/// Performs a block-wide reduction of multiple values simultaneously
template <int Num, typename T, typename Op, bool BroadcastAll, bool KillWARDependency>
__device__ inline void blockReduceAll(T val[Num], Op op, T* smem) {
  int laneId = getLaneId();
  int warpId = threadIdx.x / kWarpSize;

#pragma unroll
  for (int i = 0; i < Num; ++i) {
    val[i] = warpReduceAll<T, Op>(val[i], op);
  }

  if (laneId == 0) {
#pragma unroll
    for (int i = 0; i < Num; ++i) {
      smem[warpId * Num + i] = val[i];
    }
  }

  __syncthreads();

  if (warpId == 0) {
#pragma unroll
    for (int i = 0; i < Num; ++i) {
      val[i] =
        laneId < utils::divUp(blockDim.x, kWarpSize) ? smem[laneId * Num + i] :
        op.identity();
      val[i] = warpReduceAll<T, Op>(val[i], op);
    }

    if (BroadcastAll) {
      __threadfence_block();

      if (laneId == 0) {
#pragma unroll
        for (int i = 0; i < Num; ++i) {
          smem[i] = val[i];
        }
      }
    }
  }

  if (BroadcastAll) {
    __syncthreads();
#pragma unroll
    for (int i = 0; i < Num; ++i) {
      val[i] = smem[i];
    }
  }

  if (KillWARDependency) {
    __syncthreads();
  }
}


/// Sums a register value across the entire block
template <typename T, bool BroadcastAll, bool KillWARDependency>
__device__ inline T blockReduceAllSum(T val, T* smem) {
  return blockReduceAll<T, Sum<T>, BroadcastAll, KillWARDependency>(
    val, Sum<T>(), smem);
}

template <int Num, typename T, bool BroadcastAll, bool KillWARDependency>
__device__ inline void blockReduceAllSum(T vals[Num], T* smem) {
  return blockReduceAll<Num, T, Sum<T>, BroadcastAll, KillWARDependency>(
    vals, Sum<T>(), smem);
}

} } // namespace
