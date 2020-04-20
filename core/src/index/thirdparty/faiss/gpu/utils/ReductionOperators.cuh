/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda.h>
#include <faiss/gpu/utils/Limits.cuh>
#include <faiss/gpu/utils/MathOperators.cuh>
#include <faiss/gpu/utils/Pair.cuh>

namespace faiss { namespace gpu {

template <typename T>
struct Sum {
  __device__ inline T operator()(T a, T b) const {
    return Math<T>::add(a, b);
  }

  inline __device__ T identity() const {
    return Math<T>::zero();
  }
};

template <typename T>
struct Min {
  __device__ inline T operator()(T a, T b) const {
    return Math<T>::lt(a, b) ? a : b;
  }

  inline __device__ T identity() const {
    return Limits<T>::getMax();
  }
};

template <typename T>
struct Max {
  __device__ inline T operator()(T a, T b) const {
    return Math<T>::gt(a, b) ? a : b;
  }

  inline __device__ T identity() const {
    return Limits<T>::getMin();
  }
};

/// Used for producing segmented prefix scans; the value of the Pair
/// denotes the start of a new segment for the scan
template <typename T, typename ReduceOp>
struct SegmentedReduce {
  inline __device__ SegmentedReduce(const ReduceOp& o)
      : op(o) {
  }

  __device__
  inline Pair<T, bool>
  operator()(const Pair<T, bool>& a, const Pair<T, bool>& b) const {
    return Pair<T, bool>(b.v ? b.k : op(a.k, b.k),
                         a.v || b.v);
  }

  inline __device__ Pair<T, bool> identity() const {
    return Pair<T, bool>(op.identity(), false);
  }

  ReduceOp op;
};

} } // namespace
