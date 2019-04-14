/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

namespace faiss { namespace gpu {

template <typename T>
inline __device__ void swap(bool swap, T& x, T& y) {
  T tmp = x;
  x = swap ? y : x;
  y = swap ? tmp : y;
}

template <typename T>
inline __device__ void assign(bool assign, T& x, T y) {
  x = assign ? y : x;
}

} } // namespace
