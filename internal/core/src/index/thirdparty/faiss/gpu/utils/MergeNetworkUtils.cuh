/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
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
