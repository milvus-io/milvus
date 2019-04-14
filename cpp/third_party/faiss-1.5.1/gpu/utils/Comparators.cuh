/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda.h>
#include "Float16.cuh"

namespace faiss { namespace gpu {

template <typename T>
struct Comparator {
  __device__ static inline bool lt(T a, T b) {
    return a < b;
  }

  __device__ static inline bool gt(T a, T b) {
    return a > b;
  }
};

#ifdef FAISS_USE_FLOAT16

template <>
struct Comparator<half> {
  __device__ static inline bool lt(half a, half b) {
#if FAISS_USE_FULL_FLOAT16
    return __hlt(a, b);
#else
    return __half2float(a) < __half2float(b);
#endif // FAISS_USE_FULL_FLOAT16
  }

  __device__ static inline bool gt(half a, half b) {
#if FAISS_USE_FULL_FLOAT16
    return __hgt(a, b);
#else
    return __half2float(a) > __half2float(b);
#endif // FAISS_USE_FULL_FLOAT16
  }
};

#endif // FAISS_USE_FLOAT16

} } // namespace
