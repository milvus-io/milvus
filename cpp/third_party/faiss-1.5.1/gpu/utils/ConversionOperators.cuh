/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda.h>
#include "../../Index.h"
#include "Float16.cuh"

namespace faiss { namespace gpu {

//
// Conversion utilities
//

struct IntToIdxType {
  inline __device__ faiss::Index::idx_t operator()(int v) const {
    return (faiss::Index::idx_t) v;
  }
};

template <typename T>
struct ConvertTo {
};

template <>
struct ConvertTo<float> {
  static inline __device__ float to(float v) { return v; }
#ifdef FAISS_USE_FLOAT16
  static inline __device__ float to(half v) { return __half2float(v); }
#endif
};

template <>
struct ConvertTo<float2> {
  static inline __device__ float2 to(float2 v) { return v; }
#ifdef FAISS_USE_FLOAT16
  static inline __device__ float2 to(half2 v) { return __half22float2(v); }
#endif
};

template <>
struct ConvertTo<float4> {
  static inline __device__ float4 to(float4 v) { return v; }
#ifdef FAISS_USE_FLOAT16
  static inline __device__ float4 to(Half4 v) { return half4ToFloat4(v); }
#endif
};

#ifdef FAISS_USE_FLOAT16
template <>
struct ConvertTo<half> {
  static inline __device__ half to(float v) { return __float2half(v); }
  static inline __device__ half to(half v) { return v; }
};

template <>
struct ConvertTo<half2> {
  static inline __device__ half2 to(float2 v) { return __float22half2_rn(v); }
  static inline __device__ half2 to(half2 v) { return v; }
};

template <>
struct ConvertTo<Half4> {
  static inline __device__ Half4 to(float4 v) { return float4ToHalf4(v); }
  static inline __device__ Half4 to(Half4 v) { return v; }
};
#endif


} } // namespace
