/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda.h>
#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/utils/DeviceTensor.cuh>

// We require at least CUDA 8.0 for compilation
#if CUDA_VERSION < 8000
#error "CUDA >= 8.0 is required"
#endif

// Some compute capabilities have full float16 ALUs.
#if __CUDA_ARCH__ >= 530
#define FAISS_USE_FULL_FLOAT16 1
#endif // __CUDA_ARCH__ types

#include <cuda_fp16.h>

namespace faiss { namespace gpu {

// 64 bytes containing 4 half (float16) values
struct Half4 {
  half2 a;
  half2 b;
};

inline __device__ float4 half4ToFloat4(Half4 v) {
  float2 a = __half22float2(v.a);
  float2 b = __half22float2(v.b);

  float4 out;
  out.x = a.x;
  out.y = a.y;
  out.z = b.x;
  out.w = b.y;

  return out;
}

inline __device__ Half4 float4ToHalf4(float4 v) {
  float2 a;
  a.x = v.x;
  a.y = v.y;

  float2 b;
  b.x = v.z;
  b.y = v.w;

  Half4 out;
  out.a = __float22half2_rn(a);
  out.b = __float22half2_rn(b);

  return out;
}

// 128 bytes containing 8 half (float16) values
struct Half8 {
  Half4 a;
  Half4 b;
};

/// Returns true if the given device supports native float16 math
bool getDeviceSupportsFloat16Math(int device);

__half hostFloat2Half(float v);

} } // namespace
