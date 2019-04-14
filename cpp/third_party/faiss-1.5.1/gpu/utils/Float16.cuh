/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda.h>
#include "../GpuResources.h"
#include "DeviceTensor.cuh"

// For float16, We use the half datatype, expecting it to be a struct
// as in CUDA 7.5.
#if CUDA_VERSION >= 7050
#define FAISS_USE_FLOAT16 1

// Some compute capabilities have full float16 ALUs.
#if __CUDA_ARCH__ >= 530
#define FAISS_USE_FULL_FLOAT16 1
#endif // __CUDA_ARCH__ types

#endif // CUDA_VERSION

#ifdef FAISS_USE_FLOAT16
#include <cuda_fp16.h>
#endif

namespace faiss { namespace gpu {

#ifdef FAISS_USE_FLOAT16

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

/// Copies `in` to `out` while performing a float32 -> float16 conversion
void runConvertToFloat16(half* out,
                         const float* in,
                         size_t num,
                         cudaStream_t stream);

/// Copies `in` to `out` while performing a float16 -> float32
/// conversion
void runConvertToFloat32(float* out,
                         const half* in,
                         size_t num,
                         cudaStream_t stream);

template <int Dim>
void toHalf(cudaStream_t stream,
            Tensor<float, Dim, true>& in,
            Tensor<half, Dim, true>& out) {
  FAISS_ASSERT(in.numElements() == out.numElements());

  // The memory is contiguous (the `true`), so apply a pointwise
  // kernel to convert
  runConvertToFloat16(out.data(), in.data(), in.numElements(), stream);
}

template <int Dim>
DeviceTensor<half, Dim, true> toHalf(GpuResources* resources,
                                     cudaStream_t stream,
                                     Tensor<float, Dim, true>& in) {
  DeviceTensor<half, Dim, true> out;
  if (resources) {
    out = std::move(DeviceTensor<half, Dim, true>(
                      resources->getMemoryManagerCurrentDevice(),
                      in.sizes(),
                      stream));
  } else {
    out = std::move(DeviceTensor<half, Dim, true>(in.sizes()));
  }

  toHalf<Dim>(stream, in, out);
  return out;
}

template <int Dim>
void fromHalf(cudaStream_t stream,
            Tensor<half, Dim, true>& in,
            Tensor<float, Dim, true>& out) {
  FAISS_ASSERT(in.numElements() == out.numElements());

  // The memory is contiguous (the `true`), so apply a pointwise
  // kernel to convert
  runConvertToFloat32(out.data(), in.data(), in.numElements(), stream);
}

template <int Dim>
DeviceTensor<float, Dim, true> fromHalf(GpuResources* resources,
                                        cudaStream_t stream,
                                        Tensor<half, Dim, true>& in) {
  DeviceTensor<float, Dim, true> out;
  if (resources) {
    out = std::move(DeviceTensor<float, Dim, true>(
                      resources->getMemoryManagerCurrentDevice(),
                      in.sizes(),
                      stream));
  } else {
    out = std::move(DeviceTensor<float, Dim, true>(in.sizes()));
  }

  fromHalf<Dim>(stream, in, out);
  return out;
}

__half hostFloat2Half(float v);

#endif // FAISS_USE_FLOAT16

} } // namespace
