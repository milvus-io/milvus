/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include "Float16.cuh"
#include "nvidia/fp16_emu.cuh"
#include <thrust/execution_policy.h>
#include <thrust/transform.h>

#ifdef FAISS_USE_FLOAT16

namespace faiss { namespace gpu {

bool getDeviceSupportsFloat16Math(int device) {
  const auto& prop = getDeviceProperties(device);

  return (prop.major >= 6 ||
          (prop.major == 5 && prop.minor >= 3));
}

struct FloatToHalf {
  __device__ half operator()(float v) const { return __float2half(v); }
};

struct HalfToFloat {
  __device__ float operator()(half v) const { return __half2float(v); }
};

void runConvertToFloat16(half* out,
                         const float* in,
                         size_t num,
                         cudaStream_t stream) {
  thrust::transform(thrust::cuda::par.on(stream),
                    in, in + num, out, FloatToHalf());
}

void runConvertToFloat32(float* out,
                         const half* in,
                         size_t num,
                         cudaStream_t stream) {
  thrust::transform(thrust::cuda::par.on(stream),
                    in, in + num, out, HalfToFloat());
}

__half hostFloat2Half(float a) {
#if CUDA_VERSION >= 9000
  __half_raw raw;
  raw.x = cpu_float2half_rn(a).x;
  return __half(raw);
#else
  __half h;
  h.x = cpu_float2half_rn(a).x;
  return h;
#endif
}

} } // namespace

#endif // FAISS_USE_FLOAT16
