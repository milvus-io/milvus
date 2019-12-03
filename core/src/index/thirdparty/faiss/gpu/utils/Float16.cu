/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#include <faiss/gpu/utils/Float16.cuh>
#include <faiss/gpu/utils/nvidia/fp16_emu.cuh>
#include <thrust/execution_policy.h>
#include <thrust/transform.h>

namespace faiss { namespace gpu {

bool getDeviceSupportsFloat16Math(int device) {
  const auto& prop = getDeviceProperties(device);

  return (prop.major >= 6 ||
          (prop.major == 5 && prop.minor >= 3));
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
