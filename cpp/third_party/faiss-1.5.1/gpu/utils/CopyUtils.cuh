/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "DeviceTensor.cuh"
#include "HostTensor.cuh"

namespace faiss { namespace gpu {

/// Ensure the memory at `p` is either on the given device, or copy it
/// to the device in a new allocation.
/// If `resources` is provided, then we will perform a temporary
/// memory allocation if needed. Otherwise, we will call cudaMalloc if
/// needed.
template <typename T, int Dim>
DeviceTensor<T, Dim, true> toDevice(GpuResources* resources,
                                    int dstDevice,
                                    T* src,
                                    cudaStream_t stream,
                                    std::initializer_list<int> sizes) {
  int dev = getDeviceForAddress(src);

  if (dev == dstDevice) {
    // On device we expect
    return DeviceTensor<T, Dim, true>(src, sizes);
  } else {
    // On different device or on host
    DeviceScope scope(dstDevice);

    Tensor<T, Dim, true> oldT(src, sizes);

    if (resources) {
      DeviceTensor<T, Dim, true> newT(resources->getMemoryManager(dstDevice),
                                      sizes,
                                      stream);

      newT.copyFrom(oldT, stream);
      return newT;
    } else {
      DeviceTensor<T, Dim, true> newT(sizes);

      newT.copyFrom(oldT, stream);
      return newT;
    }
  }
}

/// Copies a device array's allocation to an address, if necessary
template <typename T>
inline void fromDevice(T* src, T* dst, size_t num, cudaStream_t stream) {
  // It is possible that the array already represents memory at `p`,
  // in which case no copy is needed
  if (src == dst) {
    return;
  }

  int dev = getDeviceForAddress(dst);

  if (dev == -1) {
    CUDA_VERIFY(cudaMemcpyAsync(dst,
                                src,
                                num * sizeof(T),
                                cudaMemcpyDeviceToHost,
                                stream));
  } else {
    CUDA_VERIFY(cudaMemcpyAsync(dst,
                                src,
                                num * sizeof(T),
                                cudaMemcpyDeviceToDevice,
                                stream));
  }
}

/// Copies a device array's allocation to an address, if necessary
template <typename T, int Dim>
void fromDevice(Tensor<T, Dim, true>& src, T* dst, cudaStream_t stream) {
  FAISS_ASSERT(src.isContiguous());
  fromDevice(src.data(), dst, src.numElements(), stream);
}

} } // namespace
