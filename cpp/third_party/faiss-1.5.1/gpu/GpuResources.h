/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "utils/DeviceMemory.h"
#include <cuda_runtime.h>
#include <cublas_v2.h>
#include <utility>
#include <vector>

namespace faiss { namespace gpu {

/// Base class of GPU-side resource provider; hides provision of
/// cuBLAS handles, CUDA streams and a temporary memory manager
class GpuResources {
 public:
  virtual ~GpuResources();

  /// Call to pre-allocate resources for a particular device. If this is
  /// not called, then resources will be allocated at the first time
  /// of demand
  virtual void initializeForDevice(int device) = 0;

  /// Returns the cuBLAS handle that we use for the given device
  virtual cublasHandle_t getBlasHandle(int device) = 0;

  /// Returns the stream that we order all computation on for the
  /// given device
  virtual cudaStream_t getDefaultStream(int device) = 0;

  /// Returns the set of alternative streams that we use for the given device
  virtual std::vector<cudaStream_t> getAlternateStreams(int device) = 0;

  /// Returns the temporary memory manager for the given device
  virtual DeviceMemory& getMemoryManager(int device) = 0;

  /// Returns the available CPU pinned memory buffer
  virtual std::pair<void*, size_t> getPinnedMemory() = 0;

  /// Returns the stream on which we perform async CPU <-> GPU copies
  virtual cudaStream_t getAsyncCopyStream(int device) = 0;

  /// Calls getBlasHandle with the current device
  cublasHandle_t getBlasHandleCurrentDevice();

  /// Calls getDefaultStream with the current device
  cudaStream_t getDefaultStreamCurrentDevice();

  /// Synchronizes the CPU with respect to the default stream for the
  /// given device
  // equivalent to cudaDeviceSynchronize(getDefaultStream(device))
  void syncDefaultStream(int device);

  /// Calls syncDefaultStream for the current device
  void syncDefaultStreamCurrentDevice();

  /// Calls getAlternateStreams for the current device
  std::vector<cudaStream_t> getAlternateStreamsCurrentDevice();

  /// Calls getMemoryManager for the current device
  DeviceMemory& getMemoryManagerCurrentDevice();

  /// Calls getAsyncCopyStream for the current device
  cudaStream_t getAsyncCopyStreamCurrentDevice();
};

} } // namespace
