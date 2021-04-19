/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/GpuResources.h>
#include <faiss/gpu/utils/StackDeviceMemory.h>
#include <faiss/gpu/utils/DeviceUtils.h>
#include <unordered_map>
#include <vector>

namespace faiss { namespace gpu {

/// Default implementation of GpuResources that allocates a cuBLAS
/// stream and 2 streams for use, as well as temporary memory
class StandardGpuResources : public GpuResources {
 public:
  StandardGpuResources();

  ~StandardGpuResources() override;

  /// Disable allocation of temporary memory; all temporary memory
  /// requests will call cudaMalloc / cudaFree at the point of use
  void noTempMemory();

  /// Specify that we wish to use a certain fixed size of memory on
  /// all devices as temporary memory. This is the upper bound for the GPU
  /// memory that we will reserve. We will never go above 1.5 GiB on any GPU;
  /// smaller GPUs (with <= 4 GiB or <= 8 GiB) will use less memory than that.
  /// To avoid any temporary memory allocation, pass 0.
  void setTempMemory(size_t size);

  /// Set amount of pinned memory to allocate, for async GPU <-> CPU
  /// transfers
  void setPinnedMemory(size_t size);

  /// Called to change the stream for work ordering
  void setDefaultStream(int device, cudaStream_t stream);

  /// Called to change the work ordering streams to the null stream
  /// for all devices
  void setDefaultNullStreamAllDevices();

  /// Enable or disable the warning about not having enough temporary memory
  /// when cudaMalloc gets called
  void setCudaMallocWarning(bool b);

 public:
  /// Internal system calls

  /// Initialize resources for this device
  void initializeForDevice(int device) override;

  cublasHandle_t getBlasHandle(int device) override;

  cudaStream_t getDefaultStream(int device) override;

  std::vector<cudaStream_t> getAlternateStreams(int device) override;

  DeviceMemory& getMemoryManager(int device) override;

  std::pair<void*, size_t> getPinnedMemory() override;

  cudaStream_t getAsyncCopyStream(int device) override;

 private:
  /// Have GPU resources been initialized for this device yet?
  bool isInitialized(int device) const;

  /// Adjust the default temporary memory allocation based on the total GPU
  /// memory size
  static size_t getDefaultTempMemForGPU(int device, size_t requested);

 private:
  /// Our default stream that work is ordered on, one per each device
  std::unordered_map<int, cudaStream_t> defaultStreams_;

  /// This contains particular streams as set by the user for
  /// ordering, if any
  std::unordered_map<int, cudaStream_t> userDefaultStreams_;

  /// Other streams we can use, per each device
  std::unordered_map<int, std::vector<cudaStream_t> > alternateStreams_;

  /// Async copy stream to use for GPU <-> CPU pinned memory copies
  std::unordered_map<int, cudaStream_t> asyncCopyStreams_;

  /// cuBLAS handle for each device
  std::unordered_map<int, cublasHandle_t> blasHandles_;

  /// Temporary memory provider, per each device
  std::unordered_map<int, std::unique_ptr<StackDeviceMemory> > memory_;

  /// Pinned memory allocation for use with this GPU
  void* pinnedMemAlloc_;
  size_t pinnedMemAllocSize_;

  /// Another option is to use a specified amount of memory on all
  /// devices
  size_t tempMemSize_;

  /// Amount of pinned memory we should allocate
  size_t pinnedMemSize_;

  /// Whether or not a warning upon cudaMalloc is generated
  bool cudaMallocWarning_;
};

} } // namespace
