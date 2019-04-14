/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <cuda_runtime.h>
#include <string>

namespace faiss { namespace gpu {

class DeviceMemory;

class DeviceMemoryReservation {
 public:
  DeviceMemoryReservation();
  DeviceMemoryReservation(DeviceMemory* state,
                          int device, void* p, size_t size,
                          cudaStream_t stream);
  DeviceMemoryReservation(DeviceMemoryReservation&& m) noexcept;
  ~DeviceMemoryReservation();

  DeviceMemoryReservation& operator=(DeviceMemoryReservation&& m);

  int device() { return device_; }
  void* get() { return data_; }
  size_t size() { return size_; }
  cudaStream_t stream() { return stream_; }

 private:
  DeviceMemory* state_;

  int device_;
  void* data_;
  size_t size_;
  cudaStream_t stream_;
};

/// Manages temporary memory allocations on a GPU device
class DeviceMemory {
 public:
  virtual ~DeviceMemory();

  /// Returns the device we are managing memory for
  virtual int getDevice() const = 0;

  /// Obtains a temporary memory allocation for our device,
  /// whose usage is ordered with respect to the given stream.
  virtual DeviceMemoryReservation getMemory(cudaStream_t stream,
                                            size_t size) = 0;

  /// Returns the current size available without calling cudaMalloc
  virtual size_t getSizeAvailable() const = 0;

  /// Returns a string containing our current memory manager state
  virtual std::string toString() const = 0;

  /// Returns the high-water mark of cudaMalloc allocations for our
  /// device
  virtual size_t getHighWaterCudaMalloc() const = 0;

 protected:
  friend class DeviceMemoryReservation;
  virtual void returnAllocation(DeviceMemoryReservation& m) = 0;
};

} } // namespace
