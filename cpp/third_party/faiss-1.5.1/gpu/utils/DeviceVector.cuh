/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "../../FaissAssert.h"
#include "DeviceUtils.h"
#include "MemorySpace.h"
#include "StaticUtils.h"
#include <algorithm>
#include <cuda.h>
#include <vector>

namespace faiss { namespace gpu {

/// A simple version of thrust::device_vector<T>, but has more control
/// over whether resize() initializes new space with T() (which we
/// don't want), and control on how much the reserved space grows by
/// upon resize/reserve. It is also meant for POD types only.
template <typename T>
class DeviceVector {
 public:
  DeviceVector(MemorySpace space = MemorySpace::Device)
      : data_(nullptr),
        num_(0),
        capacity_(0),
        space_(space) {
  }

  ~DeviceVector() {
    clear();
  }

  // Clear all allocated memory; reset to zero size
  void clear() {
    freeMemorySpace(space_, data_);
    data_ = nullptr;
    num_ = 0;
    capacity_ = 0;
  }

  size_t size() const { return num_; }
  size_t capacity() const { return capacity_; }
  T* data() { return data_; }
  const T* data() const { return data_; }

  template <typename OutT>
  std::vector<OutT> copyToHost(cudaStream_t stream) const {
    FAISS_ASSERT(num_ * sizeof(T) % sizeof(OutT) == 0);

    std::vector<OutT> out((num_ * sizeof(T)) / sizeof(OutT));
    CUDA_VERIFY(cudaMemcpyAsync(out.data(), data_, num_ * sizeof(T),
                                cudaMemcpyDeviceToHost, stream));

    return out;
  }

  // Returns true if we actually reallocated memory
  // If `reserveExact` is true, then we reserve only the memory that
  // we need for what we're appending
  bool append(const T* d,
              size_t n,
              cudaStream_t stream,
              bool reserveExact = false) {
    bool mem = false;

    if (n > 0) {
      size_t reserveSize = num_ + n;
      if (!reserveExact) {
        reserveSize = getNewCapacity_(reserveSize);
      }

      mem = reserve(reserveSize, stream);

      int dev = getDeviceForAddress(d);
      if (dev == -1) {
        CUDA_VERIFY(cudaMemcpyAsync(data_ + num_, d, n * sizeof(T),
                                    cudaMemcpyHostToDevice, stream));
      } else {
        CUDA_VERIFY(cudaMemcpyAsync(data_ + num_, d, n * sizeof(T),
                                    cudaMemcpyDeviceToDevice, stream));
      }
      num_ += n;
    }

    return mem;
  }

  // Returns true if we actually reallocated memory
  bool resize(size_t newSize, cudaStream_t stream) {
    bool mem = false;

    if (num_ < newSize) {
      mem = reserve(getNewCapacity_(newSize), stream);
    }

    // Don't bother zero initializing the newly accessible memory
    // (unlike thrust::device_vector)
    num_ = newSize;

    return mem;
  }

  // Clean up after oversized allocations, while leaving some space to
  // remain for subsequent allocations (if `exact` false) or to
  // exactly the space we need (if `exact` true); returns space
  // reclaimed in bytes
  size_t reclaim(bool exact, cudaStream_t stream) {
    size_t free = capacity_ - num_;

    if (exact) {
      realloc_(num_, stream);
      return free * sizeof(T);
    }

    // If more than 1/4th of the space is free, then we want to
    // truncate to only having 1/8th of the space free; this still
    // preserves some space for new elements, but won't force us to
    // double our size right away
    if (free > (capacity_ / 4)) {
      size_t newFree = capacity_ / 8;
      size_t newCapacity = num_ + newFree;

      size_t oldCapacity = capacity_;
      FAISS_ASSERT(newCapacity < oldCapacity);

      realloc_(newCapacity, stream);

      return (oldCapacity - newCapacity) * sizeof(T);
    }

    return 0;
  }

  // Returns true if we actually reallocated memory
  bool reserve(size_t newCapacity, cudaStream_t stream) {
    if (newCapacity <= capacity_) {
      return false;
    }

    // Otherwise, we need new space.
    realloc_(newCapacity, stream);
    return true;
  }

 private:
  void realloc_(size_t newCapacity, cudaStream_t stream) {
    FAISS_ASSERT(num_ <= newCapacity);

    T* newData = nullptr;
    allocMemorySpace(space_, &newData, newCapacity * sizeof(T));
    CUDA_VERIFY(cudaMemcpyAsync(newData, data_, num_ * sizeof(T),
                                cudaMemcpyDeviceToDevice, stream));
    freeMemorySpace(space_, data_);

    data_ = newData;
    capacity_ = newCapacity;
  }

  size_t getNewCapacity_(size_t preferredSize) {
    return utils::nextHighestPowerOf2(preferredSize);
  }

  T* data_;
  size_t num_;
  size_t capacity_;
  MemorySpace space_;
};

} } // namespace
