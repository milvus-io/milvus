/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include "DeviceMemory.h"
#include <list>
#include <memory>
#include <unordered_map>

namespace faiss { namespace gpu {

/// Device memory manager that provides temporary memory allocations
/// out of a region of memory
class StackDeviceMemory : public DeviceMemory {
 public:
  /// Allocate a new region of memory that we manage
  explicit StackDeviceMemory(int device, size_t allocPerDevice);

  /// Manage a region of memory for a particular device, with or
  /// without ownership
  StackDeviceMemory(int device, void* p, size_t size, bool isOwner);

  ~StackDeviceMemory() override;

  /// Enable or disable the warning about not having enough temporary memory
  /// when cudaMalloc gets called
  void setCudaMallocWarning(bool b);

  int getDevice() const override;

  DeviceMemoryReservation getMemory(cudaStream_t stream,
                                    size_t size) override;

  size_t getSizeAvailable() const override;
  std::string toString() const override;
  size_t getHighWaterCudaMalloc() const override;

 protected:
  void returnAllocation(DeviceMemoryReservation& m) override;

 protected:
  /// Previous allocation ranges and the streams for which
  /// synchronization is required
  struct Range {
    inline Range(char* s, char* e, cudaStream_t str) :
        start_(s), end_(e), stream_(str) {
    }

    // References a memory range [start, end)
    char* start_;
    char* end_;
    cudaStream_t stream_;
  };

  struct Stack {
    /// Constructor that allocates memory via cudaMalloc
    Stack(int device, size_t size);

    /// Constructor that references a pre-allocated region of memory
    Stack(int device, void* p, size_t size, bool isOwner);
    ~Stack();

    /// Returns how much size is available for an allocation without
    /// calling cudaMalloc
    size_t getSizeAvailable() const;

    /// Obtains an allocation; all allocations are guaranteed to be 16
    /// byte aligned
    char* getAlloc(size_t size, cudaStream_t stream);

    /// Returns an allocation
    void returnAlloc(char* p, size_t size, cudaStream_t stream);

    /// Returns the stack state
    std::string toString() const;

    /// Returns the high-water mark of cudaMalloc activity
    size_t getHighWaterCudaMalloc() const;

    /// Device this allocation is on
    int device_;

    /// Do we own our region of memory?
    bool isOwner_;

    /// Where our allocation begins and ends
    /// [start_, end_) is valid
    char* start_;
    char* end_;

    /// Total size end_ - start_
    size_t size_;

    /// Stack head within [start, end)
    char* head_;

    /// List of previous last users of allocations on our stack, for
    /// possible synchronization purposes
    std::list<Range> lastUsers_;

    /// How much cudaMalloc memory is currently outstanding?
    size_t mallocCurrent_;

    /// What's the high water mark in terms of memory used from the
    /// temporary buffer?
    size_t highWaterMemoryUsed_;

    /// What's the high water mark in terms of memory allocated via
    /// cudaMalloc?
    size_t highWaterMalloc_;

    /// Whether or not a warning upon cudaMalloc is generated
    bool cudaMallocWarning_;
  };

  /// Our device
  int device_;

  /// Memory stack
  Stack stack_;
};

} } // namespace
