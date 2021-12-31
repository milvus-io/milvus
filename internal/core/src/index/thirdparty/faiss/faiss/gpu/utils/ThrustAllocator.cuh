/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */


#pragma once

#include <faiss/gpu/utils/MemorySpace.h>
#include <cuda.h>
#include <unordered_set>

namespace faiss { namespace gpu {

/// Allocator for Thrust that comes out of a specified memory space
class GpuResourcesThrustAllocator {
 public:
  typedef char value_type;

  GpuResourcesThrustAllocator(void* mem, size_t size)
      : start_((char*) mem),
        cur_((char*) mem),
        end_((char*) mem + size) {
  }

  ~GpuResourcesThrustAllocator() {
    // In the case of an exception being thrown, we may not have called
    // deallocate on all of our sub-allocations. Free them here
    for (auto p : mallocAllocs_) {
      freeMemorySpace(MemorySpace::Device, p);
    }
  }

  char* allocate(std::ptrdiff_t size) {
    if (size <= (end_ - cur_)) {
      char* p = cur_;
      cur_ += size;
      FAISS_ASSERT(cur_ <= end_);

      return p;
    } else {
      char* p = nullptr;
      allocMemorySpace(MemorySpace::Device, &p, size);
      mallocAllocs_.insert(p);
      return p;
    }
  }

  void deallocate(char* p, size_t size) {
    // Allocations could be returned out-of-order; ignore those we
    // didn't cudaMalloc
    auto it = mallocAllocs_.find(p);
    if (it != mallocAllocs_.end()) {
      freeMemorySpace(MemorySpace::Device, p);
      mallocAllocs_.erase(it);
    }
  }

 private:
  char* start_;
  char* cur_;
  char* end_;
  std::unordered_set<char*> mallocAllocs_;
};


} } // namespace
