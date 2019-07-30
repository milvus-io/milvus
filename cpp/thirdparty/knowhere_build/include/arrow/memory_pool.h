// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef ARROW_MEMORY_POOL_H
#define ARROW_MEMORY_POOL_H

#include <atomic>
#include <cstdint>
#include <memory>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

namespace internal {

///////////////////////////////////////////////////////////////////////
// Helper tracking memory statistics

class MemoryPoolStats {
 public:
  MemoryPoolStats() : bytes_allocated_(0), max_memory_(0) {}

  int64_t max_memory() const { return max_memory_.load(); }

  int64_t bytes_allocated() const { return bytes_allocated_.load(); }

  inline void UpdateAllocatedBytes(int64_t diff) {
    auto allocated = bytes_allocated_.fetch_add(diff) + diff;
    // "maximum" allocated memory is ill-defined in multi-threaded code,
    // so don't try to be too rigorous here
    if (diff > 0 && allocated > max_memory_) {
      max_memory_ = allocated;
    }
  }

 protected:
  std::atomic<int64_t> bytes_allocated_;
  std::atomic<int64_t> max_memory_;
};

}  // namespace internal

/// Base class for memory allocation.
///
/// Besides tracking the number of allocated bytes, the allocator also should
/// take care of the required 64-byte alignment.
class ARROW_EXPORT MemoryPool {
 public:
  virtual ~MemoryPool();

  /// \brief EXPERIMENTAL. Create a new instance of the default MemoryPool
  static std::unique_ptr<MemoryPool> CreateDefault();

  /// Allocate a new memory region of at least size bytes.
  ///
  /// The allocated region shall be 64-byte aligned.
  virtual Status Allocate(int64_t size, uint8_t** out) = 0;

  /// Resize an already allocated memory section.
  ///
  /// As by default most default allocators on a platform don't support aligned
  /// reallocation, this function can involve a copy of the underlying data.
  virtual Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) = 0;

  /// Free an allocated region.
  ///
  /// @param buffer Pointer to the start of the allocated memory region
  /// @param size Allocated size located at buffer. An allocator implementation
  ///   may use this for tracking the amount of allocated bytes as well as for
  ///   faster deallocation if supported by its backend.
  virtual void Free(uint8_t* buffer, int64_t size) = 0;

  /// The number of bytes that were allocated and not yet free'd through
  /// this allocator.
  virtual int64_t bytes_allocated() const = 0;

  /// Return peak memory allocation in this memory pool
  ///
  /// \return Maximum bytes allocated. If not known (or not implemented),
  /// returns -1
  virtual int64_t max_memory() const;

 protected:
  MemoryPool();
};

class ARROW_EXPORT LoggingMemoryPool : public MemoryPool {
 public:
  explicit LoggingMemoryPool(MemoryPool* pool);
  ~LoggingMemoryPool() override = default;

  Status Allocate(int64_t size, uint8_t** out) override;
  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

 private:
  MemoryPool* pool_;
};

/// Derived class for memory allocation.
///
/// Tracks the number of bytes and maximum memory allocated through its direct
/// calls. Actual allocation is delegated to MemoryPool class.
class ARROW_EXPORT ProxyMemoryPool : public MemoryPool {
 public:
  explicit ProxyMemoryPool(MemoryPool* pool);
  ~ProxyMemoryPool() override;

  Status Allocate(int64_t size, uint8_t** out) override;
  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override;

  void Free(uint8_t* buffer, int64_t size) override;

  int64_t bytes_allocated() const override;

  int64_t max_memory() const override;

 private:
  class ProxyMemoryPoolImpl;
  std::unique_ptr<ProxyMemoryPoolImpl> impl_;
};

/// Return the process-wide default memory pool.
ARROW_EXPORT MemoryPool* default_memory_pool();

#ifdef ARROW_NO_DEFAULT_MEMORY_POOL
#define ARROW_MEMORY_POOL_DEFAULT
#else
#define ARROW_MEMORY_POOL_DEFAULT = default_memory_pool()
#endif

}  // namespace arrow

#endif  // ARROW_MEMORY_POOL_H
