//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <vector>

#include "port/jemalloc_helper.h"
#include "port/port.h"
#include "rocksdb/memory_allocator.h"
#include "util/core_local.h"
#include "util/thread_local.h"

#if defined(ROCKSDB_JEMALLOC) && defined(ROCKSDB_PLATFORM_POSIX)

#include <sys/mman.h>

#if (JEMALLOC_VERSION_MAJOR >= 5) && defined(MADV_DONTDUMP)
#define ROCKSDB_JEMALLOC_NODUMP_ALLOCATOR

namespace rocksdb {

class JemallocNodumpAllocator : public MemoryAllocator {
 public:
  JemallocNodumpAllocator(JemallocAllocatorOptions& options,
                          std::unique_ptr<extent_hooks_t>&& arena_hooks,
                          unsigned arena_index);
  ~JemallocNodumpAllocator();

  const char* Name() const override { return "JemallocNodumpAllocator"; }
  void* Allocate(size_t size) override;
  void Deallocate(void* p) override;
  size_t UsableSize(void* p, size_t allocation_size) const override;

 private:
  friend Status NewJemallocNodumpAllocator(
      JemallocAllocatorOptions& options,
      std::shared_ptr<MemoryAllocator>* memory_allocator);

  // Custom alloc hook to replace jemalloc default alloc.
  static void* Alloc(extent_hooks_t* extent, void* new_addr, size_t size,
                     size_t alignment, bool* zero, bool* commit,
                     unsigned arena_ind);

  // Destroy arena on destruction of the allocator, or on failure.
  static Status DestroyArena(unsigned arena_index);

  // Destroy tcache on destruction of the allocator, or thread exit.
  static void DestroyThreadSpecificCache(void* ptr);

  // Get or create tcache. Return flag suitable to use with `mallocx`:
  // either MALLOCX_TCACHE_NONE or MALLOCX_TCACHE(tc).
  int GetThreadSpecificCache(size_t size);

  // A function pointer to jemalloc default alloc. Use atomic to make sure
  // NewJemallocNodumpAllocator is thread-safe.
  //
  // Hack: original_alloc_ needs to be static for Alloc() to access it.
  // alloc needs to be static to pass to jemalloc as function pointer.
  static std::atomic<extent_alloc_t*> original_alloc_;

  const JemallocAllocatorOptions options_;

  // Custom hooks has to outlive corresponding arena.
  const std::unique_ptr<extent_hooks_t> arena_hooks_;

  // Arena index.
  const unsigned arena_index_;

  // Hold thread-local tcache index.
  ThreadLocalPtr tcache_;
};

}  // namespace rocksdb
#endif  // (JEMALLOC_VERSION_MAJOR >= 5) && MADV_DONTDUMP
#endif  // ROCKSDB_JEMALLOC && ROCKSDB_PLATFORM_POSIX
