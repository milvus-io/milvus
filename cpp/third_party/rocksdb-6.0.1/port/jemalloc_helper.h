//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef ROCKSDB_JEMALLOC
#ifdef __FreeBSD__
#include <malloc_np.h>
#else
#include <jemalloc/jemalloc.h>
#endif

// Declare non-standard jemalloc APIs as weak symbols. We can null-check these
// symbols to detect whether jemalloc is linked with the binary.
extern "C" void* mallocx(size_t, int) __attribute__((__weak__));
extern "C" void* rallocx(void*, size_t, int) __attribute__((__weak__));
extern "C" size_t xallocx(void*, size_t, size_t, int) __attribute__((__weak__));
extern "C" size_t sallocx(const void*, int) __attribute__((__weak__));
extern "C" void dallocx(void*, int) __attribute__((__weak__));
extern "C" void sdallocx(void*, size_t, int) __attribute__((__weak__));
extern "C" size_t nallocx(size_t, int) __attribute__((__weak__));
extern "C" int mallctl(const char*, void*, size_t*, void*, size_t)
    __attribute__((__weak__));
extern "C" int mallctlnametomib(const char*, size_t*, size_t*)
    __attribute__((__weak__));
extern "C" int mallctlbymib(const size_t*, size_t, void*, size_t*, void*,
                            size_t) __attribute__((__weak__));
extern "C" void malloc_stats_print(void (*)(void*, const char*), void*,
                                   const char*) __attribute__((__weak__));
extern "C" size_t malloc_usable_size(JEMALLOC_USABLE_SIZE_CONST void*)
    JEMALLOC_CXX_THROW __attribute__((__weak__));

// Check if Jemalloc is linked with the binary. Note the main program might be
// using a different memory allocator even this method return true.
// It is loosely based on folly::usingJEMalloc(), minus the check that actually
// allocate memory and see if it is through jemalloc, to handle the dlopen()
// case:
// https://github.com/facebook/folly/blob/76cf8b5841fb33137cfbf8b224f0226437c855bc/folly/memory/Malloc.h#L147
static inline bool HasJemalloc() {
  return mallocx != nullptr && rallocx != nullptr && xallocx != nullptr &&
         sallocx != nullptr && dallocx != nullptr && sdallocx != nullptr &&
         nallocx != nullptr && mallctl != nullptr &&
         mallctlnametomib != nullptr && mallctlbymib != nullptr &&
         malloc_stats_print != nullptr && malloc_usable_size != nullptr;
}

#endif  // ROCKSDB_JEMALLOC
