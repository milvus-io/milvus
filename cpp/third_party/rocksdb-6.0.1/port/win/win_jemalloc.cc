//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_JEMALLOC
# error This file can only be part of jemalloc aware build
#endif

#include <stdexcept>
#include "jemalloc/jemalloc.h"
#include "port/win/port_win.h"

#if defined(ZSTD) && defined(ZSTD_STATIC_LINKING_ONLY)
#include <zstd.h>
#if (ZSTD_VERSION_NUMBER >= 500)
namespace rocksdb {
namespace port {
void* JemallocAllocateForZSTD(void* /* opaque */, size_t size) {
  return je_malloc(size);
}
void JemallocDeallocateForZSTD(void* /* opaque */, void* address) {
  je_free(address);
}
ZSTD_customMem GetJeZstdAllocationOverrides() {
  return {JemallocAllocateForZSTD, JemallocDeallocateForZSTD, nullptr};
}
} // namespace port
} // namespace rocksdb
#endif // (ZSTD_VERSION_NUMBER >= 500)
#endif // defined(ZSTD) defined(ZSTD_STATIC_LINKING_ONLY)

// Global operators to be replaced by a linker when this file is
// a part of the build

namespace rocksdb {
namespace port {
void* jemalloc_aligned_alloc(size_t size, size_t alignment) ROCKSDB_NOEXCEPT {
  return je_aligned_alloc(alignment, size);
}
void jemalloc_aligned_free(void* p) ROCKSDB_NOEXCEPT { je_free(p); }
}  // namespace port
}  // namespace rocksdb

void* operator new(size_t size) {
  void* p = je_malloc(size);
  if (!p) {
    throw std::bad_alloc();
  }
  return p;
}

void* operator new[](size_t size) {
  void* p = je_malloc(size);
  if (!p) {
    throw std::bad_alloc();
  }
  return p;
}

void operator delete(void* p) {
  if (p) {
    je_free(p);
  }
}

void operator delete[](void* p) {
  if (p) {
    je_free(p);
  }
}
