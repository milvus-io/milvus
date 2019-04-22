//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#pragma once

#include <thread>
// size_t printf formatting named in the manner of C99 standard formatting
// strings such as PRIu64
// in fact, we could use that one
#define ROCKSDB_PRIszt "zu"

#define __declspec(S)

#define ROCKSDB_NOEXCEPT noexcept

#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(OS_MACOSX)
  #include <machine/endian.h>
  #if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
    #define PLATFORM_IS_LITTLE_ENDIAN \
        (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
  #endif
#elif defined(OS_SOLARIS)
  #include <sys/isa_defs.h>
  #ifdef _LITTLE_ENDIAN
    #define PLATFORM_IS_LITTLE_ENDIAN true
  #else
    #define PLATFORM_IS_LITTLE_ENDIAN false
  #endif
  #include <alloca.h>
#elif defined(OS_AIX)
  #include <sys/types.h>
  #include <arpa/nameser_compat.h>
  #define PLATFORM_IS_LITTLE_ENDIAN (BYTE_ORDER == LITTLE_ENDIAN)
  #include <alloca.h>
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || \
    defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID)
  #include <sys/endian.h>
  #include <sys/types.h>
  #define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#else
  #include <endian.h>
#endif
#include <pthread.h>

#include <stdint.h>
#include <string.h>
#include <limits>
#include <string>

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#if defined(OS_MACOSX) || defined(OS_SOLARIS) || defined(OS_FREEBSD) ||\
    defined(OS_NETBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD) ||\
    defined(OS_ANDROID) || defined(CYGWIN) || defined(OS_AIX)
// Use fread/fwrite/fflush on platforms without _unlocked variants
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) ||\
    defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD)
// Use fsync() on platforms without fdatasync()
#define fdatasync fsync
#endif

#if defined(OS_ANDROID) && __ANDROID_API__ < 9
// fdatasync() was only introduced in API level 9 on Android. Use fsync()
// when targeting older platforms.
#define fdatasync fsync
#endif

namespace rocksdb {
namespace port {

// For use at db/file_indexer.h kLevelMaxIndex
const uint32_t kMaxUint32 = std::numeric_limits<uint32_t>::max();
const int kMaxInt32 = std::numeric_limits<int32_t>::max();
const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
const int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();
const size_t kMaxSizet = std::numeric_limits<size_t>::max();

static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

class CondVar;

class Mutex {
 public:
// We want to give users opportunity to default all the mutexes to adaptive if
// not specified otherwise. This enables a quick way to conduct various
// performance related experiements.
//
// NB! Support for adaptive mutexes is turned on by definining
// ROCKSDB_PTHREAD_ADAPTIVE_MUTEX during the compilation. If you use RocksDB
// build environment then this happens automatically; otherwise it's up to the
// consumer to define the identifier.
#ifdef ROCKSDB_DEFAULT_TO_ADAPTIVE_MUTEX
  explicit Mutex(bool adaptive = true);
#else
  explicit Mutex(bool adaptive = false);
#endif
  ~Mutex();

  void Lock();
  void Unlock();
  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld();

 private:
  friend class CondVar;
  pthread_mutex_t mu_;
#ifndef NDEBUG
  bool locked_;
#endif

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class RWMutex {
 public:
  RWMutex();
  ~RWMutex();

  void ReadLock();
  void WriteLock();
  void ReadUnlock();
  void WriteUnlock();
  void AssertHeld() { }

 private:
  pthread_rwlock_t mu_; // the underlying platform mutex

  // No copying allowed
  RWMutex(const RWMutex&);
  void operator=(const RWMutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  // Timed condition wait.  Returns true if timeout occurred.
  bool TimedWait(uint64_t abs_time_us);
  void Signal();
  void SignalAll();
 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};

using Thread = std::thread;

static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("wfe");
#elif defined(__powerpc64__)
  asm volatile("or 27,27,27");
#endif
  // it's okay for other platforms to be no-ops
}

// Returns -1 if not available on this platform
extern int PhysicalCoreID();

typedef pthread_once_t OnceType;
#define LEVELDB_ONCE_INIT PTHREAD_ONCE_INIT
extern void InitOnce(OnceType* once, void (*initializer)());

#ifndef CACHE_LINE_SIZE
  #if defined(__s390__)
    #define CACHE_LINE_SIZE 256U
  #elif defined(__powerpc__) || defined(__aarch64__)
    #define CACHE_LINE_SIZE 128U
  #else
    #define CACHE_LINE_SIZE 64U
  #endif
#endif


extern void *cacheline_aligned_alloc(size_t size);

extern void cacheline_aligned_free(void *memblock);

#define ALIGN_AS(n) alignas(n)

#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)

extern void Crash(const std::string& srcfile, int srcline);

extern int GetMaxOpenFiles();

} // namespace port
} // namespace rocksdb
