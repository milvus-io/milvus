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

// Always want minimum headers
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

// Assume that for everywhere
#undef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN true

#include <windows.h>
#include <string>
#include <string.h>
#include <mutex>
#include <limits>
#include <condition_variable>
#include <malloc.h>
#include <intrin.h>

#include <stdint.h>

#include "port/win/win_thread.h"

#include "rocksdb/options.h"

#undef min
#undef max
#undef DeleteFile
#undef GetCurrentTime


#ifndef strcasecmp
#define strcasecmp _stricmp
#endif

#undef GetCurrentTime
#undef DeleteFile

#ifndef _SSIZE_T_DEFINED
typedef SSIZE_T ssize_t;
#endif

// size_t printf formatting named in the manner of C99 standard formatting
// strings such as PRIu64
// in fact, we could use that one
#ifndef ROCKSDB_PRIszt
#define ROCKSDB_PRIszt "Iu"
#endif

#ifdef _MSC_VER
#define __attribute__(A)

// Thread local storage on Linux
// There is thread_local in C++11
#ifndef __thread
#define __thread __declspec(thread)
#endif

#endif

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

namespace rocksdb {

#define PREFETCH(addr, rw, locality)

namespace port {

// VS < 2015
#if defined(_MSC_VER) && (_MSC_VER < 1900)

// VS 15 has snprintf
#define snprintf _snprintf

#define ROCKSDB_NOEXCEPT
// std::numeric_limits<size_t>::max() is not constexpr just yet
// therefore, use the same limits

// For use at db/file_indexer.h kLevelMaxIndex
const uint32_t kMaxUint32 = UINT32_MAX;
const int kMaxInt32 = INT32_MAX;
const int64_t kMaxInt64 = INT64_MAX;
const uint64_t kMaxUint64 = UINT64_MAX;

#ifdef _WIN64
const size_t kMaxSizet = UINT64_MAX;
#else
const size_t kMaxSizet = UINT_MAX;
#endif

#else // VS >= 2015 or MinGW

#define ROCKSDB_NOEXCEPT noexcept

// For use at db/file_indexer.h kLevelMaxIndex
const uint32_t kMaxUint32 = std::numeric_limits<uint32_t>::max();
const int kMaxInt32 = std::numeric_limits<int>::max();
const uint64_t kMaxUint64 = std::numeric_limits<uint64_t>::max();
const int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();

const size_t kMaxSizet = std::numeric_limits<size_t>::max();

#endif //_MSC_VER

const bool kLittleEndian = true;

class CondVar;

class Mutex {
 public:

   /* implicit */ Mutex(bool adaptive = false)
#ifndef NDEBUG
     : locked_(false)
#endif
   { }

  ~Mutex();

  void Lock() {
    mutex_.lock();
#ifndef NDEBUG
    locked_ = true;
#endif
  }

  void Unlock() {
#ifndef NDEBUG
    locked_ = false;
#endif
    mutex_.unlock();
  }

  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld() {
#ifndef NDEBUG
    assert(locked_);
#endif
  }

  // Mutex is move only with lock ownership transfer
  Mutex(const Mutex&) = delete;
  void operator=(const Mutex&) = delete;

 private:

  friend class CondVar;

  std::mutex& getLock() {
    return mutex_;
  }

  std::mutex mutex_;
#ifndef NDEBUG
  bool locked_;
#endif
};

class RWMutex {
 public:
  RWMutex() { InitializeSRWLock(&srwLock_); }

  void ReadLock() { AcquireSRWLockShared(&srwLock_); }

  void WriteLock() { AcquireSRWLockExclusive(&srwLock_); }

  void ReadUnlock() { ReleaseSRWLockShared(&srwLock_); }

  void WriteUnlock() { ReleaseSRWLockExclusive(&srwLock_); }

  // Empty as in POSIX
  void AssertHeld() {}

 private:
  SRWLOCK srwLock_;
  // No copying allowed
  RWMutex(const RWMutex&);
  void operator=(const RWMutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu) : mu_(mu) {
  }

  ~CondVar();
  void Wait();
  bool TimedWait(uint64_t expiration_time);
  void Signal();
  void SignalAll();

  // Condition var is not copy/move constructible
  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  CondVar(CondVar&&) = delete;
  CondVar& operator=(CondVar&&) = delete;

 private:
  std::condition_variable cv_;
  Mutex* mu_;
};

// Wrapper around the platform efficient
// or otherwise preferrable implementation
using Thread = WindowsThread;

// OnceInit type helps emulate
// Posix semantics with initialization
// adopted in the project
struct OnceType {

    struct Init {};

    OnceType() {}
    OnceType(const Init&) {}
    OnceType(const OnceType&) = delete;
    OnceType& operator=(const OnceType&) = delete;

    std::once_flag flag_;
};

#define LEVELDB_ONCE_INIT port::OnceType::Init()
extern void InitOnce(OnceType* once, void (*initializer)());

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64U
#endif

#ifdef ROCKSDB_JEMALLOC
// Separate inlines so they can be replaced if needed
void* jemalloc_aligned_alloc(size_t size, size_t alignment) ROCKSDB_NOEXCEPT;
void jemalloc_aligned_free(void* p) ROCKSDB_NOEXCEPT;
#endif

inline void *cacheline_aligned_alloc(size_t size) {
#ifdef ROCKSDB_JEMALLOC
  return jemalloc_aligned_alloc(size, CACHE_LINE_SIZE);
#else
  return _aligned_malloc(size, CACHE_LINE_SIZE);
#endif
}

inline void cacheline_aligned_free(void *memblock) {
#ifdef ROCKSDB_JEMALLOC
  jemalloc_aligned_free(memblock);
#else
  _aligned_free(memblock);
#endif
}

// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=52991 for MINGW32
// could not be worked around with by -mno-ms-bitfields
#ifndef __MINGW32__
#define ALIGN_AS(n) __declspec(align(n))
#else
#define ALIGN_AS(n)
#endif

static inline void AsmVolatilePause() {
#if defined(_M_IX86) || defined(_M_X64)
  YieldProcessor();
#endif
  // it would be nice to get "wfe" on ARM here
}

extern int PhysicalCoreID();

// For Thread Local Storage abstraction
typedef DWORD pthread_key_t;

inline int pthread_key_create(pthread_key_t* key, void (*destructor)(void*)) {
  // Not used
  (void)destructor;

  pthread_key_t k = TlsAlloc();
  if (TLS_OUT_OF_INDEXES == k) {
    return ENOMEM;
  }

  *key = k;
  return 0;
}

inline int pthread_key_delete(pthread_key_t key) {
  if (!TlsFree(key)) {
    return EINVAL;
  }
  return 0;
}

inline int pthread_setspecific(pthread_key_t key, const void* value) {
  if (!TlsSetValue(key, const_cast<void*>(value))) {
    return ENOMEM;
  }
  return 0;
}

inline void* pthread_getspecific(pthread_key_t key) {
  void* result = TlsGetValue(key);
  if (!result) {
    if (GetLastError() != ERROR_SUCCESS) {
      errno = EINVAL;
    } else {
      errno = NOERROR;
    }
  }
  return result;
}

// UNIX equiv although errno numbers will be off
// using C-runtime to implement. Note, this does not
// feel space with zeros in case the file is extended.
int truncate(const char* path, int64_t length);
int Truncate(std::string path, int64_t length);
void Crash(const std::string& srcfile, int srcline);
extern int GetMaxOpenFiles();
std::string utf16_to_utf8(const std::wstring& utf16);
std::wstring utf8_to_utf16(const std::string& utf8);

}  // namespace port


#ifdef ROCKSDB_WINDOWS_UTF8_FILENAMES

#define RX_FILESTRING std::wstring
#define RX_FN(a) rocksdb::port::utf8_to_utf16(a)
#define FN_TO_RX(a) rocksdb::port::utf16_to_utf8(a)
#define RX_FNLEN(a) ::wcslen(a)

#define RX_DeleteFile DeleteFileW
#define RX_CreateFile CreateFileW
#define RX_CreateFileMapping CreateFileMappingW
#define RX_GetFileAttributesEx GetFileAttributesExW
#define RX_FindFirstFileEx FindFirstFileExW
#define RX_FindNextFile FindNextFileW
#define RX_WIN32_FIND_DATA WIN32_FIND_DATAW
#define RX_CreateDirectory CreateDirectoryW
#define RX_RemoveDirectory RemoveDirectoryW
#define RX_GetFileAttributesEx GetFileAttributesExW
#define RX_MoveFileEx MoveFileExW
#define RX_CreateHardLink CreateHardLinkW
#define RX_PathIsRelative PathIsRelativeW
#define RX_GetCurrentDirectory GetCurrentDirectoryW

#else

#define RX_FILESTRING std::string
#define RX_FN(a) a
#define FN_TO_RX(a) a
#define RX_FNLEN(a) strlen(a)

#define RX_DeleteFile DeleteFileA
#define RX_CreateFile CreateFileA
#define RX_CreateFileMapping CreateFileMappingA
#define RX_GetFileAttributesEx GetFileAttributesExA
#define RX_FindFirstFileEx FindFirstFileExA
#define RX_CreateDirectory CreateDirectoryA
#define RX_FindNextFile FindNextFileA
#define RX_WIN32_FIND_DATA WIN32_FIND_DATA
#define RX_CreateDirectory CreateDirectoryA
#define RX_RemoveDirectory RemoveDirectoryA
#define RX_GetFileAttributesEx GetFileAttributesExA
#define RX_MoveFileEx MoveFileExA
#define RX_CreateHardLink CreateHardLinkA
#define RX_PathIsRelative PathIsRelativeA
#define RX_GetCurrentDirectory GetCurrentDirectoryA

#endif

using port::pthread_key_t;
using port::pthread_key_create;
using port::pthread_key_delete;
using port::pthread_setspecific;
using port::pthread_getspecific;
using port::truncate;

}  // namespace rocksdb
