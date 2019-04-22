//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "util/autovector.h"
#include "port/port.h"

namespace rocksdb {

// Cleanup function that will be called for a stored thread local
// pointer (if not NULL) when one of the following happens:
// (1) a thread terminates
// (2) a ThreadLocalPtr is destroyed
//
// Warning: this function is called while holding a global mutex. The same mutex
// is used (at least in some cases) by most methods of ThreadLocalPtr, and it's
// shared across all instances of ThreadLocalPtr. Thereforere extra care
// is needed to avoid deadlocks. In particular, the handler shouldn't lock any
// mutexes and shouldn't call any methods of any ThreadLocalPtr instances,
// unless you know what you're doing.
typedef void (*UnrefHandler)(void* ptr);

// ThreadLocalPtr stores only values of pointer type.  Different from
// the usual thread-local-storage, ThreadLocalPtr has the ability to
// distinguish data coming from different threads and different
// ThreadLocalPtr instances.  For example, if a regular thread_local
// variable A is declared in DBImpl, two DBImpl objects would share
// the same A.  However, a ThreadLocalPtr that is defined under the
// scope of DBImpl can avoid such confliction.  As a result, its memory
// usage would be O(# of threads * # of ThreadLocalPtr instances).
class ThreadLocalPtr {
 public:
  explicit ThreadLocalPtr(UnrefHandler handler = nullptr);

  ThreadLocalPtr(const ThreadLocalPtr&) = delete;
  ThreadLocalPtr& operator=(const ThreadLocalPtr&) = delete;

  ~ThreadLocalPtr();

  // Return the current pointer stored in thread local
  void* Get() const;

  // Set a new pointer value to the thread local storage.
  void Reset(void* ptr);

  // Atomically swap the supplied ptr and return the previous value
  void* Swap(void* ptr);

  // Atomically compare the stored value with expected. Set the new
  // pointer value to thread local only if the comparison is true.
  // Otherwise, expected returns the stored value.
  // Return true on success, false on failure
  bool CompareAndSwap(void* ptr, void*& expected);

  // Reset all thread local data to replacement, and return non-nullptr
  // data for all existing threads
  void Scrape(autovector<void*>* ptrs, void* const replacement);

  typedef std::function<void(void*, void*)> FoldFunc;
  // Update res by applying func on each thread-local value. Holds a lock that
  // prevents unref handler from running during this call, but clients must
  // still provide external synchronization since the owning thread can
  // access the values without internal locking, e.g., via Get() and Reset().
  void Fold(FoldFunc func, void* res);

  // Add here for testing
  // Return the next available Id without claiming it
  static uint32_t TEST_PeekId();

  // Initialize the static singletons of the ThreadLocalPtr.
  //
  // If this function is not called, then the singletons will be
  // automatically initialized when they are used.
  //
  // Calling this function twice or after the singletons have been
  // initialized will be no-op.
  static void InitSingletons();

  class StaticMeta;

private:

  static StaticMeta* Instance();

  const uint32_t id_;
};

}  // namespace rocksdb
