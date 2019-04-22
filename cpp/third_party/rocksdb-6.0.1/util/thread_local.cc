//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/thread_local.h"
#include "util/mutexlock.h"
#include "port/likely.h"
#include <stdlib.h>

namespace rocksdb {

struct Entry {
  Entry() : ptr(nullptr) {}
  Entry(const Entry& e) : ptr(e.ptr.load(std::memory_order_relaxed)) {}
  std::atomic<void*> ptr;
};

class StaticMeta;

// This is the structure that is declared as "thread_local" storage.
// The vector keep list of atomic pointer for all instances for "current"
// thread. The vector is indexed by an Id that is unique in process and
// associated with one ThreadLocalPtr instance. The Id is assigned by a
// global StaticMeta singleton. So if we instantiated 3 ThreadLocalPtr
// instances, each thread will have a ThreadData with a vector of size 3:
//     ---------------------------------------------------
//     |          | instance 1 | instance 2 | instnace 3 |
//     ---------------------------------------------------
//     | thread 1 |    void*   |    void*   |    void*   | <- ThreadData
//     ---------------------------------------------------
//     | thread 2 |    void*   |    void*   |    void*   | <- ThreadData
//     ---------------------------------------------------
//     | thread 3 |    void*   |    void*   |    void*   | <- ThreadData
//     ---------------------------------------------------
struct ThreadData {
  explicit ThreadData(ThreadLocalPtr::StaticMeta* _inst)
    : entries(),
      next(nullptr),
      prev(nullptr),
      inst(_inst) {}
  std::vector<Entry> entries;
  ThreadData* next;
  ThreadData* prev;
  ThreadLocalPtr::StaticMeta* inst;
};

class ThreadLocalPtr::StaticMeta {
public:
  StaticMeta();

  // Return the next available Id
  uint32_t GetId();
  // Return the next available Id without claiming it
  uint32_t PeekId() const;
  // Return the given Id back to the free pool. This also triggers
  // UnrefHandler for associated pointer value (if not NULL) for all threads.
  void ReclaimId(uint32_t id);

  // Return the pointer value for the given id for the current thread.
  void* Get(uint32_t id) const;
  // Reset the pointer value for the given id for the current thread.
  void Reset(uint32_t id, void* ptr);
  // Atomically swap the supplied ptr and return the previous value
  void* Swap(uint32_t id, void* ptr);
  // Atomically compare and swap the provided value only if it equals
  // to expected value.
  bool CompareAndSwap(uint32_t id, void* ptr, void*& expected);
  // Reset all thread local data to replacement, and return non-nullptr
  // data for all existing threads
  void Scrape(uint32_t id, autovector<void*>* ptrs, void* const replacement);
  // Update res by applying func on each thread-local value. Holds a lock that
  // prevents unref handler from running during this call, but clients must
  // still provide external synchronization since the owning thread can
  // access the values without internal locking, e.g., via Get() and Reset().
  void Fold(uint32_t id, FoldFunc func, void* res);

  // Register the UnrefHandler for id
  void SetHandler(uint32_t id, UnrefHandler handler);

  // protect inst, next_instance_id_, free_instance_ids_, head_,
  // ThreadData.entries
  //
  // Note that here we prefer function static variable instead of the usual
  // global static variable.  The reason is that c++ destruction order of
  // static variables in the reverse order of their construction order.
  // However, C++ does not guarantee any construction order when global
  // static variables are defined in different files, while the function
  // static variables are initialized when their function are first called.
  // As a result, the construction order of the function static variables
  // can be controlled by properly invoke their first function calls in
  // the right order.
  //
  // For instance, the following function contains a function static
  // variable.  We place a dummy function call of this inside
  // Env::Default() to ensure the construction order of the construction
  // order.
  static port::Mutex* Mutex();

  // Returns the member mutex of the current StaticMeta.  In general,
  // Mutex() should be used instead of this one.  However, in case where
  // the static variable inside Instance() goes out of scope, MemberMutex()
  // should be used.  One example is OnThreadExit() function.
  port::Mutex* MemberMutex() { return &mutex_; }

private:
  // Get UnrefHandler for id with acquiring mutex
  // REQUIRES: mutex locked
  UnrefHandler GetHandler(uint32_t id);

  // Triggered before a thread terminates
  static void OnThreadExit(void* ptr);

  // Add current thread's ThreadData to the global chain
  // REQUIRES: mutex locked
  void AddThreadData(ThreadData* d);

  // Remove current thread's ThreadData from the global chain
  // REQUIRES: mutex locked
  void RemoveThreadData(ThreadData* d);

  static ThreadData* GetThreadLocal();

  uint32_t next_instance_id_;
  // Used to recycle Ids in case ThreadLocalPtr is instantiated and destroyed
  // frequently. This also prevents it from blowing up the vector space.
  autovector<uint32_t> free_instance_ids_;
  // Chain all thread local structure together. This is necessary since
  // when one ThreadLocalPtr gets destroyed, we need to loop over each
  // thread's version of pointer corresponding to that instance and
  // call UnrefHandler for it.
  ThreadData head_;

  std::unordered_map<uint32_t, UnrefHandler> handler_map_;

  // The private mutex.  Developers should always use Mutex() instead of
  // using this variable directly.
  port::Mutex mutex_;
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
  // Thread local storage
  static __thread ThreadData* tls_;
#endif

  // Used to make thread exit trigger possible if !defined(OS_MACOSX).
  // Otherwise, used to retrieve thread data.
  pthread_key_t pthread_key_;
};


#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread ThreadData* ThreadLocalPtr::StaticMeta::tls_ = nullptr;
#endif

// Windows doesn't support a per-thread destructor with its
// TLS primitives.  So, we build it manually by inserting a
// function to be called on each thread's exit.
// See http://www.codeproject.com/Articles/8113/Thread-Local-Storage-The-C-Way
// and http://www.nynaeve.net/?p=183
//
// really we do this to have clear conscience since using TLS with thread-pools
// is iffy
// although OK within a request. But otherwise, threads have no identity in its
// modern use.

// This runs on windows only called from the System Loader
#ifdef OS_WIN

// Windows cleanup routine is invoked from a System Loader with a different
// signature so we can not directly hookup the original OnThreadExit which is
// private member
// so we make StaticMeta class share with the us the address of the function so
// we can invoke it.
namespace wintlscleanup {

// This is set to OnThreadExit in StaticMeta singleton constructor
UnrefHandler thread_local_inclass_routine = nullptr;
pthread_key_t thread_local_key = pthread_key_t (-1);

// Static callback function to call with each thread termination.
void NTAPI WinOnThreadExit(PVOID module, DWORD reason, PVOID reserved) {
  // We decided to punt on PROCESS_EXIT
  if (DLL_THREAD_DETACH == reason) {
    if (thread_local_key != pthread_key_t(-1) &&
        thread_local_inclass_routine != nullptr) {
      void* tls = TlsGetValue(thread_local_key);
      if (tls != nullptr) {
        thread_local_inclass_routine(tls);
      }
    }
  }
}

}  // wintlscleanup

// extern "C" suppresses C++ name mangling so we know the symbol name for the
// linker /INCLUDE:symbol pragma above.
extern "C" {

#ifdef _MSC_VER
// The linker must not discard thread_callback_on_exit.  (We force a reference
// to this variable with a linker /include:symbol pragma to ensure that.) If
// this variable is discarded, the OnThreadExit function will never be called.
#ifndef _X86_

// .CRT section is merged with .rdata on x64 so it must be constant data.
#pragma const_seg(".CRT$XLB")
// When defining a const variable, it must have external linkage to be sure the
// linker doesn't discard it.
extern const PIMAGE_TLS_CALLBACK p_thread_callback_on_exit;
const PIMAGE_TLS_CALLBACK p_thread_callback_on_exit =
    wintlscleanup::WinOnThreadExit;
// Reset the default section.
#pragma const_seg()

#pragma comment(linker, "/include:_tls_used")
#pragma comment(linker, "/include:p_thread_callback_on_exit")

#else  // _X86_

#pragma data_seg(".CRT$XLB")
PIMAGE_TLS_CALLBACK p_thread_callback_on_exit = wintlscleanup::WinOnThreadExit;
// Reset the default section.
#pragma data_seg()

#pragma comment(linker, "/INCLUDE:__tls_used")
#pragma comment(linker, "/INCLUDE:_p_thread_callback_on_exit")

#endif  // _X86_

#else
// https://github.com/couchbase/gperftools/blob/master/src/windows/port.cc
BOOL WINAPI DllMain(HINSTANCE h, DWORD dwReason, PVOID pv) {
  if (dwReason == DLL_THREAD_DETACH)
    wintlscleanup::WinOnThreadExit(h, dwReason, pv);
  return TRUE;
}
#endif
}  // extern "C"

#endif  // OS_WIN

void ThreadLocalPtr::InitSingletons() { ThreadLocalPtr::Instance(); }

ThreadLocalPtr::StaticMeta* ThreadLocalPtr::Instance() {
  // Here we prefer function static variable instead of global
  // static variable as function static variable is initialized
  // when the function is first call.  As a result, we can properly
  // control their construction order by properly preparing their
  // first function call.
  //
  // Note that here we decide to make "inst" a static pointer w/o deleting
  // it at the end instead of a static variable.  This is to avoid the following
  // destruction order disaster happens when a child thread using ThreadLocalPtr
  // dies AFTER the main thread dies:  When a child thread happens to use
  // ThreadLocalPtr, it will try to delete its thread-local data on its
  // OnThreadExit when the child thread dies.  However, OnThreadExit depends
  // on the following variable.  As a result, if the main thread dies before any
  // child thread happen to use ThreadLocalPtr dies, then the destruction of
  // the following variable will go first, then OnThreadExit, therefore causing
  // invalid access.
  //
  // The above problem can be solved by using thread_local to store tls_ instead
  // of using __thread.  The major difference between thread_local and __thread
  // is that thread_local supports dynamic construction and destruction of
  // non-primitive typed variables.  As a result, we can guarantee the
  // destruction order even when the main thread dies before any child threads.
  // However, thread_local is not supported in all compilers that accept -std=c++11
  // (e.g., eg Mac with XCode < 8. XCode 8+ supports thread_local).
  static ThreadLocalPtr::StaticMeta* inst = new ThreadLocalPtr::StaticMeta();
  return inst;
}

port::Mutex* ThreadLocalPtr::StaticMeta::Mutex() { return &Instance()->mutex_; }

void ThreadLocalPtr::StaticMeta::OnThreadExit(void* ptr) {
  auto* tls = static_cast<ThreadData*>(ptr);
  assert(tls != nullptr);

  // Use the cached StaticMeta::Instance() instead of directly calling
  // the variable inside StaticMeta::Instance() might already go out of
  // scope here in case this OnThreadExit is called after the main thread
  // dies.
  auto* inst = tls->inst;
  pthread_setspecific(inst->pthread_key_, nullptr);

  MutexLock l(inst->MemberMutex());
  inst->RemoveThreadData(tls);
  // Unref stored pointers of current thread from all instances
  uint32_t id = 0;
  for (auto& e : tls->entries) {
    void* raw = e.ptr.load();
    if (raw != nullptr) {
      auto unref = inst->GetHandler(id);
      if (unref != nullptr) {
        unref(raw);
      }
    }
    ++id;
  }
  // Delete thread local structure no matter if it is Mac platform
  delete tls;
}

ThreadLocalPtr::StaticMeta::StaticMeta()
  : next_instance_id_(0),
    head_(this),
    pthread_key_(0) {
  if (pthread_key_create(&pthread_key_, &OnThreadExit) != 0) {
    abort();
  }

  // OnThreadExit is not getting called on the main thread.
  // Call through the static destructor mechanism to avoid memory leak.
  //
  // Caveats: ~A() will be invoked _after_ ~StaticMeta for the global
  // singleton (destructors are invoked in reverse order of constructor
  // _completion_); the latter must not mutate internal members. This
  // cleanup mechanism inherently relies on use-after-release of the
  // StaticMeta, and is brittle with respect to compiler-specific handling
  // of memory backing destructed statically-scoped objects. Perhaps
  // registering with atexit(3) would be more robust.
  //
// This is not required on Windows.
#if !defined(OS_WIN)
  static struct A {
    ~A() {
#ifndef ROCKSDB_SUPPORT_THREAD_LOCAL
      ThreadData* tls_ =
        static_cast<ThreadData*>(pthread_getspecific(Instance()->pthread_key_));
#endif
      if (tls_) {
        OnThreadExit(tls_);
      }
    }
  } a;
#endif  // !defined(OS_WIN)

  head_.next = &head_;
  head_.prev = &head_;

#ifdef OS_WIN
  // Share with Windows its cleanup routine and the key
  wintlscleanup::thread_local_inclass_routine = OnThreadExit;
  wintlscleanup::thread_local_key = pthread_key_;
#endif
}

void ThreadLocalPtr::StaticMeta::AddThreadData(ThreadData* d) {
  Mutex()->AssertHeld();
  d->next = &head_;
  d->prev = head_.prev;
  head_.prev->next = d;
  head_.prev = d;
}

void ThreadLocalPtr::StaticMeta::RemoveThreadData(
    ThreadData* d) {
  Mutex()->AssertHeld();
  d->next->prev = d->prev;
  d->prev->next = d->next;
  d->next = d->prev = d;
}

ThreadData* ThreadLocalPtr::StaticMeta::GetThreadLocal() {
#ifndef ROCKSDB_SUPPORT_THREAD_LOCAL
  // Make this local variable name look like a member variable so that we
  // can share all the code below
  ThreadData* tls_ =
      static_cast<ThreadData*>(pthread_getspecific(Instance()->pthread_key_));
#endif

  if (UNLIKELY(tls_ == nullptr)) {
    auto* inst = Instance();
    tls_ = new ThreadData(inst);
    {
      // Register it in the global chain, needs to be done before thread exit
      // handler registration
      MutexLock l(Mutex());
      inst->AddThreadData(tls_);
    }
    // Even it is not OS_MACOSX, need to register value for pthread_key_ so that
    // its exit handler will be triggered.
    if (pthread_setspecific(inst->pthread_key_, tls_) != 0) {
      {
        MutexLock l(Mutex());
        inst->RemoveThreadData(tls_);
      }
      delete tls_;
      abort();
    }
  }
  return tls_;
}

void* ThreadLocalPtr::StaticMeta::Get(uint32_t id) const {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    return nullptr;
  }
  return tls->entries[id].ptr.load(std::memory_order_acquire);
}

void ThreadLocalPtr::StaticMeta::Reset(uint32_t id, void* ptr) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    MutexLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  tls->entries[id].ptr.store(ptr, std::memory_order_release);
}

void* ThreadLocalPtr::StaticMeta::Swap(uint32_t id, void* ptr) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    MutexLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  return tls->entries[id].ptr.exchange(ptr, std::memory_order_acquire);
}

bool ThreadLocalPtr::StaticMeta::CompareAndSwap(uint32_t id, void* ptr,
    void*& expected) {
  auto* tls = GetThreadLocal();
  if (UNLIKELY(id >= tls->entries.size())) {
    // Need mutex to protect entries access within ReclaimId
    MutexLock l(Mutex());
    tls->entries.resize(id + 1);
  }
  return tls->entries[id].ptr.compare_exchange_strong(
      expected, ptr, std::memory_order_release, std::memory_order_relaxed);
}

void ThreadLocalPtr::StaticMeta::Scrape(uint32_t id, autovector<void*>* ptrs,
    void* const replacement) {
  MutexLock l(Mutex());
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr =
          t->entries[id].ptr.exchange(replacement, std::memory_order_acquire);
      if (ptr != nullptr) {
        ptrs->push_back(ptr);
      }
    }
  }
}

void ThreadLocalPtr::StaticMeta::Fold(uint32_t id, FoldFunc func, void* res) {
  MutexLock l(Mutex());
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr = t->entries[id].ptr.load();
      if (ptr != nullptr) {
        func(ptr, res);
      }
    }
  }
}

uint32_t ThreadLocalPtr::TEST_PeekId() {
  return Instance()->PeekId();
}

void ThreadLocalPtr::StaticMeta::SetHandler(uint32_t id, UnrefHandler handler) {
  MutexLock l(Mutex());
  handler_map_[id] = handler;
}

UnrefHandler ThreadLocalPtr::StaticMeta::GetHandler(uint32_t id) {
  Mutex()->AssertHeld();
  auto iter = handler_map_.find(id);
  if (iter == handler_map_.end()) {
    return nullptr;
  }
  return iter->second;
}

uint32_t ThreadLocalPtr::StaticMeta::GetId() {
  MutexLock l(Mutex());
  if (free_instance_ids_.empty()) {
    return next_instance_id_++;
  }

  uint32_t id = free_instance_ids_.back();
  free_instance_ids_.pop_back();
  return id;
}

uint32_t ThreadLocalPtr::StaticMeta::PeekId() const {
  MutexLock l(Mutex());
  if (!free_instance_ids_.empty()) {
    return free_instance_ids_.back();
  }
  return next_instance_id_;
}

void ThreadLocalPtr::StaticMeta::ReclaimId(uint32_t id) {
  // This id is not used, go through all thread local data and release
  // corresponding value
  MutexLock l(Mutex());
  auto unref = GetHandler(id);
  for (ThreadData* t = head_.next; t != &head_; t = t->next) {
    if (id < t->entries.size()) {
      void* ptr = t->entries[id].ptr.exchange(nullptr);
      if (ptr != nullptr && unref != nullptr) {
        unref(ptr);
      }
    }
  }
  handler_map_[id] = nullptr;
  free_instance_ids_.push_back(id);
}

ThreadLocalPtr::ThreadLocalPtr(UnrefHandler handler)
    : id_(Instance()->GetId()) {
  if (handler != nullptr) {
    Instance()->SetHandler(id_, handler);
  }
}

ThreadLocalPtr::~ThreadLocalPtr() {
  Instance()->ReclaimId(id_);
}

void* ThreadLocalPtr::Get() const {
  return Instance()->Get(id_);
}

void ThreadLocalPtr::Reset(void* ptr) {
  Instance()->Reset(id_, ptr);
}

void* ThreadLocalPtr::Swap(void* ptr) {
  return Instance()->Swap(id_, ptr);
}

bool ThreadLocalPtr::CompareAndSwap(void* ptr, void*& expected) {
  return Instance()->CompareAndSwap(id_, ptr, expected);
}

void ThreadLocalPtr::Scrape(autovector<void*>* ptrs, void* const replacement) {
  Instance()->Scrape(id_, ptrs, replacement);
}

void ThreadLocalPtr::Fold(FoldFunc func, void* res) {
  Instance()->Fold(id_, func, res);
}

}  // namespace rocksdb
