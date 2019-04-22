//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/threadpool_imp.h"

#include "monitoring/thread_status_util.h"
#include "port/port.h"

#ifndef OS_WIN
#  include <unistd.h>
#endif

#ifdef OS_LINUX
#  include <sys/syscall.h>
#  include <sys/resource.h>
#endif

#include <stdlib.h>
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

namespace rocksdb {

void ThreadPoolImpl::PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
    abort();
  }
}

struct ThreadPoolImpl::Impl {

  Impl();
  ~Impl();

  void JoinThreads(bool wait_for_jobs_to_complete);

  void SetBackgroundThreadsInternal(int num, bool allow_reduce);
  int GetBackgroundThreads();

  unsigned int GetQueueLen() const {
    return queue_len_.load(std::memory_order_relaxed);
  }

  void LowerIOPriority();

  void LowerCPUPriority();

  void WakeUpAllThreads() {
    bgsignal_.notify_all();
  }

  void BGThread(size_t thread_id);

  void StartBGThreads();

  void Submit(std::function<void()>&& schedule,
    std::function<void()>&& unschedule, void* tag);

  int UnSchedule(void* arg);

  void SetHostEnv(Env* env) { env_ = env; }

  Env* GetHostEnv() const { return env_; }

  bool HasExcessiveThread() const {
    return static_cast<int>(bgthreads_.size()) > total_threads_limit_;
  }

  // Return true iff the current thread is the excessive thread to terminate.
  // Always terminate the running thread that is added last, even if there are
  // more than one thread to terminate.
  bool IsLastExcessiveThread(size_t thread_id) const {
    return HasExcessiveThread() && thread_id == bgthreads_.size() - 1;
  }

  bool IsExcessiveThread(size_t thread_id) const {
    return static_cast<int>(thread_id) >= total_threads_limit_;
  }

  // Return the thread priority.
  // This would allow its member-thread to know its priority.
  Env::Priority GetThreadPriority() const { return priority_; }

  // Set the thread priority.
  void SetThreadPriority(Env::Priority priority) { priority_ = priority; }

private:

  static void* BGThreadWrapper(void* arg);

  bool low_io_priority_;
  bool low_cpu_priority_;
  Env::Priority priority_;
  Env*         env_;

  int total_threads_limit_;
  std::atomic_uint queue_len_;  // Queue length. Used for stats reporting
  bool exit_all_threads_;
  bool wait_for_jobs_to_complete_;

  // Entry per Schedule()/Submit() call
  struct BGItem {
    void* tag = nullptr;
    std::function<void()> function;
    std::function<void()> unschedFunction;
  };

  using BGQueue = std::deque<BGItem>;
  BGQueue       queue_;

  std::mutex               mu_;
  std::condition_variable  bgsignal_;
  std::vector<port::Thread> bgthreads_;
};


inline
ThreadPoolImpl::Impl::Impl()
    :
      low_io_priority_(false),
      low_cpu_priority_(false),
      priority_(Env::LOW),
      env_(nullptr),
      total_threads_limit_(0),
      queue_len_(),
      exit_all_threads_(false),
      wait_for_jobs_to_complete_(false),
      queue_(),
      mu_(),
      bgsignal_(),
      bgthreads_() {
}

inline
ThreadPoolImpl::Impl::~Impl() { assert(bgthreads_.size() == 0U); }

void ThreadPoolImpl::Impl::JoinThreads(bool wait_for_jobs_to_complete) {

  std::unique_lock<std::mutex> lock(mu_);
  assert(!exit_all_threads_);

  wait_for_jobs_to_complete_ = wait_for_jobs_to_complete;
  exit_all_threads_ = true;
  // prevent threads from being recreated right after they're joined, in case
  // the user is concurrently submitting jobs.
  total_threads_limit_ = 0;

  lock.unlock();

  bgsignal_.notify_all();

  for (auto& th : bgthreads_) {
    th.join();
  }

  bgthreads_.clear();

  exit_all_threads_ = false;
  wait_for_jobs_to_complete_ = false;
}

inline
void ThreadPoolImpl::Impl::LowerIOPriority() {
  std::lock_guard<std::mutex> lock(mu_);
  low_io_priority_ = true;
}

inline
void ThreadPoolImpl::Impl::LowerCPUPriority() {
  std::lock_guard<std::mutex> lock(mu_);
  low_cpu_priority_ = true;
}

void ThreadPoolImpl::Impl::BGThread(size_t thread_id) {
  bool low_io_priority = false;
  bool low_cpu_priority = false;

  while (true) {
    // Wait until there is an item that is ready to run
    std::unique_lock<std::mutex> lock(mu_);
    // Stop waiting if the thread needs to do work or needs to terminate.
    while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) &&
           (queue_.empty() || IsExcessiveThread(thread_id))) {
      bgsignal_.wait(lock);
    }

    if (exit_all_threads_) {  // mechanism to let BG threads exit safely

      if (!wait_for_jobs_to_complete_ ||
          queue_.empty()) {
        break;
       }
    }

    if (IsLastExcessiveThread(thread_id)) {
      // Current thread is the last generated one and is excessive.
      // We always terminate excessive thread in the reverse order of
      // generation time.
      auto& terminating_thread = bgthreads_.back();
      terminating_thread.detach();
      bgthreads_.pop_back();

      if (HasExcessiveThread()) {
        // There is still at least more excessive thread to terminate.
        WakeUpAllThreads();
      }
      break;
    }

    auto func = std::move(queue_.front().function);
    queue_.pop_front();

    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);

    bool decrease_io_priority = (low_io_priority != low_io_priority_);
    bool decrease_cpu_priority = (low_cpu_priority != low_cpu_priority_);
    lock.unlock();

#ifdef OS_LINUX
    if (decrease_cpu_priority) {
      setpriority(
          PRIO_PROCESS,
          // Current thread.
          0,
          // Lowest priority possible.
          19);
      low_cpu_priority = true;
    }

    if (decrease_io_priority) {
#define IOPRIO_CLASS_SHIFT (13)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)
      // Put schedule into IOPRIO_CLASS_IDLE class (lowest)
      // These system calls only have an effect when used in conjunction
      // with an I/O scheduler that supports I/O priorities. As at
      // kernel 2.6.17 the only such scheduler is the Completely
      // Fair Queuing (CFQ) I/O scheduler.
      // To change scheduler:
      //  echo cfq > /sys/block/<device_name>/queue/schedule
      // Tunables to consider:
      //  /sys/block/<device_name>/queue/slice_idle
      //  /sys/block/<device_name>/queue/slice_sync
      syscall(SYS_ioprio_set, 1,  // IOPRIO_WHO_PROCESS
              0,                  // current thread
              IOPRIO_PRIO_VALUE(3, 0));
      low_io_priority = true;
    }
#else
    (void)decrease_io_priority;  // avoid 'unused variable' error
    (void)decrease_cpu_priority;
#endif
    func();
  }
}

// Helper struct for passing arguments when creating threads.
struct BGThreadMetadata {
  ThreadPoolImpl::Impl* thread_pool_;
  size_t thread_id_;  // Thread count in the thread.
  BGThreadMetadata(ThreadPoolImpl::Impl* thread_pool, size_t thread_id)
      : thread_pool_(thread_pool), thread_id_(thread_id) {}
};

void* ThreadPoolImpl::Impl::BGThreadWrapper(void* arg) {
  BGThreadMetadata* meta = reinterpret_cast<BGThreadMetadata*>(arg);
  size_t thread_id = meta->thread_id_;
  ThreadPoolImpl::Impl* tp = meta->thread_pool_;
#ifdef ROCKSDB_USING_THREAD_STATUS
  // initialize it because compiler isn't good enough to see we don't use it
  // uninitialized
  ThreadStatus::ThreadType thread_type = ThreadStatus::NUM_THREAD_TYPES;
  switch (tp->GetThreadPriority()) {
    case Env::Priority::HIGH:
      thread_type = ThreadStatus::HIGH_PRIORITY;
      break;
    case Env::Priority::LOW:
      thread_type = ThreadStatus::LOW_PRIORITY;
      break;
    case Env::Priority::BOTTOM:
      thread_type = ThreadStatus::BOTTOM_PRIORITY;
      break;
    case Env::Priority::TOTAL:
      assert(false);
      return nullptr;
  }
  assert(thread_type != ThreadStatus::NUM_THREAD_TYPES);
  ThreadStatusUtil::RegisterThread(tp->GetHostEnv(), thread_type);
#endif
  delete meta;
  tp->BGThread(thread_id);
#ifdef ROCKSDB_USING_THREAD_STATUS
  ThreadStatusUtil::UnregisterThread();
#endif
  return nullptr;
}

void ThreadPoolImpl::Impl::SetBackgroundThreadsInternal(int num,
  bool allow_reduce) {
  std::unique_lock<std::mutex> lock(mu_);
  if (exit_all_threads_) {
    lock.unlock();
    return;
  }
  if (num > total_threads_limit_ ||
      (num < total_threads_limit_ && allow_reduce)) {
    total_threads_limit_ = std::max(0, num);
    WakeUpAllThreads();
    StartBGThreads();
  }
}

int ThreadPoolImpl::Impl::GetBackgroundThreads() {
  std::unique_lock<std::mutex> lock(mu_);
  return total_threads_limit_;
}

void ThreadPoolImpl::Impl::StartBGThreads() {
  // Start background thread if necessary
  while ((int)bgthreads_.size() < total_threads_limit_) {

    port::Thread p_t(&BGThreadWrapper,
      new BGThreadMetadata(this, bgthreads_.size()));

// Set the thread name to aid debugging
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    auto th_handle = p_t.native_handle();
    std::string thread_priority = Env::PriorityToString(GetThreadPriority());
    std::ostringstream thread_name_stream;
    thread_name_stream << "rocksdb:";
    for (char c : thread_priority) {
      thread_name_stream << static_cast<char>(tolower(c));
    }
    thread_name_stream << bgthreads_.size();
    pthread_setname_np(th_handle, thread_name_stream.str().c_str());
#endif
#endif
    bgthreads_.push_back(std::move(p_t));
  }
}

void ThreadPoolImpl::Impl::Submit(std::function<void()>&& schedule,
  std::function<void()>&& unschedule, void* tag) {

  std::lock_guard<std::mutex> lock(mu_);

  if (exit_all_threads_) {
    return;
  }

  StartBGThreads();

  // Add to priority queue
  queue_.push_back(BGItem());

  auto& item = queue_.back();
  item.tag = tag;
  item.function = std::move(schedule);
  item.unschedFunction = std::move(unschedule);

  queue_len_.store(static_cast<unsigned int>(queue_.size()),
    std::memory_order_relaxed);

  if (!HasExcessiveThread()) {
    // Wake up at least one waiting thread.
    bgsignal_.notify_one();
  } else {
    // Need to wake up all threads to make sure the one woken
    // up is not the one to terminate.
    WakeUpAllThreads();
  }
}

int ThreadPoolImpl::Impl::UnSchedule(void* arg) {
  int count = 0;

  std::vector<std::function<void()>> candidates;
  {
    std::lock_guard<std::mutex> lock(mu_);

    // Remove from priority queue
    BGQueue::iterator it = queue_.begin();
    while (it != queue_.end()) {
      if (arg == (*it).tag) {
        if (it->unschedFunction) {
          candidates.push_back(std::move(it->unschedFunction));
        }
        it = queue_.erase(it);
        count++;
      } else {
        ++it;
      }
    }
    queue_len_.store(static_cast<unsigned int>(queue_.size()),
      std::memory_order_relaxed);
  }


 // Run unschedule functions outside the mutex
  for (auto& f : candidates) {
    f();
  }

  return count;
}

ThreadPoolImpl::ThreadPoolImpl() :
  impl_(new Impl()) {
}


ThreadPoolImpl::~ThreadPoolImpl() {
}

void ThreadPoolImpl::JoinAllThreads() {
  impl_->JoinThreads(false);
}

void ThreadPoolImpl::SetBackgroundThreads(int num) {
  impl_->SetBackgroundThreadsInternal(num, true);
}

int ThreadPoolImpl::GetBackgroundThreads() {
  return impl_->GetBackgroundThreads();
}

unsigned int ThreadPoolImpl::GetQueueLen() const {
  return impl_->GetQueueLen();
}

void ThreadPoolImpl::WaitForJobsAndJoinAllThreads() {
  impl_->JoinThreads(true);
}

void ThreadPoolImpl::LowerIOPriority() {
  impl_->LowerIOPriority();
}

void ThreadPoolImpl::LowerCPUPriority() {
  impl_->LowerCPUPriority();
}

void ThreadPoolImpl::IncBackgroundThreadsIfNeeded(int num) {
  impl_->SetBackgroundThreadsInternal(num, false);
}

void ThreadPoolImpl::SubmitJob(const std::function<void()>& job) {
  auto copy(job);
  impl_->Submit(std::move(copy), std::function<void()>(), nullptr);
}


void ThreadPoolImpl::SubmitJob(std::function<void()>&& job) {
  impl_->Submit(std::move(job), std::function<void()>(), nullptr);
}

void ThreadPoolImpl::Schedule(void(*function)(void* arg1), void* arg,
  void* tag, void(*unschedFunction)(void* arg)) {
  if (unschedFunction == nullptr) {
    impl_->Submit(std::bind(function, arg), std::function<void()>(), tag);
  } else {
    impl_->Submit(std::bind(function, arg), std::bind(unschedFunction, arg),
                  tag);
  }
}

int ThreadPoolImpl::UnSchedule(void* arg) {
  return impl_->UnSchedule(arg);
}

void ThreadPoolImpl::SetHostEnv(Env* env) { impl_->SetHostEnv(env); }

Env* ThreadPoolImpl::GetHostEnv() const { return impl_->GetHostEnv(); }

// Return the thread priority.
// This would allow its member-thread to know its priority.
Env::Priority ThreadPoolImpl::GetThreadPriority() const {
  return impl_->GetThreadPriority();
}

// Set the thread priority.
void ThreadPoolImpl::SetThreadPriority(Env::Priority priority) {
  impl_->SetThreadPriority(priority);
}

ThreadPool* NewThreadPool(int num_threads) {
  ThreadPoolImpl* thread_pool = new ThreadPoolImpl();
  thread_pool->SetBackgroundThreads(num_threads);
  return thread_pool;
}

}  // namespace rocksdb
