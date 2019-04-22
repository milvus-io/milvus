//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "rocksdb/threadpool.h"
#include "rocksdb/env.h"

#include <memory>
#include <functional>

namespace rocksdb {


class ThreadPoolImpl : public ThreadPool {
 public:
  ThreadPoolImpl();
  ~ThreadPoolImpl();

  ThreadPoolImpl(ThreadPoolImpl&&) = delete;
  ThreadPoolImpl& operator=(ThreadPoolImpl&&) = delete;

  // Implement ThreadPool interfaces

  // Wait for all threads to finish.
  // Discards all the jobs that did not
  // start executing and waits for those running
  // to complete
  void JoinAllThreads() override;

  // Set the number of background threads that will be executing the
  // scheduled jobs.
  void SetBackgroundThreads(int num) override;
  int GetBackgroundThreads() override;

  // Get the number of jobs scheduled in the ThreadPool queue.
  unsigned int GetQueueLen() const override;

  // Waits for all jobs to complete those
  // that already started running and those that did not
  // start yet
  void WaitForJobsAndJoinAllThreads() override;

  // Make threads to run at a lower kernel IO priority
  // Currently only has effect on Linux
  void LowerIOPriority();

  // Make threads to run at a lower kernel CPU priority
  // Currently only has effect on Linux
  void LowerCPUPriority();

  // Ensure there is at aleast num threads in the pool
  // but do not kill threads if there are more
  void IncBackgroundThreadsIfNeeded(int num);

  // Submit a fire and forget job
  // These jobs can not be unscheduled

  // This allows to submit the same job multiple times
  void SubmitJob(const std::function<void()>&) override;
  // This moves the function in for efficiency
  void SubmitJob(std::function<void()>&&) override;

  // Schedule a job with an unschedule tag and unschedule function
  // Can be used to filter and unschedule jobs by a tag
  // that are still in the queue and did not start running
  void Schedule(void (*function)(void* arg1), void* arg, void* tag,
                void (*unschedFunction)(void* arg));

  // Filter jobs that are still in a queue and match
  // the given tag. Remove them from a queue if any
  // and for each such job execute an unschedule function
  // if such was given at scheduling time.
  int UnSchedule(void* tag);

  void SetHostEnv(Env* env);

  Env* GetHostEnv() const;

  // Return the thread priority.
  // This would allow its member-thread to know its priority.
  Env::Priority GetThreadPriority() const;

  // Set the thread priority.
  void SetThreadPriority(Env::Priority priority);

  static void PthreadCall(const char* label, int result);

  struct Impl;

 private:

   // Current public virtual interface does not provide usable
   // functionality and thus can not be used internally to
   // facade different implementations.
   //
   // We propose a pimpl idiom in order to easily replace the thread pool impl
   // w/o touching the header file but providing a different .cc potentially
   // CMake option driven.
   //
   // Another option is to introduce a Env::MakeThreadPool() virtual interface
   // and override the environment. This would require refactoring ThreadPool usage.
   //
   // We can also combine these two approaches
   std::unique_ptr<Impl>   impl_;
};

}  // namespace rocksdb
