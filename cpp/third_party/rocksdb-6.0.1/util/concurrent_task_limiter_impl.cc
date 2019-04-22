//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/concurrent_task_limiter_impl.h"
#include "rocksdb/concurrent_task_limiter.h"

namespace rocksdb {

ConcurrentTaskLimiterImpl::ConcurrentTaskLimiterImpl(
    const std::string& name, int32_t max_outstanding_task)
    : name_(name),
      max_outstanding_tasks_{max_outstanding_task},
      outstanding_tasks_{0} {

}

ConcurrentTaskLimiterImpl::~ConcurrentTaskLimiterImpl() {
  assert(outstanding_tasks_ == 0);
}

const std::string& ConcurrentTaskLimiterImpl::GetName() const {
  return name_;
}

void ConcurrentTaskLimiterImpl::SetMaxOutstandingTask(int32_t limit) {
  max_outstanding_tasks_.store(limit, std::memory_order_relaxed);
}

void ConcurrentTaskLimiterImpl::ResetMaxOutstandingTask() {
  max_outstanding_tasks_.store(-1, std::memory_order_relaxed);
}

int32_t ConcurrentTaskLimiterImpl::GetOutstandingTask() const {
  return outstanding_tasks_.load(std::memory_order_relaxed);
}

std::unique_ptr<TaskLimiterToken> ConcurrentTaskLimiterImpl::GetToken(
    bool force) {
  int32_t limit = max_outstanding_tasks_.load(std::memory_order_relaxed);
  int32_t tasks = outstanding_tasks_.load(std::memory_order_relaxed);
  // force = true, bypass the throttle.
  // limit < 0 means unlimited tasks.
  while (force || limit < 0 || tasks < limit) {
    if (outstanding_tasks_.compare_exchange_weak(tasks, tasks + 1)) {
      return std::unique_ptr<TaskLimiterToken>(new TaskLimiterToken(this));
    }
  }
  return nullptr;
}

ConcurrentTaskLimiter* NewConcurrentTaskLimiter(
    const std::string& name, int32_t limit) {
  return new ConcurrentTaskLimiterImpl(name, limit);
}

TaskLimiterToken::~TaskLimiterToken() {
  --limiter_->outstanding_tasks_;
  assert(limiter_->outstanding_tasks_ >= 0);
}

}  // namespace rocksdb
