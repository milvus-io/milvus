//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <limits>
#include <list>

#include "util/mutexlock.h"

namespace rocksdb {

//
// Simple synchronized queue implementation with the option of
// bounding the queue
//
// On overflow, the elements will be discarded
//
template <class T>
class BoundedQueue {
 public:
  explicit BoundedQueue(
      const size_t max_size = std::numeric_limits<size_t>::max())
      : cond_empty_(&lock_), max_size_(max_size) {}

  virtual ~BoundedQueue() {}

  void Push(T&& t) {
    MutexLock _(&lock_);
    if (max_size_ != std::numeric_limits<size_t>::max() &&
        size_ + t.Size() >= max_size_) {
      // overflow
      return;
    }

    size_ += t.Size();
    q_.push_back(std::move(t));
    cond_empty_.SignalAll();
  }

  T Pop() {
    MutexLock _(&lock_);
    while (q_.empty()) {
      cond_empty_.Wait();
    }

    T t = std::move(q_.front());
    size_ -= t.Size();
    q_.pop_front();
    return std::move(t);
  }

  size_t Size() const {
    MutexLock _(&lock_);
    return size_;
  }

 private:
  mutable port::Mutex lock_;
  port::CondVar cond_empty_;
  std::list<T> q_;
  size_t size_ = 0;
  const size_t max_size_;
};

}  // namespace rocksdb
