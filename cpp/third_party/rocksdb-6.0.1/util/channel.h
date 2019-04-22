//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>
#include <utility>

namespace rocksdb {

template <class T>
class channel {
 public:
  explicit channel() : eof_(false) {}

  channel(const channel&) = delete;
  void operator=(const channel&) = delete;

  void sendEof() {
    std::lock_guard<std::mutex> lk(lock_);
    eof_ = true;
    cv_.notify_all();
  }

  bool eof() {
    std::lock_guard<std::mutex> lk(lock_);
    return buffer_.empty() && eof_;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lk(lock_);
    return buffer_.size();
  }

  // writes elem to the queue
  void write(T&& elem) {
    std::unique_lock<std::mutex> lk(lock_);
    buffer_.emplace(std::forward<T>(elem));
    cv_.notify_one();
  }

  /// Moves a dequeued element onto elem, blocking until an element
  /// is available.
  // returns false if EOF
  bool read(T& elem) {
    std::unique_lock<std::mutex> lk(lock_);
    cv_.wait(lk, [&] { return eof_ || !buffer_.empty(); });
    if (eof_ && buffer_.empty()) {
      return false;
    }
    elem = std::move(buffer_.front());
    buffer_.pop();
    cv_.notify_one();
    return true;
  }

 private:
  std::condition_variable cv_;
  std::mutex lock_;
  std::queue<T> buffer_;
  bool eof_;
};
}  // namespace rocksdb
