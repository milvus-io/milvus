//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <list>
#include <memory>
#include <string>

#include "include/rocksdb/comparator.h"
#include "util/arena.h"
#include "util/mutexlock.h"

namespace rocksdb {

//
// CacheWriteBuffer
//
// Buffer abstraction that can be manipulated via append
// (not thread safe)
class CacheWriteBuffer {
 public:
  explicit CacheWriteBuffer(const size_t size) : size_(size), pos_(0) {
    buf_.reset(new char[size_]);
    assert(!pos_);
    assert(size_);
  }

  virtual ~CacheWriteBuffer() {}

  void Append(const char* buf, const size_t size) {
    assert(pos_ + size <= size_);
    memcpy(buf_.get() + pos_, buf, size);
    pos_ += size;
    assert(pos_ <= size_);
  }

  void FillTrailingZeros() {
    assert(pos_ <= size_);
    memset(buf_.get() + pos_, '0', size_ - pos_);
    pos_ = size_;
  }

  void Reset() { pos_ = 0; }
  size_t Free() const { return size_ - pos_; }
  size_t Capacity() const { return size_; }
  size_t Used() const { return pos_; }
  char* Data() const { return buf_.get(); }

 private:
  std::unique_ptr<char[]> buf_;
  const size_t size_;
  size_t pos_;
};

//
// CacheWriteBufferAllocator
//
// Buffer pool abstraction(not thread safe)
//
class CacheWriteBufferAllocator {
 public:
  explicit CacheWriteBufferAllocator(const size_t buffer_size,
                                     const size_t buffer_count)
      : cond_empty_(&lock_), buffer_size_(buffer_size) {
    MutexLock _(&lock_);
    buffer_size_ = buffer_size;
    for (uint32_t i = 0; i < buffer_count; i++) {
      auto* buf = new CacheWriteBuffer(buffer_size_);
      assert(buf);
      if (buf) {
        bufs_.push_back(buf);
        cond_empty_.Signal();
      }
    }
  }

  virtual ~CacheWriteBufferAllocator() {
    MutexLock _(&lock_);
    assert(bufs_.size() * buffer_size_ == Capacity());
    for (auto* buf : bufs_) {
      delete buf;
    }
    bufs_.clear();
  }

  CacheWriteBuffer* Allocate() {
    MutexLock _(&lock_);
    if (bufs_.empty()) {
      return nullptr;
    }

    assert(!bufs_.empty());
    CacheWriteBuffer* const buf = bufs_.front();
    bufs_.pop_front();
    return buf;
  }

  void Deallocate(CacheWriteBuffer* const buf) {
    assert(buf);
    MutexLock _(&lock_);
    buf->Reset();
    bufs_.push_back(buf);
    cond_empty_.Signal();
  }

  void WaitUntilUsable() {
    // We are asked to wait till we have buffers available
    MutexLock _(&lock_);
    while (bufs_.empty()) {
      cond_empty_.Wait();
    }
  }

  size_t Capacity() const { return bufs_.size() * buffer_size_; }
  size_t Free() const { return bufs_.size() * buffer_size_; }
  size_t BufferSize() const { return buffer_size_; }

 private:
  port::Mutex lock_;                   // Sync lock
  port::CondVar cond_empty_;           // Condition var for empty buffers
  size_t buffer_size_;                 // Size of each buffer
  std::list<CacheWriteBuffer*> bufs_;  // Buffer stash
};

}  // namespace rocksdb
