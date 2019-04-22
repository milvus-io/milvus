//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#ifndef ROCKSDB_LITE

#include <atomic>

#include "util/mutexlock.h"

namespace rocksdb {

// LRU element definition
//
// Any object that needs to be part of the LRU algorithm should extend this
// class
template <class T>
struct LRUElement {
  explicit LRUElement() : next_(nullptr), prev_(nullptr), refs_(0) {}

  virtual ~LRUElement() { assert(!refs_); }

  T* next_;
  T* prev_;
  std::atomic<size_t> refs_;
};

// LRU implementation
//
// In place LRU implementation. There is no copy or allocation involved when
// inserting or removing an element. This makes the data structure slim
template <class T>
class LRUList {
 public:
  virtual ~LRUList() {
    MutexLock _(&lock_);
    assert(!head_);
    assert(!tail_);
  }

  // Push element into the LRU at the cold end
  inline void Push(T* const t) {
    assert(t);
    assert(!t->next_);
    assert(!t->prev_);

    MutexLock _(&lock_);

    assert((!head_ && !tail_) || (head_ && tail_));
    assert(!head_ || !head_->prev_);
    assert(!tail_ || !tail_->next_);

    t->next_ = head_;
    if (head_) {
      head_->prev_ = t;
    }

    head_ = t;
    if (!tail_) {
      tail_ = t;
    }
  }

  // Unlink the element from the LRU
  inline void Unlink(T* const t) {
    MutexLock _(&lock_);
    UnlinkImpl(t);
  }

  // Evict an element from the LRU
  inline T* Pop() {
    MutexLock _(&lock_);

    assert(tail_ && head_);
    assert(!tail_->next_);
    assert(!head_->prev_);

    T* t = head_;
    while (t && t->refs_) {
      t = t->next_;
    }

    if (!t) {
      // nothing can be evicted
      return nullptr;
    }

    assert(!t->refs_);

    // unlike the element
    UnlinkImpl(t);
    return t;
  }

  // Move the element from the front of the list to the back of the list
  inline void Touch(T* const t) {
    MutexLock _(&lock_);
    UnlinkImpl(t);
    PushBackImpl(t);
  }

  // Check if the LRU is empty
  inline bool IsEmpty() const {
    MutexLock _(&lock_);
    return !head_ && !tail_;
  }

 private:
  // Unlink an element from the LRU
  void UnlinkImpl(T* const t) {
    assert(t);

    lock_.AssertHeld();

    assert(head_ && tail_);
    assert(t->prev_ || head_ == t);
    assert(t->next_ || tail_ == t);

    if (t->prev_) {
      t->prev_->next_ = t->next_;
    }
    if (t->next_) {
      t->next_->prev_ = t->prev_;
    }

    if (tail_ == t) {
      tail_ = tail_->prev_;
    }
    if (head_ == t) {
      head_ = head_->next_;
    }

    t->next_ = t->prev_ = nullptr;
  }

  // Insert an element at the hot end
  inline void PushBack(T* const t) {
    MutexLock _(&lock_);
    PushBackImpl(t);
  }

  inline void PushBackImpl(T* const t) {
    assert(t);
    assert(!t->next_);
    assert(!t->prev_);

    lock_.AssertHeld();

    assert((!head_ && !tail_) || (head_ && tail_));
    assert(!head_ || !head_->prev_);
    assert(!tail_ || !tail_->next_);

    t->prev_ = tail_;
    if (tail_) {
      tail_->next_ = t;
    }

    tail_ = t;
    if (!head_) {
      head_ = tail_;
    }
  }

  mutable port::Mutex lock_;  // synchronization primitive
  T* head_ = nullptr;         // front (cold)
  T* tail_ = nullptr;         // back (hot)
};

}  // namespace rocksdb

#endif
