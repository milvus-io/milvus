// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include "table/internal_iterator.h"
#include "port/port.h"

namespace rocksdb {
class ScopedArenaIterator {

  void reset(InternalIterator* iter) ROCKSDB_NOEXCEPT {
    if (iter_ != nullptr) {
      iter_->~InternalIterator();
    }
    iter_ = iter;
  }

 public:

  explicit ScopedArenaIterator(InternalIterator* iter = nullptr)
      : iter_(iter) {}

  ScopedArenaIterator(const ScopedArenaIterator&) = delete;
  ScopedArenaIterator& operator=(const ScopedArenaIterator&) = delete;

  ScopedArenaIterator(ScopedArenaIterator&& o) ROCKSDB_NOEXCEPT {
    iter_ = o.iter_;
    o.iter_ = nullptr;
  }

  ScopedArenaIterator& operator=(ScopedArenaIterator&& o) ROCKSDB_NOEXCEPT {
    reset(o.iter_);
    o.iter_ = nullptr;
    return *this;
  }

  InternalIterator* operator->() { return iter_; }
  InternalIterator* get() { return iter_; }

  void set(InternalIterator* iter) { reset(iter); }

  InternalIterator* release() {
    assert(iter_ != nullptr);
    auto* res = iter_;
    iter_ = nullptr;
    return res;
  }

  ~ScopedArenaIterator() {
    reset(nullptr);
  }

 private:
  InternalIterator* iter_;
};
}  // namespace rocksdb
