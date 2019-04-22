//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/flush_scheduler.h"

#include <cassert>

#include "db/column_family.h"

namespace rocksdb {

void FlushScheduler::ScheduleFlush(ColumnFamilyData* cfd) {
#ifndef NDEBUG
  std::lock_guard<std::mutex> lock(checking_mutex_);
  assert(checking_set_.count(cfd) == 0);
  checking_set_.insert(cfd);
#endif  // NDEBUG
  cfd->Ref();
// Suppress false positive clang analyzer warnings.
#ifndef __clang_analyzer__
  Node* node = new Node{cfd, head_.load(std::memory_order_relaxed)};
  while (!head_.compare_exchange_strong(
      node->next, node, std::memory_order_relaxed, std::memory_order_relaxed)) {
    // failing CAS updates the first param, so we are already set for
    // retry.  TakeNextColumnFamily won't happen until after another
    // inter-thread synchronization, so we don't even need release
    // semantics for this CAS
  }
#endif  // __clang_analyzer__
}

ColumnFamilyData* FlushScheduler::TakeNextColumnFamily() {
#ifndef NDEBUG
  std::lock_guard<std::mutex> lock(checking_mutex_);
#endif  // NDEBUG
  while (true) {
    if (head_.load(std::memory_order_relaxed) == nullptr) {
      return nullptr;
    }

    // dequeue the head
    Node* node = head_.load(std::memory_order_relaxed);
    head_.store(node->next, std::memory_order_relaxed);
    ColumnFamilyData* cfd = node->column_family;
    delete node;

#ifndef NDEBUG
    auto iter = checking_set_.find(cfd);
    assert(iter != checking_set_.end());
    checking_set_.erase(iter);
#endif  // NDEBUG

    if (!cfd->IsDropped()) {
      // success
      return cfd;
    }

    // no longer relevant, retry
    if (cfd->Unref()) {
      delete cfd;
    }
  }
}

bool FlushScheduler::Empty() {
#ifndef NDEBUG
  std::lock_guard<std::mutex> lock(checking_mutex_);
#endif  // NDEBUG
  auto rv = head_.load(std::memory_order_relaxed) == nullptr;
#ifndef NDEBUG
  assert(rv == checking_set_.empty());
#endif  // NDEBUG
  return rv;
}

void FlushScheduler::Clear() {
  ColumnFamilyData* cfd;
  while ((cfd = TakeNextColumnFamily()) != nullptr) {
    if (cfd->Unref()) {
      delete cfd;
    }
  }
  assert(head_.load(std::memory_order_relaxed) == nullptr);
}

}  // namespace rocksdb
