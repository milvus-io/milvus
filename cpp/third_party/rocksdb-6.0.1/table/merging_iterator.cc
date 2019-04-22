//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merging_iterator.h"
#include <string>
#include <vector>
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "table/iter_heap.h"
#include "table/iterator_wrapper.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "util/heap.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {
// Without anonymous namespace here, we fail the warning -Wmissing-prototypes
namespace {
typedef BinaryHeap<IteratorWrapper*, MaxIteratorComparator> MergerMaxIterHeap;
typedef BinaryHeap<IteratorWrapper*, MinIteratorComparator> MergerMinIterHeap;
}  // namespace

const size_t kNumIterReserve = 4;

class MergingIterator : public InternalIterator {
 public:
  MergingIterator(const InternalKeyComparator* comparator,
                  InternalIterator** children, int n, bool is_arena_mode,
                  bool prefix_seek_mode)
      : is_arena_mode_(is_arena_mode),
        comparator_(comparator),
        current_(nullptr),
        direction_(kForward),
        minHeap_(comparator_),
        prefix_seek_mode_(prefix_seek_mode),
        pinned_iters_mgr_(nullptr) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
    for (auto& child : children_) {
      if (child.Valid()) {
        assert(child.status().ok());
        minHeap_.push(&child);
      } else {
        considerStatus(child.status());
      }
    }
    current_ = CurrentForward();
  }

  void considerStatus(Status s) {
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  virtual void AddIterator(InternalIterator* iter) {
    assert(direction_ == kForward);
    children_.emplace_back(iter);
    if (pinned_iters_mgr_) {
      iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
    auto new_wrapper = children_.back();
    if (new_wrapper.Valid()) {
      assert(new_wrapper.status().ok());
      minHeap_.push(&new_wrapper);
      current_ = CurrentForward();
    } else {
      considerStatus(new_wrapper.status());
    }
  }

  ~MergingIterator() override {
    for (auto& child : children_) {
      child.DeleteIter(is_arena_mode_);
    }
  }

  bool Valid() const override { return current_ != nullptr && status_.ok(); }

  Status status() const override { return status_; }

  void SeekToFirst() override {
    ClearHeaps();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.SeekToFirst();
      if (child.Valid()) {
        assert(child.status().ok());
        minHeap_.push(&child);
      } else {
        considerStatus(child.status());
      }
    }
    direction_ = kForward;
    current_ = CurrentForward();
  }

  void SeekToLast() override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();
    for (auto& child : children_) {
      child.SeekToLast();
      if (child.Valid()) {
        assert(child.status().ok());
        maxHeap_->push(&child);
      } else {
        considerStatus(child.status());
      }
    }
    direction_ = kReverse;
    current_ = CurrentReverse();
  }

  void Seek(const Slice& target) override {
    ClearHeaps();
    status_ = Status::OK();
    for (auto& child : children_) {
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.Seek(target);
      }
      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      if (child.Valid()) {
        assert(child.status().ok());
        PERF_TIMER_GUARD(seek_min_heap_time);
        minHeap_.push(&child);
      } else {
        considerStatus(child.status());
      }
    }
    direction_ = kForward;
    {
      PERF_TIMER_GUARD(seek_min_heap_time);
      current_ = CurrentForward();
    }
  }

  void SeekForPrev(const Slice& target) override {
    ClearHeaps();
    InitMaxHeap();
    status_ = Status::OK();

    for (auto& child : children_) {
      {
        PERF_TIMER_GUARD(seek_child_seek_time);
        child.SeekForPrev(target);
      }
      PERF_COUNTER_ADD(seek_child_seek_count, 1);

      if (child.Valid()) {
        assert(child.status().ok());
        PERF_TIMER_GUARD(seek_max_heap_time);
        maxHeap_->push(&child);
      } else {
        considerStatus(child.status());
      }
    }
    direction_ = kReverse;
    {
      PERF_TIMER_GUARD(seek_max_heap_time);
      current_ = CurrentReverse();
    }
  }

  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current children since current_ is
    // the smallest child and key() == current_->key().
    if (direction_ != kForward) {
      SwitchToForward();
      // The loop advanced all non-current children to be > key() so current_
      // should still be strictly the smallest key.
      assert(current_ == CurrentForward());
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentForward());

    // as the current points to the current record. move the iterator forward.
    current_->Next();
    if (current_->Valid()) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      minHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      minHeap_.pop();
    }
    current_ = CurrentForward();
  }

  void Prev() override {
    assert(Valid());
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current children since current_ is
    // the largest child and key() == current_->key().
    if (direction_ != kReverse) {
      // Otherwise, retreat the non-current children.  We retreat current_
      // just after the if-block.
      ClearHeaps();
      InitMaxHeap();
      Slice target = key();
      for (auto& child : children_) {
        if (&child != current_) {
          child.SeekForPrev(target);
          TEST_SYNC_POINT_CALLBACK("MergeIterator::Prev:BeforePrev", &child);
          considerStatus(child.status());
          if (child.Valid() && comparator_->Equal(target, child.key())) {
            child.Prev();
            considerStatus(child.status());
          }
        }
        if (child.Valid()) {
          assert(child.status().ok());
          maxHeap_->push(&child);
        }
      }
      direction_ = kReverse;
      if (!prefix_seek_mode_) {
        // Note that we don't do assert(current_ == CurrentReverse()) here
        // because it is possible to have some keys larger than the seek-key
        // inserted between Seek() and SeekToLast(), which makes current_ not
        // equal to CurrentReverse().
        current_ = CurrentReverse();
      }
      // The loop advanced all non-current children to be < key() so current_
      // should still be strictly the smallest key.
      assert(current_ == CurrentReverse());
    }

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == CurrentReverse());

    current_->Prev();
    if (current_->Valid()) {
      // current is still valid after the Prev() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->status().ok());
      maxHeap_->replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->status());
      maxHeap_->pop();
    }
    current_ = CurrentReverse();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           current_->IsValuePinned();
  }

 private:
  // Clears heaps for both directions, used when changing direction or seeking
  void ClearHeaps();
  // Ensures that maxHeap_ is initialized when starting to go in the reverse
  // direction
  void InitMaxHeap();

  bool is_arena_mode_;
  const InternalKeyComparator* comparator_;
  autovector<IteratorWrapper, kNumIterReserve> children_;

  // Cached pointer to child iterator with the current key, or nullptr if no
  // child iterators are valid.  This is the top of minHeap_ or maxHeap_
  // depending on the direction.
  IteratorWrapper* current_;
  // If any of the children have non-ok status, this is one of them.
  Status status_;
  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
  MergerMinIterHeap minHeap_;
  bool prefix_seek_mode_;

  // Max heap is used for reverse iteration, which is way less common than
  // forward.  Lazily initialize it to save memory.
  std::unique_ptr<MergerMaxIterHeap> maxHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;

  void SwitchToForward();

  IteratorWrapper* CurrentForward() const {
    assert(direction_ == kForward);
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  IteratorWrapper* CurrentReverse() const {
    assert(direction_ == kReverse);
    assert(maxHeap_);
    return !maxHeap_->empty() ? maxHeap_->top() : nullptr;
  }
};

void MergingIterator::SwitchToForward() {
  // Otherwise, advance the non-current children.  We advance current_
  // just after the if-block.
  ClearHeaps();
  Slice target = key();
  for (auto& child : children_) {
    if (&child != current_) {
      child.Seek(target);
      considerStatus(child.status());
      if (child.Valid() && comparator_->Equal(target, child.key())) {
        child.Next();
        considerStatus(child.status());
      }
    }
    if (child.Valid()) {
      minHeap_.push(&child);
    }
  }
  direction_ = kForward;
}

void MergingIterator::ClearHeaps() {
  minHeap_.clear();
  if (maxHeap_) {
    maxHeap_->clear();
  }
}

void MergingIterator::InitMaxHeap() {
  if (!maxHeap_) {
    maxHeap_.reset(new MergerMaxIterHeap(comparator_));
  }
}

InternalIterator* NewMergingIterator(const InternalKeyComparator* cmp,
                                     InternalIterator** list, int n,
                                     Arena* arena, bool prefix_seek_mode) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyInternalIterator<Slice>(arena);
  } else if (n == 1) {
    return list[0];
  } else {
    if (arena == nullptr) {
      return new MergingIterator(cmp, list, n, false, prefix_seek_mode);
    } else {
      auto mem = arena->AllocateAligned(sizeof(MergingIterator));
      return new (mem) MergingIterator(cmp, list, n, true, prefix_seek_mode);
    }
  }
}

MergeIteratorBuilder::MergeIteratorBuilder(
    const InternalKeyComparator* comparator, Arena* a, bool prefix_seek_mode)
    : first_iter(nullptr), use_merging_iter(false), arena(a) {
  auto mem = arena->AllocateAligned(sizeof(MergingIterator));
  merge_iter =
      new (mem) MergingIterator(comparator, nullptr, 0, true, prefix_seek_mode);
}

MergeIteratorBuilder::~MergeIteratorBuilder() {
  if (first_iter != nullptr) {
    first_iter->~InternalIterator();
  }
  if (merge_iter != nullptr) {
    merge_iter->~MergingIterator();
  }
}

void MergeIteratorBuilder::AddIterator(InternalIterator* iter) {
  if (!use_merging_iter && first_iter != nullptr) {
    merge_iter->AddIterator(first_iter);
    use_merging_iter = true;
    first_iter = nullptr;
  }
  if (use_merging_iter) {
    merge_iter->AddIterator(iter);
  } else {
    first_iter = iter;
  }
}

InternalIterator* MergeIteratorBuilder::Finish() {
  InternalIterator* ret = nullptr;
  if (!use_merging_iter) {
    ret = first_iter;
    first_iter = nullptr;
  } else {
    ret = merge_iter;
    merge_iter = nullptr;
  }
  return ret;
}

}  // namespace rocksdb
