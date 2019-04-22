//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_del_aggregator.h"

#include "db/compaction_iteration_stats.h"
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_edit.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/types.h"
#include "table/internal_iterator.h"
#include "table/scoped_arena_iterator.h"
#include "table/table_builder.h"
#include "util/heap.h"
#include "util/kv_map.h"
#include "util/vector_iterator.h"

namespace rocksdb {

TruncatedRangeDelIterator::TruncatedRangeDelIterator(
    std::unique_ptr<FragmentedRangeTombstoneIterator> iter,
    const InternalKeyComparator* icmp, const InternalKey* smallest,
    const InternalKey* largest)
    : iter_(std::move(iter)),
      icmp_(icmp),
      smallest_ikey_(smallest),
      largest_ikey_(largest) {
  if (smallest != nullptr) {
    pinned_bounds_.emplace_back();
    auto& parsed_smallest = pinned_bounds_.back();
    if (!ParseInternalKey(smallest->Encode(), &parsed_smallest)) {
      assert(false);
    }
    smallest_ = &parsed_smallest;
  }
  if (largest != nullptr) {
    pinned_bounds_.emplace_back();
    auto& parsed_largest = pinned_bounds_.back();
    if (!ParseInternalKey(largest->Encode(), &parsed_largest)) {
      assert(false);
    }
    if (parsed_largest.type == kTypeRangeDeletion &&
        parsed_largest.sequence == kMaxSequenceNumber) {
      // The file boundary has been artificially extended by a range tombstone.
      // We do not need to adjust largest to properly truncate range
      // tombstones that extend past the boundary.
    } else if (parsed_largest.sequence == 0) {
      // The largest key in the sstable has a sequence number of 0. Since we
      // guarantee that no internal keys with the same user key and sequence
      // number can exist in a DB, we know that the largest key in this sstable
      // cannot exist as the smallest key in the next sstable. This further
      // implies that no range tombstone in this sstable covers largest;
      // otherwise, the file boundary would have been artificially extended.
      //
      // Therefore, we will never truncate a range tombstone at largest, so we
      // can leave it unchanged.
    } else {
      // The same user key may straddle two sstable boundaries. To ensure that
      // the truncated end key can cover the largest key in this sstable, reduce
      // its sequence number by 1.
      parsed_largest.sequence -= 1;
    }
    largest_ = &parsed_largest;
  }
}

bool TruncatedRangeDelIterator::Valid() const {
  return iter_->Valid() &&
         (smallest_ == nullptr ||
          icmp_->Compare(*smallest_, iter_->parsed_end_key()) < 0) &&
         (largest_ == nullptr ||
          icmp_->Compare(iter_->parsed_start_key(), *largest_) < 0);
}

void TruncatedRangeDelIterator::Next() { iter_->TopNext(); }

void TruncatedRangeDelIterator::Prev() { iter_->TopPrev(); }

void TruncatedRangeDelIterator::InternalNext() { iter_->Next(); }

// NOTE: target is a user key
void TruncatedRangeDelIterator::Seek(const Slice& target) {
  if (largest_ != nullptr &&
      icmp_->Compare(*largest_, ParsedInternalKey(target, kMaxSequenceNumber,
                                                  kTypeRangeDeletion)) <= 0) {
    iter_->Invalidate();
    return;
  }
  if (smallest_ != nullptr &&
      icmp_->user_comparator()->Compare(target, smallest_->user_key) < 0) {
    iter_->Seek(smallest_->user_key);
    return;
  }
  iter_->Seek(target);
}

// NOTE: target is a user key
void TruncatedRangeDelIterator::SeekForPrev(const Slice& target) {
  if (smallest_ != nullptr &&
      icmp_->Compare(ParsedInternalKey(target, 0, kTypeRangeDeletion),
                     *smallest_) < 0) {
    iter_->Invalidate();
    return;
  }
  if (largest_ != nullptr &&
      icmp_->user_comparator()->Compare(largest_->user_key, target) < 0) {
    iter_->SeekForPrev(largest_->user_key);
    return;
  }
  iter_->SeekForPrev(target);
}

void TruncatedRangeDelIterator::SeekToFirst() {
  if (smallest_ != nullptr) {
    iter_->Seek(smallest_->user_key);
    return;
  }
  iter_->SeekToTopFirst();
}

void TruncatedRangeDelIterator::SeekToLast() {
  if (largest_ != nullptr) {
    iter_->SeekForPrev(largest_->user_key);
    return;
  }
  iter_->SeekToTopLast();
}

std::map<SequenceNumber, std::unique_ptr<TruncatedRangeDelIterator>>
TruncatedRangeDelIterator::SplitBySnapshot(
    const std::vector<SequenceNumber>& snapshots) {
  using FragmentedIterPair =
      std::pair<const SequenceNumber,
                std::unique_ptr<FragmentedRangeTombstoneIterator>>;

  auto split_untruncated_iters = iter_->SplitBySnapshot(snapshots);
  std::map<SequenceNumber, std::unique_ptr<TruncatedRangeDelIterator>>
      split_truncated_iters;
  std::for_each(
      split_untruncated_iters.begin(), split_untruncated_iters.end(),
      [&](FragmentedIterPair& iter_pair) {
        std::unique_ptr<TruncatedRangeDelIterator> truncated_iter(
            new TruncatedRangeDelIterator(std::move(iter_pair.second), icmp_,
                                          smallest_ikey_, largest_ikey_));
        split_truncated_iters.emplace(iter_pair.first,
                                      std::move(truncated_iter));
      });
  return split_truncated_iters;
}

ForwardRangeDelIterator::ForwardRangeDelIterator(
    const InternalKeyComparator* icmp)
    : icmp_(icmp),
      unused_idx_(0),
      active_seqnums_(SeqMaxComparator()),
      active_iters_(EndKeyMinComparator(icmp)),
      inactive_iters_(StartKeyMinComparator(icmp)) {}

bool ForwardRangeDelIterator::ShouldDelete(const ParsedInternalKey& parsed) {
  // Move active iterators that end before parsed.
  while (!active_iters_.empty() &&
         icmp_->Compare((*active_iters_.top())->end_key(), parsed) <= 0) {
    TruncatedRangeDelIterator* iter = PopActiveIter();
    do {
      iter->Next();
    } while (iter->Valid() && icmp_->Compare(iter->end_key(), parsed) <= 0);
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  // Move inactive iterators that start before parsed.
  while (!inactive_iters_.empty() &&
         icmp_->Compare(inactive_iters_.top()->start_key(), parsed) <= 0) {
    TruncatedRangeDelIterator* iter = PopInactiveIter();
    while (iter->Valid() && icmp_->Compare(iter->end_key(), parsed) <= 0) {
      iter->Next();
    }
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  return active_seqnums_.empty()
             ? false
             : (*active_seqnums_.begin())->seq() > parsed.sequence;
}

void ForwardRangeDelIterator::Invalidate() {
  unused_idx_ = 0;
  active_iters_.clear();
  active_seqnums_.clear();
  inactive_iters_.clear();
}

ReverseRangeDelIterator::ReverseRangeDelIterator(
    const InternalKeyComparator* icmp)
    : icmp_(icmp),
      unused_idx_(0),
      active_seqnums_(SeqMaxComparator()),
      active_iters_(StartKeyMaxComparator(icmp)),
      inactive_iters_(EndKeyMaxComparator(icmp)) {}

bool ReverseRangeDelIterator::ShouldDelete(const ParsedInternalKey& parsed) {
  // Move active iterators that start after parsed.
  while (!active_iters_.empty() &&
         icmp_->Compare(parsed, (*active_iters_.top())->start_key()) < 0) {
    TruncatedRangeDelIterator* iter = PopActiveIter();
    do {
      iter->Prev();
    } while (iter->Valid() && icmp_->Compare(parsed, iter->start_key()) < 0);
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  // Move inactive iterators that end after parsed.
  while (!inactive_iters_.empty() &&
         icmp_->Compare(parsed, inactive_iters_.top()->end_key()) < 0) {
    TruncatedRangeDelIterator* iter = PopInactiveIter();
    while (iter->Valid() && icmp_->Compare(parsed, iter->start_key()) < 0) {
      iter->Prev();
    }
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  return active_seqnums_.empty()
             ? false
             : (*active_seqnums_.begin())->seq() > parsed.sequence;
}

void ReverseRangeDelIterator::Invalidate() {
  unused_idx_ = 0;
  active_iters_.clear();
  active_seqnums_.clear();
  inactive_iters_.clear();
}

bool RangeDelAggregator::StripeRep::ShouldDelete(
    const ParsedInternalKey& parsed, RangeDelPositioningMode mode) {
  if (!InStripe(parsed.sequence) || IsEmpty()) {
    return false;
  }
  switch (mode) {
    case RangeDelPositioningMode::kForwardTraversal:
      InvalidateReverseIter();

      // Pick up previously unseen iterators.
      for (auto it = std::next(iters_.begin(), forward_iter_.UnusedIdx());
           it != iters_.end(); ++it, forward_iter_.IncUnusedIdx()) {
        auto& iter = *it;
        forward_iter_.AddNewIter(iter.get(), parsed);
      }

      return forward_iter_.ShouldDelete(parsed);
    case RangeDelPositioningMode::kBackwardTraversal:
      InvalidateForwardIter();

      // Pick up previously unseen iterators.
      for (auto it = std::next(iters_.begin(), reverse_iter_.UnusedIdx());
           it != iters_.end(); ++it, reverse_iter_.IncUnusedIdx()) {
        auto& iter = *it;
        reverse_iter_.AddNewIter(iter.get(), parsed);
      }

      return reverse_iter_.ShouldDelete(parsed);
    default:
      assert(false);
      return false;
  }
}

bool RangeDelAggregator::StripeRep::IsRangeOverlapped(const Slice& start,
                                                      const Slice& end) {
  Invalidate();

  // Set the internal start/end keys so that:
  // - if start_ikey has the same user key and sequence number as the
  // current end key, start_ikey will be considered greater; and
  // - if end_ikey has the same user key and sequence number as the current
  // start key, end_ikey will be considered greater.
  ParsedInternalKey start_ikey(start, kMaxSequenceNumber,
                               static_cast<ValueType>(0));
  ParsedInternalKey end_ikey(end, 0, static_cast<ValueType>(0));
  for (auto& iter : iters_) {
    bool checked_candidate_tombstones = false;
    for (iter->SeekForPrev(start);
         iter->Valid() && icmp_->Compare(iter->start_key(), end_ikey) <= 0;
         iter->Next()) {
      checked_candidate_tombstones = true;
      if (icmp_->Compare(start_ikey, iter->end_key()) < 0 &&
          icmp_->Compare(iter->start_key(), end_ikey) <= 0) {
        return true;
      }
    }

    if (!checked_candidate_tombstones) {
      // Do an additional check for when the end of the range is the begin
      // key of a tombstone, which we missed earlier since SeekForPrev'ing
      // to the start was invalid.
      iter->SeekForPrev(end);
      if (iter->Valid() && icmp_->Compare(start_ikey, iter->end_key()) < 0 &&
          icmp_->Compare(iter->start_key(), end_ikey) <= 0) {
        return true;
      }
    }
  }
  return false;
}

void ReadRangeDelAggregator::AddTombstones(
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
    const InternalKey* smallest, const InternalKey* largest) {
  if (input_iter == nullptr || input_iter->empty()) {
    return;
  }
  rep_.AddTombstones(
      std::unique_ptr<TruncatedRangeDelIterator>(new TruncatedRangeDelIterator(
          std::move(input_iter), icmp_, smallest, largest)));
}

bool ReadRangeDelAggregator::ShouldDelete(const ParsedInternalKey& parsed,
                                          RangeDelPositioningMode mode) {
  return rep_.ShouldDelete(parsed, mode);
}

bool ReadRangeDelAggregator::IsRangeOverlapped(const Slice& start,
                                               const Slice& end) {
  InvalidateRangeDelMapPositions();
  return rep_.IsRangeOverlapped(start, end);
}

void CompactionRangeDelAggregator::AddTombstones(
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
    const InternalKey* smallest, const InternalKey* largest) {
  if (input_iter == nullptr || input_iter->empty()) {
    return;
  }
  assert(input_iter->lower_bound() == 0);
  assert(input_iter->upper_bound() == kMaxSequenceNumber);
  parent_iters_.emplace_back(new TruncatedRangeDelIterator(
      std::move(input_iter), icmp_, smallest, largest));

  auto split_iters = parent_iters_.back()->SplitBySnapshot(*snapshots_);
  for (auto& split_iter : split_iters) {
    auto it = reps_.find(split_iter.first);
    if (it == reps_.end()) {
      bool inserted;
      SequenceNumber upper_bound = split_iter.second->upper_bound();
      SequenceNumber lower_bound = split_iter.second->lower_bound();
      std::tie(it, inserted) = reps_.emplace(
          split_iter.first, StripeRep(icmp_, upper_bound, lower_bound));
      assert(inserted);
    }
    assert(it != reps_.end());
    it->second.AddTombstones(std::move(split_iter.second));
  }
}

bool CompactionRangeDelAggregator::ShouldDelete(const ParsedInternalKey& parsed,
                                                RangeDelPositioningMode mode) {
  auto it = reps_.lower_bound(parsed.sequence);
  if (it == reps_.end()) {
    return false;
  }
  return it->second.ShouldDelete(parsed, mode);
}

namespace {

class TruncatedRangeDelMergingIter : public InternalIterator {
 public:
  TruncatedRangeDelMergingIter(
      const InternalKeyComparator* icmp, const Slice* lower_bound,
      const Slice* upper_bound, bool upper_bound_inclusive,
      const std::vector<std::unique_ptr<TruncatedRangeDelIterator>>& children)
      : icmp_(icmp),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound),
        upper_bound_inclusive_(upper_bound_inclusive),
        heap_(StartKeyMinComparator(icmp)) {
    for (auto& child : children) {
      if (child != nullptr) {
        assert(child->lower_bound() == 0);
        assert(child->upper_bound() == kMaxSequenceNumber);
        children_.push_back(child.get());
      }
    }
  }

  bool Valid() const override {
    return !heap_.empty() && BeforeEndKey(heap_.top());
  }
  Status status() const override { return Status::OK(); }

  void SeekToFirst() override {
    heap_.clear();
    for (auto& child : children_) {
      if (lower_bound_ != nullptr) {
        child->Seek(*lower_bound_);
      } else {
        child->SeekToFirst();
      }
      if (child->Valid()) {
        heap_.push(child);
      }
    }
  }

  void Next() override {
    auto* top = heap_.top();
    top->InternalNext();
    if (top->Valid()) {
      heap_.replace_top(top);
    } else {
      heap_.pop();
    }
  }

  Slice key() const override {
    auto* top = heap_.top();
    cur_start_key_.Set(top->start_key().user_key, top->seq(),
                       kTypeRangeDeletion);
    return cur_start_key_.Encode();
  }

  Slice value() const override {
    auto* top = heap_.top();
    assert(top->end_key().sequence == kMaxSequenceNumber);
    return top->end_key().user_key;
  }

  // Unused InternalIterator methods
  void Prev() override { assert(false); }
  void Seek(const Slice& /* target */) override { assert(false); }
  void SeekForPrev(const Slice& /* target */) override { assert(false); }
  void SeekToLast() override { assert(false); }

 private:
  bool BeforeEndKey(const TruncatedRangeDelIterator* iter) const {
    if (upper_bound_ == nullptr) {
      return true;
    }
    int cmp = icmp_->user_comparator()->Compare(iter->start_key().user_key,
                                                *upper_bound_);
    return upper_bound_inclusive_ ? cmp <= 0 : cmp < 0;
  }

  const InternalKeyComparator* icmp_;
  const Slice* lower_bound_;
  const Slice* upper_bound_;
  bool upper_bound_inclusive_;
  BinaryHeap<TruncatedRangeDelIterator*, StartKeyMinComparator> heap_;
  std::vector<TruncatedRangeDelIterator*> children_;

  mutable InternalKey cur_start_key_;
};

}  // namespace

std::unique_ptr<FragmentedRangeTombstoneIterator>
CompactionRangeDelAggregator::NewIterator(const Slice* lower_bound,
                                          const Slice* upper_bound,
                                          bool upper_bound_inclusive) {
  InvalidateRangeDelMapPositions();
  std::unique_ptr<TruncatedRangeDelMergingIter> merging_iter(
      new TruncatedRangeDelMergingIter(icmp_, lower_bound, upper_bound,
                                       upper_bound_inclusive, parent_iters_));

  auto fragmented_tombstone_list =
      std::make_shared<FragmentedRangeTombstoneList>(
          std::move(merging_iter), *icmp_, true /* for_compaction */,
          *snapshots_);

  return std::unique_ptr<FragmentedRangeTombstoneIterator>(
      new FragmentedRangeTombstoneIterator(
          fragmented_tombstone_list, *icmp_,
          kMaxSequenceNumber /* upper_bound */));
}

}  // namespace rocksdb
