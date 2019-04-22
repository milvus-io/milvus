//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_tombstone_fragmenter.h"

#include <algorithm>
#include <functional>
#include <set>

#include <inttypes.h>
#include <stdio.h>

#include "util/autovector.h"
#include "util/kv_map.h"
#include "util/vector_iterator.h"

namespace rocksdb {

FragmentedRangeTombstoneList::FragmentedRangeTombstoneList(
    std::unique_ptr<InternalIterator> unfragmented_tombstones,
    const InternalKeyComparator& icmp, bool for_compaction,
    const std::vector<SequenceNumber>& snapshots) {
  if (unfragmented_tombstones == nullptr) {
    return;
  }
  bool is_sorted = true;
  int num_tombstones = 0;
  InternalKey pinned_last_start_key;
  Slice last_start_key;
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next(), num_tombstones++) {
    if (num_tombstones > 0 &&
        icmp.Compare(last_start_key, unfragmented_tombstones->key()) > 0) {
      is_sorted = false;
      break;
    }
    if (unfragmented_tombstones->IsKeyPinned()) {
      last_start_key = unfragmented_tombstones->key();
    } else {
      pinned_last_start_key.DecodeFrom(unfragmented_tombstones->key());
      last_start_key = pinned_last_start_key.Encode();
    }
  }
  if (is_sorted) {
    FragmentTombstones(std::move(unfragmented_tombstones), icmp, for_compaction,
                       snapshots);
    return;
  }

  // Sort the tombstones before fragmenting them.
  std::vector<std::string> keys, values;
  keys.reserve(num_tombstones);
  values.reserve(num_tombstones);
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next()) {
    keys.emplace_back(unfragmented_tombstones->key().data(),
                      unfragmented_tombstones->key().size());
    values.emplace_back(unfragmented_tombstones->value().data(),
                        unfragmented_tombstones->value().size());
  }
  // VectorIterator implicitly sorts by key during construction.
  auto iter = std::unique_ptr<VectorIterator>(
      new VectorIterator(std::move(keys), std::move(values), &icmp));
  FragmentTombstones(std::move(iter), icmp, for_compaction, snapshots);
}

void FragmentedRangeTombstoneList::FragmentTombstones(
    std::unique_ptr<InternalIterator> unfragmented_tombstones,
    const InternalKeyComparator& icmp, bool for_compaction,
    const std::vector<SequenceNumber>& snapshots) {
  Slice cur_start_key(nullptr, 0);
  auto cmp = ParsedInternalKeyComparator(&icmp);

  // Stores the end keys and sequence numbers of range tombstones with a start
  // key less than or equal to cur_start_key. Provides an ordering by end key
  // for use in flush_current_tombstones.
  std::set<ParsedInternalKey, ParsedInternalKeyComparator> cur_end_keys(cmp);

  // Given the next start key in unfragmented_tombstones,
  // flush_current_tombstones writes every tombstone fragment that starts
  // and ends with a key before next_start_key, and starts with a key greater
  // than or equal to cur_start_key.
  auto flush_current_tombstones = [&](const Slice& next_start_key) {
    auto it = cur_end_keys.begin();
    bool reached_next_start_key = false;
    for (; it != cur_end_keys.end() && !reached_next_start_key; ++it) {
      Slice cur_end_key = it->user_key;
      if (icmp.user_comparator()->Compare(cur_start_key, cur_end_key) == 0) {
        // Empty tombstone.
        continue;
      }
      if (icmp.user_comparator()->Compare(next_start_key, cur_end_key) <= 0) {
        // All of the end keys in [it, cur_end_keys.end()) are after
        // next_start_key, so the tombstones they represent can be used in
        // fragments that start with keys greater than or equal to
        // next_start_key. However, the end keys we already passed will not be
        // used in any more tombstone fragments.
        //
        // Remove the fully fragmented tombstones and stop iteration after a
        // final round of flushing to preserve the tombstones we can create more
        // fragments from.
        reached_next_start_key = true;
        cur_end_keys.erase(cur_end_keys.begin(), it);
        cur_end_key = next_start_key;
      }

      // Flush a range tombstone fragment [cur_start_key, cur_end_key), which
      // should not overlap with the last-flushed tombstone fragment.
      assert(tombstones_.empty() ||
             icmp.user_comparator()->Compare(tombstones_.back().end_key,
                                             cur_start_key) <= 0);

      // Sort the sequence numbers of the tombstones being fragmented in
      // descending order, and then flush them in that order.
      autovector<SequenceNumber> seqnums_to_flush;
      for (auto flush_it = it; flush_it != cur_end_keys.end(); ++flush_it) {
        seqnums_to_flush.push_back(flush_it->sequence);
      }
      std::sort(seqnums_to_flush.begin(), seqnums_to_flush.end(),
                std::greater<SequenceNumber>());

      size_t start_idx = tombstone_seqs_.size();
      size_t end_idx = start_idx + seqnums_to_flush.size();

      if (for_compaction) {
        // Drop all tombstone seqnums that are not preserved by a snapshot.
        SequenceNumber next_snapshot = kMaxSequenceNumber;
        for (auto seq : seqnums_to_flush) {
          if (seq <= next_snapshot) {
            // This seqnum is visible by a lower snapshot.
            tombstone_seqs_.push_back(seq);
            seq_set_.insert(seq);
            auto upper_bound_it =
                std::lower_bound(snapshots.begin(), snapshots.end(), seq);
            if (upper_bound_it == snapshots.begin()) {
              // This seqnum is the topmost one visible by the earliest
              // snapshot. None of the seqnums below it will be visible, so we
              // can skip them.
              break;
            }
            next_snapshot = *std::prev(upper_bound_it);
          }
        }
        end_idx = tombstone_seqs_.size();
      } else {
        // The fragmentation is being done for reads, so preserve all seqnums.
        tombstone_seqs_.insert(tombstone_seqs_.end(), seqnums_to_flush.begin(),
                               seqnums_to_flush.end());
        seq_set_.insert(seqnums_to_flush.begin(), seqnums_to_flush.end());
      }

      assert(start_idx < end_idx);
      tombstones_.emplace_back(cur_start_key, cur_end_key, start_idx, end_idx);

      cur_start_key = cur_end_key;
    }
    if (!reached_next_start_key) {
      // There is a gap between the last flushed tombstone fragment and
      // the next tombstone's start key. Remove all the end keys in
      // the working set, since we have fully fragmented their corresponding
      // tombstones.
      cur_end_keys.clear();
    }
    cur_start_key = next_start_key;
  };

  pinned_iters_mgr_.StartPinning();

  bool no_tombstones = true;
  for (unfragmented_tombstones->SeekToFirst(); unfragmented_tombstones->Valid();
       unfragmented_tombstones->Next()) {
    const Slice& ikey = unfragmented_tombstones->key();
    Slice tombstone_start_key = ExtractUserKey(ikey);
    SequenceNumber tombstone_seq = GetInternalKeySeqno(ikey);
    if (!unfragmented_tombstones->IsKeyPinned()) {
      pinned_slices_.emplace_back(tombstone_start_key.data(),
                                  tombstone_start_key.size());
      tombstone_start_key = pinned_slices_.back();
    }
    no_tombstones = false;

    Slice tombstone_end_key = unfragmented_tombstones->value();
    if (!unfragmented_tombstones->IsValuePinned()) {
      pinned_slices_.emplace_back(tombstone_end_key.data(),
                                  tombstone_end_key.size());
      tombstone_end_key = pinned_slices_.back();
    }
    if (!cur_end_keys.empty() && icmp.user_comparator()->Compare(
                                     cur_start_key, tombstone_start_key) != 0) {
      // The start key has changed. Flush all tombstones that start before
      // this new start key.
      flush_current_tombstones(tombstone_start_key);
    }
    cur_start_key = tombstone_start_key;

    cur_end_keys.emplace(tombstone_end_key, tombstone_seq, kTypeRangeDeletion);
  }
  if (!cur_end_keys.empty()) {
    ParsedInternalKey last_end_key = *std::prev(cur_end_keys.end());
    flush_current_tombstones(last_end_key.user_key);
  }

  if (!no_tombstones) {
    pinned_iters_mgr_.PinIterator(unfragmented_tombstones.release(),
                                  false /* arena */);
  }
}

bool FragmentedRangeTombstoneList::ContainsRange(SequenceNumber lower,
                                                 SequenceNumber upper) const {
  auto seq_it = seq_set_.lower_bound(lower);
  return seq_it != seq_set_.end() && *seq_it <= upper;
}

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    const FragmentedRangeTombstoneList* tombstones,
    const InternalKeyComparator& icmp, SequenceNumber _upper_bound,
    SequenceNumber _lower_bound)
    : tombstone_start_cmp_(icmp.user_comparator()),
      tombstone_end_cmp_(icmp.user_comparator()),
      icmp_(&icmp),
      ucmp_(icmp.user_comparator()),
      tombstones_(tombstones),
      upper_bound_(_upper_bound),
      lower_bound_(_lower_bound) {
  assert(tombstones_ != nullptr);
  Invalidate();
}

FragmentedRangeTombstoneIterator::FragmentedRangeTombstoneIterator(
    const std::shared_ptr<const FragmentedRangeTombstoneList>& tombstones,
    const InternalKeyComparator& icmp, SequenceNumber _upper_bound,
    SequenceNumber _lower_bound)
    : tombstone_start_cmp_(icmp.user_comparator()),
      tombstone_end_cmp_(icmp.user_comparator()),
      icmp_(&icmp),
      ucmp_(icmp.user_comparator()),
      tombstones_ref_(tombstones),
      tombstones_(tombstones_ref_.get()),
      upper_bound_(_upper_bound),
      lower_bound_(_lower_bound) {
  assert(tombstones_ != nullptr);
  Invalidate();
}

void FragmentedRangeTombstoneIterator::SeekToFirst() {
  pos_ = tombstones_->begin();
  seq_pos_ = tombstones_->seq_begin();
}

void FragmentedRangeTombstoneIterator::SeekToTopFirst() {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  pos_ = tombstones_->begin();
  seq_pos_ = std::lower_bound(tombstones_->seq_iter(pos_->seq_start_idx),
                              tombstones_->seq_iter(pos_->seq_end_idx),
                              upper_bound_, std::greater<SequenceNumber>());
  ScanForwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::SeekToLast() {
  pos_ = std::prev(tombstones_->end());
  seq_pos_ = std::prev(tombstones_->seq_end());
}

void FragmentedRangeTombstoneIterator::SeekToTopLast() {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  pos_ = std::prev(tombstones_->end());
  seq_pos_ = std::lower_bound(tombstones_->seq_iter(pos_->seq_start_idx),
                              tombstones_->seq_iter(pos_->seq_end_idx),
                              upper_bound_, std::greater<SequenceNumber>());
  ScanBackwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::Seek(const Slice& target) {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  SeekToCoveringTombstone(target);
  ScanForwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::SeekForPrev(const Slice& target) {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  SeekForPrevToCoveringTombstone(target);
  ScanBackwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::SeekToCoveringTombstone(
    const Slice& target) {
  pos_ = std::upper_bound(tombstones_->begin(), tombstones_->end(), target,
                          tombstone_end_cmp_);
  if (pos_ == tombstones_->end()) {
    // All tombstones end before target.
    seq_pos_ = tombstones_->seq_end();
    return;
  }
  seq_pos_ = std::lower_bound(tombstones_->seq_iter(pos_->seq_start_idx),
                              tombstones_->seq_iter(pos_->seq_end_idx),
                              upper_bound_, std::greater<SequenceNumber>());
}

void FragmentedRangeTombstoneIterator::SeekForPrevToCoveringTombstone(
    const Slice& target) {
  if (tombstones_->empty()) {
    Invalidate();
    return;
  }
  pos_ = std::upper_bound(tombstones_->begin(), tombstones_->end(), target,
                          tombstone_start_cmp_);
  if (pos_ == tombstones_->begin()) {
    // All tombstones start after target.
    Invalidate();
    return;
  }
  --pos_;
  seq_pos_ = std::lower_bound(tombstones_->seq_iter(pos_->seq_start_idx),
                              tombstones_->seq_iter(pos_->seq_end_idx),
                              upper_bound_, std::greater<SequenceNumber>());
}

void FragmentedRangeTombstoneIterator::ScanForwardToVisibleTombstone() {
  while (pos_ != tombstones_->end() &&
         (seq_pos_ == tombstones_->seq_iter(pos_->seq_end_idx) ||
          *seq_pos_ < lower_bound_)) {
    ++pos_;
    if (pos_ == tombstones_->end()) {
      Invalidate();
      return;
    }
    seq_pos_ = std::lower_bound(tombstones_->seq_iter(pos_->seq_start_idx),
                                tombstones_->seq_iter(pos_->seq_end_idx),
                                upper_bound_, std::greater<SequenceNumber>());
  }
}

void FragmentedRangeTombstoneIterator::ScanBackwardToVisibleTombstone() {
  while (pos_ != tombstones_->end() &&
         (seq_pos_ == tombstones_->seq_iter(pos_->seq_end_idx) ||
          *seq_pos_ < lower_bound_)) {
    if (pos_ == tombstones_->begin()) {
      Invalidate();
      return;
    }
    --pos_;
    seq_pos_ = std::lower_bound(tombstones_->seq_iter(pos_->seq_start_idx),
                                tombstones_->seq_iter(pos_->seq_end_idx),
                                upper_bound_, std::greater<SequenceNumber>());
  }
}

void FragmentedRangeTombstoneIterator::Next() {
  ++seq_pos_;
  if (seq_pos_ == tombstones_->seq_iter(pos_->seq_end_idx)) {
    ++pos_;
  }
}

void FragmentedRangeTombstoneIterator::TopNext() {
  ++pos_;
  if (pos_ == tombstones_->end()) {
    return;
  }
  seq_pos_ = std::lower_bound(tombstones_->seq_iter(pos_->seq_start_idx),
                              tombstones_->seq_iter(pos_->seq_end_idx),
                              upper_bound_, std::greater<SequenceNumber>());
  ScanForwardToVisibleTombstone();
}

void FragmentedRangeTombstoneIterator::Prev() {
  if (seq_pos_ == tombstones_->seq_begin()) {
    Invalidate();
    return;
  }
  --seq_pos_;
  if (pos_ == tombstones_->end() ||
      seq_pos_ == tombstones_->seq_iter(pos_->seq_start_idx - 1)) {
    --pos_;
  }
}

void FragmentedRangeTombstoneIterator::TopPrev() {
  if (pos_ == tombstones_->begin()) {
    Invalidate();
    return;
  }
  --pos_;
  seq_pos_ = std::lower_bound(tombstones_->seq_iter(pos_->seq_start_idx),
                              tombstones_->seq_iter(pos_->seq_end_idx),
                              upper_bound_, std::greater<SequenceNumber>());
  ScanBackwardToVisibleTombstone();
}

bool FragmentedRangeTombstoneIterator::Valid() const {
  return tombstones_ != nullptr && pos_ != tombstones_->end();
}

SequenceNumber FragmentedRangeTombstoneIterator::MaxCoveringTombstoneSeqnum(
    const Slice& user_key) {
  SeekToCoveringTombstone(user_key);
  return ValidPos() && ucmp_->Compare(start_key(), user_key) <= 0 ? seq() : 0;
}

std::map<SequenceNumber, std::unique_ptr<FragmentedRangeTombstoneIterator>>
FragmentedRangeTombstoneIterator::SplitBySnapshot(
    const std::vector<SequenceNumber>& snapshots) {
  std::map<SequenceNumber, std::unique_ptr<FragmentedRangeTombstoneIterator>>
      splits;
  SequenceNumber lower = 0;
  SequenceNumber upper;
  for (size_t i = 0; i <= snapshots.size(); i++) {
    if (i >= snapshots.size()) {
      upper = kMaxSequenceNumber;
    } else {
      upper = snapshots[i];
    }
    if (tombstones_->ContainsRange(lower, upper)) {
      splits.emplace(upper, std::unique_ptr<FragmentedRangeTombstoneIterator>(
                                new FragmentedRangeTombstoneIterator(
                                    tombstones_, *icmp_, upper, lower)));
    }
    lower = upper + 1;
  }
  return splits;
}

}  // namespace rocksdb
