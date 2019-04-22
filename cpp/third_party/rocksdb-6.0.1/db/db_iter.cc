//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"
#include <string>
#include <iostream>
#include <limits>

#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "util/arena.h"
#include "util/filename.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "util/trace_replay.h"

namespace rocksdb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter final: public Iterator {
 public:
  // The following is grossly complicated. TODO: clean it up
  // Which direction is the iterator currently moving?
  // (1) When moving forward:
  //   (1a) if current_entry_is_merged_ = false, the internal iterator is
  //        positioned at the exact entry that yields this->key(), this->value()
  //   (1b) if current_entry_is_merged_ = true, the internal iterator is
  //        positioned immediately after the last entry that contributed to the
  //        current this->value(). That entry may or may not have key equal to
  //        this->key().
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction {
    kForward,
    kReverse
  };

  // LocalStatistics contain Statistics counters that will be aggregated per
  // each iterator instance and then will be sent to the global statistics when
  // the iterator is destroyed.
  //
  // The purpose of this approach is to avoid perf regression happening
  // when multiple threads bump the atomic counters from a DBIter::Next().
  struct LocalStatistics {
    explicit LocalStatistics() { ResetCounters(); }

    void ResetCounters() {
      next_count_ = 0;
      next_found_count_ = 0;
      prev_count_ = 0;
      prev_found_count_ = 0;
      bytes_read_ = 0;
      skip_count_ = 0;
    }

    void BumpGlobalStatistics(Statistics* global_statistics) {
      RecordTick(global_statistics, NUMBER_DB_NEXT, next_count_);
      RecordTick(global_statistics, NUMBER_DB_NEXT_FOUND, next_found_count_);
      RecordTick(global_statistics, NUMBER_DB_PREV, prev_count_);
      RecordTick(global_statistics, NUMBER_DB_PREV_FOUND, prev_found_count_);
      RecordTick(global_statistics, ITER_BYTES_READ, bytes_read_);
      RecordTick(global_statistics, NUMBER_ITER_SKIP, skip_count_);
      PERF_COUNTER_ADD(iter_read_bytes, bytes_read_);
      ResetCounters();
    }

    // Map to Tickers::NUMBER_DB_NEXT
    uint64_t next_count_;
    // Map to Tickers::NUMBER_DB_NEXT_FOUND
    uint64_t next_found_count_;
    // Map to Tickers::NUMBER_DB_PREV
    uint64_t prev_count_;
    // Map to Tickers::NUMBER_DB_PREV_FOUND
    uint64_t prev_found_count_;
    // Map to Tickers::ITER_BYTES_READ
    uint64_t bytes_read_;
    // Map to Tickers::NUMBER_ITER_SKIP
    uint64_t skip_count_;
  };

  DBIter(Env* _env, const ReadOptions& read_options,
         const ImmutableCFOptions& cf_options,
         const MutableCFOptions& mutable_cf_options, const Comparator* cmp,
         InternalIterator* iter, SequenceNumber s, bool arena_mode,
         uint64_t max_sequential_skip_in_iterations,
         ReadCallback* read_callback, DBImpl* db_impl, ColumnFamilyData* cfd,
         bool allow_blob)
      : arena_mode_(arena_mode),
        env_(_env),
        logger_(cf_options.info_log),
        user_comparator_(cmp),
        merge_operator_(cf_options.merge_operator),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        current_entry_is_merged_(false),
        statistics_(cf_options.statistics),
        num_internal_keys_skipped_(0),
        iterate_lower_bound_(read_options.iterate_lower_bound),
        iterate_upper_bound_(read_options.iterate_upper_bound),
        prefix_same_as_start_(read_options.prefix_same_as_start),
        pin_thru_lifetime_(read_options.pin_data),
        total_order_seek_(read_options.total_order_seek),
        range_del_agg_(&cf_options.internal_comparator, s),
        read_callback_(read_callback),
        db_impl_(db_impl),
        cfd_(cfd),
        allow_blob_(allow_blob),
        is_blob_(false),
        start_seqnum_(read_options.iter_start_seqnum) {
    RecordTick(statistics_, NO_ITERATOR_CREATED);
    prefix_extractor_ = mutable_cf_options.prefix_extractor.get();
    max_skip_ = max_sequential_skip_in_iterations;
    max_skippable_internal_keys_ = read_options.max_skippable_internal_keys;
    if (pin_thru_lifetime_) {
      pinned_iters_mgr_.StartPinning();
    }
    if (iter_) {
      iter_->SetPinnedItersMgr(&pinned_iters_mgr_);
    }
  }
  ~DBIter() override {
    // Release pinned data if any
    if (pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
    RecordTick(statistics_, NO_ITERATOR_DELETED);
    ResetInternalKeysSkippedCounter();
    local_stats_.BumpGlobalStatistics(statistics_);
    if (!arena_mode_) {
      delete iter_;
    } else {
      iter_->~InternalIterator();
    }
  }
  virtual void SetIter(InternalIterator* iter) {
    assert(iter_ == nullptr);
    iter_ = iter;
    iter_->SetPinnedItersMgr(&pinned_iters_mgr_);
  }
  virtual ReadRangeDelAggregator* GetRangeDelAggregator() {
    return &range_del_agg_;
  }

  bool Valid() const override { return valid_; }
  Slice key() const override {
    assert(valid_);
    if(start_seqnum_ > 0) {
      return saved_key_.GetInternalKey();
    } else {
      return saved_key_.GetUserKey();
    }
  }
  Slice value() const override {
    assert(valid_);
    if (current_entry_is_merged_) {
      // If pinned_value_ is set then the result of merge operator is one of
      // the merge operands and we should return it.
      return pinned_value_.data() ? pinned_value_ : saved_value_;
    } else if (direction_ == kReverse) {
      return pinned_value_;
    } else {
      return iter_->value();
    }
  }
  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      assert(!valid_);
      return status_;
    }
  }
  bool IsBlob() const {
    assert(valid_ && (allow_blob_ || !is_blob_));
    return is_blob_;
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    if (prop == nullptr) {
      return Status::InvalidArgument("prop is nullptr");
    }
    if (prop_name == "rocksdb.iterator.super-version-number") {
      // First try to pass the value returned from inner iterator.
      return iter_->GetProperty(prop_name, prop);
    } else if (prop_name == "rocksdb.iterator.is-key-pinned") {
      if (valid_) {
        *prop = (pin_thru_lifetime_ && saved_key_.IsKeyPinned()) ? "1" : "0";
      } else {
        *prop = "Iterator is not valid.";
      }
      return Status::OK();
    } else if (prop_name == "rocksdb.iterator.internal-key") {
      *prop = saved_key_.GetUserKey().ToString();
      return Status::OK();
    }
    return Status::InvalidArgument("Unidentified property.");
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  Env* env() { return env_; }
  void set_sequence(uint64_t s) { sequence_ = s; }
  void set_valid(bool v) { valid_ = v; }

 private:
  // For all methods in this block:
  // PRE: iter_->Valid() && status_.ok()
  // Return false if there was an error, and status() is non-ok, valid_ = false;
  // in this case callers would usually stop what they were doing and return.
  bool ReverseToForward();
  bool ReverseToBackward();
  bool FindValueForCurrentKey();
  bool FindValueForCurrentKeyUsingSeek();
  bool FindUserKeyBeforeSavedKey();
  inline bool FindNextUserEntry(bool skipping, bool prefix_check);
  bool FindNextUserEntryInternal(bool skipping, bool prefix_check);
  bool ParseKey(ParsedInternalKey* key);
  bool MergeValuesNewToOld();

  void PrevInternal();
  bool TooManyInternalKeysSkipped(bool increment = true);
  bool IsVisible(SequenceNumber sequence);

  // CanReseekToSkip() returns whether the iterator can use the optimization
  // where it reseek by sequence number to get the next key when there are too
  // many versions. This is disabled for write unprepared because seeking to
  // sequence number does not guarantee that it is visible.
  inline bool CanReseekToSkip();

  // MaxVisibleSequenceNumber() returns the maximum visible sequence number
  // for this snapshot. This sequence number may be greater than snapshot
  // seqno because uncommitted data written to DB for write unprepared will
  // have a higher sequence number.
  inline SequenceNumber MaxVisibleSequenceNumber();

  // Temporarily pin the blocks that we encounter until ReleaseTempPinnedData()
  // is called
  void TempPinData() {
    if (!pin_thru_lifetime_) {
      pinned_iters_mgr_.StartPinning();
    }
  }

  // Release blocks pinned by TempPinData()
  void ReleaseTempPinnedData() {
    if (!pin_thru_lifetime_ && pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  inline void ResetInternalKeysSkippedCounter() {
    local_stats_.skip_count_ += num_internal_keys_skipped_;
    if (valid_) {
      local_stats_.skip_count_--;
    }
    num_internal_keys_skipped_ = 0;
  }

  const SliceTransform* prefix_extractor_;
  bool arena_mode_;
  Env* const env_;
  Logger* logger_;
  const Comparator* const user_comparator_;
  const MergeOperator* const merge_operator_;
  InternalIterator* iter_;
  SequenceNumber sequence_;

  Status status_;
  IterKey saved_key_;
  // Reusable internal key data structure. This is only used inside one function
  // and should not be used across functions. Reusing this object can reduce
  // overhead of calling construction of the function if creating it each time.
  ParsedInternalKey ikey_;
  std::string saved_value_;
  Slice pinned_value_;
  Direction direction_;
  bool valid_;
  bool current_entry_is_merged_;
  // for prefix seek mode to support prev()
  Statistics* statistics_;
  uint64_t max_skip_;
  uint64_t max_skippable_internal_keys_;
  uint64_t num_internal_keys_skipped_;
  const Slice* iterate_lower_bound_;
  const Slice* iterate_upper_bound_;
  IterKey prefix_start_buf_;
  Slice prefix_start_key_;
  const bool prefix_same_as_start_;
  // Means that we will pin all data blocks we read as long the Iterator
  // is not deleted, will be true if ReadOptions::pin_data is true
  const bool pin_thru_lifetime_;
  const bool total_order_seek_;
  // List of operands for merge operator.
  MergeContext merge_context_;
  ReadRangeDelAggregator range_del_agg_;
  LocalStatistics local_stats_;
  PinnedIteratorsManager pinned_iters_mgr_;
  ReadCallback* read_callback_;
  DBImpl* db_impl_;
  ColumnFamilyData* cfd_;
  bool allow_blob_;
  bool is_blob_;
  // for diff snapshots we want the lower bound on the seqnum;
  // if this value > 0 iterator will return internal keys
  SequenceNumber start_seqnum_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  if (!ParseInternalKey(iter_->key(), ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    valid_ = false;
    ROCKS_LOG_ERROR(logger_, "corrupted internal key in DBIter: %s",
                    iter_->key().ToString(true).c_str());
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);
  assert(status_.ok());

  // Release temporarily pinned blocks from last operation
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  bool ok = true;
  if (direction_ == kReverse) {
    if (!ReverseToForward()) {
      ok = false;
    }
  } else if (iter_->Valid() && !current_entry_is_merged_) {
    // If the current value is not a merge, the iter position is the
    // current key, which is already returned. We can safely issue a
    // Next() without checking the current key.
    // If the current key is a merge, very likely iter already points
    // to the next internal position.
    iter_->Next();
    PERF_COUNTER_ADD(internal_key_skipped_count, 1);
  }

  if (statistics_ != nullptr) {
    local_stats_.next_count_++;
  }
  if (ok && iter_->Valid()) {
    FindNextUserEntry(true /* skipping the current user key */,
                      prefix_same_as_start_);
  } else {
    valid_ = false;
  }
  if (statistics_ != nullptr && valid_) {
    local_stats_.next_found_count_++;
    local_stats_.bytes_read_ += (key().size() + value().size());
  }
}

// PRE: saved_key_ has the current user key if skipping
// POST: saved_key_ should have the next user key if valid_,
//       if the current entry is a result of merge
//           current_entry_is_merged_ => true
//           saved_value_             => the merged value
//
// NOTE: In between, saved_key_ can point to a user key that has
//       a delete marker or a sequence number higher than sequence_
//       saved_key_ MUST have a proper user_key before calling this function
//
// The prefix_check parameter controls whether we check the iterated
// keys against the prefix of the seeked key. Set to false when
// performing a seek without a key (e.g. SeekToFirst). Set to
// prefix_same_as_start_ for other iterations.
inline bool DBIter::FindNextUserEntry(bool skipping, bool prefix_check) {
  PERF_TIMER_GUARD(find_next_user_entry_time);
  return FindNextUserEntryInternal(skipping, prefix_check);
}

// Actual implementation of DBIter::FindNextUserEntry()
bool DBIter::FindNextUserEntryInternal(bool skipping, bool prefix_check) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(status_.ok());
  assert(direction_ == kForward);
  current_entry_is_merged_ = false;

  // How many times in a row we have skipped an entry with user key less than
  // or equal to saved_key_. We could skip these entries either because
  // sequence numbers were too high or because skipping = true.
  // What saved_key_ contains throughout this method:
  //  - if skipping        : saved_key_ contains the key that we need to skip,
  //                         and we haven't seen any keys greater than that,
  //  - if num_skipped > 0 : saved_key_ contains the key that we have skipped
  //                         num_skipped times, and we haven't seen any keys
  //                         greater than that,
  //  - none of the above  : saved_key_ can contain anything, it doesn't matter.
  uint64_t num_skipped = 0;

  is_blob_ = false;

  do {
    if (!ParseKey(&ikey_)) {
      return false;
    }

    if (iterate_upper_bound_ != nullptr &&
        user_comparator_->Compare(ikey_.user_key, *iterate_upper_bound_) >= 0) {
      break;
    }

    if (prefix_extractor_ && prefix_check &&
        prefix_extractor_->Transform(ikey_.user_key)
                .compare(prefix_start_key_) != 0) {
      break;
    }

    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    if (IsVisible(ikey_.sequence)) {
      if (skipping && user_comparator_->Compare(ikey_.user_key,
                                                saved_key_.GetUserKey()) <= 0) {
        num_skipped++;  // skip this entry
        PERF_COUNTER_ADD(internal_key_skipped_count, 1);
      } else {
        num_skipped = 0;
        switch (ikey_.type) {
          case kTypeDeletion:
          case kTypeSingleDeletion:
            // Arrange to skip all upcoming entries for this key since
            // they are hidden by this deletion.
            // if iterartor specified start_seqnum we
            // 1) return internal key, including the type
            // 2) return ikey only if ikey.seqnum >= start_seqnum_
            // note that if deletion seqnum is < start_seqnum_ we
            // just skip it like in normal iterator.
            if (start_seqnum_ > 0 && ikey_.sequence >= start_seqnum_)  {
              saved_key_.SetInternalKey(ikey_);
              valid_ = true;
              return true;
            } else {
              saved_key_.SetUserKey(
                ikey_.user_key,
                !pin_thru_lifetime_ || !iter_->IsKeyPinned() /* copy */);
              skipping = true;
              PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
            }
            break;
          case kTypeValue:
          case kTypeBlobIndex:
            if (start_seqnum_ > 0) {
              // we are taking incremental snapshot here
              // incremental snapshots aren't supported on DB with range deletes
              assert(!(
                (ikey_.type == kTypeBlobIndex) && (start_seqnum_ > 0)
              ));
              if (ikey_.sequence >= start_seqnum_) {
                saved_key_.SetInternalKey(ikey_);
                valid_ = true;
                return true;
              } else {
                // this key and all previous versions shouldn't be included,
                // skipping
                saved_key_.SetUserKey(ikey_.user_key,
                  !pin_thru_lifetime_ || !iter_->IsKeyPinned() /* copy */);
                skipping = true;
              }
            } else {
              saved_key_.SetUserKey(
                  ikey_.user_key,
                  !pin_thru_lifetime_ || !iter_->IsKeyPinned() /* copy */);
              if (range_del_agg_.ShouldDelete(
                      ikey_, RangeDelPositioningMode::kForwardTraversal)) {
                // Arrange to skip all upcoming entries for this key since
                // they are hidden by this deletion.
                skipping = true;
                num_skipped = 0;
                PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
              } else if (ikey_.type == kTypeBlobIndex) {
                if (!allow_blob_) {
                  ROCKS_LOG_ERROR(logger_, "Encounter unexpected blob index.");
                  status_ = Status::NotSupported(
                      "Encounter unexpected blob index. Please open DB with "
                      "rocksdb::blob_db::BlobDB instead.");
                  valid_ = false;
                  return false;
                }

                is_blob_ = true;
                valid_ = true;
                return true;
              } else {
                valid_ = true;
                return true;
              }
            }
            break;
          case kTypeMerge:
            saved_key_.SetUserKey(
                ikey_.user_key,
                !pin_thru_lifetime_ || !iter_->IsKeyPinned() /* copy */);
            if (range_del_agg_.ShouldDelete(
                    ikey_, RangeDelPositioningMode::kForwardTraversal)) {
              // Arrange to skip all upcoming entries for this key since
              // they are hidden by this deletion.
              skipping = true;
              num_skipped = 0;
              PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
            } else {
              // By now, we are sure the current ikey is going to yield a
              // value
              current_entry_is_merged_ = true;
              valid_ = true;
              return MergeValuesNewToOld();  // Go to a different state machine
            }
            break;
          default:
            assert(false);
            break;
        }
      }
    } else {
      PERF_COUNTER_ADD(internal_recent_skipped_count, 1);

      // This key was inserted after our snapshot was taken.
      // If this happens too many times in a row for the same user key, we want
      // to seek to the target sequence number.
      int cmp =
          user_comparator_->Compare(ikey_.user_key, saved_key_.GetUserKey());
      if (cmp == 0 || (skipping && cmp <= 0)) {
        num_skipped++;
      } else {
        saved_key_.SetUserKey(
            ikey_.user_key,
            !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
        skipping = false;
        num_skipped = 0;
      }
    }

    // If we have sequentially iterated via numerous equal keys, then it's
    // better to seek so that we can avoid too many key comparisons.
    if (num_skipped > max_skip_ && CanReseekToSkip()) {
      num_skipped = 0;
      std::string last_key;
      if (skipping) {
        // We're looking for the next user-key but all we see are the same
        // user-key with decreasing sequence numbers. Fast forward to
        // sequence number 0 and type deletion (the smallest type).
        AppendInternalKey(&last_key, ParsedInternalKey(saved_key_.GetUserKey(),
                                                       0, kTypeDeletion));
        // Don't set skipping = false because we may still see more user-keys
        // equal to saved_key_.
      } else {
        // We saw multiple entries with this user key and sequence numbers
        // higher than sequence_. Fast forward to sequence_.
        // Note that this only covers a case when a higher key was overwritten
        // many times since our snapshot was taken, not the case when a lot of
        // different keys were inserted after our snapshot was taken.
        AppendInternalKey(&last_key,
                          ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                                            kValueTypeForSeek));
      }
      iter_->Seek(last_key);
      RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
    } else {
      iter_->Next();
    }
  } while (iter_->Valid());

  valid_ = false;
  return iter_->status().ok();
}

// Merge values of the same user key starting from the current iter_ position
// Scan from the newer entries to older entries.
// PRE: iter_->key() points to the first merge type entry
//      saved_key_ stores the user key
// POST: saved_value_ has the merged value for the user key
//       iter_ points to the next entry (or invalid)
bool DBIter::MergeValuesNewToOld() {
  if (!merge_operator_) {
    ROCKS_LOG_ERROR(logger_, "Options::merge_operator is null.");
    status_ = Status::InvalidArgument("merge_operator_ must be set.");
    valid_ = false;
    return false;
  }

  // Temporarily pin the blocks that hold merge operands
  TempPinData();
  merge_context_.Clear();
  // Start the merge process by pushing the first operand
  merge_context_.PushOperand(iter_->value(),
                             iter_->IsValuePinned() /* operand_pinned */);
  TEST_SYNC_POINT("DBIter::MergeValuesNewToOld:PushedFirstOperand");

  ParsedInternalKey ikey;
  Status s;
  for (iter_->Next(); iter_->Valid(); iter_->Next()) {
    TEST_SYNC_POINT("DBIter::MergeValuesNewToOld:SteppedToNextOperand");
    if (!ParseKey(&ikey)) {
      return false;
    }

    if (!user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
      // hit the next user key, stop right here
      break;
    } else if (kTypeDeletion == ikey.type || kTypeSingleDeletion == ikey.type ||
               range_del_agg_.ShouldDelete(
                   ikey, RangeDelPositioningMode::kForwardTraversal)) {
      // hit a delete with the same user key, stop right here
      // iter_ is positioned after delete
      iter_->Next();
      break;
    } else if (kTypeValue == ikey.type) {
      // hit a put, merge the put value with operands and store the
      // final result in saved_value_. We are done!
      const Slice val = iter_->value();
      s = MergeHelper::TimedFullMerge(
          merge_operator_, ikey.user_key, &val, merge_context_.GetOperands(),
          &saved_value_, logger_, statistics_, env_, &pinned_value_, true);
      if (!s.ok()) {
        valid_ = false;
        status_ = s;
        return false;
      }
      // iter_ is positioned after put
      iter_->Next();
      if (!iter_->status().ok()) {
        valid_ = false;
        return false;
      }
      return true;
    } else if (kTypeMerge == ikey.type) {
      // hit a merge, add the value as an operand and run associative merge.
      // when complete, add result to operands and continue.
      merge_context_.PushOperand(iter_->value(),
                                 iter_->IsValuePinned() /* operand_pinned */);
      PERF_COUNTER_ADD(internal_merge_count, 1);
    } else if (kTypeBlobIndex == ikey.type) {
      if (!allow_blob_) {
        ROCKS_LOG_ERROR(logger_, "Encounter unexpected blob index.");
        status_ = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "rocksdb::blob_db::BlobDB instead.");
      } else {
        status_ =
            Status::NotSupported("Blob DB does not support merge operator.");
      }
      valid_ = false;
      return false;
    } else {
      assert(false);
    }
  }

  if (!iter_->status().ok()) {
    valid_ = false;
    return false;
  }

  // we either exhausted all internal keys under this user key, or hit
  // a deletion marker.
  // feed null as the existing value to the merge operator, such that
  // client can differentiate this scenario and do things accordingly.
  s = MergeHelper::TimedFullMerge(merge_operator_, saved_key_.GetUserKey(),
                                  nullptr, merge_context_.GetOperands(),
                                  &saved_value_, logger_, statistics_, env_,
                                  &pinned_value_, true);
  if (!s.ok()) {
    valid_ = false;
    status_ = s;
    return false;
  }

  assert(status_.ok());
  return true;
}

void DBIter::Prev() {
  assert(valid_);
  assert(status_.ok());
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  bool ok = true;
  if (direction_ == kForward) {
    if (!ReverseToBackward()) {
      ok = false;
    }
  }
  if (ok) {
    PrevInternal();
  }
  if (statistics_ != nullptr) {
    local_stats_.prev_count_++;
    if (valid_) {
      local_stats_.prev_found_count_++;
      local_stats_.bytes_read_ += (key().size() + value().size());
    }
  }
}

bool DBIter::ReverseToForward() {
  assert(iter_->status().ok());

  // When moving backwards, iter_ is positioned on _previous_ key, which may
  // not exist or may have different prefix than the current key().
  // If that's the case, seek iter_ to current key.
  if ((prefix_extractor_ != nullptr && !total_order_seek_) || !iter_->Valid()) {
    IterKey last_key;
    last_key.SetInternalKey(ParsedInternalKey(
        saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));
    iter_->Seek(last_key.GetInternalKey());
  }

  direction_ = kForward;
  // Skip keys less than the current key() (a.k.a. saved_key_).
  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }
    if (user_comparator_->Compare(ikey.user_key, saved_key_.GetUserKey()) >=
        0) {
      return true;
    }
    iter_->Next();
  }

  if (!iter_->status().ok()) {
    valid_ = false;
    return false;
  }

  return true;
}

// Move iter_ to the key before saved_key_.
bool DBIter::ReverseToBackward() {
  assert(iter_->status().ok());

  // When current_entry_is_merged_ is true, iter_ may be positioned on the next
  // key, which may not exist or may have prefix different from current.
  // If that's the case, seek to saved_key_.
  if (current_entry_is_merged_ &&
      ((prefix_extractor_ != nullptr && !total_order_seek_) ||
       !iter_->Valid())) {
    IterKey last_key;
    // Using kMaxSequenceNumber and kValueTypeForSeek
    // (not kValueTypeForSeekForPrev) to seek to a key strictly smaller
    // than saved_key_.
    last_key.SetInternalKey(ParsedInternalKey(
        saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));
    if (prefix_extractor_ != nullptr && !total_order_seek_) {
      iter_->SeekForPrev(last_key.GetInternalKey());
    } else {
      // Some iterators may not support SeekForPrev(), so we avoid using it
      // when prefix seek mode is disabled. This is somewhat expensive
      // (an extra Prev(), as well as an extra change of direction of iter_),
      // so we may need to reconsider it later.
      iter_->Seek(last_key.GetInternalKey());
      if (!iter_->Valid() && iter_->status().ok()) {
        iter_->SeekToLast();
      }
    }
  }

  direction_ = kReverse;
  return FindUserKeyBeforeSavedKey();
}

void DBIter::PrevInternal() {
  while (iter_->Valid()) {
    saved_key_.SetUserKey(
        ExtractUserKey(iter_->key()),
        !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);

    if (prefix_extractor_ && prefix_same_as_start_ &&
        prefix_extractor_->Transform(saved_key_.GetUserKey())
                .compare(prefix_start_key_) != 0) {
      // Current key does not have the same prefix as start
      valid_ = false;
      return;
    }

    if (iterate_lower_bound_ != nullptr &&
        user_comparator_->Compare(saved_key_.GetUserKey(),
                                  *iterate_lower_bound_) < 0) {
      // We've iterated earlier than the user-specified lower bound.
      valid_ = false;
      return;
    }

    if (!FindValueForCurrentKey()) {  // assigns valid_
      return;
    }

    // Whether or not we found a value for current key, we need iter_ to end up
    // on a smaller key.
    if (!FindUserKeyBeforeSavedKey()) {
      return;
    }

    if (valid_) {
      // Found the value.
      return;
    }

    if (TooManyInternalKeysSkipped(false)) {
      return;
    }
  }

  // We haven't found any key - iterator is not valid
  valid_ = false;
}

// Used for backwards iteration.
// Looks at the entries with user key saved_key_ and finds the most up-to-date
// value for it, or executes a merge, or determines that the value was deleted.
// Sets valid_ to true if the value is found and is ready to be presented to
// the user through value().
// Sets valid_ to false if the value was deleted, and we should try another key.
// Returns false if an error occurred, and !status().ok() and !valid_.
//
// PRE: iter_ is positioned on the last entry with user key equal to saved_key_.
// POST: iter_ is positioned on one of the entries equal to saved_key_, or on
//       the entry just before them, or on the entry just after them.
bool DBIter::FindValueForCurrentKey() {
  assert(iter_->Valid());
  merge_context_.Clear();
  current_entry_is_merged_ = false;
  // last entry before merge (could be kTypeDeletion, kTypeSingleDeletion or
  // kTypeValue)
  ValueType last_not_merge_type = kTypeDeletion;
  ValueType last_key_entry_type = kTypeDeletion;

  // Temporarily pin blocks that hold (merge operands / the value)
  ReleaseTempPinnedData();
  TempPinData();
  size_t num_skipped = 0;
  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }

    if (!IsVisible(ikey.sequence) ||
        !user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
      break;
    }
    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    // This user key has lots of entries.
    // We're going from old to new, and it's taking too long. Let's do a Seek()
    // and go from new to old. This helps when a key was overwritten many times.
    if (num_skipped >= max_skip_ && CanReseekToSkip()) {
      return FindValueForCurrentKeyUsingSeek();
    }

    last_key_entry_type = ikey.type;
    switch (last_key_entry_type) {
      case kTypeValue:
      case kTypeBlobIndex:
        if (range_del_agg_.ShouldDelete(
                ikey, RangeDelPositioningMode::kBackwardTraversal)) {
          last_key_entry_type = kTypeRangeDeletion;
          PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
        } else {
          assert(iter_->IsValuePinned());
          pinned_value_ = iter_->value();
        }
        merge_context_.Clear();
        last_not_merge_type = last_key_entry_type;
        break;
      case kTypeDeletion:
      case kTypeSingleDeletion:
        merge_context_.Clear();
        last_not_merge_type = last_key_entry_type;
        PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
        break;
      case kTypeMerge:
        if (range_del_agg_.ShouldDelete(
                ikey, RangeDelPositioningMode::kBackwardTraversal)) {
          merge_context_.Clear();
          last_key_entry_type = kTypeRangeDeletion;
          last_not_merge_type = last_key_entry_type;
          PERF_COUNTER_ADD(internal_delete_skipped_count, 1);
        } else {
          assert(merge_operator_ != nullptr);
          merge_context_.PushOperandBack(
              iter_->value(), iter_->IsValuePinned() /* operand_pinned */);
          PERF_COUNTER_ADD(internal_merge_count, 1);
        }
        break;
      default:
        assert(false);
    }

    PERF_COUNTER_ADD(internal_key_skipped_count, 1);
    iter_->Prev();
    ++num_skipped;
  }

  if (!iter_->status().ok()) {
    valid_ = false;
    return false;
  }

  Status s;
  is_blob_ = false;
  switch (last_key_entry_type) {
    case kTypeDeletion:
    case kTypeSingleDeletion:
    case kTypeRangeDeletion:
      valid_ = false;
      return true;
    case kTypeMerge:
      current_entry_is_merged_ = true;
      if (last_not_merge_type == kTypeDeletion ||
          last_not_merge_type == kTypeSingleDeletion ||
          last_not_merge_type == kTypeRangeDeletion) {
        s = MergeHelper::TimedFullMerge(
            merge_operator_, saved_key_.GetUserKey(), nullptr,
            merge_context_.GetOperands(), &saved_value_, logger_, statistics_,
            env_, &pinned_value_, true);
      } else if (last_not_merge_type == kTypeBlobIndex) {
        if (!allow_blob_) {
          ROCKS_LOG_ERROR(logger_, "Encounter unexpected blob index.");
          status_ = Status::NotSupported(
              "Encounter unexpected blob index. Please open DB with "
              "rocksdb::blob_db::BlobDB instead.");
        } else {
          status_ =
              Status::NotSupported("Blob DB does not support merge operator.");
        }
        valid_ = false;
        return false;
      } else {
        assert(last_not_merge_type == kTypeValue);
        s = MergeHelper::TimedFullMerge(
            merge_operator_, saved_key_.GetUserKey(), &pinned_value_,
            merge_context_.GetOperands(), &saved_value_, logger_, statistics_,
            env_, &pinned_value_, true);
      }
      break;
    case kTypeValue:
      // do nothing - we've already has value in pinned_value_
      break;
    case kTypeBlobIndex:
      if (!allow_blob_) {
        ROCKS_LOG_ERROR(logger_, "Encounter unexpected blob index.");
        status_ = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "rocksdb::blob_db::BlobDB instead.");
        valid_ = false;
        return false;
      }
      is_blob_ = true;
      break;
    default:
      assert(false);
      break;
  }
  if (!s.ok()) {
    valid_ = false;
    status_ = s;
    return false;
  }
  valid_ = true;
  return true;
}

// This function is used in FindValueForCurrentKey.
// We use Seek() function instead of Prev() to find necessary value
// TODO: This is very similar to FindNextUserEntry() and MergeValuesNewToOld().
//       Would be nice to reuse some code.
bool DBIter::FindValueForCurrentKeyUsingSeek() {
  // FindValueForCurrentKey will enable pinning before calling
  // FindValueForCurrentKeyUsingSeek()
  assert(pinned_iters_mgr_.PinningEnabled());
  std::string last_key;
  AppendInternalKey(&last_key, ParsedInternalKey(saved_key_.GetUserKey(),
                                                 sequence_, kValueTypeForSeek));
  iter_->Seek(last_key);
  RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);

  // In case read_callback presents, the value we seek to may not be visible.
  // Find the next value that's visible.
  ParsedInternalKey ikey;
  while (true) {
    if (!iter_->Valid()) {
      valid_ = false;
      return iter_->status().ok();
    }

    if (!ParseKey(&ikey)) {
      return false;
    }
    if (!user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
      // No visible values for this key, even though FindValueForCurrentKey()
      // has seen some. This is possible if we're using a tailing iterator, and
      // the entries were discarded in a compaction.
      valid_ = false;
      return true;
    }

    if (IsVisible(ikey.sequence)) {
      break;
    }

    iter_->Next();
  }

  if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion ||
      range_del_agg_.ShouldDelete(
          ikey, RangeDelPositioningMode::kBackwardTraversal)) {
    valid_ = false;
    return true;
  }
  if (ikey.type == kTypeBlobIndex && !allow_blob_) {
    ROCKS_LOG_ERROR(logger_, "Encounter unexpected blob index.");
    status_ = Status::NotSupported(
        "Encounter unexpected blob index. Please open DB with "
        "rocksdb::blob_db::BlobDB instead.");
    valid_ = false;
    return false;
  }
  if (ikey.type == kTypeValue || ikey.type == kTypeBlobIndex) {
    assert(iter_->IsValuePinned());
    pinned_value_ = iter_->value();
    valid_ = true;
    return true;
  }

  // kTypeMerge. We need to collect all kTypeMerge values and save them
  // in operands
  assert(ikey.type == kTypeMerge);
  current_entry_is_merged_ = true;
  merge_context_.Clear();
  merge_context_.PushOperand(iter_->value(),
                             iter_->IsValuePinned() /* operand_pinned */);
  while (true) {
    iter_->Next();

    if (!iter_->Valid()) {
      if (!iter_->status().ok()) {
        valid_ = false;
        return false;
      }
      break;
    }
    if (!ParseKey(&ikey)) {
      return false;
    }
    if (!user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
      break;
    }

    if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion ||
        range_del_agg_.ShouldDelete(
            ikey, RangeDelPositioningMode::kForwardTraversal)) {
      break;
    } else if (ikey.type == kTypeValue) {
      const Slice val = iter_->value();
      Status s = MergeHelper::TimedFullMerge(
          merge_operator_, saved_key_.GetUserKey(), &val,
          merge_context_.GetOperands(), &saved_value_, logger_, statistics_,
          env_, &pinned_value_, true);
      if (!s.ok()) {
        valid_ = false;
        status_ = s;
        return false;
      }
      valid_ = true;
      return true;
    } else if (ikey.type == kTypeMerge) {
      merge_context_.PushOperand(iter_->value(),
                                 iter_->IsValuePinned() /* operand_pinned */);
      PERF_COUNTER_ADD(internal_merge_count, 1);
    } else if (ikey.type == kTypeBlobIndex) {
      if (!allow_blob_) {
        ROCKS_LOG_ERROR(logger_, "Encounter unexpected blob index.");
        status_ = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "rocksdb::blob_db::BlobDB instead.");
      } else {
        status_ =
            Status::NotSupported("Blob DB does not support merge operator.");
      }
      valid_ = false;
      return false;
    } else {
      assert(false);
    }
  }

  Status s = MergeHelper::TimedFullMerge(
      merge_operator_, saved_key_.GetUserKey(), nullptr,
      merge_context_.GetOperands(), &saved_value_, logger_, statistics_, env_,
      &pinned_value_, true);
  if (!s.ok()) {
    valid_ = false;
    status_ = s;
    return false;
  }

  // Make sure we leave iter_ in a good state. If it's valid and we don't care
  // about prefixes, that's already good enough. Otherwise it needs to be
  // seeked to the current key.
  if ((prefix_extractor_ != nullptr && !total_order_seek_) || !iter_->Valid()) {
    if (prefix_extractor_ != nullptr && !total_order_seek_) {
      iter_->SeekForPrev(last_key);
    } else {
      iter_->Seek(last_key);
      if (!iter_->Valid() && iter_->status().ok()) {
        iter_->SeekToLast();
      }
    }
    RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
  }

  valid_ = true;
  return true;
}

// Move backwards until the key smaller than saved_key_.
// Changes valid_ only if return value is false.
bool DBIter::FindUserKeyBeforeSavedKey() {
  assert(status_.ok());
  size_t num_skipped = 0;
  while (iter_->Valid()) {
    ParsedInternalKey ikey;
    if (!ParseKey(&ikey)) {
      return false;
    }

    if (user_comparator_->Compare(ikey.user_key, saved_key_.GetUserKey()) < 0) {
      return true;
    }

    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    assert(ikey.sequence != kMaxSequenceNumber);
    if (!IsVisible(ikey.sequence)) {
      PERF_COUNTER_ADD(internal_recent_skipped_count, 1);
    } else {
      PERF_COUNTER_ADD(internal_key_skipped_count, 1);
    }

    if (num_skipped >= max_skip_ && CanReseekToSkip()) {
      num_skipped = 0;
      IterKey last_key;
      last_key.SetInternalKey(ParsedInternalKey(
          saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));
      // It would be more efficient to use SeekForPrev() here, but some
      // iterators may not support it.
      iter_->Seek(last_key.GetInternalKey());
      RecordTick(statistics_, NUMBER_OF_RESEEKS_IN_ITERATION);
      if (!iter_->Valid()) {
        break;
      }
    } else {
      ++num_skipped;
    }

    iter_->Prev();
  }

  if (!iter_->status().ok()) {
    valid_ = false;
    return false;
  }

  return true;
}

bool DBIter::TooManyInternalKeysSkipped(bool increment) {
  if ((max_skippable_internal_keys_ > 0) &&
      (num_internal_keys_skipped_ > max_skippable_internal_keys_)) {
    valid_ = false;
    status_ = Status::Incomplete("Too many internal keys skipped.");
    return true;
  } else if (increment) {
    num_internal_keys_skipped_++;
  }
  return false;
}

bool DBIter::IsVisible(SequenceNumber sequence) {
  return sequence <= MaxVisibleSequenceNumber() &&
         (read_callback_ == nullptr || read_callback_->IsVisible(sequence));
}

bool DBIter::CanReseekToSkip() {
  return read_callback_ == nullptr ||
         read_callback_->MaxUnpreparedSequenceNumber() == 0;
}

SequenceNumber DBIter::MaxVisibleSequenceNumber() {
  if (read_callback_ == nullptr) {
    return sequence_;
  }

  return std::max(sequence_, read_callback_->MaxUnpreparedSequenceNumber());
}

void DBIter::Seek(const Slice& target) {
  StopWatch sw(env_, statistics_, DB_SEEK);
  status_ = Status::OK();
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();

  SequenceNumber seq = MaxVisibleSequenceNumber();
  saved_key_.Clear();
  saved_key_.SetInternalKey(target, seq);

#ifndef ROCKSDB_LITE
  if (db_impl_ != nullptr && cfd_ != nullptr) {
    db_impl_->TraceIteratorSeek(cfd_->GetID(), target);
  }
#endif  // ROCKSDB_LITE

  if (iterate_lower_bound_ != nullptr &&
      user_comparator_->Compare(saved_key_.GetUserKey(),
                                *iterate_lower_bound_) < 0) {
    saved_key_.Clear();
    saved_key_.SetInternalKey(*iterate_lower_bound_, seq);
  }

  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    iter_->Seek(saved_key_.GetInternalKey());
    range_del_agg_.InvalidateRangeDelMapPositions();
  }
  RecordTick(statistics_, NUMBER_DB_SEEK);
  if (iter_->Valid()) {
    if (prefix_extractor_ && prefix_same_as_start_) {
      prefix_start_key_ = prefix_extractor_->Transform(target);
    }
    direction_ = kForward;
    ClearSavedValue();
    FindNextUserEntry(false /* not skipping */, prefix_same_as_start_);
    if (!valid_) {
      prefix_start_key_.clear();
    }
    if (statistics_ != nullptr) {
      if (valid_) {
        // Decrement since we don't want to count this key as skipped
        RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
        RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
        PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
      }
    }
  } else {
    valid_ = false;
  }

  if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
    prefix_start_buf_.SetUserKey(prefix_start_key_);
    prefix_start_key_ = prefix_start_buf_.GetUserKey();
  }
}

void DBIter::SeekForPrev(const Slice& target) {
  StopWatch sw(env_, statistics_, DB_SEEK);
  status_ = Status::OK();
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  saved_key_.Clear();
  // now saved_key is used to store internal key.
  saved_key_.SetInternalKey(target, 0 /* sequence_number */,
                            kValueTypeForSeekForPrev);

  if (iterate_upper_bound_ != nullptr &&
      user_comparator_->Compare(saved_key_.GetUserKey(),
                                *iterate_upper_bound_) >= 0) {
    saved_key_.Clear();
    saved_key_.SetInternalKey(*iterate_upper_bound_, kMaxSequenceNumber);
  }

  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    iter_->SeekForPrev(saved_key_.GetInternalKey());
    range_del_agg_.InvalidateRangeDelMapPositions();
  }

#ifndef ROCKSDB_LITE
  if (db_impl_ != nullptr && cfd_ != nullptr) {
    db_impl_->TraceIteratorSeekForPrev(cfd_->GetID(), target);
  }
#endif  // ROCKSDB_LITE

  RecordTick(statistics_, NUMBER_DB_SEEK);
  if (iter_->Valid()) {
    if (prefix_extractor_ && prefix_same_as_start_) {
      prefix_start_key_ = prefix_extractor_->Transform(target);
    }
    direction_ = kReverse;
    ClearSavedValue();
    PrevInternal();
    if (!valid_) {
      prefix_start_key_.clear();
    }
    if (statistics_ != nullptr) {
      if (valid_) {
        RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
        RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
        PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
      }
    }
  } else {
    valid_ = false;
  }
  if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
    prefix_start_buf_.SetUserKey(prefix_start_key_);
    prefix_start_key_ = prefix_start_buf_.GetUserKey();
  }
}

void DBIter::SeekToFirst() {
  if (iterate_lower_bound_ != nullptr) {
    Seek(*iterate_lower_bound_);
    return;
  }
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (prefix_extractor_ != nullptr && !total_order_seek_) {
    max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  status_ = Status::OK();
  direction_ = kForward;
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  ClearSavedValue();

  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    iter_->SeekToFirst();
    range_del_agg_.InvalidateRangeDelMapPositions();
  }

  RecordTick(statistics_, NUMBER_DB_SEEK);
  if (iter_->Valid()) {
    saved_key_.SetUserKey(
        ExtractUserKey(iter_->key()),
        !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
    FindNextUserEntry(false /* not skipping */, false /* no prefix check */);
    if (statistics_ != nullptr) {
      if (valid_) {
        RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
        RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
        PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
      }
    }
  } else {
    valid_ = false;
  }
  if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
    prefix_start_buf_.SetUserKey(
        prefix_extractor_->Transform(saved_key_.GetUserKey()));
    prefix_start_key_ = prefix_start_buf_.GetUserKey();
  }
}

void DBIter::SeekToLast() {
  if (iterate_upper_bound_ != nullptr) {
    // Seek to last key strictly less than ReadOptions.iterate_upper_bound.
    SeekForPrev(*iterate_upper_bound_);
    if (Valid() && user_comparator_->Equal(*iterate_upper_bound_, key())) {
      ReleaseTempPinnedData();
      PrevInternal();
    }
    return;
  }

  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (prefix_extractor_ != nullptr && !total_order_seek_) {
    max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  status_ = Status::OK();
  direction_ = kReverse;
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  ClearSavedValue();

  {
    PERF_TIMER_GUARD(seek_internal_seek_time);
    iter_->SeekToLast();
    range_del_agg_.InvalidateRangeDelMapPositions();
  }
  PrevInternal();
  if (statistics_ != nullptr) {
    RecordTick(statistics_, NUMBER_DB_SEEK);
    if (valid_) {
      RecordTick(statistics_, NUMBER_DB_SEEK_FOUND);
      RecordTick(statistics_, ITER_BYTES_READ, key().size() + value().size());
      PERF_COUNTER_ADD(iter_read_bytes, key().size() + value().size());
    }
  }
  if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
    prefix_start_buf_.SetUserKey(
        prefix_extractor_->Transform(saved_key_.GetUserKey()));
    prefix_start_key_ = prefix_start_buf_.GetUserKey();
  }
}

Iterator* NewDBIterator(Env* env, const ReadOptions& read_options,
                        const ImmutableCFOptions& cf_options,
                        const MutableCFOptions& mutable_cf_options,
                        const Comparator* user_key_comparator,
                        InternalIterator* internal_iter,
                        const SequenceNumber& sequence,
                        uint64_t max_sequential_skip_in_iterations,
                        ReadCallback* read_callback, DBImpl* db_impl,
                        ColumnFamilyData* cfd, bool allow_blob) {
  DBIter* db_iter = new DBIter(
      env, read_options, cf_options, mutable_cf_options, user_key_comparator,
      internal_iter, sequence, false, max_sequential_skip_in_iterations,
      read_callback, db_impl, cfd, allow_blob);
  return db_iter;
}

ArenaWrappedDBIter::~ArenaWrappedDBIter() { db_iter_->~DBIter(); }

ReadRangeDelAggregator* ArenaWrappedDBIter::GetRangeDelAggregator() {
  return db_iter_->GetRangeDelAggregator();
}

void ArenaWrappedDBIter::SetIterUnderDBIter(InternalIterator* iter) {
  static_cast<DBIter*>(db_iter_)->SetIter(iter);
}

inline bool ArenaWrappedDBIter::Valid() const { return db_iter_->Valid(); }
inline void ArenaWrappedDBIter::SeekToFirst() { db_iter_->SeekToFirst(); }
inline void ArenaWrappedDBIter::SeekToLast() { db_iter_->SeekToLast(); }
inline void ArenaWrappedDBIter::Seek(const Slice& target) {
  db_iter_->Seek(target);
}
inline void ArenaWrappedDBIter::SeekForPrev(const Slice& target) {
  db_iter_->SeekForPrev(target);
}
inline void ArenaWrappedDBIter::Next() { db_iter_->Next(); }
inline void ArenaWrappedDBIter::Prev() { db_iter_->Prev(); }
inline Slice ArenaWrappedDBIter::key() const { return db_iter_->key(); }
inline Slice ArenaWrappedDBIter::value() const { return db_iter_->value(); }
inline Status ArenaWrappedDBIter::status() const { return db_iter_->status(); }
bool ArenaWrappedDBIter::IsBlob() const { return db_iter_->IsBlob(); }
inline Status ArenaWrappedDBIter::GetProperty(std::string prop_name,
                                              std::string* prop) {
  if (prop_name == "rocksdb.iterator.super-version-number") {
    // First try to pass the value returned from inner iterator.
    if (!db_iter_->GetProperty(prop_name, prop).ok()) {
      *prop = ToString(sv_number_);
    }
    return Status::OK();
  }
  return db_iter_->GetProperty(prop_name, prop);
}

void ArenaWrappedDBIter::Init(Env* env, const ReadOptions& read_options,
                              const ImmutableCFOptions& cf_options,
                              const MutableCFOptions& mutable_cf_options,
                              const SequenceNumber& sequence,
                              uint64_t max_sequential_skip_in_iteration,
                              uint64_t version_number,
                              ReadCallback* read_callback, DBImpl* db_impl,
                              ColumnFamilyData* cfd, bool allow_blob,
                              bool allow_refresh) {
  auto mem = arena_.AllocateAligned(sizeof(DBIter));
  db_iter_ = new (mem) DBIter(env, read_options, cf_options, mutable_cf_options,
                              cf_options.user_comparator, nullptr, sequence,
                              true, max_sequential_skip_in_iteration,
                              read_callback, db_impl, cfd, allow_blob);
  sv_number_ = version_number;
  allow_refresh_ = allow_refresh;
}

Status ArenaWrappedDBIter::Refresh() {
  if (cfd_ == nullptr || db_impl_ == nullptr || !allow_refresh_) {
    return Status::NotSupported("Creating renew iterator is not allowed.");
  }
  assert(db_iter_ != nullptr);
  // TODO(yiwu): For last_seq_same_as_publish_seq_==false, this is not the
  // correct behavior. Will be corrected automatically when we take a snapshot
  // here for the case of WritePreparedTxnDB.
  SequenceNumber latest_seq = db_impl_->GetLatestSequenceNumber();
  uint64_t cur_sv_number = cfd_->GetSuperVersionNumber();
  if (sv_number_ != cur_sv_number) {
    Env* env = db_iter_->env();
    db_iter_->~DBIter();
    arena_.~Arena();
    new (&arena_) Arena();

    SuperVersion* sv = cfd_->GetReferencedSuperVersion(db_impl_->mutex());
    Init(env, read_options_, *(cfd_->ioptions()), sv->mutable_cf_options,
         latest_seq, sv->mutable_cf_options.max_sequential_skip_in_iterations,
         cur_sv_number, read_callback_, db_impl_, cfd_, allow_blob_,
         allow_refresh_);

    InternalIterator* internal_iter = db_impl_->NewInternalIterator(
        read_options_, cfd_, sv, &arena_, db_iter_->GetRangeDelAggregator(),
        latest_seq);
    SetIterUnderDBIter(internal_iter);
  } else {
    db_iter_->set_sequence(latest_seq);
    db_iter_->set_valid(false);
  }
  return Status::OK();
}

ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options,
    const ImmutableCFOptions& cf_options,
    const MutableCFOptions& mutable_cf_options, const SequenceNumber& sequence,
    uint64_t max_sequential_skip_in_iterations, uint64_t version_number,
    ReadCallback* read_callback, DBImpl* db_impl, ColumnFamilyData* cfd,
    bool allow_blob, bool allow_refresh) {
  ArenaWrappedDBIter* iter = new ArenaWrappedDBIter();
  iter->Init(env, read_options, cf_options, mutable_cf_options, sequence,
             max_sequential_skip_in_iterations, version_number, read_callback,
             db_impl, cfd, allow_blob, allow_refresh);
  if (db_impl != nullptr && cfd != nullptr && allow_refresh) {
    iter->StoreRefreshInfo(read_options, db_impl, cfd, read_callback,
                           allow_blob);
  }

  return iter;
}

}  // namespace rocksdb
