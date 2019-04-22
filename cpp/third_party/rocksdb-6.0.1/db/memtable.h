//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/read_callback.h"
#include "db/version_edit.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "util/allocator.h"
#include "util/concurrent_arena.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"

namespace rocksdb {

class Mutex;
class MemTableIterator;
class MergeContext;

struct ImmutableMemTableOptions {
  explicit ImmutableMemTableOptions(const ImmutableCFOptions& ioptions,
                                    const MutableCFOptions& mutable_cf_options);
  size_t arena_block_size;
  uint32_t memtable_prefix_bloom_bits;
  size_t memtable_huge_page_size;
  bool memtable_whole_key_filtering;
  bool inplace_update_support;
  size_t inplace_update_num_locks;
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);
  size_t max_successive_merges;
  Statistics* statistics;
  MergeOperator* merge_operator;
  Logger* info_log;
};

// Batched counters to updated when inserting keys in one write batch.
// In post process of the write batch, these can be updated together.
// Only used in concurrent memtable insert case.
struct MemTablePostProcessInfo {
  uint64_t data_size = 0;
  uint64_t num_entries = 0;
  uint64_t num_deletes = 0;
};

// Note:  Many of the methods in this class have comments indicating that
// external synchronization is required as these methods are not thread-safe.
// It is up to higher layers of code to decide how to prevent concurrent
// invokation of these methods.  This is usually done by acquiring either
// the db mutex or the single writer thread.
//
// Some of these methods are documented to only require external
// synchronization if this memtable is immutable.  Calling MarkImmutable() is
// not sufficient to guarantee immutability.  It is up to higher layers of
// code to determine if this MemTable can still be modified by other threads.
// Eg: The Superversion stores a pointer to the current MemTable (that can
// be modified) and a separate list of the MemTables that can no longer be
// written to (aka the 'immutable memtables').
class MemTable {
 public:
  struct KeyComparator : public MemTableRep::KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) { }
    virtual int operator()(const char* prefix_len_key1,
                           const char* prefix_len_key2) const override;
    virtual int operator()(const char* prefix_len_key,
                           const DecodedType& key) const override;
  };

  // MemTables are reference counted.  The initial reference count
  // is zero and the caller must call Ref() at least once.
  //
  // earliest_seq should be the current SequenceNumber in the db such that any
  // key inserted into this memtable will have an equal or larger seq number.
  // (When a db is first created, the earliest sequence number will be 0).
  // If the earliest sequence number is not known, kMaxSequenceNumber may be
  // used, but this may prevent some transactions from succeeding until the
  // first key is inserted into the memtable.
  explicit MemTable(const InternalKeyComparator& comparator,
                    const ImmutableCFOptions& ioptions,
                    const MutableCFOptions& mutable_cf_options,
                    WriteBufferManager* write_buffer_manager,
                    SequenceNumber earliest_seq, uint32_t column_family_id);

  // Do not delete this MemTable unless Unref() indicates it not in use.
  ~MemTable();

  // Increase reference count.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void Ref() { ++refs_; }

  // Drop reference count.
  // If the refcount goes to zero return this memtable, otherwise return null.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  MemTable* Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      return this;
    }
    return nullptr;
  }

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  size_t ApproximateMemoryUsage();

  // This method heuristically determines if the memtable should continue to
  // host more data.
  bool ShouldScheduleFlush() const {
    return flush_state_.load(std::memory_order_relaxed) == FLUSH_REQUESTED;
  }

  // Returns true if a flush should be scheduled and the caller should
  // be the one to schedule it
  bool MarkFlushScheduled() {
    auto before = FLUSH_REQUESTED;
    return flush_state_.compare_exchange_strong(before, FLUSH_SCHEDULED,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed);
  }

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/dbformat.{h,cc} module.
  //
  // By default, it returns an iterator for prefix seek if prefix_extractor
  // is configured in Options.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        Calling ~Iterator of the iterator will destroy all the states but
  //        those allocated in arena.
  InternalIterator* NewIterator(const ReadOptions& read_options, Arena* arena);

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options, SequenceNumber read_seq);

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically value will be empty if type==kTypeDeletion.
  //
  // REQUIRES: if allow_concurrent = false, external synchronization to prevent
  // simultaneous operations on the same MemTable.
  //
  // Returns false if MemTableRepFactory::CanHandleDuplicatedKey() is true and
  // the <key, seq> already exists.
  bool Add(SequenceNumber seq, ValueType type, const Slice& key,
           const Slice& value, bool allow_concurrent = false,
           MemTablePostProcessInfo* post_process_info = nullptr);

  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store a NotFound() error
  // in *status and return true.
  // If memtable contains Merge operation as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operand to *operands.
  //   store MergeInProgress in s, and return false.
  // Else, return false.
  // If any operation was found, its most recent sequence number
  // will be stored in *seq on success (regardless of whether true/false is
  // returned).  Otherwise, *seq will be set to kMaxSequenceNumber.
  // On success, *s may be set to OK, NotFound, or MergeInProgress.  Any other
  // status returned indicates a corruption or other unexpected error.
  bool Get(const LookupKey& key, std::string* value, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
           const ReadOptions& read_opts, ReadCallback* callback = nullptr,
           bool* is_blob_index = nullptr);

  bool Get(const LookupKey& key, std::string* value, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           const ReadOptions& read_opts, ReadCallback* callback = nullptr,
           bool* is_blob_index = nullptr) {
    SequenceNumber seq;
    return Get(key, value, s, merge_context, max_covering_tombstone_seq, &seq,
               read_opts, callback, is_blob_index);
  }

  // Attempts to update the new_value inplace, else does normal Add
  // Pseudocode
  //   if key exists in current memtable && prev_value is of type kTypeValue
  //     if new sizeof(new_value) <= sizeof(prev_value)
  //       update inplace
  //     else add(key, new_value)
  //   else add(key, new_value)
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void Update(SequenceNumber seq,
              const Slice& key,
              const Slice& value);

  // If prev_value for key exists, attempts to update it inplace.
  // else returns false
  // Pseudocode
  //   if key exists in current memtable && prev_value is of type kTypeValue
  //     new_value = delta(prev_value)
  //     if sizeof(new_value) <= sizeof(prev_value)
  //       update inplace
  //     else add(key, new_value)
  //   else return false
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  bool UpdateCallback(SequenceNumber seq,
                      const Slice& key,
                      const Slice& delta);

  // Returns the number of successive merge entries starting from the newest
  // entry for the key up to the last non-merge entry or last entry for the
  // key in the memtable.
  size_t CountSuccessiveMergeEntries(const LookupKey& key);

  // Update counters and flush status after inserting a whole write batch
  // Used in concurrent memtable inserts.
  void BatchPostProcess(const MemTablePostProcessInfo& update_counters) {
    num_entries_.fetch_add(update_counters.num_entries,
                           std::memory_order_relaxed);
    data_size_.fetch_add(update_counters.data_size, std::memory_order_relaxed);
    if (update_counters.num_deletes != 0) {
      num_deletes_.fetch_add(update_counters.num_deletes,
                             std::memory_order_relaxed);
    }
    UpdateFlushState();
  }

  // Get total number of entries in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  uint64_t num_entries() const {
    return num_entries_.load(std::memory_order_relaxed);
  }

  // Get total number of deletes in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  uint64_t num_deletes() const {
    return num_deletes_.load(std::memory_order_relaxed);
  }

  uint64_t get_data_size() const {
    return data_size_.load(std::memory_order_relaxed);
  }

  // Dynamically change the memtable's capacity. If set below the current usage,
  // the next key added will trigger a flush. Can only increase size when
  // memtable prefix bloom is disabled, since we can't easily allocate more
  // space.
  void UpdateWriteBufferSize(size_t new_write_buffer_size) {
    if (bloom_filter_ == nullptr ||
        new_write_buffer_size < write_buffer_size_) {
      write_buffer_size_.store(new_write_buffer_size,
                               std::memory_order_relaxed);
    }
  }

  // Returns the edits area that is needed for flushing the memtable
  VersionEdit* GetEdits() { return &edit_; }

  // Returns if there is no entry inserted to the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  bool IsEmpty() const { return first_seqno_ == 0; }

  // Returns the sequence number of the first element that was inserted
  // into the memtable.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  SequenceNumber GetFirstSequenceNumber() {
    return first_seqno_.load(std::memory_order_relaxed);
  }

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  SequenceNumber GetEarliestSequenceNumber() {
    return earliest_seqno_.load(std::memory_order_relaxed);
  }

  // DB's latest sequence ID when the memtable is created. This number
  // may be updated to a more recent one before any key is inserted.
  SequenceNumber GetCreationSeq() const { return creation_seq_; }

  void SetCreationSeq(SequenceNumber sn) { creation_seq_ = sn; }

  // Returns the next active logfile number when this memtable is about to
  // be flushed to storage
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  uint64_t GetNextLogNumber() { return mem_next_logfile_number_; }

  // Sets the next active logfile number when this memtable is about to
  // be flushed to storage
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void SetNextLogNumber(uint64_t num) { mem_next_logfile_number_ = num; }

  // if this memtable contains data from a committed
  // two phase transaction we must take note of the
  // log which contains that data so we can know
  // when to relese that log
  void RefLogContainingPrepSection(uint64_t log);
  uint64_t GetMinLogContainingPrepSection();

  // Notify the underlying storage that no more items will be added.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  // After MarkImmutable() is called, you should not attempt to
  // write anything to this MemTable().  (Ie. do not call Add() or Update()).
  void MarkImmutable() {
    table_->MarkReadOnly();
    mem_tracker_.DoneAllocating();
  }

  // Notify the underlying storage that all data it contained has been
  // persisted.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void MarkFlushed() {
    table_->MarkFlushed();
  }

  // return true if the current MemTableRep supports merge operator.
  bool IsMergeOperatorSupported() const {
    return table_->IsMergeOperatorSupported();
  }

  // return true if the current MemTableRep supports snapshots.
  // inplace update prevents snapshots,
  bool IsSnapshotSupported() const {
    return table_->IsSnapshotSupported() && !moptions_.inplace_update_support;
  }

  struct MemTableStats {
    uint64_t size;
    uint64_t count;
  };

  MemTableStats ApproximateStats(const Slice& start_ikey,
                                 const Slice& end_ikey);

  // Get the lock associated for the key
  port::RWMutex* GetLock(const Slice& key);

  const InternalKeyComparator& GetInternalKeyComparator() const {
    return comparator_.comparator;
  }

  const ImmutableMemTableOptions* GetImmutableMemTableOptions() const {
    return &moptions_;
  }

  uint64_t ApproximateOldestKeyTime() const {
    return oldest_key_time_.load(std::memory_order_relaxed);
  }

  // REQUIRES: db_mutex held.
  void SetID(uint64_t id) { id_ = id; }

  uint64_t GetID() const { return id_; }

  void SetFlushCompleted(bool completed) { flush_completed_ = completed; }

  uint64_t GetFileNumber() const { return file_number_; }

  void SetFileNumber(uint64_t file_num) { file_number_ = file_num; }

  void SetFlushInProgress(bool in_progress) {
    flush_in_progress_ = in_progress;
  }

 private:
  enum FlushStateEnum { FLUSH_NOT_REQUESTED, FLUSH_REQUESTED, FLUSH_SCHEDULED };

  friend class MemTableIterator;
  friend class MemTableBackwardIterator;
  friend class MemTableList;

  KeyComparator comparator_;
  const ImmutableMemTableOptions moptions_;
  int refs_;
  const size_t kArenaBlockSize;
  AllocTracker mem_tracker_;
  ConcurrentArena arena_;
  std::unique_ptr<MemTableRep> table_;
  std::unique_ptr<MemTableRep> range_del_table_;
  std::atomic_bool is_range_del_table_empty_;

  // Total data size of all data inserted
  std::atomic<uint64_t> data_size_;
  std::atomic<uint64_t> num_entries_;
  std::atomic<uint64_t> num_deletes_;

  // Dynamically changeable memtable option
  std::atomic<size_t> write_buffer_size_;

  // These are used to manage memtable flushes to storage
  bool flush_in_progress_; // started the flush
  bool flush_completed_;   // finished the flush
  uint64_t file_number_;    // filled up after flush is complete

  // The updates to be applied to the transaction log when this
  // memtable is flushed to storage.
  VersionEdit edit_;

  // The sequence number of the kv that was inserted first
  std::atomic<SequenceNumber> first_seqno_;

  // The db sequence number at the time of creation or kMaxSequenceNumber
  // if not set.
  std::atomic<SequenceNumber> earliest_seqno_;

  SequenceNumber creation_seq_;

  // The log files earlier than this number can be deleted.
  uint64_t mem_next_logfile_number_;

  // the earliest log containing a prepared section
  // which has been inserted into this memtable.
  std::atomic<uint64_t> min_prep_log_referenced_;

  // rw locks for inplace updates
  std::vector<port::RWMutex> locks_;

  const SliceTransform* const prefix_extractor_;
  std::unique_ptr<DynamicBloom> bloom_filter_;

  std::atomic<FlushStateEnum> flush_state_;

  Env* env_;

  // Extract sequential insert prefixes.
  const SliceTransform* insert_with_hint_prefix_extractor_;

  // Insert hints for each prefix.
  std::unordered_map<Slice, void*, SliceHasher> insert_hints_;

  // Timestamp of oldest key
  std::atomic<uint64_t> oldest_key_time_;

  // Memtable id to track flush.
  uint64_t id_ = 0;

  // Sequence number of the atomic flush that is responsible for this memtable.
  // The sequence number of atomic flush is a seq, such that no writes with
  // sequence numbers greater than or equal to seq are flushed, while all
  // writes with sequence number smaller than seq are flushed.
  SequenceNumber atomic_flush_seqno_;

  // Returns a heuristic flush decision
  bool ShouldFlushNow() const;

  // Updates flush_state_ using ShouldFlushNow()
  void UpdateFlushState();

  void UpdateOldestKeyTime();

  // No copying allowed
  MemTable(const MemTable&);
  MemTable& operator=(const MemTable&);
};

extern const char* EncodeKey(std::string* scratch, const Slice& target);

}  // namespace rocksdb
