//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <deque>
#include <limits>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/logs_with_prep_tracker.h"
#include "db/memtable.h"
#include "db/range_del_aggregator.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "util/autovector.h"
#include "util/filename.h"
#include "util/log_buffer.h"

namespace rocksdb {

class ColumnFamilyData;
class InternalKeyComparator;
class InstrumentedMutex;
class MergeIteratorBuilder;
class MemTableList;

// keeps a list of immutable memtables in a vector. the list is immutable
// if refcount is bigger than one. It is used as a state for Get() and
// Iterator code paths
//
// This class is not thread-safe.  External synchronization is required
// (such as holding the db mutex or being on the write thread).
class MemTableListVersion {
 public:
  explicit MemTableListVersion(size_t* parent_memtable_list_memory_usage,
                               MemTableListVersion* old = nullptr);
  explicit MemTableListVersion(size_t* parent_memtable_list_memory_usage,
                               int max_write_buffer_number_to_maintain);

  void Ref();
  void Unref(autovector<MemTable*>* to_delete = nullptr);

  // Search all the memtables starting from the most recent one.
  // Return the most recent value found, if any.
  //
  // If any operation was found for this key, its most recent sequence number
  // will be stored in *seq on success (regardless of whether true/false is
  // returned).  Otherwise, *seq will be set to kMaxSequenceNumber.
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

  // Similar to Get(), but searches the Memtable history of memtables that
  // have already been flushed.  Should only be used from in-memory only
  // queries (such as Transaction validation) as the history may contain
  // writes that are also present in the SST files.
  bool GetFromHistory(const LookupKey& key, std::string* value, Status* s,
                      MergeContext* merge_context,
                      SequenceNumber* max_covering_tombstone_seq,
                      SequenceNumber* seq, const ReadOptions& read_opts,
                      bool* is_blob_index = nullptr);
  bool GetFromHistory(const LookupKey& key, std::string* value, Status* s,
                      MergeContext* merge_context,
                      SequenceNumber* max_covering_tombstone_seq,
                      const ReadOptions& read_opts,
                      bool* is_blob_index = nullptr) {
    SequenceNumber seq;
    return GetFromHistory(key, value, s, merge_context,
                          max_covering_tombstone_seq, &seq, read_opts,
                          is_blob_index);
  }

  Status AddRangeTombstoneIterators(const ReadOptions& read_opts, Arena* arena,
                                    RangeDelAggregator* range_del_agg);

  void AddIterators(const ReadOptions& options,
                    std::vector<InternalIterator*>* iterator_list,
                    Arena* arena);

  void AddIterators(const ReadOptions& options,
                    MergeIteratorBuilder* merge_iter_builder);

  uint64_t GetTotalNumEntries() const;

  uint64_t GetTotalNumDeletes() const;

  MemTable::MemTableStats ApproximateStats(const Slice& start_ikey,
                                           const Slice& end_ikey);

  // Returns the value of MemTable::GetEarliestSequenceNumber() on the most
  // recent MemTable in this list or kMaxSequenceNumber if the list is empty.
  // If include_history=true, will also search Memtables in MemTableList
  // History.
  SequenceNumber GetEarliestSequenceNumber(bool include_history = false) const;

 private:
  friend class MemTableList;

  friend Status InstallMemtableAtomicFlushResults(
      const autovector<MemTableList*>* imm_lists,
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const MutableCFOptions*>& mutable_cf_options_list,
      const autovector<const autovector<MemTable*>*>& mems_list,
      VersionSet* vset, InstrumentedMutex* mu,
      const autovector<FileMetaData*>& file_meta,
      autovector<MemTable*>* to_delete, Directory* db_directory,
      LogBuffer* log_buffer);

  // REQUIRE: m is an immutable memtable
  void Add(MemTable* m, autovector<MemTable*>* to_delete);
  // REQUIRE: m is an immutable memtable
  void Remove(MemTable* m, autovector<MemTable*>* to_delete);

  void TrimHistory(autovector<MemTable*>* to_delete);

  bool GetFromList(std::list<MemTable*>* list, const LookupKey& key,
                   std::string* value, Status* s, MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   SequenceNumber* seq, const ReadOptions& read_opts,
                   ReadCallback* callback = nullptr,
                   bool* is_blob_index = nullptr);

  void AddMemTable(MemTable* m);

  void UnrefMemTable(autovector<MemTable*>* to_delete, MemTable* m);

  // Immutable MemTables that have not yet been flushed.
  std::list<MemTable*> memlist_;

  // MemTables that have already been flushed
  // (used during Transaction validation)
  std::list<MemTable*> memlist_history_;

  // Maximum number of MemTables to keep in memory (including both flushed
  // and not-yet-flushed tables).
  const int max_write_buffer_number_to_maintain_;

  int refs_ = 0;

  size_t* parent_memtable_list_memory_usage_;
};

// This class stores references to all the immutable memtables.
// The memtables are flushed to L0 as soon as possible and in
// any order. If there are more than one immutable memtable, their
// flushes can occur concurrently.  However, they are 'committed'
// to the manifest in FIFO order to maintain correctness and
// recoverability from a crash.
//
//
// Other than imm_flush_needed, this class is not thread-safe and requires
// external synchronization (such as holding the db mutex or being on the
// write thread.)
class MemTableList {
 public:
  // A list of memtables.
  explicit MemTableList(int min_write_buffer_number_to_merge,
                        int max_write_buffer_number_to_maintain)
      : imm_flush_needed(false),
        min_write_buffer_number_to_merge_(min_write_buffer_number_to_merge),
        current_(new MemTableListVersion(&current_memory_usage_,
                                         max_write_buffer_number_to_maintain)),
        num_flush_not_started_(0),
        commit_in_progress_(false),
        flush_requested_(false) {
    current_->Ref();
    current_memory_usage_ = 0;
  }

  // Should not delete MemTableList without making sure MemTableList::current()
  // is Unref()'d.
  ~MemTableList() {}

  MemTableListVersion* current() { return current_; }

  // so that background threads can detect non-nullptr pointer to
  // determine whether there is anything more to start flushing.
  std::atomic<bool> imm_flush_needed;

  // Returns the total number of memtables in the list that haven't yet
  // been flushed and logged.
  int NumNotFlushed() const;

  // Returns total number of memtables in the list that have been
  // completely flushed and logged.
  int NumFlushed() const;

  // Returns true if there is at least one memtable on which flush has
  // not yet started.
  bool IsFlushPending() const;

  // Returns the earliest memtables that needs to be flushed. The returned
  // memtables are guaranteed to be in the ascending order of created time.
  void PickMemtablesToFlush(const uint64_t* max_memtable_id,
                            autovector<MemTable*>* mems);

  // Reset status of the given memtable list back to pending state so that
  // they can get picked up again on the next round of flush.
  void RollbackMemtableFlush(const autovector<MemTable*>& mems,
                             uint64_t file_number);

  // Try commit a successful flush in the manifest file. It might just return
  // Status::OK letting a concurrent flush to do the actual the recording.
  Status TryInstallMemtableFlushResults(
      ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
      const autovector<MemTable*>& m, LogsWithPrepTracker* prep_tracker,
      VersionSet* vset, InstrumentedMutex* mu, uint64_t file_number,
      autovector<MemTable*>* to_delete, Directory* db_directory,
      LogBuffer* log_buffer);

  // New memtables are inserted at the front of the list.
  // Takes ownership of the referenced held on *m by the caller of Add().
  void Add(MemTable* m, autovector<MemTable*>* to_delete);

  // Returns an estimate of the number of bytes of data in use.
  size_t ApproximateMemoryUsage();

  // Returns an estimate of the number of bytes of data used by
  // the unflushed mem-tables.
  size_t ApproximateUnflushedMemTablesMemoryUsage();

  // Returns an estimate of the timestamp of the earliest key.
  uint64_t ApproximateOldestKeyTime() const;

  // Request a flush of all existing memtables to storage.  This will
  // cause future calls to IsFlushPending() to return true if this list is
  // non-empty (regardless of the min_write_buffer_number_to_merge
  // parameter). This flush request will persist until the next time
  // PickMemtablesToFlush() is called.
  void FlushRequested() { flush_requested_ = true; }

  bool HasFlushRequested() { return flush_requested_; }

  // Copying allowed
  // MemTableList(const MemTableList&);
  // void operator=(const MemTableList&);

  size_t* current_memory_usage() { return &current_memory_usage_; }

  // Returns the min log containing the prep section after memtables listsed in
  // `memtables_to_flush` are flushed and their status is persisted in manifest.
  uint64_t PrecomputeMinLogContainingPrepSection(
      const autovector<MemTable*>& memtables_to_flush);

  uint64_t GetEarliestMemTableID() const {
    auto& memlist = current_->memlist_;
    if (memlist.empty()) {
      return std::numeric_limits<uint64_t>::max();
    }
    return memlist.back()->GetID();
  }

  uint64_t GetLatestMemTableID() const {
    auto& memlist = current_->memlist_;
    if (memlist.empty()) {
      return 0;
    }
    return memlist.front()->GetID();
  }

  void AssignAtomicFlushSeq(const SequenceNumber& seq) {
    const auto& memlist = current_->memlist_;
    // Scan the memtable list from new to old
    for (auto it = memlist.begin(); it != memlist.end(); ++it) {
      MemTable* mem = *it;
      if (mem->atomic_flush_seqno_ == kMaxSequenceNumber) {
        mem->atomic_flush_seqno_ = seq;
      } else {
        // Earlier memtables must have been assigned a atomic flush seq, no
        // need to continue scan.
        break;
      }
    }
  }

 private:
  friend Status InstallMemtableAtomicFlushResults(
      const autovector<MemTableList*>* imm_lists,
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const MutableCFOptions*>& mutable_cf_options_list,
      const autovector<const autovector<MemTable*>*>& mems_list,
      VersionSet* vset, InstrumentedMutex* mu,
      const autovector<FileMetaData*>& file_meta,
      autovector<MemTable*>* to_delete, Directory* db_directory,
      LogBuffer* log_buffer);

  // DB mutex held
  void InstallNewVersion();

  const int min_write_buffer_number_to_merge_;

  MemTableListVersion* current_;

  // the number of elements that still need flushing
  int num_flush_not_started_;

  // committing in progress
  bool commit_in_progress_;

  // Requested a flush of memtables to storage. It's possible to request that
  // a subset of memtables be flushed.
  bool flush_requested_;

  // The current memory usage.
  size_t current_memory_usage_;
};

// Installs memtable atomic flush results.
// In most cases, imm_lists is nullptr, and the function simply uses the
// immutable memtable lists associated with the cfds. There are unit tests that
// installs flush results for external immutable memtable lists other than the
// cfds' own immutable memtable lists, e.g. MemTableLIstTest. In this case,
// imm_lists parameter is not nullptr.
extern Status InstallMemtableAtomicFlushResults(
    const autovector<MemTableList*>* imm_lists,
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const MutableCFOptions*>& mutable_cf_options_list,
    const autovector<const autovector<MemTable*>*>& mems_list, VersionSet* vset,
    InstrumentedMutex* mu, const autovector<FileMetaData*>& file_meta,
    autovector<MemTable*>* to_delete, Directory* db_directory,
    LogBuffer* log_buffer);
}  // namespace rocksdb
