//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/memtable_list.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <limits>
#include <queue>
#include <string>
#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_set.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "table/merging_iterator.h"
#include "util/coding.h"
#include "util/log_buffer.h"
#include "util/sync_point.h"

namespace rocksdb {

class InternalKeyComparator;
class Mutex;
class VersionSet;

void MemTableListVersion::AddMemTable(MemTable* m) {
  memlist_.push_front(m);
  *parent_memtable_list_memory_usage_ += m->ApproximateMemoryUsage();
}

void MemTableListVersion::UnrefMemTable(autovector<MemTable*>* to_delete,
                                        MemTable* m) {
  if (m->Unref()) {
    to_delete->push_back(m);
    assert(*parent_memtable_list_memory_usage_ >= m->ApproximateMemoryUsage());
    *parent_memtable_list_memory_usage_ -= m->ApproximateMemoryUsage();
  }
}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage, MemTableListVersion* old)
    : max_write_buffer_number_to_maintain_(
          old->max_write_buffer_number_to_maintain_),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {
  if (old != nullptr) {
    memlist_ = old->memlist_;
    for (auto& m : memlist_) {
      m->Ref();
    }

    memlist_history_ = old->memlist_history_;
    for (auto& m : memlist_history_) {
      m->Ref();
    }
  }
}

MemTableListVersion::MemTableListVersion(
    size_t* parent_memtable_list_memory_usage,
    int max_write_buffer_number_to_maintain)
    : max_write_buffer_number_to_maintain_(max_write_buffer_number_to_maintain),
      parent_memtable_list_memory_usage_(parent_memtable_list_memory_usage) {}

void MemTableListVersion::Ref() { ++refs_; }

// called by superversion::clean()
void MemTableListVersion::Unref(autovector<MemTable*>* to_delete) {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    // if to_delete is equal to nullptr it means we're confident
    // that refs_ will not be zero
    assert(to_delete != nullptr);
    for (const auto& m : memlist_) {
      UnrefMemTable(to_delete, m);
    }
    for (const auto& m : memlist_history_) {
      UnrefMemTable(to_delete, m);
    }
    delete this;
  }
}

int MemTableList::NumNotFlushed() const {
  int size = static_cast<int>(current_->memlist_.size());
  assert(num_flush_not_started_ <= size);
  return size;
}

int MemTableList::NumFlushed() const {
  return static_cast<int>(current_->memlist_history_.size());
}

// Search all the memtables starting from the most recent one.
// Return the most recent value found, if any.
// Operands stores the list of merge operations to apply, so far.
bool MemTableListVersion::Get(const LookupKey& key, std::string* value,
                              Status* s, MergeContext* merge_context,
                              SequenceNumber* max_covering_tombstone_seq,
                              SequenceNumber* seq, const ReadOptions& read_opts,
                              ReadCallback* callback, bool* is_blob_index) {
  return GetFromList(&memlist_, key, value, s, merge_context,
                     max_covering_tombstone_seq, seq, read_opts, callback,
                     is_blob_index);
}

bool MemTableListVersion::GetFromHistory(
    const LookupKey& key, std::string* value, Status* s,
    MergeContext* merge_context, SequenceNumber* max_covering_tombstone_seq,
    SequenceNumber* seq, const ReadOptions& read_opts, bool* is_blob_index) {
  return GetFromList(&memlist_history_, key, value, s, merge_context,
                     max_covering_tombstone_seq, seq, read_opts,
                     nullptr /*read_callback*/, is_blob_index);
}

bool MemTableListVersion::GetFromList(
    std::list<MemTable*>* list, const LookupKey& key, std::string* value,
    Status* s, MergeContext* merge_context,
    SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
    const ReadOptions& read_opts, ReadCallback* callback, bool* is_blob_index) {
  *seq = kMaxSequenceNumber;

  for (auto& memtable : *list) {
    SequenceNumber current_seq = kMaxSequenceNumber;

    bool done =
        memtable->Get(key, value, s, merge_context, max_covering_tombstone_seq,
                      &current_seq, read_opts, callback, is_blob_index);
    if (*seq == kMaxSequenceNumber) {
      // Store the most recent sequence number of any operation on this key.
      // Since we only care about the most recent change, we only need to
      // return the first operation found when searching memtables in
      // reverse-chronological order.
      // current_seq would be equal to kMaxSequenceNumber if the value was to be
      // skipped. This allows seq to be assigned again when the next value is
      // read.
      *seq = current_seq;
    }

    if (done) {
      assert(*seq != kMaxSequenceNumber || s->IsNotFound());
      return true;
    }
    if (!done && !s->ok() && !s->IsMergeInProgress() && !s->IsNotFound()) {
      return false;
    }
  }
  return false;
}

Status MemTableListVersion::AddRangeTombstoneIterators(
    const ReadOptions& read_opts, Arena* /*arena*/,
    RangeDelAggregator* range_del_agg) {
  assert(range_del_agg != nullptr);
  for (auto& m : memlist_) {
    // Using kMaxSequenceNumber is OK because these are immutable memtables.
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        m->NewRangeTombstoneIterator(read_opts,
                                     kMaxSequenceNumber /* read_seq */));
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }
  return Status::OK();
}

void MemTableListVersion::AddIterators(
    const ReadOptions& options, std::vector<InternalIterator*>* iterator_list,
    Arena* arena) {
  for (auto& m : memlist_) {
    iterator_list->push_back(m->NewIterator(options, arena));
  }
}

void MemTableListVersion::AddIterators(
    const ReadOptions& options, MergeIteratorBuilder* merge_iter_builder) {
  for (auto& m : memlist_) {
    merge_iter_builder->AddIterator(
        m->NewIterator(options, merge_iter_builder->GetArena()));
  }
}

uint64_t MemTableListVersion::GetTotalNumEntries() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->num_entries();
  }
  return total_num;
}

MemTable::MemTableStats MemTableListVersion::ApproximateStats(
    const Slice& start_ikey, const Slice& end_ikey) {
  MemTable::MemTableStats total_stats = {0, 0};
  for (auto& m : memlist_) {
    auto mStats = m->ApproximateStats(start_ikey, end_ikey);
    total_stats.size += mStats.size;
    total_stats.count += mStats.count;
  }
  return total_stats;
}

uint64_t MemTableListVersion::GetTotalNumDeletes() const {
  uint64_t total_num = 0;
  for (auto& m : memlist_) {
    total_num += m->num_deletes();
  }
  return total_num;
}

SequenceNumber MemTableListVersion::GetEarliestSequenceNumber(
    bool include_history) const {
  if (include_history && !memlist_history_.empty()) {
    return memlist_history_.back()->GetEarliestSequenceNumber();
  } else if (!memlist_.empty()) {
    return memlist_.back()->GetEarliestSequenceNumber();
  } else {
    return kMaxSequenceNumber;
  }
}

// caller is responsible for referencing m
void MemTableListVersion::Add(MemTable* m, autovector<MemTable*>* to_delete) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  AddMemTable(m);

  TrimHistory(to_delete);
}

// Removes m from list of memtables not flushed.  Caller should NOT Unref m.
void MemTableListVersion::Remove(MemTable* m,
                                 autovector<MemTable*>* to_delete) {
  assert(refs_ == 1);  // only when refs_ == 1 is MemTableListVersion mutable
  memlist_.remove(m);

  m->MarkFlushed();
  if (max_write_buffer_number_to_maintain_ > 0) {
    memlist_history_.push_front(m);
    TrimHistory(to_delete);
  } else {
    UnrefMemTable(to_delete, m);
  }
}

// Make sure we don't use up too much space in history
void MemTableListVersion::TrimHistory(autovector<MemTable*>* to_delete) {
  while (memlist_.size() + memlist_history_.size() >
             static_cast<size_t>(max_write_buffer_number_to_maintain_) &&
         !memlist_history_.empty()) {
    MemTable* x = memlist_history_.back();
    memlist_history_.pop_back();

    UnrefMemTable(to_delete, x);
  }
}

// Returns true if there is at least one memtable on which flush has
// not yet started.
bool MemTableList::IsFlushPending() const {
  if ((flush_requested_ && num_flush_not_started_ > 0) ||
      (num_flush_not_started_ >= min_write_buffer_number_to_merge_)) {
    assert(imm_flush_needed.load(std::memory_order_relaxed));
    return true;
  }
  return false;
}

// Returns the memtables that need to be flushed.
void MemTableList::PickMemtablesToFlush(const uint64_t* max_memtable_id,
                                        autovector<MemTable*>* ret) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_PICK_MEMTABLES_TO_FLUSH);
  const auto& memlist = current_->memlist_;
  for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
    MemTable* m = *it;
    if (max_memtable_id != nullptr && m->GetID() > *max_memtable_id) {
      break;
    }
    if (!m->flush_in_progress_) {
      assert(!m->flush_completed_);
      num_flush_not_started_--;
      if (num_flush_not_started_ == 0) {
        imm_flush_needed.store(false, std::memory_order_release);
      }
      m->flush_in_progress_ = true;  // flushing will start very soon
      ret->push_back(m);
    }
  }
  flush_requested_ = false;  // start-flush request is complete
}

void MemTableList::RollbackMemtableFlush(const autovector<MemTable*>& mems,
                                         uint64_t /*file_number*/) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_ROLLBACK);
  assert(!mems.empty());

  // If the flush was not successful, then just reset state.
  // Maybe a succeeding attempt to flush will be successful.
  for (MemTable* m : mems) {
    assert(m->flush_in_progress_);
    assert(m->file_number_ == 0);

    m->flush_in_progress_ = false;
    m->flush_completed_ = false;
    m->edit_.Clear();
    num_flush_not_started_++;
  }
  imm_flush_needed.store(true, std::memory_order_release);
}

// Try record a successful flush in the manifest file. It might just return
// Status::OK letting a concurrent flush to do actual the recording..
Status MemTableList::TryInstallMemtableFlushResults(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const autovector<MemTable*>& mems, LogsWithPrepTracker* prep_tracker,
    VersionSet* vset, InstrumentedMutex* mu, uint64_t file_number,
    autovector<MemTable*>* to_delete, Directory* db_directory,
    LogBuffer* log_buffer) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
  mu->AssertHeld();

  // Flush was successful
  // Record the status on the memtable object. Either this call or a call by a
  // concurrent flush thread will read the status and write it to manifest.
  for (size_t i = 0; i < mems.size(); ++i) {
    // All the edits are associated with the first memtable of this batch.
    assert(i == 0 || mems[i]->GetEdits()->NumEntries() == 0);

    mems[i]->flush_completed_ = true;
    mems[i]->file_number_ = file_number;
  }

  // if some other thread is already committing, then return
  Status s;
  if (commit_in_progress_) {
    TEST_SYNC_POINT("MemTableList::TryInstallMemtableFlushResults:InProgress");
    return s;
  }

  // Only a single thread can be executing this piece of code
  commit_in_progress_ = true;

  // Retry until all completed flushes are committed. New flushes can finish
  // while the current thread is writing manifest where mutex is released.
  while (s.ok()) {
    auto& memlist = current_->memlist_;
    // The back is the oldest; if flush_completed_ is not set to it, it means
    // that we were assigned a more recent memtable. The memtables' flushes must
    // be recorded in manifest in order. A concurrent flush thread, who is
    // assigned to flush the oldest memtable, will later wake up and does all
    // the pending writes to manifest, in order.
    if (memlist.empty() || !memlist.back()->flush_completed_) {
      break;
    }
    // scan all memtables from the earliest, and commit those
    // (in that order) that have finished flushing. Memtables
    // are always committed in the order that they were created.
    uint64_t batch_file_number = 0;
    size_t batch_count = 0;
    autovector<VersionEdit*> edit_list;
    autovector<MemTable*> memtables_to_flush;
    // enumerate from the last (earliest) element to see how many batch finished
    for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
      MemTable* m = *it;
      if (!m->flush_completed_) {
        break;
      }
      if (it == memlist.rbegin() || batch_file_number != m->file_number_) {
        batch_file_number = m->file_number_;
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Level-0 commit table #%" PRIu64 " started",
                         cfd->GetName().c_str(), m->file_number_);
        edit_list.push_back(&m->edit_);
        memtables_to_flush.push_back(m);
      }
      batch_count++;
    }

    // TODO(myabandeh): Not sure how batch_count could be 0 here.
    if (batch_count > 0) {
      if (vset->db_options()->allow_2pc) {
        assert(edit_list.size() > 0);
        // We piggyback the information of  earliest log file to keep in the
        // manifest entry for the last file flushed.
        edit_list.back()->SetMinLogNumberToKeep(PrecomputeMinLogNumberToKeep(
            vset, *cfd, edit_list, memtables_to_flush, prep_tracker));
      }

      // this can release and reacquire the mutex.
      s = vset->LogAndApply(cfd, mutable_cf_options, edit_list, mu,
                            db_directory);

      // we will be changing the version in the next code path,
      // so we better create a new one, since versions are immutable
      InstallNewVersion();

      // All the later memtables that have the same filenum
      // are part of the same batch. They can be committed now.
      uint64_t mem_id = 1;  // how many memtables have been flushed.

      // commit new state only if the column family is NOT dropped.
      // The reason is as follows (refer to
      // ColumnFamilyTest.FlushAndDropRaceCondition).
      // If the column family is dropped, then according to LogAndApply, its
      // corresponding flush operation is NOT written to the MANIFEST. This
      // means the DB is not aware of the L0 files generated from the flush.
      // By committing the new state, we remove the memtable from the memtable
      // list. Creating an iterator on this column family will not be able to
      // read full data since the memtable is removed, and the DB is not aware
      // of the L0 files, causing MergingIterator unable to build child
      // iterators. RocksDB contract requires that the iterator can be created
      // on a dropped column family, and we must be able to
      // read full data as long as column family handle is not deleted, even if
      // the column family is dropped.
      if (s.ok() && !cfd->IsDropped()) {  // commit new state
        while (batch_count-- > 0) {
          MemTable* m = current_->memlist_.back();
          ROCKS_LOG_BUFFER(log_buffer, "[%s] Level-0 commit table #%" PRIu64
                                       ": memtable #%" PRIu64 " done",
                           cfd->GetName().c_str(), m->file_number_, mem_id);
          assert(m->file_number_ > 0);
          current_->Remove(m, to_delete);
          ++mem_id;
        }
      } else {
        for (auto it = current_->memlist_.rbegin(); batch_count-- > 0; it++) {
          MemTable* m = *it;
          // commit failed. setup state so that we can flush again.
          ROCKS_LOG_BUFFER(log_buffer, "Level-0 commit table #%" PRIu64
                                       ": memtable #%" PRIu64 " failed",
                           m->file_number_, mem_id);
          m->flush_completed_ = false;
          m->flush_in_progress_ = false;
          m->edit_.Clear();
          num_flush_not_started_++;
          m->file_number_ = 0;
          imm_flush_needed.store(true, std::memory_order_release);
          ++mem_id;
        }
      }
    }
  }
  commit_in_progress_ = false;
  return s;
}

// New memtables are inserted at the front of the list.
void MemTableList::Add(MemTable* m, autovector<MemTable*>* to_delete) {
  assert(static_cast<int>(current_->memlist_.size()) >= num_flush_not_started_);
  InstallNewVersion();
  // this method is used to move mutable memtable into an immutable list.
  // since mutable memtable is already refcounted by the DBImpl,
  // and when moving to the imutable list we don't unref it,
  // we don't have to ref the memtable here. we just take over the
  // reference from the DBImpl.
  current_->Add(m, to_delete);
  m->MarkImmutable();
  num_flush_not_started_++;
  if (num_flush_not_started_ == 1) {
    imm_flush_needed.store(true, std::memory_order_release);
  }
}

// Returns an estimate of the number of bytes of data in use.
size_t MemTableList::ApproximateUnflushedMemTablesMemoryUsage() {
  size_t total_size = 0;
  for (auto& memtable : current_->memlist_) {
    total_size += memtable->ApproximateMemoryUsage();
  }
  return total_size;
}

size_t MemTableList::ApproximateMemoryUsage() { return current_memory_usage_; }

uint64_t MemTableList::ApproximateOldestKeyTime() const {
  if (!current_->memlist_.empty()) {
    return current_->memlist_.back()->ApproximateOldestKeyTime();
  }
  return std::numeric_limits<uint64_t>::max();
}

void MemTableList::InstallNewVersion() {
  if (current_->refs_ == 1) {
    // we're the only one using the version, just keep using it
  } else {
    // somebody else holds the current version, we need to create new one
    MemTableListVersion* version = current_;
    current_ = new MemTableListVersion(&current_memory_usage_, current_);
    current_->Ref();
    version->Unref();
  }
}

uint64_t MemTableList::PrecomputeMinLogContainingPrepSection(
    const autovector<MemTable*>& memtables_to_flush) {
  uint64_t min_log = 0;

  for (auto& m : current_->memlist_) {
    // Assume the list is very short, we can live with O(m*n). We can optimize
    // if the performance has some problem.
    bool should_skip = false;
    for (MemTable* m_to_flush : memtables_to_flush) {
      if (m == m_to_flush) {
        should_skip = true;
        break;
      }
    }
    if (should_skip) {
      continue;
    }

    auto log = m->GetMinLogContainingPrepSection();

    if (log > 0 && (min_log == 0 || log < min_log)) {
      min_log = log;
    }
  }

  return min_log;
}

// Commit a successful atomic flush in the manifest file.
Status InstallMemtableAtomicFlushResults(
    const autovector<MemTableList*>* imm_lists,
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const MutableCFOptions*>& mutable_cf_options_list,
    const autovector<const autovector<MemTable*>*>& mems_list, VersionSet* vset,
    InstrumentedMutex* mu, const autovector<FileMetaData*>& file_metas,
    autovector<MemTable*>* to_delete, Directory* db_directory,
    LogBuffer* log_buffer) {
  AutoThreadOperationStageUpdater stage_updater(
      ThreadStatus::STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS);
  mu->AssertHeld();

  size_t num = mems_list.size();
  assert(cfds.size() == num);
  if (imm_lists != nullptr) {
    assert(imm_lists->size() == num);
  }
  for (size_t k = 0; k != num; ++k) {
#ifndef NDEBUG
    const auto* imm =
        (imm_lists == nullptr) ? cfds[k]->imm() : imm_lists->at(k);
    if (!mems_list[k]->empty()) {
      assert((*mems_list[k])[0]->GetID() == imm->GetEarliestMemTableID());
    }
#endif
    assert(nullptr != file_metas[k]);
    for (size_t i = 0; i != mems_list[k]->size(); ++i) {
      assert(i == 0 || (*mems_list[k])[i]->GetEdits()->NumEntries() == 0);
      (*mems_list[k])[i]->SetFlushCompleted(true);
      (*mems_list[k])[i]->SetFileNumber(file_metas[k]->fd.GetNumber());
    }
  }

  Status s;

  autovector<autovector<VersionEdit*>> edit_lists;
  uint32_t num_entries = 0;
  for (const auto mems : mems_list) {
    assert(mems != nullptr);
    autovector<VersionEdit*> edits;
    assert(!mems->empty());
    edits.emplace_back((*mems)[0]->GetEdits());
    ++num_entries;
    edit_lists.emplace_back(edits);
  }
  // Mark the version edits as an atomic group if the number of version edits
  // exceeds 1.
  if (cfds.size() > 1) {
    for (auto& edits : edit_lists) {
      assert(edits.size() == 1);
      edits[0]->MarkAtomicGroup(--num_entries);
    }
    assert(0 == num_entries);
  }

  // this can release and reacquire the mutex.
  s = vset->LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu,
                        db_directory);

  for (size_t k = 0; k != cfds.size(); ++k) {
    auto* imm = (imm_lists == nullptr) ? cfds[k]->imm() : imm_lists->at(k);
    imm->InstallNewVersion();
  }

  if (s.ok() || s.IsShutdownInProgress()) {
    for (size_t i = 0; i != cfds.size(); ++i) {
      if (cfds[i]->IsDropped()) {
        continue;
      }
      auto* imm = (imm_lists == nullptr) ? cfds[i]->imm() : imm_lists->at(i);
      for (auto m : *mems_list[i]) {
        assert(m->GetFileNumber() > 0);
        uint64_t mem_id = m->GetID();
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Level-0 commit table #%" PRIu64
                         ": memtable #%" PRIu64 " done",
                         cfds[i]->GetName().c_str(), m->GetFileNumber(),
                         mem_id);
        imm->current_->Remove(m, to_delete);
      }
    }
  } else {
    for (size_t i = 0; i != cfds.size(); ++i) {
      auto* imm = (imm_lists == nullptr) ? cfds[i]->imm() : imm_lists->at(i);
      for (auto m : *mems_list[i]) {
        uint64_t mem_id = m->GetID();
        ROCKS_LOG_BUFFER(log_buffer,
                         "[%s] Level-0 commit table #%" PRIu64
                         ": memtable #%" PRIu64 " failed",
                         cfds[i]->GetName().c_str(), m->GetFileNumber(),
                         mem_id);
        m->SetFlushCompleted(false);
        m->SetFlushInProgress(false);
        m->GetEdits()->Clear();
        m->SetFileNumber(0);
        imm->num_flush_not_started_++;
      }
      imm->imm_flush_needed.store(true, std::memory_order_release);
    }
  }

  return s;
}

}  // namespace rocksdb
