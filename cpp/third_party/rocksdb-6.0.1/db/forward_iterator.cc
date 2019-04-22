//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "db/forward_iterator.h"

#include <limits>
#include <string>
#include <utility>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/job_context.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "table/merging_iterator.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

// Usage:
//     ForwardLevelIterator iter;
//     iter.SetFileIndex(file_index);
//     iter.Seek(target); // or iter.SeekToFirst();
//     iter.Next()
class ForwardLevelIterator : public InternalIterator {
 public:
  ForwardLevelIterator(const ColumnFamilyData* const cfd,
                       const ReadOptions& read_options,
                       const std::vector<FileMetaData*>& files,
                       const SliceTransform* prefix_extractor)
      : cfd_(cfd),
        read_options_(read_options),
        files_(files),
        valid_(false),
        file_index_(std::numeric_limits<uint32_t>::max()),
        file_iter_(nullptr),
        pinned_iters_mgr_(nullptr),
        prefix_extractor_(prefix_extractor) {}

  ~ForwardLevelIterator() override {
    // Reset current pointer
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinIterator(file_iter_);
    } else {
      delete file_iter_;
    }
  }

  void SetFileIndex(uint32_t file_index) {
    assert(file_index < files_.size());
    status_ = Status::OK();
    if (file_index != file_index_) {
      file_index_ = file_index;
      Reset();
    }
  }
  void Reset() {
    assert(file_index_ < files_.size());

    // Reset current pointer
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinIterator(file_iter_);
    } else {
      delete file_iter_;
    }

    ReadRangeDelAggregator range_del_agg(&cfd_->internal_comparator(),
                                         kMaxSequenceNumber /* upper_bound */);
    file_iter_ = cfd_->table_cache()->NewIterator(
        read_options_, *(cfd_->soptions()), cfd_->internal_comparator(),
        *files_[file_index_],
        read_options_.ignore_range_deletions ? nullptr : &range_del_agg,
        prefix_extractor_, nullptr /* table_reader_ptr */, nullptr, false);
    file_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
    valid_ = false;
    if (!range_del_agg.IsEmpty()) {
      status_ = Status::NotSupported(
          "Range tombstones unsupported with ForwardIterator");
    }
  }
  void SeekToLast() override {
    status_ = Status::NotSupported("ForwardLevelIterator::SeekToLast()");
    valid_ = false;
  }
  void Prev() override {
    status_ = Status::NotSupported("ForwardLevelIterator::Prev()");
    valid_ = false;
  }
  bool Valid() const override {
    return valid_;
  }
  void SeekToFirst() override {
    assert(file_iter_ != nullptr);
    if (!status_.ok()) {
      assert(!valid_);
      return;
    }
    file_iter_->SeekToFirst();
    valid_ = file_iter_->Valid();
  }
  void Seek(const Slice& internal_key) override {
    assert(file_iter_ != nullptr);

    // This deviates from the usual convention for InternalIterator::Seek() in
    // that it doesn't discard pre-existing error status. That's because this
    // Seek() is only supposed to be called immediately after SetFileIndex()
    // (which discards pre-existing error status), and SetFileIndex() may set
    // an error status, which we shouldn't discard.
    if (!status_.ok()) {
      assert(!valid_);
      return;
    }

    file_iter_->Seek(internal_key);
    valid_ = file_iter_->Valid();
  }
  void SeekForPrev(const Slice& /*internal_key*/) override {
    status_ = Status::NotSupported("ForwardLevelIterator::SeekForPrev()");
    valid_ = false;
  }
  void Next() override {
    assert(valid_);
    file_iter_->Next();
    for (;;) {
      valid_ = file_iter_->Valid();
      if (!file_iter_->status().ok()) {
        assert(!valid_);
        return;
      }
      if (valid_) {
        return;
      }
      if (file_index_ + 1 >= files_.size()) {
        valid_ = false;
        return;
      }
      SetFileIndex(file_index_ + 1);
      if (!status_.ok()) {
        assert(!valid_);
        return;
      }
      file_iter_->SeekToFirst();
    }
  }
  Slice key() const override {
    assert(valid_);
    return file_iter_->key();
  }
  Slice value() const override {
    assert(valid_);
    return file_iter_->value();
  }
  Status status() const override {
    if (!status_.ok()) {
      return status_;
    } else if (file_iter_) {
      return file_iter_->status();
    }
    return Status::OK();
  }
  bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_->IsKeyPinned();
  }
  bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_->IsValuePinned();
  }
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    if (file_iter_) {
      file_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

 private:
  const ColumnFamilyData* const cfd_;
  const ReadOptions& read_options_;
  const std::vector<FileMetaData*>& files_;

  bool valid_;
  uint32_t file_index_;
  Status status_;
  InternalIterator* file_iter_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  const SliceTransform* prefix_extractor_;
};

ForwardIterator::ForwardIterator(DBImpl* db, const ReadOptions& read_options,
                                 ColumnFamilyData* cfd,
                                 SuperVersion* current_sv)
    : db_(db),
      read_options_(read_options),
      cfd_(cfd),
      prefix_extractor_(current_sv->mutable_cf_options.prefix_extractor.get()),
      user_comparator_(cfd->user_comparator()),
      immutable_min_heap_(MinIterComparator(&cfd_->internal_comparator())),
      sv_(current_sv),
      mutable_iter_(nullptr),
      current_(nullptr),
      valid_(false),
      status_(Status::OK()),
      immutable_status_(Status::OK()),
      has_iter_trimmed_for_upper_bound_(false),
      current_over_upper_bound_(false),
      is_prev_set_(false),
      is_prev_inclusive_(false),
      pinned_iters_mgr_(nullptr) {
  if (sv_) {
    RebuildIterators(false);
  }
}

ForwardIterator::~ForwardIterator() {
  Cleanup(true);
}

void ForwardIterator::SVCleanup(DBImpl* db, SuperVersion* sv,
                                bool background_purge_on_iterator_cleanup) {
  if (sv->Unref()) {
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    JobContext job_context(0);
    db->mutex_.Lock();
    sv->Cleanup();
    db->FindObsoleteFiles(&job_context, false, true);
    if (background_purge_on_iterator_cleanup) {
      db->ScheduleBgLogWriterClose(&job_context);
    }
    db->mutex_.Unlock();
    delete sv;
    if (job_context.HaveSomethingToDelete()) {
      db->PurgeObsoleteFiles(job_context, background_purge_on_iterator_cleanup);
    }
    job_context.Clean();
  }
}

namespace {
struct SVCleanupParams {
  DBImpl* db;
  SuperVersion* sv;
  bool background_purge_on_iterator_cleanup;
};
}

// Used in PinnedIteratorsManager to release pinned SuperVersion
void ForwardIterator::DeferredSVCleanup(void* arg) {
  auto d = reinterpret_cast<SVCleanupParams*>(arg);
  ForwardIterator::SVCleanup(
    d->db, d->sv, d->background_purge_on_iterator_cleanup);
  delete d;
}

void ForwardIterator::SVCleanup() {
  if (sv_ == nullptr) {
    return;
  }
  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    // pinned_iters_mgr_ tells us to make sure that all visited key-value slices
    // are alive until pinned_iters_mgr_->ReleasePinnedData() is called.
    // The slices may point into some memtables owned by sv_, so we need to keep
    // sv_ referenced until pinned_iters_mgr_ unpins everything.
    auto p = new SVCleanupParams{
      db_, sv_, read_options_.background_purge_on_iterator_cleanup};
    pinned_iters_mgr_->PinPtr(p, &ForwardIterator::DeferredSVCleanup);
  } else {
    SVCleanup(db_, sv_, read_options_.background_purge_on_iterator_cleanup);
  }
}

void ForwardIterator::Cleanup(bool release_sv) {
  if (mutable_iter_ != nullptr) {
    DeleteIterator(mutable_iter_, true /* is_arena */);
  }

  for (auto* m : imm_iters_) {
    DeleteIterator(m, true /* is_arena */);
  }
  imm_iters_.clear();

  for (auto* f : l0_iters_) {
    DeleteIterator(f);
  }
  l0_iters_.clear();

  for (auto* l : level_iters_) {
    DeleteIterator(l);
  }
  level_iters_.clear();

  if (release_sv) {
    SVCleanup();
  }
}

bool ForwardIterator::Valid() const {
  // See UpdateCurrent().
  return valid_ ? !current_over_upper_bound_ : false;
}

void ForwardIterator::SeekToFirst() {
  if (sv_ == nullptr) {
    RebuildIterators(true);
  } else if (sv_->version_number != cfd_->GetSuperVersionNumber()) {
    RenewIterators();
  } else if (immutable_status_.IsIncomplete()) {
    ResetIncompleteIterators();
  }
  SeekInternal(Slice(), true);
}

bool ForwardIterator::IsOverUpperBound(const Slice& internal_key) const {
  return !(read_options_.iterate_upper_bound == nullptr ||
           cfd_->internal_comparator().user_comparator()->Compare(
               ExtractUserKey(internal_key),
               *read_options_.iterate_upper_bound) < 0);
}

void ForwardIterator::Seek(const Slice& internal_key) {
  if (sv_ == nullptr) {
    RebuildIterators(true);
  } else if (sv_->version_number != cfd_->GetSuperVersionNumber()) {
    RenewIterators();
  } else if (immutable_status_.IsIncomplete()) {
    ResetIncompleteIterators();
  }
  SeekInternal(internal_key, false);
}

void ForwardIterator::SeekInternal(const Slice& internal_key,
                                   bool seek_to_first) {
  assert(mutable_iter_);
  // mutable
  seek_to_first ? mutable_iter_->SeekToFirst() :
                  mutable_iter_->Seek(internal_key);

  // immutable
  // TODO(ljin): NeedToSeekImmutable has negative impact on performance
  // if it turns to need to seek immutable often. We probably want to have
  // an option to turn it off.
  if (seek_to_first || NeedToSeekImmutable(internal_key)) {
    immutable_status_ = Status::OK();
    if (has_iter_trimmed_for_upper_bound_ &&
        (
            // prev_ is not set yet
            is_prev_set_ == false ||
            // We are doing SeekToFirst() and internal_key.size() = 0
            seek_to_first ||
            // prev_key_ > internal_key
            cfd_->internal_comparator().InternalKeyComparator::Compare(
                prev_key_.GetInternalKey(), internal_key) > 0)) {
      // Some iterators are trimmed. Need to rebuild.
      RebuildIterators(true);
      // Already seeked mutable iter, so seek again
      seek_to_first ? mutable_iter_->SeekToFirst()
                    : mutable_iter_->Seek(internal_key);
    }
    {
      auto tmp = MinIterHeap(MinIterComparator(&cfd_->internal_comparator()));
      immutable_min_heap_.swap(tmp);
    }
    for (size_t i = 0; i < imm_iters_.size(); i++) {
      auto* m = imm_iters_[i];
      seek_to_first ? m->SeekToFirst() : m->Seek(internal_key);
      if (!m->status().ok()) {
        immutable_status_ = m->status();
      } else if (m->Valid()) {
        immutable_min_heap_.push(m);
      }
    }

    Slice user_key;
    if (!seek_to_first) {
      user_key = ExtractUserKey(internal_key);
    }
    const VersionStorageInfo* vstorage = sv_->current->storage_info();
    const std::vector<FileMetaData*>& l0 = vstorage->LevelFiles(0);
    for (size_t i = 0; i < l0.size(); ++i) {
      if (!l0_iters_[i]) {
        continue;
      }
      if (seek_to_first) {
        l0_iters_[i]->SeekToFirst();
      } else {
        // If the target key passes over the larget key, we are sure Next()
        // won't go over this file.
        if (user_comparator_->Compare(user_key,
              l0[i]->largest.user_key()) > 0) {
          if (read_options_.iterate_upper_bound != nullptr) {
            has_iter_trimmed_for_upper_bound_ = true;
            DeleteIterator(l0_iters_[i]);
            l0_iters_[i] = nullptr;
          }
          continue;
        }
        l0_iters_[i]->Seek(internal_key);
      }

      if (!l0_iters_[i]->status().ok()) {
        immutable_status_ = l0_iters_[i]->status();
      } else if (l0_iters_[i]->Valid() &&
                 !IsOverUpperBound(l0_iters_[i]->key())) {
        immutable_min_heap_.push(l0_iters_[i]);
      } else {
        has_iter_trimmed_for_upper_bound_ = true;
        DeleteIterator(l0_iters_[i]);
        l0_iters_[i] = nullptr;
      }
    }

    for (int32_t level = 1; level < vstorage->num_levels(); ++level) {
      const std::vector<FileMetaData*>& level_files =
          vstorage->LevelFiles(level);
      if (level_files.empty()) {
        continue;
      }
      if (level_iters_[level - 1] == nullptr) {
        continue;
      }
      uint32_t f_idx = 0;
      if (!seek_to_first) {
        f_idx = FindFileInRange(level_files, internal_key, 0,
                                static_cast<uint32_t>(level_files.size()));
      }

      // Seek
      if (f_idx < level_files.size()) {
        level_iters_[level - 1]->SetFileIndex(f_idx);
        seek_to_first ? level_iters_[level - 1]->SeekToFirst() :
                        level_iters_[level - 1]->Seek(internal_key);

        if (!level_iters_[level - 1]->status().ok()) {
          immutable_status_ = level_iters_[level - 1]->status();
        } else if (level_iters_[level - 1]->Valid() &&
                   !IsOverUpperBound(level_iters_[level - 1]->key())) {
          immutable_min_heap_.push(level_iters_[level - 1]);
        } else {
          // Nothing in this level is interesting. Remove.
          has_iter_trimmed_for_upper_bound_ = true;
          DeleteIterator(level_iters_[level - 1]);
          level_iters_[level - 1] = nullptr;
        }
      }
    }

    if (seek_to_first) {
      is_prev_set_ = false;
    } else {
      prev_key_.SetInternalKey(internal_key);
      is_prev_set_ = true;
      is_prev_inclusive_ = true;
    }

    TEST_SYNC_POINT_CALLBACK("ForwardIterator::SeekInternal:Immutable", this);
  } else if (current_ && current_ != mutable_iter_) {
    // current_ is one of immutable iterators, push it back to the heap
    immutable_min_heap_.push(current_);
  }

  UpdateCurrent();
  TEST_SYNC_POINT_CALLBACK("ForwardIterator::SeekInternal:Return", this);
}

void ForwardIterator::Next() {
  assert(valid_);
  bool update_prev_key = false;

  if (sv_ == nullptr ||
      sv_->version_number != cfd_->GetSuperVersionNumber()) {
    std::string current_key = key().ToString();
    Slice old_key(current_key.data(), current_key.size());

    if (sv_ == nullptr) {
      RebuildIterators(true);
    } else {
      RenewIterators();
    }
    SeekInternal(old_key, false);
    if (!valid_ || key().compare(old_key) != 0) {
      return;
    }
  } else if (current_ != mutable_iter_) {
    // It is going to advance immutable iterator

    if (is_prev_set_ && prefix_extractor_) {
      // advance prev_key_ to current_ only if they share the same prefix
      update_prev_key =
          prefix_extractor_->Transform(prev_key_.GetUserKey())
              .compare(prefix_extractor_->Transform(current_->key())) == 0;
    } else {
      update_prev_key = true;
    }


    if (update_prev_key) {
      prev_key_.SetInternalKey(current_->key());
      is_prev_set_ = true;
      is_prev_inclusive_ = false;
    }
  }

  current_->Next();
  if (current_ != mutable_iter_) {
    if (!current_->status().ok()) {
      immutable_status_ = current_->status();
    } else if ((current_->Valid()) && (!IsOverUpperBound(current_->key()))) {
      immutable_min_heap_.push(current_);
    } else {
      if ((current_->Valid()) && (IsOverUpperBound(current_->key()))) {
        // remove the current iterator
        DeleteCurrentIter();
        current_ = nullptr;
      }
      if (update_prev_key) {
        mutable_iter_->Seek(prev_key_.GetInternalKey());
      }
    }
  }
  UpdateCurrent();
  TEST_SYNC_POINT_CALLBACK("ForwardIterator::Next:Return", this);
}

Slice ForwardIterator::key() const {
  assert(valid_);
  return current_->key();
}

Slice ForwardIterator::value() const {
  assert(valid_);
  return current_->value();
}

Status ForwardIterator::status() const {
  if (!status_.ok()) {
    return status_;
  } else if (!mutable_iter_->status().ok()) {
    return mutable_iter_->status();
  }

  return immutable_status_;
}

Status ForwardIterator::GetProperty(std::string prop_name, std::string* prop) {
  assert(prop != nullptr);
  if (prop_name == "rocksdb.iterator.super-version-number") {
    *prop = ToString(sv_->version_number);
    return Status::OK();
  }
  return Status::InvalidArgument();
}

void ForwardIterator::SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) {
  pinned_iters_mgr_ = pinned_iters_mgr;
  UpdateChildrenPinnedItersMgr();
}

void ForwardIterator::UpdateChildrenPinnedItersMgr() {
  // Set PinnedIteratorsManager for mutable memtable iterator.
  if (mutable_iter_) {
    mutable_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  // Set PinnedIteratorsManager for immutable memtable iterators.
  for (InternalIterator* child_iter : imm_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

  // Set PinnedIteratorsManager for L0 files iterators.
  for (InternalIterator* child_iter : l0_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

  // Set PinnedIteratorsManager for L1+ levels iterators.
  for (ForwardLevelIterator* child_iter : level_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }
}

bool ForwardIterator::IsKeyPinned() const {
  return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
         current_->IsKeyPinned();
}

bool ForwardIterator::IsValuePinned() const {
  return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
         current_->IsValuePinned();
}

void ForwardIterator::RebuildIterators(bool refresh_sv) {
  // Clean up
  Cleanup(refresh_sv);
  if (refresh_sv) {
    // New
    sv_ = cfd_->GetReferencedSuperVersion(&(db_->mutex_));
  }
  ReadRangeDelAggregator range_del_agg(&cfd_->internal_comparator(),
                                       kMaxSequenceNumber /* upper_bound */);
  mutable_iter_ = sv_->mem->NewIterator(read_options_, &arena_);
  sv_->imm->AddIterators(read_options_, &imm_iters_, &arena_);
  if (!read_options_.ignore_range_deletions) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        sv_->mem->NewRangeTombstoneIterator(
            read_options_, sv_->current->version_set()->LastSequence()));
    range_del_agg.AddTombstones(std::move(range_del_iter));
    sv_->imm->AddRangeTombstoneIterators(read_options_, &arena_,
                                         &range_del_agg);
  }
  has_iter_trimmed_for_upper_bound_ = false;

  const auto* vstorage = sv_->current->storage_info();
  const auto& l0_files = vstorage->LevelFiles(0);
  l0_iters_.reserve(l0_files.size());
  for (const auto* l0 : l0_files) {
    if ((read_options_.iterate_upper_bound != nullptr) &&
        cfd_->internal_comparator().user_comparator()->Compare(
            l0->smallest.user_key(), *read_options_.iterate_upper_bound) > 0) {
      // No need to set has_iter_trimmed_for_upper_bound_: this ForwardIterator
      // will never be interested in files with smallest key above
      // iterate_upper_bound, since iterate_upper_bound can't be changed.
      l0_iters_.push_back(nullptr);
      continue;
    }
    l0_iters_.push_back(cfd_->table_cache()->NewIterator(
        read_options_, *cfd_->soptions(), cfd_->internal_comparator(), *l0,
        read_options_.ignore_range_deletions ? nullptr : &range_del_agg,
        sv_->mutable_cf_options.prefix_extractor.get()));
  }
  BuildLevelIterators(vstorage);
  current_ = nullptr;
  is_prev_set_ = false;

  UpdateChildrenPinnedItersMgr();
  if (!range_del_agg.IsEmpty()) {
    status_ = Status::NotSupported(
        "Range tombstones unsupported with ForwardIterator");
    valid_ = false;
  }
}

void ForwardIterator::RenewIterators() {
  SuperVersion* svnew;
  assert(sv_);
  svnew = cfd_->GetReferencedSuperVersion(&(db_->mutex_));

  if (mutable_iter_ != nullptr) {
    DeleteIterator(mutable_iter_, true /* is_arena */);
  }
  for (auto* m : imm_iters_) {
    DeleteIterator(m, true /* is_arena */);
  }
  imm_iters_.clear();

  mutable_iter_ = svnew->mem->NewIterator(read_options_, &arena_);
  svnew->imm->AddIterators(read_options_, &imm_iters_, &arena_);
  ReadRangeDelAggregator range_del_agg(&cfd_->internal_comparator(),
                                       kMaxSequenceNumber /* upper_bound */);
  if (!read_options_.ignore_range_deletions) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        svnew->mem->NewRangeTombstoneIterator(
            read_options_, sv_->current->version_set()->LastSequence()));
    range_del_agg.AddTombstones(std::move(range_del_iter));
    svnew->imm->AddRangeTombstoneIterators(read_options_, &arena_,
                                           &range_del_agg);
  }

  const auto* vstorage = sv_->current->storage_info();
  const auto& l0_files = vstorage->LevelFiles(0);
  const auto* vstorage_new = svnew->current->storage_info();
  const auto& l0_files_new = vstorage_new->LevelFiles(0);
  size_t iold, inew;
  bool found;
  std::vector<InternalIterator*> l0_iters_new;
  l0_iters_new.reserve(l0_files_new.size());

  for (inew = 0; inew < l0_files_new.size(); inew++) {
    found = false;
    for (iold = 0; iold < l0_files.size(); iold++) {
      if (l0_files[iold] == l0_files_new[inew]) {
        found = true;
        break;
      }
    }
    if (found) {
      if (l0_iters_[iold] == nullptr) {
        l0_iters_new.push_back(nullptr);
        TEST_SYNC_POINT_CALLBACK("ForwardIterator::RenewIterators:Null", this);
      } else {
        l0_iters_new.push_back(l0_iters_[iold]);
        l0_iters_[iold] = nullptr;
        TEST_SYNC_POINT_CALLBACK("ForwardIterator::RenewIterators:Copy", this);
      }
      continue;
    }
    l0_iters_new.push_back(cfd_->table_cache()->NewIterator(
        read_options_, *cfd_->soptions(), cfd_->internal_comparator(),
        *l0_files_new[inew],
        read_options_.ignore_range_deletions ? nullptr : &range_del_agg,
        svnew->mutable_cf_options.prefix_extractor.get()));
  }

  for (auto* f : l0_iters_) {
    DeleteIterator(f);
  }
  l0_iters_.clear();
  l0_iters_ = l0_iters_new;

  for (auto* l : level_iters_) {
    DeleteIterator(l);
  }
  level_iters_.clear();
  BuildLevelIterators(vstorage_new);
  current_ = nullptr;
  is_prev_set_ = false;
  SVCleanup();
  sv_ = svnew;

  UpdateChildrenPinnedItersMgr();
  if (!range_del_agg.IsEmpty()) {
    status_ = Status::NotSupported(
        "Range tombstones unsupported with ForwardIterator");
    valid_ = false;
  }
}

void ForwardIterator::BuildLevelIterators(const VersionStorageInfo* vstorage) {
  level_iters_.reserve(vstorage->num_levels() - 1);
  for (int32_t level = 1; level < vstorage->num_levels(); ++level) {
    const auto& level_files = vstorage->LevelFiles(level);
    if ((level_files.empty()) ||
        ((read_options_.iterate_upper_bound != nullptr) &&
         (user_comparator_->Compare(*read_options_.iterate_upper_bound,
                                    level_files[0]->smallest.user_key()) <
          0))) {
      level_iters_.push_back(nullptr);
      if (!level_files.empty()) {
        has_iter_trimmed_for_upper_bound_ = true;
      }
    } else {
      level_iters_.push_back(new ForwardLevelIterator(
          cfd_, read_options_, level_files,
          sv_->mutable_cf_options.prefix_extractor.get()));
    }
  }
}

void ForwardIterator::ResetIncompleteIterators() {
  const auto& l0_files = sv_->current->storage_info()->LevelFiles(0);
  for (size_t i = 0; i < l0_iters_.size(); ++i) {
    assert(i < l0_files.size());
    if (!l0_iters_[i] || !l0_iters_[i]->status().IsIncomplete()) {
      continue;
    }
    DeleteIterator(l0_iters_[i]);
    l0_iters_[i] = cfd_->table_cache()->NewIterator(
        read_options_, *cfd_->soptions(), cfd_->internal_comparator(),
        *l0_files[i], nullptr /* range_del_agg */,
        sv_->mutable_cf_options.prefix_extractor.get());
    l0_iters_[i]->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  for (auto* level_iter : level_iters_) {
    if (level_iter && level_iter->status().IsIncomplete()) {
      level_iter->Reset();
    }
  }

  current_ = nullptr;
  is_prev_set_ = false;
}

void ForwardIterator::UpdateCurrent() {
  if (immutable_min_heap_.empty() && !mutable_iter_->Valid()) {
    current_ = nullptr;
  } else if (immutable_min_heap_.empty()) {
    current_ = mutable_iter_;
  } else if (!mutable_iter_->Valid()) {
    current_ = immutable_min_heap_.top();
    immutable_min_heap_.pop();
  } else {
    current_ = immutable_min_heap_.top();
    assert(current_ != nullptr);
    assert(current_->Valid());
    int cmp = cfd_->internal_comparator().InternalKeyComparator::Compare(
        mutable_iter_->key(), current_->key());
    assert(cmp != 0);
    if (cmp > 0) {
      immutable_min_heap_.pop();
    } else {
      current_ = mutable_iter_;
    }
  }
  valid_ = current_ != nullptr && immutable_status_.ok();
  if (!status_.ok()) {
    status_ = Status::OK();
  }

  // Upper bound doesn't apply to the memtable iterator. We want Valid() to
  // return false when all iterators are over iterate_upper_bound, but can't
  // just set valid_ to false, as that would effectively disable the tailing
  // optimization (Seek() would be called on all immutable iterators regardless
  // of whether the target key is greater than prev_key_).
  current_over_upper_bound_ = valid_ && IsOverUpperBound(current_->key());
}

bool ForwardIterator::NeedToSeekImmutable(const Slice& target) {
  // We maintain the interval (prev_key_, immutable_min_heap_.top()->key())
  // such that there are no records with keys within that range in
  // immutable_min_heap_. Since immutable structures (SST files and immutable
  // memtables) can't change in this version, we don't need to do a seek if
  // 'target' belongs to that interval (immutable_min_heap_.top() is already
  // at the correct position).

  if (!valid_ || !current_ || !is_prev_set_ || !immutable_status_.ok()) {
    return true;
  }
  Slice prev_key = prev_key_.GetInternalKey();
  if (prefix_extractor_ && prefix_extractor_->Transform(target).compare(
    prefix_extractor_->Transform(prev_key)) != 0) {
    return true;
  }
  if (cfd_->internal_comparator().InternalKeyComparator::Compare(
        prev_key, target) >= (is_prev_inclusive_ ? 1 : 0)) {
    return true;
  }

  if (immutable_min_heap_.empty() && current_ == mutable_iter_) {
    // Nothing to seek on.
    return false;
  }
  if (cfd_->internal_comparator().InternalKeyComparator::Compare(
        target, current_ == mutable_iter_ ? immutable_min_heap_.top()->key()
                                          : current_->key()) > 0) {
    return true;
  }
  return false;
}

void ForwardIterator::DeleteCurrentIter() {
  const VersionStorageInfo* vstorage = sv_->current->storage_info();
  const std::vector<FileMetaData*>& l0 = vstorage->LevelFiles(0);
  for (size_t i = 0; i < l0.size(); ++i) {
    if (!l0_iters_[i]) {
      continue;
    }
    if (l0_iters_[i] == current_) {
      has_iter_trimmed_for_upper_bound_ = true;
      DeleteIterator(l0_iters_[i]);
      l0_iters_[i] = nullptr;
      return;
    }
  }

  for (int32_t level = 1; level < vstorage->num_levels(); ++level) {
    if (level_iters_[level - 1] == nullptr) {
      continue;
    }
    if (level_iters_[level - 1] == current_) {
      has_iter_trimmed_for_upper_bound_ = true;
      DeleteIterator(level_iters_[level - 1]);
      level_iters_[level - 1] = nullptr;
    }
  }
}

bool ForwardIterator::TEST_CheckDeletedIters(int* pdeleted_iters,
                                             int* pnum_iters) {
  bool retval = false;
  int deleted_iters = 0;
  int num_iters = 0;

  const VersionStorageInfo* vstorage = sv_->current->storage_info();
  const std::vector<FileMetaData*>& l0 = vstorage->LevelFiles(0);
  for (size_t i = 0; i < l0.size(); ++i) {
    if (!l0_iters_[i]) {
      retval = true;
      deleted_iters++;
    } else {
      num_iters++;
    }
  }

  for (int32_t level = 1; level < vstorage->num_levels(); ++level) {
    if ((level_iters_[level - 1] == nullptr) &&
        (!vstorage->LevelFiles(level).empty())) {
      retval = true;
      deleted_iters++;
    } else if (!vstorage->LevelFiles(level).empty()) {
      num_iters++;
    }
  }
  if ((!retval) && num_iters <= 1) {
    retval = true;
  }
  if (pdeleted_iters) {
    *pdeleted_iters = deleted_iters;
  }
  if (pnum_iters) {
    *pnum_iters = num_iters;
  }
  return retval;
}

uint32_t ForwardIterator::FindFileInRange(
    const std::vector<FileMetaData*>& files, const Slice& internal_key,
    uint32_t left, uint32_t right) {
  auto cmp = [&](const FileMetaData* f, const Slice& key) -> bool {
    return cfd_->internal_comparator().InternalKeyComparator::Compare(
            f->largest.Encode(), key) < 0;
  };
  const auto &b = files.begin();
  return static_cast<uint32_t>(std::lower_bound(b + left,
                                 b + right, internal_key, cmp) - b);
}

void ForwardIterator::DeleteIterator(InternalIterator* iter, bool is_arena) {
  if (iter == nullptr) {
    return;
  }

  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    pinned_iters_mgr_->PinIterator(iter, is_arena);
  } else {
    if (is_arena) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
