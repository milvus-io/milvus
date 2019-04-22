//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdio.h>
#include <algorithm>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include "db/compaction.h"
#include "db/internal_stats.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "monitoring/file_read_sample.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/table_reader.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"

namespace rocksdb {

namespace {

// Find File in LevelFilesBrief data structure
// Within an index range defined by left and right
int FindFileInRange(const InternalKeyComparator& icmp,
    const LevelFilesBrief& file_level,
    const Slice& key,
    uint32_t left,
    uint32_t right) {
  auto cmp = [&](const FdWithKeyRange& f, const Slice& k) -> bool {
    return icmp.InternalKeyComparator::Compare(f.largest_key, k) < 0;
  };
  const auto &b = file_level.files;
  return static_cast<int>(std::lower_bound(b + left,
                                           b + right, key, cmp) - b);
}

Status OverlapWithIterator(const Comparator* ucmp,
    const Slice& smallest_user_key,
    const Slice& largest_user_key,
    InternalIterator* iter,
    bool* overlap) {
  InternalKey range_start(smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
  iter->Seek(range_start.Encode());
  if (!iter->status().ok()) {
    return iter->status();
  }

  *overlap = false;
  if (iter->Valid()) {
    ParsedInternalKey seek_result;
    if (!ParseInternalKey(iter->key(), &seek_result)) {
      return Status::Corruption("DB have corrupted keys");
    }

    if (ucmp->Compare(seek_result.user_key, largest_user_key) <= 0) {
      *overlap = true;
    }
  }

  return iter->status();
}

// Class to help choose the next file to search for the particular key.
// Searches and returns files level by level.
// We can search level-by-level since entries never hop across
// levels. Therefore we are guaranteed that if we find data
// in a smaller level, later levels are irrelevant (unless we
// are MergeInProgress).
class FilePicker {
 public:
  FilePicker(std::vector<FileMetaData*>* files, const Slice& user_key,
             const Slice& ikey, autovector<LevelFilesBrief>* file_levels,
             unsigned int num_levels, FileIndexer* file_indexer,
             const Comparator* user_comparator,
             const InternalKeyComparator* internal_comparator)
      : num_levels_(num_levels),
        curr_level_(static_cast<unsigned int>(-1)),
        returned_file_level_(static_cast<unsigned int>(-1)),
        hit_file_level_(static_cast<unsigned int>(-1)),
        search_left_bound_(0),
        search_right_bound_(FileIndexer::kLevelMaxIndex),
#ifndef NDEBUG
        files_(files),
#endif
        level_files_brief_(file_levels),
        is_hit_file_last_in_level_(false),
        curr_file_level_(nullptr),
        user_key_(user_key),
        ikey_(ikey),
        file_indexer_(file_indexer),
        user_comparator_(user_comparator),
        internal_comparator_(internal_comparator) {
#ifdef NDEBUG
    (void)files;
#endif
    // Setup member variables to search first level.
    search_ended_ = !PrepareNextLevel();
    if (!search_ended_) {
      // Prefetch Level 0 table data to avoid cache miss if possible.
      for (unsigned int i = 0; i < (*level_files_brief_)[0].num_files; ++i) {
        auto* r = (*level_files_brief_)[0].files[i].fd.table_reader;
        if (r) {
          r->Prepare(ikey);
        }
      }
    }
  }

  int GetCurrentLevel() const { return curr_level_; }

  FdWithKeyRange* GetNextFile() {
    while (!search_ended_) {  // Loops over different levels.
      while (curr_index_in_curr_level_ < curr_file_level_->num_files) {
        // Loops over all files in current level.
        FdWithKeyRange* f = &curr_file_level_->files[curr_index_in_curr_level_];
        hit_file_level_ = curr_level_;
        is_hit_file_last_in_level_ =
            curr_index_in_curr_level_ == curr_file_level_->num_files - 1;
        int cmp_largest = -1;

        // Do key range filtering of files or/and fractional cascading if:
        // (1) not all the files are in level 0, or
        // (2) there are more than 3 current level files
        // If there are only 3 or less current level files in the system, we skip
        // the key range filtering. In this case, more likely, the system is
        // highly tuned to minimize number of tables queried by each query,
        // so it is unlikely that key range filtering is more efficient than
        // querying the files.
        if (num_levels_ > 1 || curr_file_level_->num_files > 3) {
          // Check if key is within a file's range. If search left bound and
          // right bound point to the same find, we are sure key falls in
          // range.
          assert(
              curr_level_ == 0 ||
              curr_index_in_curr_level_ == start_index_in_curr_level_ ||
              user_comparator_->Compare(user_key_,
                ExtractUserKey(f->smallest_key)) <= 0);

          int cmp_smallest = user_comparator_->Compare(user_key_,
              ExtractUserKey(f->smallest_key));
          if (cmp_smallest >= 0) {
            cmp_largest = user_comparator_->Compare(user_key_,
                ExtractUserKey(f->largest_key));
          }

          // Setup file search bound for the next level based on the
          // comparison results
          if (curr_level_ > 0) {
            file_indexer_->GetNextLevelIndex(curr_level_,
                                            curr_index_in_curr_level_,
                                            cmp_smallest, cmp_largest,
                                            &search_left_bound_,
                                            &search_right_bound_);
          }
          // Key falls out of current file's range
          if (cmp_smallest < 0 || cmp_largest > 0) {
            if (curr_level_ == 0) {
              ++curr_index_in_curr_level_;
              continue;
            } else {
              // Search next level.
              break;
            }
          }
        }
#ifndef NDEBUG
        // Sanity check to make sure that the files are correctly sorted
        if (prev_file_) {
          if (curr_level_ != 0) {
            int comp_sign = internal_comparator_->Compare(
                prev_file_->largest_key, f->smallest_key);
            assert(comp_sign < 0);
          } else {
            // level == 0, the current file cannot be newer than the previous
            // one. Use compressed data structure, has no attribute seqNo
            assert(curr_index_in_curr_level_ > 0);
            assert(!NewestFirstBySeqNo(files_[0][curr_index_in_curr_level_],
                  files_[0][curr_index_in_curr_level_-1]));
          }
        }
        prev_file_ = f;
#endif
        returned_file_level_ = curr_level_;
        if (curr_level_ > 0 && cmp_largest < 0) {
          // No more files to search in this level.
          search_ended_ = !PrepareNextLevel();
        } else {
          ++curr_index_in_curr_level_;
        }
        return f;
      }
      // Start searching next level.
      search_ended_ = !PrepareNextLevel();
    }
    // Search ended.
    return nullptr;
  }

  // getter for current file level
  // for GET_HIT_L0, GET_HIT_L1 & GET_HIT_L2_AND_UP counts
  unsigned int GetHitFileLevel() { return hit_file_level_; }

  // Returns true if the most recent "hit file" (i.e., one returned by
  // GetNextFile()) is at the last index in its level.
  bool IsHitFileLastInLevel() { return is_hit_file_last_in_level_; }

 private:
  unsigned int num_levels_;
  unsigned int curr_level_;
  unsigned int returned_file_level_;
  unsigned int hit_file_level_;
  int32_t search_left_bound_;
  int32_t search_right_bound_;
#ifndef NDEBUG
  std::vector<FileMetaData*>* files_;
#endif
  autovector<LevelFilesBrief>* level_files_brief_;
  bool search_ended_;
  bool is_hit_file_last_in_level_;
  LevelFilesBrief* curr_file_level_;
  unsigned int curr_index_in_curr_level_;
  unsigned int start_index_in_curr_level_;
  Slice user_key_;
  Slice ikey_;
  FileIndexer* file_indexer_;
  const Comparator* user_comparator_;
  const InternalKeyComparator* internal_comparator_;
#ifndef NDEBUG
  FdWithKeyRange* prev_file_;
#endif

  // Setup local variables to search next level.
  // Returns false if there are no more levels to search.
  bool PrepareNextLevel() {
    curr_level_++;
    while (curr_level_ < num_levels_) {
      curr_file_level_ = &(*level_files_brief_)[curr_level_];
      if (curr_file_level_->num_files == 0) {
        // When current level is empty, the search bound generated from upper
        // level must be [0, -1] or [0, FileIndexer::kLevelMaxIndex] if it is
        // also empty.
        assert(search_left_bound_ == 0);
        assert(search_right_bound_ == -1 ||
               search_right_bound_ == FileIndexer::kLevelMaxIndex);
        // Since current level is empty, it will need to search all files in
        // the next level
        search_left_bound_ = 0;
        search_right_bound_ = FileIndexer::kLevelMaxIndex;
        curr_level_++;
        continue;
      }

      // Some files may overlap each other. We find
      // all files that overlap user_key and process them in order from
      // newest to oldest. In the context of merge-operator, this can occur at
      // any level. Otherwise, it only occurs at Level-0 (since Put/Deletes
      // are always compacted into a single entry).
      int32_t start_index;
      if (curr_level_ == 0) {
        // On Level-0, we read through all files to check for overlap.
        start_index = 0;
      } else {
        // On Level-n (n>=1), files are sorted. Binary search to find the
        // earliest file whose largest key >= ikey. Search left bound and
        // right bound are used to narrow the range.
        if (search_left_bound_ <= search_right_bound_) {
          if (search_right_bound_ == FileIndexer::kLevelMaxIndex) {
            search_right_bound_ =
                static_cast<int32_t>(curr_file_level_->num_files) - 1;
          }
          // `search_right_bound_` is an inclusive upper-bound, but since it was
          // determined based on user key, it is still possible the lookup key
          // falls to the right of `search_right_bound_`'s corresponding file.
          // So, pass a limit one higher, which allows us to detect this case.
          start_index =
              FindFileInRange(*internal_comparator_, *curr_file_level_, ikey_,
                              static_cast<uint32_t>(search_left_bound_),
                              static_cast<uint32_t>(search_right_bound_) + 1);
          if (start_index == search_right_bound_ + 1) {
            // `ikey_` comes after `search_right_bound_`. The lookup key does
            // not exist on this level, so let's skip this level and do a full
            // binary search on the next level.
            search_left_bound_ = 0;
            search_right_bound_ = FileIndexer::kLevelMaxIndex;
            curr_level_++;
            continue;
          }
        } else {
          // search_left_bound > search_right_bound, key does not exist in
          // this level. Since no comparison is done in this level, it will
          // need to search all files in the next level.
          search_left_bound_ = 0;
          search_right_bound_ = FileIndexer::kLevelMaxIndex;
          curr_level_++;
          continue;
        }
      }
      start_index_in_curr_level_ = start_index;
      curr_index_in_curr_level_ = start_index;
#ifndef NDEBUG
      prev_file_ = nullptr;
#endif
      return true;
    }
    // curr_level_ = num_levels_. So, no more levels to search.
    return false;
  }
};
}  // anonymous namespace

VersionStorageInfo::~VersionStorageInfo() { delete[] files_; }

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    for (size_t i = 0; i < storage_info_.files_[level].size(); i++) {
      FileMetaData* f = storage_info_.files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        assert(cfd_ != nullptr);
        uint32_t path_id = f->fd.GetPathId();
        assert(path_id < cfd_->ioptions()->cf_paths.size());
        vset_->obsolete_files_.push_back(
            ObsoleteFileInfo(f, cfd_->ioptions()->cf_paths[path_id].path));
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
             const LevelFilesBrief& file_level,
             const Slice& key) {
  return FindFileInRange(icmp, file_level, key, 0,
                         static_cast<uint32_t>(file_level.num_files));
}

void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
        const std::vector<FileMetaData*>& files,
        Arena* arena) {
  assert(file_level);
  assert(arena);

  size_t num = files.size();
  file_level->num_files = num;
  char* mem = arena->AllocateAligned(num * sizeof(FdWithKeyRange));
  file_level->files = new (mem)FdWithKeyRange[num];

  for (size_t i = 0; i < num; i++) {
    Slice smallest_key = files[i]->smallest.Encode();
    Slice largest_key = files[i]->largest.Encode();

    // Copy key slice to sequential memory
    size_t smallest_size = smallest_key.size();
    size_t largest_size = largest_key.size();
    mem = arena->AllocateAligned(smallest_size + largest_size);
    memcpy(mem, smallest_key.data(), smallest_size);
    memcpy(mem + smallest_size, largest_key.data(), largest_size);

    FdWithKeyRange& f = file_level->files[i];
    f.fd = files[i]->fd;
    f.file_metadata = files[i];
    f.smallest_key = Slice(mem, smallest_size);
    f.largest_key = Slice(mem + smallest_size, largest_size);
  }
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FdWithKeyRange* f) {
  // nullptr user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, ExtractUserKey(f->largest_key)) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FdWithKeyRange* f) {
  // nullptr user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, ExtractUserKey(f->smallest_key)) < 0);
}

bool SomeFileOverlapsRange(
    const InternalKeyComparator& icmp,
    bool disjoint_sorted_files,
    const LevelFilesBrief& file_level,
    const Slice* smallest_user_key,
    const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < file_level.num_files; i++) {
      const FdWithKeyRange* f = &(file_level.files[i]);
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the leftmost possible internal key for smallest_user_key
    InternalKey small;
    small.SetMinPossibleForUserKey(*smallest_user_key);
    index = FindFile(icmp, file_level, small.Encode());
  }

  if (index >= file_level.num_files) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, &file_level.files[index]);
}

namespace {

class LevelIterator final : public InternalIterator {
 public:
  LevelIterator(
      TableCache* table_cache, const ReadOptions& read_options,
      const EnvOptions& env_options, const InternalKeyComparator& icomparator,
      const LevelFilesBrief* flevel, const SliceTransform* prefix_extractor,
      bool should_sample, HistogramImpl* file_read_hist, bool for_compaction,
      bool skip_filters, int level, RangeDelAggregator* range_del_agg,
      const std::vector<AtomicCompactionUnitBoundary>* compaction_boundaries =
          nullptr)
      : table_cache_(table_cache),
        read_options_(read_options),
        env_options_(env_options),
        icomparator_(icomparator),
        flevel_(flevel),
        prefix_extractor_(prefix_extractor),
        file_read_hist_(file_read_hist),
        should_sample_(should_sample),
        for_compaction_(for_compaction),
        skip_filters_(skip_filters),
        file_index_(flevel_->num_files),
        level_(level),
        range_del_agg_(range_del_agg),
        pinned_iters_mgr_(nullptr),
        compaction_boundaries_(compaction_boundaries) {
    // Empty level is not supported.
    assert(flevel_ != nullptr && flevel_->num_files > 0);
  }

  ~LevelIterator() override { delete file_iter_.Set(nullptr); }

  void Seek(const Slice& target) override;
  void SeekForPrev(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return file_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return file_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return file_iter_.value();
  }
  Status status() const override {
    return file_iter_.iter() ? file_iter_.status() : Status::OK();
  }
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    if (file_iter_.iter()) {
      file_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }
  bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_.iter() && file_iter_.IsKeyPinned();
  }
  bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_.iter() && file_iter_.IsValuePinned();
  }

 private:
  void SkipEmptyFileForward();
  void SkipEmptyFileBackward();
  void SetFileIterator(InternalIterator* iter);
  void InitFileIterator(size_t new_file_index);

  const Slice& file_smallest_key(size_t file_index) {
    assert(file_index < flevel_->num_files);
    return flevel_->files[file_index].smallest_key;
  }

  bool KeyReachedUpperBound(const Slice& internal_key) {
    return read_options_.iterate_upper_bound != nullptr &&
           icomparator_.user_comparator()->Compare(
               ExtractUserKey(internal_key),
               *read_options_.iterate_upper_bound) >= 0;
  }

  InternalIterator* NewFileIterator() {
    assert(file_index_ < flevel_->num_files);
    auto file_meta = flevel_->files[file_index_];
    if (should_sample_) {
      sample_file_read_inc(file_meta.file_metadata);
    }

    const InternalKey* smallest_compaction_key = nullptr;
    const InternalKey* largest_compaction_key = nullptr;
    if (compaction_boundaries_ != nullptr) {
      smallest_compaction_key = (*compaction_boundaries_)[file_index_].smallest;
      largest_compaction_key = (*compaction_boundaries_)[file_index_].largest;
    }
    return table_cache_->NewIterator(
        read_options_, env_options_, icomparator_, *file_meta.file_metadata,
        range_del_agg_, prefix_extractor_,
        nullptr /* don't need reference to table */,
        file_read_hist_, for_compaction_, nullptr /* arena */, skip_filters_,
        level_, smallest_compaction_key, largest_compaction_key);
  }

  TableCache* table_cache_;
  const ReadOptions read_options_;
  const EnvOptions& env_options_;
  const InternalKeyComparator& icomparator_;
  const LevelFilesBrief* flevel_;
  mutable FileDescriptor current_value_;
  const SliceTransform* prefix_extractor_;

  HistogramImpl* file_read_hist_;
  bool should_sample_;
  bool for_compaction_;
  bool skip_filters_;
  size_t file_index_;
  int level_;
  RangeDelAggregator* range_del_agg_;
  IteratorWrapper file_iter_;  // May be nullptr
  PinnedIteratorsManager* pinned_iters_mgr_;

  // To be propagated to RangeDelAggregator in order to safely truncate range
  // tombstones.
  const std::vector<AtomicCompactionUnitBoundary>* compaction_boundaries_;
};

void LevelIterator::Seek(const Slice& target) {
  size_t new_file_index = FindFile(icomparator_, *flevel_, target);

  InitFileIterator(new_file_index);
  if (file_iter_.iter() != nullptr) {
    file_iter_.Seek(target);
  }
  SkipEmptyFileForward();
}

void LevelIterator::SeekForPrev(const Slice& target) {
  size_t new_file_index = FindFile(icomparator_, *flevel_, target);
  if (new_file_index >= flevel_->num_files) {
    new_file_index = flevel_->num_files - 1;
  }

  InitFileIterator(new_file_index);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekForPrev(target);
    SkipEmptyFileBackward();
  }
}

void LevelIterator::SeekToFirst() {
  InitFileIterator(0);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekToFirst();
  }
  SkipEmptyFileForward();
}

void LevelIterator::SeekToLast() {
  InitFileIterator(flevel_->num_files - 1);
  if (file_iter_.iter() != nullptr) {
    file_iter_.SeekToLast();
  }
  SkipEmptyFileBackward();
}

void LevelIterator::Next() {
  assert(Valid());
  file_iter_.Next();
  SkipEmptyFileForward();
}

void LevelIterator::Prev() {
  assert(Valid());
  file_iter_.Prev();
  SkipEmptyFileBackward();
}

void LevelIterator::SkipEmptyFileForward() {
  while (file_iter_.iter() == nullptr ||
         (!file_iter_.Valid() && file_iter_.status().ok() &&
          !file_iter_.iter()->IsOutOfBound())) {
    // Move to next file
    if (file_index_ >= flevel_->num_files - 1) {
      // Already at the last file
      SetFileIterator(nullptr);
      return;
    }
    if (KeyReachedUpperBound(file_smallest_key(file_index_ + 1))) {
      SetFileIterator(nullptr);
      return;
    }
    InitFileIterator(file_index_ + 1);
    if (file_iter_.iter() != nullptr) {
      file_iter_.SeekToFirst();
    }
  }
}

void LevelIterator::SkipEmptyFileBackward() {
  while (file_iter_.iter() == nullptr ||
         (!file_iter_.Valid() && file_iter_.status().ok())) {
    // Move to previous file
    if (file_index_ == 0) {
      // Already the first file
      SetFileIterator(nullptr);
      return;
    }
    InitFileIterator(file_index_ - 1);
    if (file_iter_.iter() != nullptr) {
      file_iter_.SeekToLast();
    }
  }
}

void LevelIterator::SetFileIterator(InternalIterator* iter) {
  if (pinned_iters_mgr_ && iter) {
    iter->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  InternalIterator* old_iter = file_iter_.Set(iter);
  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    pinned_iters_mgr_->PinIterator(old_iter);
  } else {
    delete old_iter;
  }
}

void LevelIterator::InitFileIterator(size_t new_file_index) {
  if (new_file_index >= flevel_->num_files) {
    file_index_ = new_file_index;
    SetFileIterator(nullptr);
    return;
  } else {
    // If the file iterator shows incomplete, we try it again if users seek
    // to the same file, as this time we may go to a different data block
    // which is cached in block cache.
    //
    if (file_iter_.iter() != nullptr && !file_iter_.status().IsIncomplete() &&
        new_file_index == file_index_) {
      // file_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      file_index_ = new_file_index;
      InternalIterator* iter = NewFileIterator();
      SetFileIterator(iter);
    }
  }
}

// A wrapper of version builder which references the current version in
// constructor and unref it in the destructor.
// Both of the constructor and destructor need to be called inside DB Mutex.
class BaseReferencedVersionBuilder {
 public:
  explicit BaseReferencedVersionBuilder(ColumnFamilyData* cfd)
      : version_builder_(new VersionBuilder(
            cfd->current()->version_set()->env_options(), cfd->table_cache(),
            cfd->current()->storage_info(), cfd->ioptions()->info_log)),
        version_(cfd->current()) {
    version_->Ref();
  }
  ~BaseReferencedVersionBuilder() {
    delete version_builder_;
    version_->Unref();
  }
  VersionBuilder* version_builder() { return version_builder_; }

 private:
  VersionBuilder* version_builder_;
  Version* version_;
};
}  // anonymous namespace

Status Version::GetTableProperties(std::shared_ptr<const TableProperties>* tp,
                                   const FileMetaData* file_meta,
                                   const std::string* fname) const {
  auto table_cache = cfd_->table_cache();
  auto ioptions = cfd_->ioptions();
  Status s = table_cache->GetTableProperties(
      env_options_, cfd_->internal_comparator(), file_meta->fd, tp,
      mutable_cf_options_.prefix_extractor.get(), true /* no io */);
  if (s.ok()) {
    return s;
  }

  // We only ignore error type `Incomplete` since it's by design that we
  // disallow table when it's not in table cache.
  if (!s.IsIncomplete()) {
    return s;
  }

  // 2. Table is not present in table cache, we'll read the table properties
  // directly from the properties block in the file.
  std::unique_ptr<RandomAccessFile> file;
  std::string file_name;
  if (fname != nullptr) {
    file_name = *fname;
  } else {
    file_name =
      TableFileName(ioptions->cf_paths, file_meta->fd.GetNumber(),
                    file_meta->fd.GetPathId());
  }
  s = ioptions->env->NewRandomAccessFile(file_name, &file, env_options_);
  if (!s.ok()) {
    return s;
  }

  TableProperties* raw_table_properties;
  // By setting the magic number to kInvalidTableMagicNumber, we can by
  // pass the magic number check in the footer.
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(
          std::move(file), file_name, nullptr /* env */, nullptr /* stats */,
          0 /* hist_type */, nullptr /* file_read_hist */,
          nullptr /* rate_limiter */, false /* for_compaction*/,
          ioptions->listeners));
  s = ReadTableProperties(
      file_reader.get(), file_meta->fd.GetFileSize(),
      Footer::kInvalidTableMagicNumber /* table's magic number */, *ioptions,
      &raw_table_properties, false /* compression_type_missing */);
  if (!s.ok()) {
    return s;
  }
  RecordTick(ioptions->statistics, NUMBER_DIRECT_LOAD_TABLE_PROPERTIES);

  *tp = std::shared_ptr<const TableProperties>(raw_table_properties);
  return s;
}

Status Version::GetPropertiesOfAllTables(TablePropertiesCollection* props) {
  Status s;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    s = GetPropertiesOfAllTables(props, level);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status Version::GetPropertiesOfAllTables(TablePropertiesCollection* props,
                                         int level) {
  for (const auto& file_meta : storage_info_.files_[level]) {
    auto fname =
        TableFileName(cfd_->ioptions()->cf_paths, file_meta->fd.GetNumber(),
                      file_meta->fd.GetPathId());
    // 1. If the table is already present in table cache, load table
    // properties from there.
    std::shared_ptr<const TableProperties> table_properties;
    Status s = GetTableProperties(&table_properties, file_meta, &fname);
    if (s.ok()) {
      props->insert({fname, table_properties});
    } else {
      return s;
    }
  }

  return Status::OK();
}

Status Version::GetPropertiesOfTablesInRange(
    const Range* range, std::size_t n, TablePropertiesCollection* props) const {
  for (int level = 0; level < storage_info_.num_non_empty_levels(); level++) {
    for (decltype(n) i = 0; i < n; i++) {
      // Convert user_key into a corresponding internal key.
      InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
      InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
      std::vector<FileMetaData*> files;
      storage_info_.GetOverlappingInputs(level, &k1, &k2, &files, -1, nullptr,
                                         false);
      for (const auto& file_meta : files) {
        auto fname =
            TableFileName(cfd_->ioptions()->cf_paths,
                          file_meta->fd.GetNumber(), file_meta->fd.GetPathId());
        if (props->count(fname) == 0) {
          // 1. If the table is already present in table cache, load table
          // properties from there.
          std::shared_ptr<const TableProperties> table_properties;
          Status s = GetTableProperties(&table_properties, file_meta, &fname);
          if (s.ok()) {
            props->insert({fname, table_properties});
          } else {
            return s;
          }
        }
      }
    }
  }

  return Status::OK();
}

Status Version::GetAggregatedTableProperties(
    std::shared_ptr<const TableProperties>* tp, int level) {
  TablePropertiesCollection props;
  Status s;
  if (level < 0) {
    s = GetPropertiesOfAllTables(&props);
  } else {
    s = GetPropertiesOfAllTables(&props, level);
  }
  if (!s.ok()) {
    return s;
  }

  auto* new_tp = new TableProperties();
  for (const auto& item : props) {
    new_tp->Add(*item.second);
  }
  tp->reset(new_tp);
  return Status::OK();
}

size_t Version::GetMemoryUsageByTableReaders() {
  size_t total_usage = 0;
  for (auto& file_level : storage_info_.level_files_brief_) {
    for (size_t i = 0; i < file_level.num_files; i++) {
      total_usage += cfd_->table_cache()->GetMemoryUsageByTableReader(
          env_options_, cfd_->internal_comparator(), file_level.files[i].fd,
          mutable_cf_options_.prefix_extractor.get());
    }
  }
  return total_usage;
}

void Version::GetColumnFamilyMetaData(ColumnFamilyMetaData* cf_meta) {
  assert(cf_meta);
  assert(cfd_);

  cf_meta->name = cfd_->GetName();
  cf_meta->size = 0;
  cf_meta->file_count = 0;
  cf_meta->levels.clear();

  auto* ioptions = cfd_->ioptions();
  auto* vstorage = storage_info();

  for (int level = 0; level < cfd_->NumberLevels(); level++) {
    uint64_t level_size = 0;
    cf_meta->file_count += vstorage->LevelFiles(level).size();
    std::vector<SstFileMetaData> files;
    for (const auto& file : vstorage->LevelFiles(level)) {
      uint32_t path_id = file->fd.GetPathId();
      std::string file_path;
      if (path_id < ioptions->cf_paths.size()) {
        file_path = ioptions->cf_paths[path_id].path;
      } else {
        assert(!ioptions->cf_paths.empty());
        file_path = ioptions->cf_paths.back().path;
      }
      files.emplace_back(SstFileMetaData{
          MakeTableFileName("", file->fd.GetNumber()),
          file_path,
          static_cast<size_t>(file->fd.GetFileSize()),
          file->fd.smallest_seqno,
          file->fd.largest_seqno,
          file->smallest.user_key().ToString(),
          file->largest.user_key().ToString(),
          file->stats.num_reads_sampled.load(std::memory_order_relaxed),
          file->being_compacted});
      files.back().num_entries = file->num_entries;
      files.back().num_deletions = file->num_deletions;
      level_size += file->fd.GetFileSize();
    }
    cf_meta->levels.emplace_back(
        level, level_size, std::move(files));
    cf_meta->size += level_size;
  }
}

uint64_t Version::GetSstFilesSize() {
  uint64_t sst_files_size = 0;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    for (const auto& file_meta : storage_info_.LevelFiles(level)) {
      sst_files_size += file_meta->fd.GetFileSize();
    }
  }
  return sst_files_size;
}

uint64_t VersionStorageInfo::GetEstimatedActiveKeys() const {
  // Estimation will be inaccurate when:
  // (1) there exist merge keys
  // (2) keys are directly overwritten
  // (3) deletion on non-existing keys
  // (4) low number of samples
  if (current_num_samples_ == 0) {
    return 0;
  }

  if (current_num_non_deletions_ <= current_num_deletions_) {
    return 0;
  }

  uint64_t est = current_num_non_deletions_ - current_num_deletions_;

  uint64_t file_count = 0;
  for (int level = 0; level < num_levels_; ++level) {
    file_count += files_[level].size();
  }

  if (current_num_samples_ < file_count) {
    // casting to avoid overflowing
    return
      static_cast<uint64_t>(
        (est * static_cast<double>(file_count) / current_num_samples_)
      );
  } else {
    return est;
  }
}

double VersionStorageInfo::GetEstimatedCompressionRatioAtLevel(
    int level) const {
  assert(level < num_levels_);
  uint64_t sum_file_size_bytes = 0;
  uint64_t sum_data_size_bytes = 0;
  for (auto* file_meta : files_[level]) {
    sum_file_size_bytes += file_meta->fd.GetFileSize();
    sum_data_size_bytes += file_meta->raw_key_size + file_meta->raw_value_size;
  }
  if (sum_file_size_bytes == 0) {
    return -1.0;
  }
  return static_cast<double>(sum_data_size_bytes) / sum_file_size_bytes;
}

void Version::AddIterators(const ReadOptions& read_options,
                           const EnvOptions& soptions,
                           MergeIteratorBuilder* merge_iter_builder,
                           RangeDelAggregator* range_del_agg) {
  assert(storage_info_.finalized_);

  for (int level = 0; level < storage_info_.num_non_empty_levels(); level++) {
    AddIteratorsForLevel(read_options, soptions, merge_iter_builder, level,
                         range_del_agg);
  }
}

void Version::AddIteratorsForLevel(const ReadOptions& read_options,
                                   const EnvOptions& soptions,
                                   MergeIteratorBuilder* merge_iter_builder,
                                   int level,
                                   RangeDelAggregator* range_del_agg) {
  assert(storage_info_.finalized_);
  if (level >= storage_info_.num_non_empty_levels()) {
    // This is an empty level
    return;
  } else if (storage_info_.LevelFilesBrief(level).num_files == 0) {
    // No files in this level
    return;
  }

  bool should_sample = should_sample_file_read();

  auto* arena = merge_iter_builder->GetArena();
  if (level == 0) {
    // Merge all level zero files together since they may overlap
    for (size_t i = 0; i < storage_info_.LevelFilesBrief(0).num_files; i++) {
      const auto& file = storage_info_.LevelFilesBrief(0).files[i];
      merge_iter_builder->AddIterator(cfd_->table_cache()->NewIterator(
          read_options, soptions, cfd_->internal_comparator(), *file.file_metadata,
          range_del_agg, mutable_cf_options_.prefix_extractor.get(), nullptr,
          cfd_->internal_stats()->GetFileReadHist(0), false, arena,
          false /* skip_filters */, 0 /* level */));
    }
    if (should_sample) {
      // Count ones for every L0 files. This is done per iterator creation
      // rather than Seek(), while files in other levels are recored per seek.
      // If users execute one range query per iterator, there may be some
      // discrepancy here.
      for (FileMetaData* meta : storage_info_.LevelFiles(0)) {
        sample_file_read_inc(meta);
      }
    }
  } else if (storage_info_.LevelFilesBrief(level).num_files > 0) {
    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    auto* mem = arena->AllocateAligned(sizeof(LevelIterator));
    merge_iter_builder->AddIterator(new (mem) LevelIterator(
        cfd_->table_cache(), read_options, soptions,
        cfd_->internal_comparator(), &storage_info_.LevelFilesBrief(level),
        mutable_cf_options_.prefix_extractor.get(), should_sample_file_read(),
        cfd_->internal_stats()->GetFileReadHist(level),
        false /* for_compaction */, IsFilterSkipped(level), level,
        range_del_agg));
  }
}

Status Version::OverlapWithLevelIterator(const ReadOptions& read_options,
                                         const EnvOptions& env_options,
                                         const Slice& smallest_user_key,
                                         const Slice& largest_user_key,
                                         int level, bool* overlap) {
  assert(storage_info_.finalized_);

  auto icmp = cfd_->internal_comparator();
  auto ucmp = icmp.user_comparator();

  Arena arena;
  Status status;
  ReadRangeDelAggregator range_del_agg(&icmp,
                                       kMaxSequenceNumber /* upper_bound */);

  *overlap = false;

  if (level == 0) {
    for (size_t i = 0; i < storage_info_.LevelFilesBrief(0).num_files; i++) {
      const auto file = &storage_info_.LevelFilesBrief(0).files[i];
      if (AfterFile(ucmp, &smallest_user_key, file) ||
          BeforeFile(ucmp, &largest_user_key, file)) {
        continue;
      }
      ScopedArenaIterator iter(cfd_->table_cache()->NewIterator(
          read_options, env_options, cfd_->internal_comparator(), *file->file_metadata,
          &range_del_agg, mutable_cf_options_.prefix_extractor.get(), nullptr,
          cfd_->internal_stats()->GetFileReadHist(0), false, &arena,
          false /* skip_filters */, 0 /* level */));
      status = OverlapWithIterator(
          ucmp, smallest_user_key, largest_user_key, iter.get(), overlap);
      if (!status.ok() || *overlap) {
        break;
      }
    }
  } else if (storage_info_.LevelFilesBrief(level).num_files > 0) {
    auto mem = arena.AllocateAligned(sizeof(LevelIterator));
    ScopedArenaIterator iter(new (mem) LevelIterator(
        cfd_->table_cache(), read_options, env_options,
        cfd_->internal_comparator(), &storage_info_.LevelFilesBrief(level),
        mutable_cf_options_.prefix_extractor.get(), should_sample_file_read(),
        cfd_->internal_stats()->GetFileReadHist(level),
        false /* for_compaction */, IsFilterSkipped(level), level,
        &range_del_agg));
    status = OverlapWithIterator(
        ucmp, smallest_user_key, largest_user_key, iter.get(), overlap);
  }

  if (status.ok() && *overlap == false &&
      range_del_agg.IsRangeOverlapped(smallest_user_key, largest_user_key)) {
    *overlap = true;
  }
  return status;
}

VersionStorageInfo::VersionStorageInfo(
    const InternalKeyComparator* internal_comparator,
    const Comparator* user_comparator, int levels,
    CompactionStyle compaction_style, VersionStorageInfo* ref_vstorage,
    bool _force_consistency_checks)
    : internal_comparator_(internal_comparator),
      user_comparator_(user_comparator),
      // cfd is nullptr if Version is dummy
      num_levels_(levels),
      num_non_empty_levels_(0),
      file_indexer_(user_comparator),
      compaction_style_(compaction_style),
      files_(new std::vector<FileMetaData*>[num_levels_]),
      base_level_(num_levels_ == 1 ? -1 : 1),
      level_multiplier_(0.0),
      files_by_compaction_pri_(num_levels_),
      level0_non_overlapping_(false),
      next_file_to_compact_by_size_(num_levels_),
      compaction_score_(num_levels_),
      compaction_level_(num_levels_),
      l0_delay_trigger_count_(0),
      accumulated_file_size_(0),
      accumulated_raw_key_size_(0),
      accumulated_raw_value_size_(0),
      accumulated_num_non_deletions_(0),
      accumulated_num_deletions_(0),
      current_num_non_deletions_(0),
      current_num_deletions_(0),
      current_num_samples_(0),
      estimated_compaction_needed_bytes_(0),
      finalized_(false),
      force_consistency_checks_(_force_consistency_checks) {
  if (ref_vstorage != nullptr) {
    accumulated_file_size_ = ref_vstorage->accumulated_file_size_;
    accumulated_raw_key_size_ = ref_vstorage->accumulated_raw_key_size_;
    accumulated_raw_value_size_ = ref_vstorage->accumulated_raw_value_size_;
    accumulated_num_non_deletions_ =
        ref_vstorage->accumulated_num_non_deletions_;
    accumulated_num_deletions_ = ref_vstorage->accumulated_num_deletions_;
    current_num_non_deletions_ = ref_vstorage->current_num_non_deletions_;
    current_num_deletions_ = ref_vstorage->current_num_deletions_;
    current_num_samples_ = ref_vstorage->current_num_samples_;
    oldest_snapshot_seqnum_ = ref_vstorage->oldest_snapshot_seqnum_;
  }
}

Version::Version(ColumnFamilyData* column_family_data, VersionSet* vset,
                 const EnvOptions& env_opt,
                 const MutableCFOptions mutable_cf_options,
                 uint64_t version_number)
    : env_(vset->env_),
      cfd_(column_family_data),
      info_log_((cfd_ == nullptr) ? nullptr : cfd_->ioptions()->info_log),
      db_statistics_((cfd_ == nullptr) ? nullptr
                                       : cfd_->ioptions()->statistics),
      table_cache_((cfd_ == nullptr) ? nullptr : cfd_->table_cache()),
      merge_operator_((cfd_ == nullptr) ? nullptr
                                        : cfd_->ioptions()->merge_operator),
      storage_info_(
          (cfd_ == nullptr) ? nullptr : &cfd_->internal_comparator(),
          (cfd_ == nullptr) ? nullptr : cfd_->user_comparator(),
          cfd_ == nullptr ? 0 : cfd_->NumberLevels(),
          cfd_ == nullptr ? kCompactionStyleLevel
                          : cfd_->ioptions()->compaction_style,
          (cfd_ == nullptr || cfd_->current() == nullptr)
              ? nullptr
              : cfd_->current()->storage_info(),
          cfd_ == nullptr ? false : cfd_->ioptions()->force_consistency_checks),
      vset_(vset),
      next_(this),
      prev_(this),
      refs_(0),
      env_options_(env_opt),
      mutable_cf_options_(mutable_cf_options),
      version_number_(version_number) {}

void Version::Get(const ReadOptions& read_options, const LookupKey& k,
                  PinnableSlice* value, Status* status,
                  MergeContext* merge_context,
                  SequenceNumber* max_covering_tombstone_seq, bool* value_found,
                  bool* key_exists, SequenceNumber* seq, ReadCallback* callback,
                  bool* is_blob) {
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();

  assert(status->ok() || status->IsMergeInProgress());

  if (key_exists != nullptr) {
    // will falsify below if not found
    *key_exists = true;
  }

  PinnedIteratorsManager pinned_iters_mgr;
  GetContext get_context(
      user_comparator(), merge_operator_, info_log_, db_statistics_,
      status->ok() ? GetContext::kNotFound : GetContext::kMerge, user_key,
      value, value_found, merge_context, max_covering_tombstone_seq, this->env_,
      seq, merge_operator_ ? &pinned_iters_mgr : nullptr, callback, is_blob);

  // Pin blocks that we read to hold merge operands
  if (merge_operator_) {
    pinned_iters_mgr.StartPinning();
  }

  FilePicker fp(
      storage_info_.files_, user_key, ikey, &storage_info_.level_files_brief_,
      storage_info_.num_non_empty_levels_, &storage_info_.file_indexer_,
      user_comparator(), internal_comparator());
  FdWithKeyRange* f = fp.GetNextFile();

  while (f != nullptr) {
    if (*max_covering_tombstone_seq > 0) {
      // The remaining files we look at will only contain covered keys, so we
      // stop here.
      break;
    }
    if (get_context.sample()) {
      sample_file_read_inc(f->file_metadata);
    }

    bool timer_enabled =
        GetPerfLevel() >= PerfLevel::kEnableTimeExceptForMutex &&
        get_perf_context()->per_level_perf_context_enabled;
    StopWatchNano timer(env_, timer_enabled /* auto_start */);
    *status = table_cache_->Get(
        read_options, *internal_comparator(), *f->file_metadata, ikey,
        &get_context, mutable_cf_options_.prefix_extractor.get(),
        cfd_->internal_stats()->GetFileReadHist(fp.GetHitFileLevel()),
        IsFilterSkipped(static_cast<int>(fp.GetHitFileLevel()),
                        fp.IsHitFileLastInLevel()),
        fp.GetCurrentLevel());
    // TODO: examine the behavior for corrupted key
    if (timer_enabled) {
      PERF_COUNTER_BY_LEVEL_ADD(get_from_table_nanos, timer.ElapsedNanos(),
                                fp.GetCurrentLevel());
    }
    if (!status->ok()) {
      return;
    }

    // report the counters before returning
    if (get_context.State() != GetContext::kNotFound &&
        get_context.State() != GetContext::kMerge &&
        db_statistics_ != nullptr) {
      get_context.ReportCounters();
    }
    switch (get_context.State()) {
      case GetContext::kNotFound:
        // Keep searching in other files
        break;
      case GetContext::kMerge:
        // TODO: update per-level perfcontext user_key_return_count for kMerge
        break;
      case GetContext::kFound:
        if (fp.GetHitFileLevel() == 0) {
          RecordTick(db_statistics_, GET_HIT_L0);
        } else if (fp.GetHitFileLevel() == 1) {
          RecordTick(db_statistics_, GET_HIT_L1);
        } else if (fp.GetHitFileLevel() >= 2) {
          RecordTick(db_statistics_, GET_HIT_L2_AND_UP);
        }
        PERF_COUNTER_BY_LEVEL_ADD(user_key_return_count, 1, fp.GetHitFileLevel());
        return;
      case GetContext::kDeleted:
        // Use empty error message for speed
        *status = Status::NotFound();
        return;
      case GetContext::kCorrupt:
        *status = Status::Corruption("corrupted key for ", user_key);
        return;
      case GetContext::kBlobIndex:
        ROCKS_LOG_ERROR(info_log_, "Encounter unexpected blob index.");
        *status = Status::NotSupported(
            "Encounter unexpected blob index. Please open DB with "
            "rocksdb::blob_db::BlobDB instead.");
        return;
    }
    f = fp.GetNextFile();
  }

  if (db_statistics_ != nullptr) {
    get_context.ReportCounters();
  }
  if (GetContext::kMerge == get_context.State()) {
    if (!merge_operator_) {
      *status =  Status::InvalidArgument(
          "merge_operator is not properly initialized.");
      return;
    }
    // merge_operands are in saver and we hit the beginning of the key history
    // do a final merge of nullptr and operands;
    std::string* str_value = value != nullptr ? value->GetSelf() : nullptr;
    *status = MergeHelper::TimedFullMerge(
        merge_operator_, user_key, nullptr, merge_context->GetOperands(),
        str_value, info_log_, db_statistics_, env_,
        nullptr /* result_operand */, true);
    if (LIKELY(value != nullptr)) {
      value->PinSelf();
    }
  } else {
    if (key_exists != nullptr) {
      *key_exists = false;
    }
    *status = Status::NotFound(); // Use an empty error message for speed
  }
}

bool Version::IsFilterSkipped(int level, bool is_file_last_in_level) {
  // Reaching the bottom level implies misses at all upper levels, so we'll
  // skip checking the filters when we predict a hit.
  return cfd_->ioptions()->optimize_filters_for_hits &&
         (level > 0 || is_file_last_in_level) &&
         level == storage_info_.num_non_empty_levels() - 1;
}

void VersionStorageInfo::GenerateLevelFilesBrief() {
  level_files_brief_.resize(num_non_empty_levels_);
  for (int level = 0; level < num_non_empty_levels_; level++) {
    DoGenerateLevelFilesBrief(
        &level_files_brief_[level], files_[level], &arena_);
  }
}

void Version::PrepareApply(
    const MutableCFOptions& mutable_cf_options,
    bool update_stats) {
  UpdateAccumulatedStats(update_stats);
  storage_info_.UpdateNumNonEmptyLevels();
  storage_info_.CalculateBaseBytes(*cfd_->ioptions(), mutable_cf_options);
  storage_info_.UpdateFilesByCompactionPri(cfd_->ioptions()->compaction_pri);
  storage_info_.GenerateFileIndexer();
  storage_info_.GenerateLevelFilesBrief();
  storage_info_.GenerateLevel0NonOverlapping();
  storage_info_.GenerateBottommostFiles();
}

bool Version::MaybeInitializeFileMetaData(FileMetaData* file_meta) {
  if (file_meta->init_stats_from_file ||
      file_meta->compensated_file_size > 0) {
    return false;
  }
  std::shared_ptr<const TableProperties> tp;
  Status s = GetTableProperties(&tp, file_meta);
  file_meta->init_stats_from_file = true;
  if (!s.ok()) {
    ROCKS_LOG_ERROR(vset_->db_options_->info_log,
                    "Unable to load table properties for file %" PRIu64
                    " --- %s\n",
                    file_meta->fd.GetNumber(), s.ToString().c_str());
    return false;
  }
  if (tp.get() == nullptr) return false;
  file_meta->num_entries = tp->num_entries;
  file_meta->num_deletions = tp->num_deletions;
  file_meta->raw_value_size = tp->raw_value_size;
  file_meta->raw_key_size = tp->raw_key_size;

  return true;
}

void VersionStorageInfo::UpdateAccumulatedStats(FileMetaData* file_meta) {
  assert(file_meta->init_stats_from_file);
  accumulated_file_size_ += file_meta->fd.GetFileSize();
  accumulated_raw_key_size_ += file_meta->raw_key_size;
  accumulated_raw_value_size_ += file_meta->raw_value_size;
  accumulated_num_non_deletions_ +=
      file_meta->num_entries - file_meta->num_deletions;
  accumulated_num_deletions_ += file_meta->num_deletions;

  current_num_non_deletions_ +=
      file_meta->num_entries - file_meta->num_deletions;
  current_num_deletions_ += file_meta->num_deletions;
  current_num_samples_++;
}

void VersionStorageInfo::RemoveCurrentStats(FileMetaData* file_meta) {
  if (file_meta->init_stats_from_file) {
    current_num_non_deletions_ -=
        file_meta->num_entries - file_meta->num_deletions;
    current_num_deletions_ -= file_meta->num_deletions;
    current_num_samples_--;
  }
}

void Version::UpdateAccumulatedStats(bool update_stats) {
  if (update_stats) {
    // maximum number of table properties loaded from files.
    const int kMaxInitCount = 20;
    int init_count = 0;
    // here only the first kMaxInitCount files which haven't been
    // initialized from file will be updated with num_deletions.
    // The motivation here is to cap the maximum I/O per Version creation.
    // The reason for choosing files from lower-level instead of higher-level
    // is that such design is able to propagate the initialization from
    // lower-level to higher-level:  When the num_deletions of lower-level
    // files are updated, it will make the lower-level files have accurate
    // compensated_file_size, making lower-level to higher-level compaction
    // will be triggered, which creates higher-level files whose num_deletions
    // will be updated here.
    for (int level = 0;
         level < storage_info_.num_levels_ && init_count < kMaxInitCount;
         ++level) {
      for (auto* file_meta : storage_info_.files_[level]) {
        if (MaybeInitializeFileMetaData(file_meta)) {
          // each FileMeta will be initialized only once.
          storage_info_.UpdateAccumulatedStats(file_meta);
          // when option "max_open_files" is -1, all the file metadata has
          // already been read, so MaybeInitializeFileMetaData() won't incur
          // any I/O cost. "max_open_files=-1" means that the table cache passed
          // to the VersionSet and then to the ColumnFamilySet has a size of
          // TableCache::kInfiniteCapacity
          if (vset_->GetColumnFamilySet()->get_table_cache()->GetCapacity() ==
              TableCache::kInfiniteCapacity) {
            continue;
          }
          if (++init_count >= kMaxInitCount) {
            break;
          }
        }
      }
    }
    // In case all sampled-files contain only deletion entries, then we
    // load the table-property of a file in higher-level to initialize
    // that value.
    for (int level = storage_info_.num_levels_ - 1;
         storage_info_.accumulated_raw_value_size_ == 0 && level >= 0;
         --level) {
      for (int i = static_cast<int>(storage_info_.files_[level].size()) - 1;
           storage_info_.accumulated_raw_value_size_ == 0 && i >= 0; --i) {
        if (MaybeInitializeFileMetaData(storage_info_.files_[level][i])) {
          storage_info_.UpdateAccumulatedStats(storage_info_.files_[level][i]);
        }
      }
    }
  }

  storage_info_.ComputeCompensatedSizes();
}

void VersionStorageInfo::ComputeCompensatedSizes() {
  static const int kDeletionWeightOnCompaction = 2;
  uint64_t average_value_size = GetAverageValueSize();

  // compute the compensated size
  for (int level = 0; level < num_levels_; level++) {
    for (auto* file_meta : files_[level]) {
      // Here we only compute compensated_file_size for those file_meta
      // which compensated_file_size is uninitialized (== 0). This is true only
      // for files that have been created right now and no other thread has
      // access to them. That's why we can safely mutate compensated_file_size.
      if (file_meta->compensated_file_size == 0) {
        file_meta->compensated_file_size = file_meta->fd.GetFileSize();
        // Here we only boost the size of deletion entries of a file only
        // when the number of deletion entries is greater than the number of
        // non-deletion entries in the file.  The motivation here is that in
        // a stable workload, the number of deletion entries should be roughly
        // equal to the number of non-deletion entries.  If we compensate the
        // size of deletion entries in a stable workload, the deletion
        // compensation logic might introduce unwanted effet which changes the
        // shape of LSM tree.
        if (file_meta->num_deletions * 2 >= file_meta->num_entries) {
          file_meta->compensated_file_size +=
              (file_meta->num_deletions * 2 - file_meta->num_entries) *
              average_value_size * kDeletionWeightOnCompaction;
        }
      }
    }
  }
}

int VersionStorageInfo::MaxInputLevel() const {
  if (compaction_style_ == kCompactionStyleLevel) {
    return num_levels() - 2;
  }
  return 0;
}

int VersionStorageInfo::MaxOutputLevel(bool allow_ingest_behind) const {
  if (allow_ingest_behind) {
    assert(num_levels() > 1);
    return num_levels() - 2;
  }
  return num_levels() - 1;
}

void VersionStorageInfo::EstimateCompactionBytesNeeded(
    const MutableCFOptions& mutable_cf_options) {
  // Only implemented for level-based compaction
  if (compaction_style_ != kCompactionStyleLevel) {
    estimated_compaction_needed_bytes_ = 0;
    return;
  }

  // Start from Level 0, if level 0 qualifies compaction to level 1,
  // we estimate the size of compaction.
  // Then we move on to the next level and see whether it qualifies compaction
  // to the next level. The size of the level is estimated as the actual size
  // on the level plus the input bytes from the previous level if there is any.
  // If it exceeds, take the exceeded bytes as compaction input and add the size
  // of the compaction size to tatal size.
  // We keep doing it to Level 2, 3, etc, until the last level and return the
  // accumulated bytes.

  uint64_t bytes_compact_to_next_level = 0;
  uint64_t level_size = 0;
  for (auto* f : files_[0]) {
    level_size += f->fd.GetFileSize();
  }
  // Level 0
  bool level0_compact_triggered = false;
  if (static_cast<int>(files_[0].size()) >=
          mutable_cf_options.level0_file_num_compaction_trigger ||
      level_size >= mutable_cf_options.max_bytes_for_level_base) {
    level0_compact_triggered = true;
    estimated_compaction_needed_bytes_ = level_size;
    bytes_compact_to_next_level = level_size;
  } else {
    estimated_compaction_needed_bytes_ = 0;
  }

  // Level 1 and up.
  uint64_t bytes_next_level = 0;
  for (int level = base_level(); level <= MaxInputLevel(); level++) {
    level_size = 0;
    if (bytes_next_level > 0) {
#ifndef NDEBUG
      uint64_t level_size2 = 0;
      for (auto* f : files_[level]) {
        level_size2 += f->fd.GetFileSize();
      }
      assert(level_size2 == bytes_next_level);
#endif
      level_size = bytes_next_level;
      bytes_next_level = 0;
    } else {
      for (auto* f : files_[level]) {
        level_size += f->fd.GetFileSize();
      }
    }
    if (level == base_level() && level0_compact_triggered) {
      // Add base level size to compaction if level0 compaction triggered.
      estimated_compaction_needed_bytes_ += level_size;
    }
    // Add size added by previous compaction
    level_size += bytes_compact_to_next_level;
    bytes_compact_to_next_level = 0;
    uint64_t level_target = MaxBytesForLevel(level);
    if (level_size > level_target) {
      bytes_compact_to_next_level = level_size - level_target;
      // Estimate the actual compaction fan-out ratio as size ratio between
      // the two levels.

      assert(bytes_next_level == 0);
      if (level + 1 < num_levels_) {
        for (auto* f : files_[level + 1]) {
          bytes_next_level += f->fd.GetFileSize();
        }
      }
      if (bytes_next_level > 0) {
        assert(level_size > 0);
        estimated_compaction_needed_bytes_ += static_cast<uint64_t>(
            static_cast<double>(bytes_compact_to_next_level) *
            (static_cast<double>(bytes_next_level) /
                 static_cast<double>(level_size) +
             1));
      }
    }
  }
}

namespace {
uint32_t GetExpiredTtlFilesCount(const ImmutableCFOptions& ioptions,
                                 const MutableCFOptions& mutable_cf_options,
                                 const std::vector<FileMetaData*>& files) {
  uint32_t ttl_expired_files_count = 0;

  int64_t _current_time;
  auto status = ioptions.env->GetCurrentTime(&_current_time);
  if (status.ok()) {
    const uint64_t current_time = static_cast<uint64_t>(_current_time);
    for (auto f : files) {
      if (!f->being_compacted && f->fd.table_reader != nullptr &&
          f->fd.table_reader->GetTableProperties() != nullptr) {
        auto creation_time =
            f->fd.table_reader->GetTableProperties()->creation_time;
        if (creation_time > 0 &&
            creation_time < (current_time - mutable_cf_options.ttl)) {
          ttl_expired_files_count++;
        }
      }
    }
  }
  return ttl_expired_files_count;
}
}  // anonymous namespace

void VersionStorageInfo::ComputeCompactionScore(
    const ImmutableCFOptions& immutable_cf_options,
    const MutableCFOptions& mutable_cf_options) {
  for (int level = 0; level <= MaxInputLevel(); level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      int num_sorted_runs = 0;
      uint64_t total_size = 0;
      for (auto* f : files_[level]) {
        if (!f->being_compacted) {
          total_size += f->compensated_file_size;
          num_sorted_runs++;
        }
      }
      if (compaction_style_ == kCompactionStyleUniversal) {
        // For universal compaction, we use level0 score to indicate
        // compaction score for the whole DB. Adding other levels as if
        // they are L0 files.
        for (int i = 1; i < num_levels(); i++) {
          if (!files_[i].empty() && !files_[i][0]->being_compacted) {
            num_sorted_runs++;
          }
        }
      }

      if (compaction_style_ == kCompactionStyleFIFO) {
        score = static_cast<double>(total_size) /
                mutable_cf_options.compaction_options_fifo.max_table_files_size;
        if (mutable_cf_options.compaction_options_fifo.allow_compaction) {
          score = std::max(
              static_cast<double>(num_sorted_runs) /
                  mutable_cf_options.level0_file_num_compaction_trigger,
              score);
        }
        if (mutable_cf_options.ttl > 0) {
          score = std::max(
              static_cast<double>(GetExpiredTtlFilesCount(
                  immutable_cf_options, mutable_cf_options, files_[level])),
              score);
        }

      } else {
        score = static_cast<double>(num_sorted_runs) /
                mutable_cf_options.level0_file_num_compaction_trigger;
        if (compaction_style_ == kCompactionStyleLevel && num_levels() > 1) {
          // Level-based involves L0->L0 compactions that can lead to oversized
          // L0 files. Take into account size as well to avoid later giant
          // compactions to the base level.
          score = std::max(
              score, static_cast<double>(total_size) /
                     mutable_cf_options.max_bytes_for_level_base);
        }
      }
    } else {
      // Compute the ratio of current size to size limit.
      uint64_t level_bytes_no_compacting = 0;
      for (auto f : files_[level]) {
        if (!f->being_compacted) {
          level_bytes_no_compacting += f->compensated_file_size;
        }
      }
      score = static_cast<double>(level_bytes_no_compacting) /
              MaxBytesForLevel(level);
    }
    compaction_level_[level] = level;
    compaction_score_[level] = score;
  }

  // sort all the levels based on their score. Higher scores get listed
  // first. Use bubble sort because the number of entries are small.
  for (int i = 0; i < num_levels() - 2; i++) {
    for (int j = i + 1; j < num_levels() - 1; j++) {
      if (compaction_score_[i] < compaction_score_[j]) {
        double score = compaction_score_[i];
        int level = compaction_level_[i];
        compaction_score_[i] = compaction_score_[j];
        compaction_level_[i] = compaction_level_[j];
        compaction_score_[j] = score;
        compaction_level_[j] = level;
      }
    }
  }
  ComputeFilesMarkedForCompaction();
  ComputeBottommostFilesMarkedForCompaction();
  if (mutable_cf_options.ttl > 0) {
    ComputeExpiredTtlFiles(immutable_cf_options, mutable_cf_options.ttl);
  }
  EstimateCompactionBytesNeeded(mutable_cf_options);
}

void VersionStorageInfo::ComputeFilesMarkedForCompaction() {
  files_marked_for_compaction_.clear();
  int last_qualify_level = 0;

  // Do not include files from the last level with data
  // If table properties collector suggests a file on the last level,
  // we should not move it to a new level.
  for (int level = num_levels() - 1; level >= 1; level--) {
    if (!files_[level].empty()) {
      last_qualify_level = level - 1;
      break;
    }
  }

  for (int level = 0; level <= last_qualify_level; level++) {
    for (auto* f : files_[level]) {
      if (!f->being_compacted && f->marked_for_compaction) {
        files_marked_for_compaction_.emplace_back(level, f);
      }
    }
  }
}

void VersionStorageInfo::ComputeExpiredTtlFiles(
    const ImmutableCFOptions& ioptions, const uint64_t ttl) {
  assert(ttl > 0);

  expired_ttl_files_.clear();

  int64_t _current_time;
  auto status = ioptions.env->GetCurrentTime(&_current_time);
  if (!status.ok()) {
    return;
  }
  const uint64_t current_time = static_cast<uint64_t>(_current_time);

  for (int level = 0; level < num_levels() - 1; level++) {
    for (auto f : files_[level]) {
      if (!f->being_compacted && f->fd.table_reader != nullptr &&
          f->fd.table_reader->GetTableProperties() != nullptr) {
        auto creation_time =
            f->fd.table_reader->GetTableProperties()->creation_time;
        if (creation_time > 0 && creation_time < (current_time - ttl)) {
          expired_ttl_files_.emplace_back(level, f);
        }
      }
    }
  }
}

namespace {

// used to sort files by size
struct Fsize {
  size_t index;
  FileMetaData* file;
};

// Compator that is used to sort files based on their size
// In normal mode: descending size
bool CompareCompensatedSizeDescending(const Fsize& first, const Fsize& second) {
  return (first.file->compensated_file_size >
      second.file->compensated_file_size);
}
} // anonymous namespace

void VersionStorageInfo::AddFile(int level, FileMetaData* f, Logger* info_log) {
  auto* level_files = &files_[level];
  // Must not overlap
#ifndef NDEBUG
  if (level > 0 && !level_files->empty() &&
      internal_comparator_->Compare(
          (*level_files)[level_files->size() - 1]->largest, f->smallest) >= 0) {
    auto* f2 = (*level_files)[level_files->size() - 1];
    if (info_log != nullptr) {
      Error(info_log, "Adding new file %" PRIu64
                      " range (%s, %s) to level %d but overlapping "
                      "with existing file %" PRIu64 " %s %s",
            f->fd.GetNumber(), f->smallest.DebugString(true).c_str(),
            f->largest.DebugString(true).c_str(), level, f2->fd.GetNumber(),
            f2->smallest.DebugString(true).c_str(),
            f2->largest.DebugString(true).c_str());
      LogFlush(info_log);
    }
    assert(false);
  }
#else
  (void)info_log;
#endif
  f->refs++;
  level_files->push_back(f);
}

// Version::PrepareApply() need to be called before calling the function, or
// following functions called:
// 1. UpdateNumNonEmptyLevels();
// 2. CalculateBaseBytes();
// 3. UpdateFilesByCompactionPri();
// 4. GenerateFileIndexer();
// 5. GenerateLevelFilesBrief();
// 6. GenerateLevel0NonOverlapping();
// 7. GenerateBottommostFiles();
void VersionStorageInfo::SetFinalized() {
  finalized_ = true;
#ifndef NDEBUG
  if (compaction_style_ != kCompactionStyleLevel) {
    // Not level based compaction.
    return;
  }
  assert(base_level_ < 0 || num_levels() == 1 ||
         (base_level_ >= 1 && base_level_ < num_levels()));
  // Verify all levels newer than base_level are empty except L0
  for (int level = 1; level < base_level(); level++) {
    assert(NumLevelBytes(level) == 0);
  }
  uint64_t max_bytes_prev_level = 0;
  for (int level = base_level(); level < num_levels() - 1; level++) {
    if (LevelFiles(level).size() == 0) {
      continue;
    }
    assert(MaxBytesForLevel(level) >= max_bytes_prev_level);
    max_bytes_prev_level = MaxBytesForLevel(level);
  }
  int num_empty_non_l0_level = 0;
  for (int level = 0; level < num_levels(); level++) {
    assert(LevelFiles(level).size() == 0 ||
           LevelFiles(level).size() == LevelFilesBrief(level).num_files);
    if (level > 0 && NumLevelBytes(level) > 0) {
      num_empty_non_l0_level++;
    }
    if (LevelFiles(level).size() > 0) {
      assert(level < num_non_empty_levels());
    }
  }
  assert(compaction_level_.size() > 0);
  assert(compaction_level_.size() == compaction_score_.size());
#endif
}

void VersionStorageInfo::UpdateNumNonEmptyLevels() {
  num_non_empty_levels_ = num_levels_;
  for (int i = num_levels_ - 1; i >= 0; i--) {
    if (files_[i].size() != 0) {
      return;
    } else {
      num_non_empty_levels_ = i;
    }
  }
}

namespace {
// Sort `temp` based on ratio of overlapping size over file size
void SortFileByOverlappingRatio(
    const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files,
    const std::vector<FileMetaData*>& next_level_files,
    std::vector<Fsize>* temp) {
  std::unordered_map<uint64_t, uint64_t> file_to_order;
  auto next_level_it = next_level_files.begin();

  for (auto& file : files) {
    uint64_t overlapping_bytes = 0;
    // Skip files in next level that is smaller than current file
    while (next_level_it != next_level_files.end() &&
           icmp.Compare((*next_level_it)->largest, file->smallest) < 0) {
      next_level_it++;
    }

    while (next_level_it != next_level_files.end() &&
           icmp.Compare((*next_level_it)->smallest, file->largest) < 0) {
      overlapping_bytes += (*next_level_it)->fd.file_size;

      if (icmp.Compare((*next_level_it)->largest, file->largest) > 0) {
        // next level file cross large boundary of current file.
        break;
      }
      next_level_it++;
    }

    assert(file->compensated_file_size != 0);
    file_to_order[file->fd.GetNumber()] =
        overlapping_bytes * 1024u / file->compensated_file_size;
  }

  std::sort(temp->begin(), temp->end(),
            [&](const Fsize& f1, const Fsize& f2) -> bool {
              return file_to_order[f1.file->fd.GetNumber()] <
                     file_to_order[f2.file->fd.GetNumber()];
            });
}
}  // namespace

void VersionStorageInfo::UpdateFilesByCompactionPri(
    CompactionPri compaction_pri) {
  if (compaction_style_ == kCompactionStyleNone ||
      compaction_style_ == kCompactionStyleFIFO ||
      compaction_style_ == kCompactionStyleUniversal) {
    // don't need this
    return;
  }
  // No need to sort the highest level because it is never compacted.
  for (int level = 0; level < num_levels() - 1; level++) {
    const std::vector<FileMetaData*>& files = files_[level];
    auto& files_by_compaction_pri = files_by_compaction_pri_[level];
    assert(files_by_compaction_pri.size() == 0);

    // populate a temp vector for sorting based on size
    std::vector<Fsize> temp(files.size());
    for (size_t i = 0; i < files.size(); i++) {
      temp[i].index = i;
      temp[i].file = files[i];
    }

    // sort the top number_of_files_to_sort_ based on file size
    size_t num = VersionStorageInfo::kNumberFilesToSort;
    if (num > temp.size()) {
      num = temp.size();
    }
    switch (compaction_pri) {
      case kByCompensatedSize:
        std::partial_sort(temp.begin(), temp.begin() + num, temp.end(),
                          CompareCompensatedSizeDescending);
        break;
      case kOldestLargestSeqFirst:
        std::sort(temp.begin(), temp.end(),
                  [](const Fsize& f1, const Fsize& f2) -> bool {
                    return f1.file->fd.largest_seqno <
                           f2.file->fd.largest_seqno;
                  });
        break;
      case kOldestSmallestSeqFirst:
        std::sort(temp.begin(), temp.end(),
                  [](const Fsize& f1, const Fsize& f2) -> bool {
                    return f1.file->fd.smallest_seqno <
                           f2.file->fd.smallest_seqno;
                  });
        break;
      case kMinOverlappingRatio:
        SortFileByOverlappingRatio(*internal_comparator_, files_[level],
                                   files_[level + 1], &temp);
        break;
      default:
        assert(false);
    }
    assert(temp.size() == files.size());

    // initialize files_by_compaction_pri_
    for (size_t i = 0; i < temp.size(); i++) {
      files_by_compaction_pri.push_back(static_cast<int>(temp[i].index));
    }
    next_file_to_compact_by_size_[level] = 0;
    assert(files_[level].size() == files_by_compaction_pri_[level].size());
  }
}

void VersionStorageInfo::GenerateLevel0NonOverlapping() {
  assert(!finalized_);
  level0_non_overlapping_ = true;
  if (level_files_brief_.size() == 0) {
    return;
  }

  // A copy of L0 files sorted by smallest key
  std::vector<FdWithKeyRange> level0_sorted_file(
      level_files_brief_[0].files,
      level_files_brief_[0].files + level_files_brief_[0].num_files);
  std::sort(level0_sorted_file.begin(), level0_sorted_file.end(),
            [this](const FdWithKeyRange& f1, const FdWithKeyRange& f2) -> bool {
              return (internal_comparator_->Compare(f1.smallest_key,
                                                    f2.smallest_key) < 0);
            });

  for (size_t i = 1; i < level0_sorted_file.size(); ++i) {
    FdWithKeyRange& f = level0_sorted_file[i];
    FdWithKeyRange& prev = level0_sorted_file[i - 1];
    if (internal_comparator_->Compare(prev.largest_key, f.smallest_key) >= 0) {
      level0_non_overlapping_ = false;
      break;
    }
  }
}

void VersionStorageInfo::GenerateBottommostFiles() {
  assert(!finalized_);
  assert(bottommost_files_.empty());
  for (size_t level = 0; level < level_files_brief_.size(); ++level) {
    for (size_t file_idx = 0; file_idx < level_files_brief_[level].num_files;
         ++file_idx) {
      const FdWithKeyRange& f = level_files_brief_[level].files[file_idx];
      int l0_file_idx;
      if (level == 0) {
        l0_file_idx = static_cast<int>(file_idx);
      } else {
        l0_file_idx = -1;
      }
      Slice smallest_user_key = ExtractUserKey(f.smallest_key);
      Slice largest_user_key = ExtractUserKey(f.largest_key);
      if (!RangeMightExistAfterSortedRun(smallest_user_key, largest_user_key,
                                         static_cast<int>(level),
                                         l0_file_idx)) {
        bottommost_files_.emplace_back(static_cast<int>(level),
                                       f.file_metadata);
      }
    }
  }
}

void VersionStorageInfo::UpdateOldestSnapshot(SequenceNumber seqnum) {
  assert(seqnum >= oldest_snapshot_seqnum_);
  oldest_snapshot_seqnum_ = seqnum;
  if (oldest_snapshot_seqnum_ > bottommost_files_mark_threshold_) {
    ComputeBottommostFilesMarkedForCompaction();
  }
}

void VersionStorageInfo::ComputeBottommostFilesMarkedForCompaction() {
  bottommost_files_marked_for_compaction_.clear();
  bottommost_files_mark_threshold_ = kMaxSequenceNumber;
  for (auto& level_and_file : bottommost_files_) {
    if (!level_and_file.second->being_compacted &&
        level_and_file.second->fd.largest_seqno != 0 &&
        level_and_file.second->num_deletions > 1) {
      // largest_seqno might be nonzero due to containing the final key in an
      // earlier compaction, whose seqnum we didn't zero out. Multiple deletions
      // ensures the file really contains deleted or overwritten keys.
      if (level_and_file.second->fd.largest_seqno < oldest_snapshot_seqnum_) {
        bottommost_files_marked_for_compaction_.push_back(level_and_file);
      } else {
        bottommost_files_mark_threshold_ =
            std::min(bottommost_files_mark_threshold_,
                     level_and_file.second->fd.largest_seqno);
      }
    }
  }
}

void Version::Ref() {
  ++refs_;
}

bool Version::Unref() {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
    return true;
  }
  return false;
}

bool VersionStorageInfo::OverlapInLevel(int level,
                                        const Slice* smallest_user_key,
                                        const Slice* largest_user_key) {
  if (level >= num_non_empty_levels_) {
    // empty level, no overlap
    return false;
  }
  return SomeFileOverlapsRange(*internal_comparator_, (level > 0),
                               level_files_brief_[level], smallest_user_key,
                               largest_user_key);
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// If hint_index is specified, then it points to a file in the
// overlapping range.
// The file_index returns a pointer to any file in an overlapping range.
void VersionStorageInfo::GetOverlappingInputs(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index,
    bool expand_range, InternalKey** next_smallest) const {
  if (level >= num_non_empty_levels_) {
    // this level is empty, no overlapping inputs
    return;
  }

  inputs->clear();
  if (file_index) {
    *file_index = -1;
  }
  const Comparator* user_cmp = user_comparator_;
  if (level > 0) {
    GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs, hint_index,
                                          file_index, false, next_smallest);
    return;
  }

  if (next_smallest) {
    // next_smallest key only makes sense for non-level 0, where files are
    // non-overlapping
    *next_smallest = nullptr;
  }

  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }

  // index stores the file index need to check.
  std::list<size_t> index;
  for (size_t i = 0; i < level_files_brief_[level].num_files; i++) {
    index.emplace_back(i);
  }

  while (!index.empty()) {
    bool found_overlapping_file = false;
    auto iter = index.begin();
    while (iter != index.end()) {
      FdWithKeyRange* f = &(level_files_brief_[level].files[*iter]);
      const Slice file_start = ExtractUserKey(f->smallest_key);
      const Slice file_limit = ExtractUserKey(f->largest_key);
      if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
        // "f" is completely before specified range; skip it
        iter++;
      } else if (end != nullptr &&
                 user_cmp->Compare(file_start, user_end) > 0) {
        // "f" is completely after specified range; skip it
        iter++;
      } else {
        // if overlap
        inputs->emplace_back(files_[level][*iter]);
        found_overlapping_file = true;
        // record the first file index.
        if (file_index && *file_index == -1) {
          *file_index = static_cast<int>(*iter);
        }
        // the related file is overlap, erase to avoid checking again.
        iter = index.erase(iter);
        if (expand_range) {
          if (begin != nullptr &&
              user_cmp->Compare(file_start, user_begin) < 0) {
            user_begin = file_start;
          }
          if (end != nullptr && user_cmp->Compare(file_limit, user_end) > 0) {
            user_end = file_limit;
          }
        }
      }
    }
    // if all the files left are not overlap, break
    if (!found_overlapping_file) {
      break;
    }
  }
}

// Store in "*inputs" files in "level" that within range [begin,end]
// Guarantee a "clean cut" boundary between the files in inputs
// and the surrounding files and the maxinum number of files.
// This will ensure that no parts of a key are lost during compaction.
// If hint_index is specified, then it points to a file in the range.
// The file_index returns a pointer to any file in an overlapping range.
void VersionStorageInfo::GetCleanInputsWithinInterval(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index) const {
  inputs->clear();
  if (file_index) {
    *file_index = -1;
  }
  if (level >= num_non_empty_levels_ || level == 0 ||
      level_files_brief_[level].num_files == 0) {
    // this level is empty, no inputs within range
    // also don't support clean input interval within L0
    return;
  }

  const auto& level_files = level_files_brief_[level];
  if (begin == nullptr) {
    begin = &level_files.files[0].file_metadata->smallest;
  }
  if (end == nullptr) {
    end = &level_files.files[level_files.num_files - 1].file_metadata->largest;
  }

  GetOverlappingInputsRangeBinarySearch(level, begin, end, inputs,
                                        hint_index, file_index,
                                        true /* within_interval */);
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// Employ binary search to find at least one file that overlaps the
// specified range. From that file, iterate backwards and
// forwards to find all overlapping files.
// if within_range is set, then only store the maximum clean inputs
// within range [begin, end]. "clean" means there is a boudnary
// between the files in "*inputs" and the surrounding files
void VersionStorageInfo::GetOverlappingInputsRangeBinarySearch(
    int level, const InternalKey* begin, const InternalKey* end,
    std::vector<FileMetaData*>* inputs, int hint_index, int* file_index,
    bool within_interval, InternalKey** next_smallest) const {
  assert(level > 0);
  int min = 0;
  int mid = 0;
  int max = static_cast<int>(files_[level].size()) - 1;
  bool foundOverlap = false;
  auto user_cmp = user_comparator_;

  // if the caller already knows the index of a file that has overlap,
  // then we can skip the binary search.
  if (hint_index != -1) {
    mid = hint_index;
    foundOverlap = true;
  }

  while (!foundOverlap && min <= max) {
    mid = (min + max)/2;
    FdWithKeyRange* f = &(level_files_brief_[level].files[mid]);
    auto& smallest = f->file_metadata->smallest;
    auto& largest = f->file_metadata->largest;
    if ((!within_interval && sstableKeyCompare(user_cmp, begin, largest) > 0) ||
        (within_interval && sstableKeyCompare(user_cmp, begin, smallest) > 0)) {
      min = mid + 1;
    } else if ((!within_interval &&
                sstableKeyCompare(user_cmp, smallest, end) > 0) ||
               (within_interval &&
                sstableKeyCompare(user_cmp, largest, end) > 0)) {
      max = mid - 1;
    } else {
      foundOverlap = true;
      break;
    }
  }

  // If there were no overlapping files, return immediately.
  if (!foundOverlap) {
    if (next_smallest) {
      next_smallest = nullptr;
    }
    return;
  }
  // returns the index where an overlap is found
  if (file_index) {
    *file_index = mid;
  }

  int start_index, end_index;
  if (within_interval) {
    ExtendFileRangeWithinInterval(level, begin, end, mid,
                                  &start_index, &end_index);
  } else {
    ExtendFileRangeOverlappingInterval(level, begin, end, mid,
                                       &start_index, &end_index);
    assert(end_index >= start_index);
  }
  // insert overlapping files into vector
  for (int i = start_index; i <= end_index; i++) {
    inputs->push_back(files_[level][i]);
  }

  if (next_smallest != nullptr) {
    // Provide the next key outside the range covered by inputs
    if (++end_index < static_cast<int>(files_[level].size())) {
      **next_smallest = files_[level][end_index]->smallest;
    } else {
      *next_smallest = nullptr;
    }
  }
}

// Store in *start_index and *end_index the range of all files in
// "level" that overlap [begin,end]
// The mid_index specifies the index of at least one file that
// overlaps the specified range. From that file, iterate backward
// and forward to find all overlapping files.
// Use FileLevel in searching, make it faster
void VersionStorageInfo::ExtendFileRangeOverlappingInterval(
    int level, const InternalKey* begin, const InternalKey* end,
    unsigned int mid_index, int* start_index, int* end_index) const {
  auto user_cmp = user_comparator_;
  const FdWithKeyRange* files = level_files_brief_[level].files;
#ifndef NDEBUG
  {
    // assert that the file at mid_index overlaps with the range
    assert(mid_index < level_files_brief_[level].num_files);
    const FdWithKeyRange* f = &files[mid_index];
    auto& smallest = f->file_metadata->smallest;
    auto& largest = f->file_metadata->largest;
    if (sstableKeyCompare(user_cmp, begin, smallest) <= 0) {
      assert(sstableKeyCompare(user_cmp, smallest, end) <= 0);
    } else {
      // fprintf(stderr, "ExtendFileRangeOverlappingInterval\n%s - %s\n%s - %s\n%d %d\n",
      //         begin ? begin->DebugString().c_str() : "(null)",
      //         end ? end->DebugString().c_str() : "(null)",
      //         smallest->DebugString().c_str(),
      //         largest->DebugString().c_str(),
      //         sstableKeyCompare(user_cmp, smallest, begin),
      //         sstableKeyCompare(user_cmp, largest, begin));
      assert(sstableKeyCompare(user_cmp, begin, largest) <= 0);
    }
  }
#endif
  *start_index = mid_index + 1;
  *end_index = mid_index;
  int count __attribute__((__unused__));
  count = 0;

  // check backwards from 'mid' to lower indices
  for (int i = mid_index; i >= 0 ; i--) {
    const FdWithKeyRange* f = &files[i];
    auto& largest = f->file_metadata->largest;
    if (sstableKeyCompare(user_cmp, begin, largest) <= 0) {
      *start_index = i;
      assert((count++, true));
    } else {
      break;
    }
  }
  // check forward from 'mid+1' to higher indices
  for (unsigned int i = mid_index+1;
       i < level_files_brief_[level].num_files; i++) {
    const FdWithKeyRange* f = &files[i];
    auto& smallest = f->file_metadata->smallest;
    if (sstableKeyCompare(user_cmp, smallest, end) <= 0) {
      assert((count++, true));
      *end_index = i;
    } else {
      break;
    }
  }
  assert(count == *end_index - *start_index + 1);
}

// Store in *start_index and *end_index the clean range of all files in
// "level" within [begin,end]
// The mid_index specifies the index of at least one file within
// the specified range. From that file, iterate backward
// and forward to find all overlapping files and then "shrink" to
// the clean range required.
// Use FileLevel in searching, make it faster
void VersionStorageInfo::ExtendFileRangeWithinInterval(
    int level, const InternalKey* begin, const InternalKey* end,
    unsigned int mid_index, int* start_index, int* end_index) const {
  assert(level != 0);
  auto* user_cmp = user_comparator_;
  const FdWithKeyRange* files = level_files_brief_[level].files;
#ifndef NDEBUG
  {
    // assert that the file at mid_index is within the range
    assert(mid_index < level_files_brief_[level].num_files);
    const FdWithKeyRange* f = &files[mid_index];
    auto& smallest = f->file_metadata->smallest;
    auto& largest = f->file_metadata->largest;
    assert(sstableKeyCompare(user_cmp, begin, smallest) <= 0 &&
           sstableKeyCompare(user_cmp, largest, end) <= 0);
  }
#endif
  ExtendFileRangeOverlappingInterval(level, begin, end, mid_index,
                                     start_index, end_index);
  int left = *start_index;
  int right = *end_index;
  // shrink from left to right
  while (left <= right) {
    auto& smallest = files[left].file_metadata->smallest;
    if (sstableKeyCompare(user_cmp, begin, smallest) > 0) {
      left++;
      continue;
    }
    if (left > 0) {  // If not first file
      auto& largest = files[left - 1].file_metadata->largest;
      if (sstableKeyCompare(user_cmp, smallest, largest) == 0) {
        left++;
        continue;
      }
    }
    break;
  }
  // shrink from right to left
  while (left <= right) {
    auto& largest = files[right].file_metadata->largest;
    if (sstableKeyCompare(user_cmp, largest, end) > 0) {
      right--;
      continue;
    }
    if (right < static_cast<int>(level_files_brief_[level].num_files) -
                    1) {  // If not the last file
      auto& smallest = files[right + 1].file_metadata->smallest;
      if (sstableKeyCompare(user_cmp, smallest, largest) == 0) {
        // The last user key in range overlaps with the next file's first key
        right--;
        continue;
      }
    }
    break;
  }

  *start_index = left;
  *end_index = right;
}

uint64_t VersionStorageInfo::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < num_levels());
  return TotalFileSize(files_[level]);
}

const char* VersionStorageInfo::LevelSummary(
    LevelSummaryStorage* scratch) const {
  int len = 0;
  if (compaction_style_ == kCompactionStyleLevel && num_levels() > 1) {
    assert(base_level_ < static_cast<int>(level_max_bytes_.size()));
    if (level_multiplier_ != 0.0) {
      len = snprintf(
          scratch->buffer, sizeof(scratch->buffer),
          "base level %d level multiplier %.2f max bytes base %" PRIu64 " ",
          base_level_, level_multiplier_, level_max_bytes_[base_level_]);
    }
  }
  len +=
      snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "files[");
  for (int i = 0; i < num_levels(); i++) {
    int sz = sizeof(scratch->buffer) - len;
    int ret = snprintf(scratch->buffer + len, sz, "%d ", int(files_[i].size()));
    if (ret < 0 || ret >= sz) break;
    len += ret;
  }
  if (len > 0) {
    // overwrite the last space
    --len;
  }
  len += snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
                  "] max score %.2f", compaction_score_[0]);

  if (!files_marked_for_compaction_.empty()) {
    snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len,
             " (%" ROCKSDB_PRIszt " files need compaction)",
             files_marked_for_compaction_.size());
  }

  return scratch->buffer;
}

const char* VersionStorageInfo::LevelFileSummary(FileSummaryStorage* scratch,
                                                 int level) const {
  int len = snprintf(scratch->buffer, sizeof(scratch->buffer), "files_size[");
  for (const auto& f : files_[level]) {
    int sz = sizeof(scratch->buffer) - len;
    char sztxt[16];
    AppendHumanBytes(f->fd.GetFileSize(), sztxt, sizeof(sztxt));
    int ret = snprintf(scratch->buffer + len, sz,
                       "#%" PRIu64 "(seq=%" PRIu64 ",sz=%s,%d) ",
                       f->fd.GetNumber(), f->fd.smallest_seqno, sztxt,
                       static_cast<int>(f->being_compacted));
    if (ret < 0 || ret >= sz)
      break;
    len += ret;
  }
  // overwrite the last space (only if files_[level].size() is non-zero)
  if (files_[level].size() && len > 0) {
    --len;
  }
  snprintf(scratch->buffer + len, sizeof(scratch->buffer) - len, "]");
  return scratch->buffer;
}

int64_t VersionStorageInfo::MaxNextLevelOverlappingBytes() {
  uint64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < num_levels() - 1; level++) {
    for (const auto& f : files_[level]) {
      GetOverlappingInputs(level + 1, &f->smallest, &f->largest, &overlaps);
      const uint64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

uint64_t VersionStorageInfo::MaxBytesForLevel(int level) const {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  assert(level >= 0);
  assert(level < static_cast<int>(level_max_bytes_.size()));
  return level_max_bytes_[level];
}

void VersionStorageInfo::CalculateBaseBytes(const ImmutableCFOptions& ioptions,
                                            const MutableCFOptions& options) {
  // Special logic to set number of sorted runs.
  // It is to match the previous behavior when all files are in L0.
  int num_l0_count = static_cast<int>(files_[0].size());
  if (compaction_style_ == kCompactionStyleUniversal) {
    // For universal compaction, we use level0 score to indicate
    // compaction score for the whole DB. Adding other levels as if
    // they are L0 files.
    for (int i = 1; i < num_levels(); i++) {
      if (!files_[i].empty()) {
        num_l0_count++;
      }
    }
  }
  set_l0_delay_trigger_count(num_l0_count);

  level_max_bytes_.resize(ioptions.num_levels);
  if (!ioptions.level_compaction_dynamic_level_bytes) {
    base_level_ = (ioptions.compaction_style == kCompactionStyleLevel) ? 1 : -1;

    // Calculate for static bytes base case
    for (int i = 0; i < ioptions.num_levels; ++i) {
      if (i == 0 && ioptions.compaction_style == kCompactionStyleUniversal) {
        level_max_bytes_[i] = options.max_bytes_for_level_base;
      } else if (i > 1) {
        level_max_bytes_[i] = MultiplyCheckOverflow(
            MultiplyCheckOverflow(level_max_bytes_[i - 1],
                                  options.max_bytes_for_level_multiplier),
            options.MaxBytesMultiplerAdditional(i - 1));
      } else {
        level_max_bytes_[i] = options.max_bytes_for_level_base;
      }
    }
  } else {
    uint64_t max_level_size = 0;

    int first_non_empty_level = -1;
    // Find size of non-L0 level of most data.
    // Cannot use the size of the last level because it can be empty or less
    // than previous levels after compaction.
    for (int i = 1; i < num_levels_; i++) {
      uint64_t total_size = 0;
      for (const auto& f : files_[i]) {
        total_size += f->fd.GetFileSize();
      }
      if (total_size > 0 && first_non_empty_level == -1) {
        first_non_empty_level = i;
      }
      if (total_size > max_level_size) {
        max_level_size = total_size;
      }
    }

    // Prefill every level's max bytes to disallow compaction from there.
    for (int i = 0; i < num_levels_; i++) {
      level_max_bytes_[i] = std::numeric_limits<uint64_t>::max();
    }

    if (max_level_size == 0) {
      // No data for L1 and up. L0 compacts to last level directly.
      // No compaction from L1+ needs to be scheduled.
      base_level_ = num_levels_ - 1;
    } else {
      uint64_t l0_size = 0;
      for (const auto& f : files_[0]) {
        l0_size += f->fd.GetFileSize();
      }

      uint64_t base_bytes_max =
          std::max(options.max_bytes_for_level_base, l0_size);
      uint64_t base_bytes_min = static_cast<uint64_t>(
          base_bytes_max / options.max_bytes_for_level_multiplier);

      // Try whether we can make last level's target size to be max_level_size
      uint64_t cur_level_size = max_level_size;
      for (int i = num_levels_ - 2; i >= first_non_empty_level; i--) {
        // Round up after dividing
        cur_level_size = static_cast<uint64_t>(
            cur_level_size / options.max_bytes_for_level_multiplier);
      }

      // Calculate base level and its size.
      uint64_t base_level_size;
      if (cur_level_size <= base_bytes_min) {
        // Case 1. If we make target size of last level to be max_level_size,
        // target size of the first non-empty level would be smaller than
        // base_bytes_min. We set it be base_bytes_min.
        base_level_size = base_bytes_min + 1U;
        base_level_ = first_non_empty_level;
        ROCKS_LOG_WARN(ioptions.info_log,
                       "More existing levels in DB than needed. "
                       "max_bytes_for_level_multiplier may not be guaranteed.");
      } else {
        // Find base level (where L0 data is compacted to).
        base_level_ = first_non_empty_level;
        while (base_level_ > 1 && cur_level_size > base_bytes_max) {
          --base_level_;
          cur_level_size = static_cast<uint64_t>(
              cur_level_size / options.max_bytes_for_level_multiplier);
        }
        if (cur_level_size > base_bytes_max) {
          // Even L1 will be too large
          assert(base_level_ == 1);
          base_level_size = base_bytes_max;
        } else {
          base_level_size = cur_level_size;
        }
      }

      level_multiplier_ = options.max_bytes_for_level_multiplier;
      assert(base_level_size > 0);
      if (l0_size > base_level_size &&
          (l0_size > options.max_bytes_for_level_base ||
           static_cast<int>(files_[0].size() / 2) >=
               options.level0_file_num_compaction_trigger)) {
        // We adjust the base level according to actual L0 size, and adjust
        // the level multiplier accordingly, when:
        //   1. the L0 size is larger than level size base, or
        //   2. number of L0 files reaches twice the L0->L1 compaction trigger
        // We don't do this otherwise to keep the LSM-tree structure stable
        // unless the L0 compation is backlogged.
        base_level_size = l0_size;
        if (base_level_ == num_levels_ - 1) {
          level_multiplier_ = 1.0;
        } else {
          level_multiplier_ = std::pow(
              static_cast<double>(max_level_size) /
                  static_cast<double>(base_level_size),
              1.0 / static_cast<double>(num_levels_ - base_level_ - 1));
        }
      }

      uint64_t level_size = base_level_size;
      for (int i = base_level_; i < num_levels_; i++) {
        if (i > base_level_) {
          level_size = MultiplyCheckOverflow(level_size, level_multiplier_);
        }
        // Don't set any level below base_bytes_max. Otherwise, the LSM can
        // assume an hourglass shape where L1+ sizes are smaller than L0. This
        // causes compaction scoring, which depends on level sizes, to favor L1+
        // at the expense of L0, which may fill up and stall.
        level_max_bytes_[i] = std::max(level_size, base_bytes_max);
      }
    }
  }
}

uint64_t VersionStorageInfo::EstimateLiveDataSize() const {
  // Estimate the live data size by adding up the size of the last level for all
  // key ranges. Note: Estimate depends on the ordering of files in level 0
  // because files in level 0 can be overlapping.
  uint64_t size = 0;

  auto ikey_lt = [this](InternalKey* x, InternalKey* y) {
    return internal_comparator_->Compare(*x, *y) < 0;
  };
  // (Ordered) map of largest keys in non-overlapping files
  std::map<InternalKey*, FileMetaData*, decltype(ikey_lt)> ranges(ikey_lt);

  for (int l = num_levels_ - 1; l >= 0; l--) {
    bool found_end = false;
    for (auto file : files_[l]) {
      // Find the first file where the largest key is larger than the smallest
      // key of the current file. If this file does not overlap with the
      // current file, none of the files in the map does. If there is
      // no potential overlap, we can safely insert the rest of this level
      // (if the level is not 0) into the map without checking again because
      // the elements in the level are sorted and non-overlapping.
      auto lb = (found_end && l != 0) ?
        ranges.end() : ranges.lower_bound(&file->smallest);
      found_end = (lb == ranges.end());
      if (found_end || internal_comparator_->Compare(
            file->largest, (*lb).second->smallest) < 0) {
          ranges.emplace_hint(lb, &file->largest, file);
          size += file->fd.file_size;
      }
    }
  }
  return size;
}

bool VersionStorageInfo::RangeMightExistAfterSortedRun(
    const Slice& smallest_user_key, const Slice& largest_user_key,
    int last_level, int last_l0_idx) {
  assert((last_l0_idx != -1) == (last_level == 0));
  // TODO(ajkr): this preserves earlier behavior where we considered an L0 file
  // bottommost only if it's the oldest L0 file and there are no files on older
  // levels. It'd be better to consider it bottommost if there's no overlap in
  // older levels/files.
  if (last_level == 0 &&
      last_l0_idx != static_cast<int>(LevelFiles(0).size() - 1)) {
    return true;
  }

  // Checks whether there are files living beyond the `last_level`. If lower
  // levels have files, it checks for overlap between [`smallest_key`,
  // `largest_key`] and those files. Bottomlevel optimizations can be made if
  // there are no files in lower levels or if there is no overlap with the files
  // in the lower levels.
  for (int level = last_level + 1; level < num_levels(); level++) {
    // The range is not in the bottommost level if there are files in lower
    // levels when the `last_level` is 0 or if there are files in lower levels
    // which overlap with [`smallest_key`, `largest_key`].
    if (files_[level].size() > 0 &&
        (last_level == 0 ||
         OverlapInLevel(level, &smallest_user_key, &largest_user_key))) {
      return true;
    }
  }
  return false;
}

void Version::AddLiveFiles(std::vector<FileDescriptor>* live) {
  for (int level = 0; level < storage_info_.num_levels(); level++) {
    const std::vector<FileMetaData*>& files = storage_info_.files_[level];
    for (const auto& file : files) {
      live->push_back(file->fd);
    }
  }
}

std::string Version::DebugString(bool hex, bool print_stats) const {
  std::string r;
  for (int level = 0; level < storage_info_.num_levels_; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    //
    // if print_stats=true:
    //   17:123['a' .. 'd'](4096)
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" --- version# ");
    AppendNumberTo(&r, version_number_);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = storage_info_.files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->fd.GetNumber());
      r.push_back(':');
      AppendNumberTo(&r, files[i]->fd.GetFileSize());
      r.append("[");
      r.append(files[i]->smallest.DebugString(hex));
      r.append(" .. ");
      r.append(files[i]->largest.DebugString(hex));
      r.append("]");
      if (print_stats) {
        r.append("(");
        r.append(ToString(
            files[i]->stats.num_reads_sampled.load(std::memory_order_relaxed)));
        r.append(")");
      }
      r.append("\n");
    }
  }
  return r;
}

// this is used to batch writes to the manifest file
struct VersionSet::ManifestWriter {
  Status status;
  bool done;
  InstrumentedCondVar cv;
  ColumnFamilyData* cfd;
  const MutableCFOptions mutable_cf_options;
  const autovector<VersionEdit*>& edit_list;

  explicit ManifestWriter(InstrumentedMutex* mu, ColumnFamilyData* _cfd,
                          const MutableCFOptions& cf_options,
                          const autovector<VersionEdit*>& e)
      : done(false),
        cv(mu),
        cfd(_cfd),
        mutable_cf_options(cf_options),
        edit_list(e) {}
};

VersionSet::VersionSet(const std::string& dbname,
                       const ImmutableDBOptions* _db_options,
                       const EnvOptions& storage_options, Cache* table_cache,
                       WriteBufferManager* write_buffer_manager,
                       WriteController* write_controller)
    : column_family_set_(
          new ColumnFamilySet(dbname, _db_options, storage_options, table_cache,
                              write_buffer_manager, write_controller)),
      env_(_db_options->env),
      dbname_(dbname),
      db_options_(_db_options),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      options_file_number_(0),
      pending_manifest_file_number_(0),
      last_sequence_(0),
      last_allocated_sequence_(0),
      last_published_sequence_(0),
      prev_log_number_(0),
      current_version_number_(0),
      manifest_file_size_(0),
      env_options_(storage_options) {}

void CloseTables(void* ptr, size_t) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(ptr);
  table_reader->Close();
}

VersionSet::~VersionSet() {
  // we need to delete column_family_set_ because its destructor depends on
  // VersionSet
  Cache* table_cache = column_family_set_->get_table_cache();
  table_cache->ApplyToAllCacheEntries(&CloseTables, false /* thread_safe */);
  column_family_set_.reset();
  for (auto& file : obsolete_files_) {
    if (file.metadata->table_reader_handle) {
      table_cache->Release(file.metadata->table_reader_handle);
      TableCache::Evict(table_cache, file.metadata->fd.GetNumber());
    }
    file.DeleteMetadata();
  }
  obsolete_files_.clear();
}

void VersionSet::AppendVersion(ColumnFamilyData* column_family_data,
                               Version* v) {
  // compute new compaction score
  v->storage_info()->ComputeCompactionScore(
      *column_family_data->ioptions(),
      *column_family_data->GetLatestMutableCFOptions());

  // Mark v finalized
  v->storage_info_.SetFinalized();

  // Make "v" current
  assert(v->refs_ == 0);
  Version* current = column_family_data->current();
  assert(v != current);
  if (current != nullptr) {
    assert(current->refs_ > 0);
    current->Unref();
  }
  column_family_data->SetCurrent(v);
  v->Ref();

  // Append to linked list
  v->prev_ = column_family_data->dummy_versions()->prev_;
  v->next_ = column_family_data->dummy_versions();
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::ProcessManifestWrites(
    std::deque<ManifestWriter>& writers, InstrumentedMutex* mu,
    Directory* db_directory, bool new_descriptor_log,
    const ColumnFamilyOptions* new_cf_options) {
  assert(!writers.empty());
  ManifestWriter& first_writer = writers.front();
  ManifestWriter* last_writer = &first_writer;

  assert(!manifest_writers_.empty());
  assert(manifest_writers_.front() == &first_writer);

  autovector<VersionEdit*> batch_edits;
  autovector<Version*> versions;
  autovector<const MutableCFOptions*> mutable_cf_options_ptrs;
  std::vector<std::unique_ptr<BaseReferencedVersionBuilder>> builder_guards;

  if (first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
    // No group commits for column family add or drop
    LogAndApplyCFHelper(first_writer.edit_list.front());
    batch_edits.push_back(first_writer.edit_list.front());
  } else {
    auto it = manifest_writers_.cbegin();
    size_t group_start = std::numeric_limits<size_t>::max();
    while (it != manifest_writers_.cend()) {
      if ((*it)->edit_list.front()->IsColumnFamilyManipulation()) {
        // no group commits for column family add or drop
        break;
      }
      last_writer = *(it++);
      assert(last_writer != nullptr);
      assert(last_writer->cfd != nullptr);
      if (last_writer->cfd->IsDropped()) {
        // If we detect a dropped CF at this point, and the corresponding
        // version edits belong to an atomic group, then we need to find out
        // the preceding version edits in the same atomic group, and update
        // their `remaining_entries_` member variable because we are NOT going
        // to write the version edits' of dropped CF to the MANIFEST. If we
        // don't update, then Recover can report corrupted atomic group because
        // the `remaining_entries_` do not match.
        if (!batch_edits.empty()) {
          if (batch_edits.back()->is_in_atomic_group_ &&
              batch_edits.back()->remaining_entries_ > 0) {
            assert(group_start < batch_edits.size());
            const auto& edit_list = last_writer->edit_list;
            size_t k = 0;
            while (k < edit_list.size()) {
              if (!edit_list[k]->is_in_atomic_group_) {
                break;
              } else if (edit_list[k]->remaining_entries_ == 0) {
                ++k;
                break;
              }
              ++k;
            }
            for (auto i = group_start; i < batch_edits.size(); ++i) {
              assert(static_cast<uint32_t>(k) <=
                     batch_edits.back()->remaining_entries_);
              batch_edits[i]->remaining_entries_ -= static_cast<uint32_t>(k);
            }
          }
        }
        continue;
      }
      // We do a linear search on versions because versions is small.
      // TODO(yanqin) maybe consider unordered_map
      Version* version = nullptr;
      VersionBuilder* builder = nullptr;
      for (int i = 0; i != static_cast<int>(versions.size()); ++i) {
        uint32_t cf_id = last_writer->cfd->GetID();
        if (versions[i]->cfd()->GetID() == cf_id) {
          version = versions[i];
          assert(!builder_guards.empty() &&
                 builder_guards.size() == versions.size());
          builder = builder_guards[i]->version_builder();
          TEST_SYNC_POINT_CALLBACK(
              "VersionSet::ProcessManifestWrites:SameColumnFamily", &cf_id);
          break;
        }
      }
      if (version == nullptr) {
        version = new Version(last_writer->cfd, this, env_options_,
                              last_writer->mutable_cf_options,
                              current_version_number_++);
        versions.push_back(version);
        mutable_cf_options_ptrs.push_back(&last_writer->mutable_cf_options);
        builder_guards.emplace_back(
            new BaseReferencedVersionBuilder(last_writer->cfd));
        builder = builder_guards.back()->version_builder();
      }
      assert(builder != nullptr);  // make checker happy
      for (const auto& e : last_writer->edit_list) {
        if (e->is_in_atomic_group_) {
          if (batch_edits.empty() || !batch_edits.back()->is_in_atomic_group_ ||
              (batch_edits.back()->is_in_atomic_group_ &&
               batch_edits.back()->remaining_entries_ == 0)) {
            group_start = batch_edits.size();
          }
        } else if (group_start != std::numeric_limits<size_t>::max()) {
          group_start = std::numeric_limits<size_t>::max();
        }
        LogAndApplyHelper(last_writer->cfd, builder, version, e, mu);
        batch_edits.push_back(e);
      }
    }
    for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
      assert(!builder_guards.empty() &&
             builder_guards.size() == versions.size());
      auto* builder = builder_guards[i]->version_builder();
      builder->SaveTo(versions[i]->storage_info());
    }
  }

#ifndef NDEBUG
  // Verify that version edits of atomic groups have correct
  // remaining_entries_.
  size_t k = 0;
  while (k < batch_edits.size()) {
    while (k < batch_edits.size() && !batch_edits[k]->is_in_atomic_group_) {
      ++k;
    }
    if (k == batch_edits.size()) {
      break;
    }
    size_t i = k;
    while (i < batch_edits.size()) {
      if (!batch_edits[i]->is_in_atomic_group_) {
        break;
      }
      assert(i - k + batch_edits[i]->remaining_entries_ ==
             batch_edits[k]->remaining_entries_);
      if (batch_edits[i]->remaining_entries_ == 0) {
        ++i;
        break;
      }
      ++i;
    }
    assert(batch_edits[i - 1]->is_in_atomic_group_);
    assert(0 == batch_edits[i - 1]->remaining_entries_);
    std::vector<VersionEdit*> tmp;
    for (size_t j = k; j != i; ++j) {
      tmp.emplace_back(batch_edits[j]);
    }
    TEST_SYNC_POINT_CALLBACK(
        "VersionSet::ProcessManifestWrites:CheckOneAtomicGroup", &tmp);
    k = i;
  }
#endif  // NDEBUG

  uint64_t new_manifest_file_size = 0;
  Status s;

  assert(pending_manifest_file_number_ == 0);
  if (!descriptor_log_ ||
      manifest_file_size_ > db_options_->max_manifest_file_size) {
    pending_manifest_file_number_ = NewFileNumber();
    batch_edits.back()->SetNextFile(next_file_number_.load());
    new_descriptor_log = true;
  } else {
    pending_manifest_file_number_ = manifest_file_number_;
  }

  if (new_descriptor_log) {
    // if we are writing out new snapshot make sure to persist max column
    // family.
    if (column_family_set_->GetMaxColumnFamily() > 0) {
      first_writer.edit_list.front()->SetMaxColumnFamily(
          column_family_set_->GetMaxColumnFamily());
    }
  }

  {
    EnvOptions opt_env_opts = env_->OptimizeForManifestWrite(env_options_);
    mu->Unlock();

    TEST_SYNC_POINT("VersionSet::LogAndApply:WriteManifest");
    if (!first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
      for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
        assert(!builder_guards.empty() &&
               builder_guards.size() == versions.size());
        assert(!mutable_cf_options_ptrs.empty() &&
               builder_guards.size() == versions.size());
        ColumnFamilyData* cfd = versions[i]->cfd_;
        builder_guards[i]->version_builder()->LoadTableHandlers(
            cfd->internal_stats(), cfd->ioptions()->optimize_filters_for_hits,
            true /* prefetch_index_and_filter_in_cache */,
            false /* is_initial_load */,
            mutable_cf_options_ptrs[i]->prefix_extractor.get());
      }
    }

    // This is fine because everything inside of this block is serialized --
    // only one thread can be here at the same time
    if (new_descriptor_log) {
      // create new manifest file
      ROCKS_LOG_INFO(db_options_->info_log, "Creating manifest %" PRIu64 "\n",
                     pending_manifest_file_number_);
      std::string descriptor_fname =
          DescriptorFileName(dbname_, pending_manifest_file_number_);
      std::unique_ptr<WritableFile> descriptor_file;
      s = NewWritableFile(env_, descriptor_fname, &descriptor_file,
                          opt_env_opts);
      if (s.ok()) {
        descriptor_file->SetPreallocationBlockSize(
            db_options_->manifest_preallocation_size);

        std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
            std::move(descriptor_file), descriptor_fname, opt_env_opts, env_,
            nullptr, db_options_->listeners));
        descriptor_log_.reset(
            new log::Writer(std::move(file_writer), 0, false));
        s = WriteSnapshot(descriptor_log_.get());
      }
    }

    if (!first_writer.edit_list.front()->IsColumnFamilyManipulation()) {
      for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
        versions[i]->PrepareApply(*mutable_cf_options_ptrs[i], true);
      }
    }

    // Write new records to MANIFEST log
    if (s.ok()) {
#ifndef NDEBUG
      size_t idx = 0;
#endif
      for (auto& e : batch_edits) {
        std::string record;
        if (!e->EncodeTo(&record)) {
          s = Status::Corruption("Unable to encode VersionEdit:" +
                                 e->DebugString(true));
          break;
        }
        TEST_KILL_RANDOM("VersionSet::LogAndApply:BeforeAddRecord",
                         rocksdb_kill_odds * REDUCE_ODDS2);
#ifndef NDEBUG
        if (batch_edits.size() > 1 && batch_edits.size() - 1 == idx) {
          TEST_SYNC_POINT(
              "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0");
          TEST_SYNC_POINT(
              "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:1");
        }
        ++idx;
#endif /* !NDEBUG */
        s = descriptor_log_->AddRecord(record);
        if (!s.ok()) {
          break;
        }
      }
      if (s.ok()) {
        s = SyncManifest(env_, db_options_, descriptor_log_->file());
      }
      if (!s.ok()) {
        ROCKS_LOG_ERROR(db_options_->info_log, "MANIFEST write %s\n",
                        s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    if (s.ok() && new_descriptor_log) {
      s = SetCurrentFile(env_, dbname_, pending_manifest_file_number_,
                         db_directory);
    }

    if (s.ok()) {
      // find offset in manifest file where this version is stored.
      new_manifest_file_size = descriptor_log_->file()->GetFileSize();
    }

    if (first_writer.edit_list.front()->is_column_family_drop_) {
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:0");
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:1");
      TEST_SYNC_POINT("VersionSet::LogAndApply::ColumnFamilyDrop:2");
    }

    LogFlush(db_options_->info_log);
    TEST_SYNC_POINT("VersionSet::LogAndApply:WriteManifestDone");
    mu->Lock();
  }

  // Append the old manifest file to the obsolete_manifest_ list to be deleted
  // by PurgeObsoleteFiles later.
  if (s.ok() && new_descriptor_log) {
    obsolete_manifests_.emplace_back(
        DescriptorFileName("", manifest_file_number_));
  }

  // Install the new versions
  if (s.ok()) {
    if (first_writer.edit_list.front()->is_column_family_add_) {
      assert(batch_edits.size() == 1);
      assert(new_cf_options != nullptr);
      CreateColumnFamily(*new_cf_options, first_writer.edit_list.front());
    } else if (first_writer.edit_list.front()->is_column_family_drop_) {
      assert(batch_edits.size() == 1);
      first_writer.cfd->SetDropped();
      if (first_writer.cfd->Unref()) {
        delete first_writer.cfd;
      }
    } else {
      // Each version in versions corresponds to a column family.
      // For each column family, update its log number indicating that logs
      // with number smaller than this should be ignored.
      for (const auto version : versions) {
        uint64_t max_log_number_in_batch = 0;
        uint32_t cf_id = version->cfd_->GetID();
        for (const auto& e : batch_edits) {
          if (e->has_log_number_ && e->column_family_ == cf_id) {
            max_log_number_in_batch =
                std::max(max_log_number_in_batch, e->log_number_);
          }
        }
        if (max_log_number_in_batch != 0) {
          assert(version->cfd_->GetLogNumber() <= max_log_number_in_batch);
          version->cfd_->SetLogNumber(max_log_number_in_batch);
        }
      }

      uint64_t last_min_log_number_to_keep = 0;
      for (auto& e : batch_edits) {
        if (e->has_min_log_number_to_keep_) {
          last_min_log_number_to_keep =
              std::max(last_min_log_number_to_keep, e->min_log_number_to_keep_);
        }
      }

      if (last_min_log_number_to_keep != 0) {
        // Should only be set in 2PC mode.
        MarkMinLogNumberToKeep2PC(last_min_log_number_to_keep);
      }

      for (int i = 0; i < static_cast<int>(versions.size()); ++i) {
        ColumnFamilyData* cfd = versions[i]->cfd_;
        AppendVersion(cfd, versions[i]);
      }
    }
    manifest_file_number_ = pending_manifest_file_number_;
    manifest_file_size_ = new_manifest_file_size;
    prev_log_number_ = first_writer.edit_list.front()->prev_log_number_;
  } else {
    std::string version_edits;
    for (auto& e : batch_edits) {
      version_edits += ("\n" + e->DebugString(true));
    }
    ROCKS_LOG_ERROR(db_options_->info_log,
                    "Error in committing version edit to MANIFEST: %s",
                    version_edits.c_str());
    for (auto v : versions) {
      delete v;
    }
    if (new_descriptor_log) {
      ROCKS_LOG_INFO(db_options_->info_log,
                     "Deleting manifest %" PRIu64 " current manifest %" PRIu64
                     "\n",
                     manifest_file_number_, pending_manifest_file_number_);
      descriptor_log_.reset();
      env_->DeleteFile(
          DescriptorFileName(dbname_, pending_manifest_file_number_));
    }
  }

  pending_manifest_file_number_ = 0;

  // wake up all the waiting writers
  while (true) {
    ManifestWriter* ready = manifest_writers_.front();
    manifest_writers_.pop_front();
    bool need_signal = true;
    for (const auto& w : writers) {
      if (&w == ready) {
        need_signal = false;
        break;
      }
    }
    ready->status = s;
    ready->done = true;
    if (need_signal) {
      ready->cv.Signal();
    }
    if (ready == last_writer) {
      break;
    }
  }
  if (!manifest_writers_.empty()) {
    manifest_writers_.front()->cv.Signal();
  }
  return s;
}

// 'datas' is gramatically incorrect. We still use this notation is to indicate
// that this variable represents a collection of column_family_data.
Status VersionSet::LogAndApply(
    const autovector<ColumnFamilyData*>& column_family_datas,
    const autovector<const MutableCFOptions*>& mutable_cf_options_list,
    const autovector<autovector<VersionEdit*>>& edit_lists,
    InstrumentedMutex* mu, Directory* db_directory, bool new_descriptor_log,
    const ColumnFamilyOptions* new_cf_options) {
  mu->AssertHeld();
  int num_edits = 0;
  for (const auto& elist : edit_lists) {
    num_edits += static_cast<int>(elist.size());
  }
  if (num_edits == 0) {
    return Status::OK();
  } else if (num_edits > 1) {
#ifndef NDEBUG
    for (const auto& edit_list : edit_lists) {
      for (const auto& edit : edit_list) {
        assert(!edit->IsColumnFamilyManipulation());
      }
    }
#endif /* ! NDEBUG */
  }

  int num_cfds = static_cast<int>(column_family_datas.size());
  if (num_cfds == 1 && column_family_datas[0] == nullptr) {
    assert(edit_lists.size() == 1 && edit_lists[0].size() == 1);
    assert(edit_lists[0][0]->is_column_family_add_);
    assert(new_cf_options != nullptr);
  }
  std::deque<ManifestWriter> writers;
  if (num_cfds > 0) {
    assert(static_cast<size_t>(num_cfds) == mutable_cf_options_list.size());
    assert(static_cast<size_t>(num_cfds) == edit_lists.size());
  }
  for (int i = 0; i < num_cfds; ++i) {
    writers.emplace_back(mu, column_family_datas[i],
                         *mutable_cf_options_list[i], edit_lists[i]);
    manifest_writers_.push_back(&writers[i]);
  }
  assert(!writers.empty());
  ManifestWriter& first_writer = writers.front();
  while (!first_writer.done && &first_writer != manifest_writers_.front()) {
    first_writer.cv.Wait();
  }
  if (first_writer.done) {
    // All non-CF-manipulation operations can be grouped together and committed
    // to MANIFEST. They should all have finished. The status code is stored in
    // the first manifest writer.
#ifndef NDEBUG
    for (const auto& writer : writers) {
      assert(writer.done);
    }
#endif /* !NDEBUG */
    return first_writer.status;
  }

  int num_undropped_cfds = 0;
  for (auto cfd : column_family_datas) {
    // if cfd == nullptr, it is a column family add.
    if (cfd == nullptr || !cfd->IsDropped()) {
      ++num_undropped_cfds;
    }
  }
  if (0 == num_undropped_cfds) {
    // TODO (yanqin) maybe use a different status code to denote column family
    // drop other than OK and ShutdownInProgress
    for (int i = 0; i != num_cfds; ++i) {
      manifest_writers_.pop_front();
    }
    // Notify new head of manifest write queue.
    if (!manifest_writers_.empty()) {
      manifest_writers_.front()->cv.Signal();
    }
    return Status::ShutdownInProgress();
  }

  return ProcessManifestWrites(writers, mu, db_directory, new_descriptor_log,
                               new_cf_options);
}

void VersionSet::LogAndApplyCFHelper(VersionEdit* edit) {
  assert(edit->IsColumnFamilyManipulation());
  edit->SetNextFile(next_file_number_.load());
  // The log might have data that is not visible to memtbale and hence have not
  // updated the last_sequence_ yet. It is also possible that the log has is
  // expecting some new data that is not written yet. Since LastSequence is an
  // upper bound on the sequence, it is ok to record
  // last_allocated_sequence_ as the last sequence.
  edit->SetLastSequence(db_options_->two_write_queues ? last_allocated_sequence_
                                                      : last_sequence_);
  if (edit->is_column_family_drop_) {
    // if we drop column family, we have to make sure to save max column family,
    // so that we don't reuse existing ID
    edit->SetMaxColumnFamily(column_family_set_->GetMaxColumnFamily());
  }
}

void VersionSet::LogAndApplyHelper(ColumnFamilyData* cfd,
                                   VersionBuilder* builder, Version* /*v*/,
                                   VersionEdit* edit, InstrumentedMutex* mu) {
#ifdef NDEBUG
  (void)cfd;
#endif
  mu->AssertHeld();
  assert(!edit->IsColumnFamilyManipulation());

  if (edit->has_log_number_) {
    assert(edit->log_number_ >= cfd->GetLogNumber());
    assert(edit->log_number_ < next_file_number_.load());
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }
  edit->SetNextFile(next_file_number_.load());
  // The log might have data that is not visible to memtbale and hence have not
  // updated the last_sequence_ yet. It is also possible that the log has is
  // expecting some new data that is not written yet. Since LastSequence is an
  // upper bound on the sequence, it is ok to record
  // last_allocated_sequence_ as the last sequence.
  edit->SetLastSequence(db_options_->two_write_queues ? last_allocated_sequence_
                                                      : last_sequence_);

  builder->Apply(edit);
}

Status VersionSet::ApplyOneVersionEdit(
    VersionEdit& edit,
    const std::unordered_map<std::string, ColumnFamilyOptions>& name_to_options,
    std::unordered_map<int, std::string>& column_families_not_found,
    std::unordered_map<uint32_t, BaseReferencedVersionBuilder*>& builders,
    bool* have_log_number, uint64_t* /* log_number */,
    bool* have_prev_log_number, uint64_t* previous_log_number,
    bool* have_next_file, uint64_t* next_file, bool* have_last_sequence,
    SequenceNumber* last_sequence, uint64_t* min_log_number_to_keep,
    uint32_t* max_column_family) {
  // Not found means that user didn't supply that column
  // family option AND we encountered column family add
  // record. Once we encounter column family drop record,
  // we will delete the column family from
  // column_families_not_found.
  bool cf_in_not_found = (column_families_not_found.find(edit.column_family_) !=
                          column_families_not_found.end());
  // in builders means that user supplied that column family
  // option AND that we encountered column family add record
  bool cf_in_builders = builders.find(edit.column_family_) != builders.end();

  // they can't both be true
  assert(!(cf_in_not_found && cf_in_builders));

  ColumnFamilyData* cfd = nullptr;

  if (edit.is_column_family_add_) {
    if (cf_in_builders || cf_in_not_found) {
      return Status::Corruption(
          "Manifest adding the same column family twice: " +
          edit.column_family_name_);
    }
    auto cf_options = name_to_options.find(edit.column_family_name_);
    if (cf_options == name_to_options.end()) {
      column_families_not_found.insert(
          {edit.column_family_, edit.column_family_name_});
    } else {
      cfd = CreateColumnFamily(cf_options->second, &edit);
      cfd->set_initialized();
      builders.insert(
          {edit.column_family_, new BaseReferencedVersionBuilder(cfd)});
    }
  } else if (edit.is_column_family_drop_) {
    if (cf_in_builders) {
      auto builder = builders.find(edit.column_family_);
      assert(builder != builders.end());
      delete builder->second;
      builders.erase(builder);
      cfd = column_family_set_->GetColumnFamily(edit.column_family_);
      assert(cfd != nullptr);
      if (cfd->Unref()) {
        delete cfd;
        cfd = nullptr;
      } else {
        // who else can have reference to cfd!?
        assert(false);
      }
    } else if (cf_in_not_found) {
      column_families_not_found.erase(edit.column_family_);
    } else {
      return Status::Corruption(
          "Manifest - dropping non-existing column family");
    }
  } else if (!cf_in_not_found) {
    if (!cf_in_builders) {
      return Status::Corruption(
          "Manifest record referencing unknown column family");
    }

    cfd = column_family_set_->GetColumnFamily(edit.column_family_);
    // this should never happen since cf_in_builders is true
    assert(cfd != nullptr);

    // if it is not column family add or column family drop,
    // then it's a file add/delete, which should be forwarded
    // to builder
    auto builder = builders.find(edit.column_family_);
    assert(builder != builders.end());
    builder->second->version_builder()->Apply(&edit);
  }

  if (cfd != nullptr) {
    if (edit.has_log_number_) {
      if (cfd->GetLogNumber() > edit.log_number_) {
        ROCKS_LOG_WARN(
            db_options_->info_log,
            "MANIFEST corruption detected, but ignored - Log numbers in "
            "records NOT monotonically increasing");
      } else {
        cfd->SetLogNumber(edit.log_number_);
        *have_log_number = true;
      }
    }
    if (edit.has_comparator_ &&
        edit.comparator_ != cfd->user_comparator()->Name()) {
      return Status::InvalidArgument(
          cfd->user_comparator()->Name(),
          "does not match existing comparator " + edit.comparator_);
    }
  }

  if (edit.has_prev_log_number_) {
    *previous_log_number = edit.prev_log_number_;
    *have_prev_log_number = true;
  }

  if (edit.has_next_file_number_) {
    *next_file = edit.next_file_number_;
    *have_next_file = true;
  }

  if (edit.has_max_column_family_) {
    *max_column_family = edit.max_column_family_;
  }

  if (edit.has_min_log_number_to_keep_) {
    *min_log_number_to_keep =
        std::max(*min_log_number_to_keep, edit.min_log_number_to_keep_);
  }

  if (edit.has_last_sequence_) {
    *last_sequence = edit.last_sequence_;
    *have_last_sequence = true;
  }
  return Status::OK();
}

Status VersionSet::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    bool read_only) {
  std::unordered_map<std::string, ColumnFamilyOptions> cf_name_to_options;
  for (auto cf : column_families) {
    cf_name_to_options.insert({cf.name, cf.options});
  }
  // keeps track of column families in manifest that were not found in
  // column families parameters. if those column families are not dropped
  // by subsequent manifest records, Recover() will return failure status
  std::unordered_map<int, std::string> column_families_not_found;

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string manifest_filename;
  Status s = ReadFileToString(
      env_, CurrentFileName(dbname_), &manifest_filename
  );
  if (!s.ok()) {
    return s;
  }
  if (manifest_filename.empty() ||
      manifest_filename.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  // remove the trailing '\n'
  manifest_filename.resize(manifest_filename.size() - 1);
  FileType type;
  bool parse_ok =
      ParseFileName(manifest_filename, &manifest_file_number_, &type);
  if (!parse_ok || type != kDescriptorFile) {
    return Status::Corruption("CURRENT file corrupted");
  }

  ROCKS_LOG_INFO(db_options_->info_log, "Recovering from manifest file: %s\n",
                 manifest_filename.c_str());

  manifest_filename = dbname_ + "/" + manifest_filename;
  std::unique_ptr<SequentialFileReader> manifest_file_reader;
  {
    std::unique_ptr<SequentialFile> manifest_file;
    s = env_->NewSequentialFile(manifest_filename, &manifest_file,
                                env_->OptimizeForManifestRead(env_options_));
    if (!s.ok()) {
      return s;
    }
    manifest_file_reader.reset(
        new SequentialFileReader(std::move(manifest_file), manifest_filename));
  }
  uint64_t current_manifest_file_size;
  s = env_->GetFileSize(manifest_filename, &current_manifest_file_size);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t previous_log_number = 0;
  uint32_t max_column_family = 0;
  uint64_t min_log_number_to_keep = 0;
  std::unordered_map<uint32_t, BaseReferencedVersionBuilder*> builders;

  // add default column family
  auto default_cf_iter = cf_name_to_options.find(kDefaultColumnFamilyName);
  if (default_cf_iter == cf_name_to_options.end()) {
    return Status::InvalidArgument("Default column family not specified");
  }
  VersionEdit default_cf_edit;
  default_cf_edit.AddColumnFamily(kDefaultColumnFamilyName);
  default_cf_edit.SetColumnFamily(0);
  ColumnFamilyData* default_cfd =
      CreateColumnFamily(default_cf_iter->second, &default_cf_edit);
  // In recovery, nobody else can access it, so it's fine to set it to be
  // initialized earlier.
  default_cfd->set_initialized();
  builders.insert({0, new BaseReferencedVersionBuilder(default_cfd)});

  {
    VersionSet::LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(manifest_file_reader), &reporter,
                       true /* checksum */, 0 /* log_number */,
                       false /* retry_after_eof */);
    Slice record;
    std::string scratch;
    std::vector<VersionEdit> replay_buffer;
    size_t num_entries_decoded = 0;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (!s.ok()) {
        break;
      }

      if (edit.is_in_atomic_group_) {
        if (replay_buffer.empty()) {
          replay_buffer.resize(edit.remaining_entries_ + 1);
          TEST_SYNC_POINT_CALLBACK("VersionSet::Recover:FirstInAtomicGroup",
                                   &edit);
        }
        ++num_entries_decoded;
        if (num_entries_decoded + edit.remaining_entries_ !=
            static_cast<uint32_t>(replay_buffer.size())) {
          TEST_SYNC_POINT_CALLBACK(
              "VersionSet::Recover:IncorrectAtomicGroupSize", &edit);
          s = Status::Corruption("corrupted atomic group");
          break;
        }
        replay_buffer[num_entries_decoded - 1] = std::move(edit);
        if (num_entries_decoded == replay_buffer.size()) {
          TEST_SYNC_POINT_CALLBACK("VersionSet::Recover:LastInAtomicGroup",
                                   &edit);
          for (auto& e : replay_buffer) {
            s = ApplyOneVersionEdit(
                e, cf_name_to_options, column_families_not_found, builders,
                &have_log_number, &log_number, &have_prev_log_number,
                &previous_log_number, &have_next_file, &next_file,
                &have_last_sequence, &last_sequence, &min_log_number_to_keep,
                &max_column_family);
            if (!s.ok()) {
              break;
            }
          }
          replay_buffer.clear();
          num_entries_decoded = 0;
        }
        TEST_SYNC_POINT("VersionSet::Recover:AtomicGroup");
      } else {
        if (!replay_buffer.empty()) {
          TEST_SYNC_POINT_CALLBACK(
              "VersionSet::Recover:AtomicGroupMixedWithNormalEdits", &edit);
          s = Status::Corruption("corrupted atomic group");
          break;
        }
        s = ApplyOneVersionEdit(
            edit, cf_name_to_options, column_families_not_found, builders,
            &have_log_number, &log_number, &have_prev_log_number,
            &previous_log_number, &have_next_file, &next_file,
            &have_last_sequence, &last_sequence, &min_log_number_to_keep,
            &max_column_family);
      }
      if (!s.ok()) {
        break;
      }
    }
  }

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      previous_log_number = 0;
    }

    column_family_set_->UpdateMaxColumnFamily(max_column_family);

    // When reading DB generated using old release, min_log_number_to_keep=0.
    // All log files will be scanned for potential prepare entries.
    MarkMinLogNumberToKeep2PC(min_log_number_to_keep);
    MarkFileNumberUsed(previous_log_number);
    MarkFileNumberUsed(log_number);
  }

  // there were some column families in the MANIFEST that weren't specified
  // in the argument. This is OK in read_only mode
  if (read_only == false && !column_families_not_found.empty()) {
    std::string list_of_not_found;
    for (const auto& cf : column_families_not_found) {
      list_of_not_found += ", " + cf.second;
    }
    list_of_not_found = list_of_not_found.substr(2);
    s = Status::InvalidArgument(
        "You have to open all column families. Column families not opened: " +
        list_of_not_found);
  }

  if (s.ok()) {
    for (auto cfd : *column_family_set_) {
      assert(builders.count(cfd->GetID()) > 0);
      auto* builder = builders[cfd->GetID()]->version_builder();
      if (!builder->CheckConsistencyForNumLevels()) {
        s = Status::InvalidArgument(
            "db has more levels than options.num_levels");
        break;
      }
    }
  }

  if (s.ok()) {
    for (auto cfd : *column_family_set_) {
      if (cfd->IsDropped()) {
        continue;
      }
      if (read_only) {
        cfd->table_cache()->SetTablesAreImmortal();
      }
      assert(cfd->initialized());
      auto builders_iter = builders.find(cfd->GetID());
      assert(builders_iter != builders.end());
      auto* builder = builders_iter->second->version_builder();

      // unlimited table cache. Pre-load table handle now.
      // Need to do it out of the mutex.
      builder->LoadTableHandlers(
          cfd->internal_stats(), db_options_->max_file_opening_threads,
          false /* prefetch_index_and_filter_in_cache */,
          true /* is_initial_load */,
          cfd->GetLatestMutableCFOptions()->prefix_extractor.get());

      Version* v = new Version(cfd, this, env_options_,
                               *cfd->GetLatestMutableCFOptions(),
                               current_version_number_++);
      builder->SaveTo(v->storage_info());

      // Install recovered version
      v->PrepareApply(*cfd->GetLatestMutableCFOptions(),
          !(db_options_->skip_stats_update_on_db_open));
      AppendVersion(cfd, v);
    }

    manifest_file_size_ = current_manifest_file_size;
    next_file_number_.store(next_file + 1);
    last_allocated_sequence_ = last_sequence;
    last_published_sequence_ = last_sequence;
    last_sequence_ = last_sequence;
    prev_log_number_ = previous_log_number;

    ROCKS_LOG_INFO(
        db_options_->info_log,
        "Recovered from manifest file:%s succeeded,"
        "manifest_file_number is %lu, next_file_number is %lu, "
        "last_sequence is %lu, log_number is %lu,"
        "prev_log_number is %lu,"
        "max_column_family is %u,"
        "min_log_number_to_keep is %lu\n",
        manifest_filename.c_str(), (unsigned long)manifest_file_number_,
        (unsigned long)next_file_number_.load(), (unsigned long)last_sequence_,
        (unsigned long)log_number, (unsigned long)prev_log_number_,
        column_family_set_->GetMaxColumnFamily(), min_log_number_to_keep_2pc());

    for (auto cfd : *column_family_set_) {
      if (cfd->IsDropped()) {
        continue;
      }
      ROCKS_LOG_INFO(db_options_->info_log,
                     "Column family [%s] (ID %u), log number is %" PRIu64 "\n",
                     cfd->GetName().c_str(), cfd->GetID(), cfd->GetLogNumber());
    }
  }

  for (auto& builder : builders) {
    delete builder.second;
  }

  return s;
}

Status VersionSet::ListColumnFamilies(std::vector<std::string>* column_families,
                                      const std::string& dbname, Env* env) {
  // these are just for performance reasons, not correcntes,
  // so we're fine using the defaults
  EnvOptions soptions;
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env, CurrentFileName(dbname), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname + "/" + current;

  std::unique_ptr<SequentialFileReader> file_reader;
  {
    std::unique_ptr<SequentialFile> file;
    s = env->NewSequentialFile(dscname, &file, soptions);
    if (!s.ok()) {
      return s;
  }
  file_reader.reset(new SequentialFileReader(std::move(file), dscname));
  }

  std::map<uint32_t, std::string> column_family_names;
  // default column family is always implicitly there
  column_family_names.insert({0, kDefaultColumnFamilyName});
  VersionSet::LogReporter reporter;
  reporter.status = &s;
  log::Reader reader(nullptr, std::move(file_reader), &reporter,
                     true /* checksum */, 0 /* log_number */,
                     false /* retry_after_eof */);
  Slice record;
  std::string scratch;
  while (reader.ReadRecord(&record, &scratch) && s.ok()) {
    VersionEdit edit;
    s = edit.DecodeFrom(record);
    if (!s.ok()) {
      break;
    }
    if (edit.is_column_family_add_) {
      if (column_family_names.find(edit.column_family_) !=
          column_family_names.end()) {
        s = Status::Corruption("Manifest adding the same column family twice");
        break;
      }
      column_family_names.insert(
          {edit.column_family_, edit.column_family_name_});
    } else if (edit.is_column_family_drop_) {
      if (column_family_names.find(edit.column_family_) ==
          column_family_names.end()) {
        s = Status::Corruption(
            "Manifest - dropping non-existing column family");
        break;
      }
      column_family_names.erase(edit.column_family_);
    }
  }

  column_families->clear();
  if (s.ok()) {
    for (const auto& iter : column_family_names) {
      column_families->push_back(iter.second);
    }
  }

  return s;
}

#ifndef ROCKSDB_LITE
Status VersionSet::ReduceNumberOfLevels(const std::string& dbname,
                                        const Options* options,
                                        const EnvOptions& env_options,
                                        int new_levels) {
  if (new_levels <= 1) {
    return Status::InvalidArgument(
        "Number of levels needs to be bigger than 1");
  }

  ImmutableDBOptions db_options(*options);
  ColumnFamilyOptions cf_options(*options);
  std::shared_ptr<Cache> tc(NewLRUCache(options->max_open_files - 10,
                                        options->table_cache_numshardbits));
  WriteController wc(options->delayed_write_rate);
  WriteBufferManager wb(options->db_write_buffer_size);
  VersionSet versions(dbname, &db_options, env_options, tc.get(), &wb, &wc);
  Status status;

  std::vector<ColumnFamilyDescriptor> dummy;
  ColumnFamilyDescriptor dummy_descriptor(kDefaultColumnFamilyName,
                                          ColumnFamilyOptions(*options));
  dummy.push_back(dummy_descriptor);
  status = versions.Recover(dummy);
  if (!status.ok()) {
    return status;
  }

  Version* current_version =
      versions.GetColumnFamilySet()->GetDefault()->current();
  auto* vstorage = current_version->storage_info();
  int current_levels = vstorage->num_levels();

  if (current_levels <= new_levels) {
    return Status::OK();
  }

  // Make sure there are file only on one level from
  // (new_levels-1) to (current_levels-1)
  int first_nonempty_level = -1;
  int first_nonempty_level_filenum = 0;
  for (int i = new_levels - 1; i < current_levels; i++) {
    int file_num = vstorage->NumLevelFiles(i);
    if (file_num != 0) {
      if (first_nonempty_level < 0) {
        first_nonempty_level = i;
        first_nonempty_level_filenum = file_num;
      } else {
        char msg[255];
        snprintf(msg, sizeof(msg),
                 "Found at least two levels containing files: "
                 "[%d:%d],[%d:%d].\n",
                 first_nonempty_level, first_nonempty_level_filenum, i,
                 file_num);
        return Status::InvalidArgument(msg);
      }
    }
  }

  // we need to allocate an array with the old number of levels size to
  // avoid SIGSEGV in WriteSnapshot()
  // however, all levels bigger or equal to new_levels will be empty
  std::vector<FileMetaData*>* new_files_list =
      new std::vector<FileMetaData*>[current_levels];
  for (int i = 0; i < new_levels - 1; i++) {
    new_files_list[i] = vstorage->LevelFiles(i);
  }

  if (first_nonempty_level > 0) {
    new_files_list[new_levels - 1] = vstorage->LevelFiles(first_nonempty_level);
  }

  delete[] vstorage -> files_;
  vstorage->files_ = new_files_list;
  vstorage->num_levels_ = new_levels;

  MutableCFOptions mutable_cf_options(*options);
  VersionEdit ve;
  InstrumentedMutex dummy_mutex;
  InstrumentedMutexLock l(&dummy_mutex);
  return versions.LogAndApply(
      versions.GetColumnFamilySet()->GetDefault(),
      mutable_cf_options, &ve, &dummy_mutex, nullptr, true);
}

Status VersionSet::DumpManifest(Options& options, std::string& dscname,
                                bool verbose, bool hex, bool json) {
  // Open the specified manifest file.
  std::unique_ptr<SequentialFileReader> file_reader;
  Status s;
  {
    std::unique_ptr<SequentialFile> file;
    s = options.env->NewSequentialFile(
        dscname, &file, env_->OptimizeForManifestRead(env_options_));
    if (!s.ok()) {
      return s;
    }
    file_reader.reset(new SequentialFileReader(std::move(file), dscname));
  }

  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t previous_log_number = 0;
  int count = 0;
  std::unordered_map<uint32_t, std::string> comparators;
  std::unordered_map<uint32_t, BaseReferencedVersionBuilder*> builders;

  // add default column family
  VersionEdit default_cf_edit;
  default_cf_edit.AddColumnFamily(kDefaultColumnFamilyName);
  default_cf_edit.SetColumnFamily(0);
  ColumnFamilyData* default_cfd =
      CreateColumnFamily(ColumnFamilyOptions(options), &default_cf_edit);
  builders.insert({0, new BaseReferencedVersionBuilder(default_cfd)});

  {
    VersionSet::LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(file_reader), &reporter,
                       true /* checksum */, 0 /* log_number */,
                       false /* retry_after_eof */);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (!s.ok()) {
        break;
      }

      // Write out each individual edit
      if (verbose && !json) {
        printf("%s\n", edit.DebugString(hex).c_str());
      } else if (json) {
        printf("%s\n", edit.DebugJSON(count, hex).c_str());
      }
      count++;

      bool cf_in_builders =
          builders.find(edit.column_family_) != builders.end();

      if (edit.has_comparator_) {
        comparators.insert({edit.column_family_, edit.comparator_});
      }

      ColumnFamilyData* cfd = nullptr;

      if (edit.is_column_family_add_) {
        if (cf_in_builders) {
          s = Status::Corruption(
              "Manifest adding the same column family twice");
          break;
        }
        cfd = CreateColumnFamily(ColumnFamilyOptions(options), &edit);
        cfd->set_initialized();
        builders.insert(
            {edit.column_family_, new BaseReferencedVersionBuilder(cfd)});
      } else if (edit.is_column_family_drop_) {
        if (!cf_in_builders) {
          s = Status::Corruption(
              "Manifest - dropping non-existing column family");
          break;
        }
        auto builder_iter = builders.find(edit.column_family_);
        delete builder_iter->second;
        builders.erase(builder_iter);
        comparators.erase(edit.column_family_);
        cfd = column_family_set_->GetColumnFamily(edit.column_family_);
        assert(cfd != nullptr);
        cfd->Unref();
        delete cfd;
        cfd = nullptr;
      } else {
        if (!cf_in_builders) {
          s = Status::Corruption(
              "Manifest record referencing unknown column family");
          break;
        }

        cfd = column_family_set_->GetColumnFamily(edit.column_family_);
        // this should never happen since cf_in_builders is true
        assert(cfd != nullptr);

        // if it is not column family add or column family drop,
        // then it's a file add/delete, which should be forwarded
        // to builder
        auto builder = builders.find(edit.column_family_);
        assert(builder != builders.end());
        builder->second->version_builder()->Apply(&edit);
      }

      if (cfd != nullptr && edit.has_log_number_) {
        cfd->SetLogNumber(edit.log_number_);
      }


      if (edit.has_prev_log_number_) {
        previous_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }

      if (edit.has_max_column_family_) {
        column_family_set_->UpdateMaxColumnFamily(edit.max_column_family_);
      }

      if (edit.has_min_log_number_to_keep_) {
        MarkMinLogNumberToKeep2PC(edit.min_log_number_to_keep_);
      }
    }
  }
  file_reader.reset();

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
      printf("no meta-nextfile entry in descriptor");
    } else if (!have_last_sequence) {
      printf("no last-sequence-number entry in descriptor");
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      previous_log_number = 0;
    }
  }

  if (s.ok()) {
    for (auto cfd : *column_family_set_) {
      if (cfd->IsDropped()) {
        continue;
      }
      auto builders_iter = builders.find(cfd->GetID());
      assert(builders_iter != builders.end());
      auto builder = builders_iter->second->version_builder();

      Version* v = new Version(cfd, this, env_options_,
                               *cfd->GetLatestMutableCFOptions(),
                               current_version_number_++);
      builder->SaveTo(v->storage_info());
      v->PrepareApply(*cfd->GetLatestMutableCFOptions(), false);

      printf("--------------- Column family \"%s\"  (ID %u) --------------\n",
             cfd->GetName().c_str(), (unsigned int)cfd->GetID());
      printf("log number: %lu\n", (unsigned long)cfd->GetLogNumber());
      auto comparator = comparators.find(cfd->GetID());
      if (comparator != comparators.end()) {
        printf("comparator: %s\n", comparator->second.c_str());
      } else {
        printf("comparator: <NO COMPARATOR>\n");
      }
      printf("%s \n", v->DebugString(hex).c_str());
      delete v;
    }

    // Free builders
    for (auto& builder : builders) {
      delete builder.second;
    }

    next_file_number_.store(next_file + 1);
    last_allocated_sequence_ = last_sequence;
    last_published_sequence_ = last_sequence;
    last_sequence_ = last_sequence;
    prev_log_number_ = previous_log_number;

    printf(
        "next_file_number %lu last_sequence "
        "%lu  prev_log_number %lu max_column_family %u min_log_number_to_keep "
        "%" PRIu64 "\n",
        (unsigned long)next_file_number_.load(), (unsigned long)last_sequence,
        (unsigned long)previous_log_number,
        column_family_set_->GetMaxColumnFamily(), min_log_number_to_keep_2pc());
  }

  return s;
}
#endif  // ROCKSDB_LITE

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  // only called during recovery and repair which are single threaded, so this
  // works because there can't be concurrent calls
  if (next_file_number_.load(std::memory_order_relaxed) <= number) {
    next_file_number_.store(number + 1, std::memory_order_relaxed);
  }
}

// Called only either from ::LogAndApply which is protected by mutex or during
// recovery which is single-threaded.
void VersionSet::MarkMinLogNumberToKeep2PC(uint64_t number) {
  if (min_log_number_to_keep_2pc_.load(std::memory_order_relaxed) < number) {
    min_log_number_to_keep_2pc_.store(number, std::memory_order_relaxed);
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // WARNING: This method doesn't hold a mutex!!

  // This is done without DB mutex lock held, but only within single-threaded
  // LogAndApply. Column family manipulations can only happen within LogAndApply
  // (the same single thread), so we're safe to iterate.
  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped()) {
      continue;
    }
    assert(cfd->initialized());
    {
      // Store column family info
      VersionEdit edit;
      if (cfd->GetID() != 0) {
        // default column family is always there,
        // no need to explicitly write it
        edit.AddColumnFamily(cfd->GetName());
        edit.SetColumnFamily(cfd->GetID());
      }
      edit.SetComparatorName(
          cfd->internal_comparator().user_comparator()->Name());
      std::string record;
      if (!edit.EncodeTo(&record)) {
        return Status::Corruption(
            "Unable to Encode VersionEdit:" + edit.DebugString(true));
      }
      Status s = log->AddRecord(record);
      if (!s.ok()) {
        return s;
      }
    }

    {
      // Save files
      VersionEdit edit;
      edit.SetColumnFamily(cfd->GetID());

      for (int level = 0; level < cfd->NumberLevels(); level++) {
        for (const auto& f :
             cfd->current()->storage_info()->LevelFiles(level)) {
          edit.AddFile(level, f->fd.GetNumber(), f->fd.GetPathId(),
                       f->fd.GetFileSize(), f->smallest, f->largest,
                       f->fd.smallest_seqno, f->fd.largest_seqno,
                       f->marked_for_compaction);
        }
      }
      edit.SetLogNumber(cfd->GetLogNumber());
      std::string record;
      if (!edit.EncodeTo(&record)) {
        return Status::Corruption(
            "Unable to Encode VersionEdit:" + edit.DebugString(true));
      }
      Status s = log->AddRecord(record);
      if (!s.ok()) {
        return s;
      }
    }
  }

  return Status::OK();
}

// TODO(aekmekji): in CompactionJob::GenSubcompactionBoundaries(), this
// function is called repeatedly with consecutive pairs of slices. For example
// if the slice list is [a, b, c, d] this function is called with arguments
// (a,b) then (b,c) then (c,d). Knowing this, an optimization is possible where
// we avoid doing binary search for the keys b and c twice and instead somehow
// maintain state of where they first appear in the files.
uint64_t VersionSet::ApproximateSize(Version* v, const Slice& start,
                                     const Slice& end, int start_level,
                                     int end_level) {
  // pre-condition
  assert(v->cfd_->internal_comparator().Compare(start, end) <= 0);

  uint64_t size = 0;
  const auto* vstorage = v->storage_info();
  end_level = end_level == -1
                  ? vstorage->num_non_empty_levels()
                  : std::min(end_level, vstorage->num_non_empty_levels());

  assert(start_level <= end_level);

  for (int level = start_level; level < end_level; level++) {
    const LevelFilesBrief& files_brief = vstorage->LevelFilesBrief(level);
    if (!files_brief.num_files) {
      // empty level, skip exploration
      continue;
    }

    if (!level) {
      // level 0 data is sorted order, handle the use case explicitly
      size += ApproximateSizeLevel0(v, files_brief, start, end);
      continue;
    }

    assert(level > 0);
    assert(files_brief.num_files > 0);

    // identify the file position for starting key
    const uint64_t idx_start = FindFileInRange(
        v->cfd_->internal_comparator(), files_brief, start,
        /*start=*/0, static_cast<uint32_t>(files_brief.num_files - 1));
    assert(idx_start < files_brief.num_files);

    // scan all files from the starting position until the ending position
    // inferred from the sorted order
    for (uint64_t i = idx_start; i < files_brief.num_files; i++) {
      uint64_t val;
      val = ApproximateSize(v, files_brief.files[i], end);
      if (!val) {
        // the files after this will not have the range
        break;
      }

      size += val;

      if (i == idx_start) {
        // subtract the bytes needed to be scanned to get to the starting
        // key
        val = ApproximateSize(v, files_brief.files[i], start);
        assert(size >= val);
        size -= val;
      }
    }
  }

  return size;
}

uint64_t VersionSet::ApproximateSizeLevel0(Version* v,
                                           const LevelFilesBrief& files_brief,
                                           const Slice& key_start,
                                           const Slice& key_end) {
  // level 0 files are not in sorted order, we need to iterate through
  // the list to compute the total bytes that require scanning
  uint64_t size = 0;
  for (size_t i = 0; i < files_brief.num_files; i++) {
    const uint64_t start = ApproximateSize(v, files_brief.files[i], key_start);
    const uint64_t end = ApproximateSize(v, files_brief.files[i], key_end);
    assert(end >= start);
    size += end - start;
  }
  return size;
}

uint64_t VersionSet::ApproximateSize(Version* v, const FdWithKeyRange& f,
                                     const Slice& key) {
  // pre-condition
  assert(v);

  uint64_t result = 0;
  if (v->cfd_->internal_comparator().Compare(f.largest_key, key) <= 0) {
    // Entire file is before "key", so just add the file size
    result = f.fd.GetFileSize();
  } else if (v->cfd_->internal_comparator().Compare(f.smallest_key, key) > 0) {
    // Entire file is after "key", so ignore
    result = 0;
  } else {
    // "key" falls in the range for this table.  Add the
    // approximate offset of "key" within the table.
    TableReader* table_reader_ptr;
    InternalIterator* iter = v->cfd_->table_cache()->NewIterator(
        ReadOptions(), v->env_options_, v->cfd_->internal_comparator(),
        *f.file_metadata, nullptr /* range_del_agg */,
        v->GetMutableCFOptions().prefix_extractor.get(), &table_reader_ptr);
    if (table_reader_ptr != nullptr) {
      result = table_reader_ptr->ApproximateOffsetOf(key);
    }
    delete iter;
  }
  return result;
}

void VersionSet::AddLiveFiles(std::vector<FileDescriptor>* live_list) {
  // pre-calculate space requirement
  int64_t total_files = 0;
  for (auto cfd : *column_family_set_) {
    if (!cfd->initialized()) {
      continue;
    }
    Version* dummy_versions = cfd->dummy_versions();
    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      const auto* vstorage = v->storage_info();
      for (int level = 0; level < vstorage->num_levels(); level++) {
        total_files += vstorage->LevelFiles(level).size();
      }
    }
  }

  // just one time extension to the right size
  live_list->reserve(live_list->size() + static_cast<size_t>(total_files));

  for (auto cfd : *column_family_set_) {
    if (!cfd->initialized()) {
      continue;
    }
    auto* current = cfd->current();
    bool found_current = false;
    Version* dummy_versions = cfd->dummy_versions();
    for (Version* v = dummy_versions->next_; v != dummy_versions;
         v = v->next_) {
      v->AddLiveFiles(live_list);
      if (v == current) {
        found_current = true;
      }
    }
    if (!found_current && current != nullptr) {
      // Should never happen unless it is a bug.
      assert(false);
      current->AddLiveFiles(live_list);
    }
  }
}

InternalIterator* VersionSet::MakeInputIterator(
    const Compaction* c, RangeDelAggregator* range_del_agg,
    const EnvOptions& env_options_compactions) {
  auto cfd = c->column_family_data();
  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.fill_cache = false;
  // Compaction iterators shouldn't be confined to a single prefix.
  // Compactions use Seek() for
  // (a) concurrent compactions,
  // (b) CompactionFilter::Decision::kRemoveAndSkipUntil.
  read_options.total_order_seek = true;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const size_t space = (c->level() == 0 ? c->input_levels(0)->num_files +
                                              c->num_input_levels() - 1
                                        : c->num_input_levels());
  InternalIterator** list = new InternalIterator* [space];
  size_t num = 0;
  for (size_t which = 0; which < c->num_input_levels(); which++) {
    if (c->input_levels(which)->num_files != 0) {
      if (c->level(which) == 0) {
        const LevelFilesBrief* flevel = c->input_levels(which);
        for (size_t i = 0; i < flevel->num_files; i++) {
          list[num++] = cfd->table_cache()->NewIterator(
              read_options, env_options_compactions, cfd->internal_comparator(),
              *flevel->files[i].file_metadata, range_del_agg,
              c->mutable_cf_options()->prefix_extractor.get(),
              nullptr /* table_reader_ptr */,
              nullptr /* no per level latency histogram */,
              true /* for_compaction */, nullptr /* arena */,
              false /* skip_filters */, static_cast<int>(which) /* level */);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = new LevelIterator(
            cfd->table_cache(), read_options, env_options_compactions,
            cfd->internal_comparator(), c->input_levels(which),
            c->mutable_cf_options()->prefix_extractor.get(),
            false /* should_sample */,
            nullptr /* no per level latency histogram */,
            true /* for_compaction */, false /* skip_filters */,
            static_cast<int>(which) /* level */, range_del_agg,
            c->boundaries(which));
      }
    }
  }
  assert(num <= space);
  InternalIterator* result =
      NewMergingIterator(&c->column_family_data()->internal_comparator(), list,
                         static_cast<int>(num));
  delete[] list;
  return result;
}

// verify that the files listed in this compaction are present
// in the current version
bool VersionSet::VerifyCompactionFileConsistency(Compaction* c) {
#ifndef NDEBUG
  Version* version = c->column_family_data()->current();
  const VersionStorageInfo* vstorage = version->storage_info();
  if (c->input_version() != version) {
    ROCKS_LOG_INFO(
        db_options_->info_log,
        "[%s] compaction output being applied to a different base version from"
        " input version",
        c->column_family_data()->GetName().c_str());

    if (vstorage->compaction_style_ == kCompactionStyleLevel &&
        c->start_level() == 0 && c->num_input_levels() > 2U) {
      // We are doing a L0->base_level compaction. The assumption is if
      // base level is not L1, levels from L1 to base_level - 1 is empty.
      // This is ensured by having one compaction from L0 going on at the
      // same time in level-based compaction. So that during the time, no
      // compaction/flush can put files to those levels.
      for (int l = c->start_level() + 1; l < c->output_level(); l++) {
        if (vstorage->NumLevelFiles(l) != 0) {
          return false;
        }
      }
    }
  }

  for (size_t input = 0; input < c->num_input_levels(); ++input) {
    int level = c->level(input);
    for (size_t i = 0; i < c->num_input_files(input); ++i) {
      uint64_t number = c->input(input, i)->fd.GetNumber();
      bool found = false;
      for (size_t j = 0; j < vstorage->files_[level].size(); j++) {
        FileMetaData* f = vstorage->files_[level][j];
        if (f->fd.GetNumber() == number) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false;  // input files non existent in current version
      }
    }
  }
#else
  (void)c;
#endif
  return true;     // everything good
}

Status VersionSet::GetMetadataForFile(uint64_t number, int* filelevel,
                                      FileMetaData** meta,
                                      ColumnFamilyData** cfd) {
  for (auto cfd_iter : *column_family_set_) {
    if (!cfd_iter->initialized()) {
      continue;
    }
    Version* version = cfd_iter->current();
    const auto* vstorage = version->storage_info();
    for (int level = 0; level < vstorage->num_levels(); level++) {
      for (const auto& file : vstorage->LevelFiles(level)) {
        if (file->fd.GetNumber() == number) {
          *meta = file;
          *filelevel = level;
          *cfd = cfd_iter;
          return Status::OK();
        }
      }
    }
  }
  return Status::NotFound("File not present in any level");
}

void VersionSet::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  for (auto cfd : *column_family_set_) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    for (int level = 0; level < cfd->NumberLevels(); level++) {
      for (const auto& file :
           cfd->current()->storage_info()->LevelFiles(level)) {
        LiveFileMetaData filemetadata;
        filemetadata.column_family_name = cfd->GetName();
        uint32_t path_id = file->fd.GetPathId();
        if (path_id < cfd->ioptions()->cf_paths.size()) {
          filemetadata.db_path = cfd->ioptions()->cf_paths[path_id].path;
        } else {
          assert(!cfd->ioptions()->cf_paths.empty());
          filemetadata.db_path = cfd->ioptions()->cf_paths.back().path;
        }
        filemetadata.name = MakeTableFileName("", file->fd.GetNumber());
        filemetadata.level = level;
        filemetadata.size = static_cast<size_t>(file->fd.GetFileSize());
        filemetadata.smallestkey = file->smallest.user_key().ToString();
        filemetadata.largestkey = file->largest.user_key().ToString();
        filemetadata.smallest_seqno = file->fd.smallest_seqno;
        filemetadata.largest_seqno = file->fd.largest_seqno;
        filemetadata.num_reads_sampled = file->stats.num_reads_sampled.load(
            std::memory_order_relaxed);
        filemetadata.being_compacted = file->being_compacted;
        filemetadata.num_entries = file->num_entries;
        filemetadata.num_deletions = file->num_deletions;
        metadata->push_back(filemetadata);
      }
    }
  }
}

void VersionSet::GetObsoleteFiles(std::vector<ObsoleteFileInfo>* files,
                                  std::vector<std::string>* manifest_filenames,
                                  uint64_t min_pending_output) {
  assert(manifest_filenames->empty());
  obsolete_manifests_.swap(*manifest_filenames);
  std::vector<ObsoleteFileInfo> pending_files;
  for (auto& f : obsolete_files_) {
    if (f.metadata->fd.GetNumber() < min_pending_output) {
      files->push_back(std::move(f));
    } else {
      pending_files.push_back(std::move(f));
    }
  }
  obsolete_files_.swap(pending_files);
}

ColumnFamilyData* VersionSet::CreateColumnFamily(
    const ColumnFamilyOptions& cf_options, VersionEdit* edit) {
  assert(edit->is_column_family_add_);

  MutableCFOptions dummy_cf_options;
  Version* dummy_versions =
      new Version(nullptr, this, env_options_, dummy_cf_options);
  // Ref() dummy version once so that later we can call Unref() to delete it
  // by avoiding calling "delete" explicitly (~Version is private)
  dummy_versions->Ref();
  auto new_cfd = column_family_set_->CreateColumnFamily(
      edit->column_family_name_, edit->column_family_, dummy_versions,
      cf_options);

  Version* v = new Version(new_cfd, this, env_options_,
                           *new_cfd->GetLatestMutableCFOptions(),
                           current_version_number_++);

  // Fill level target base information.
  v->storage_info()->CalculateBaseBytes(*new_cfd->ioptions(),
                                        *new_cfd->GetLatestMutableCFOptions());
  AppendVersion(new_cfd, v);
  // GetLatestMutableCFOptions() is safe here without mutex since the
  // cfd is not available to client
  new_cfd->CreateNewMemtable(*new_cfd->GetLatestMutableCFOptions(),
                             LastSequence());
  new_cfd->SetLogNumber(edit->log_number_);
  return new_cfd;
}

uint64_t VersionSet::GetNumLiveVersions(Version* dummy_versions) {
  uint64_t count = 0;
  for (Version* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    count++;
  }
  return count;
}

uint64_t VersionSet::GetTotalSstFilesSize(Version* dummy_versions) {
  std::unordered_set<uint64_t> unique_files;
  uint64_t total_files_size = 0;
  for (Version* v = dummy_versions->next_; v != dummy_versions; v = v->next_) {
    VersionStorageInfo* storage_info = v->storage_info();
    for (int level = 0; level < storage_info->num_levels_; level++) {
      for (const auto& file_meta : storage_info->LevelFiles(level)) {
        if (unique_files.find(file_meta->fd.packed_number_and_path_id) ==
            unique_files.end()) {
          unique_files.insert(file_meta->fd.packed_number_and_path_id);
          total_files_size += file_meta->fd.GetFileSize();
        }
      }
    }
  }
  return total_files_size;
}

}  // namespace rocksdb
