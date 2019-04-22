//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#pragma once
#include <atomic>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/compaction.h"
#include "db/compaction_picker.h"
#include "db/dbformat.h"
#include "db/file_indexer.h"
#include "db/log_reader.h"
#include "db/range_del_aggregator.h"
#include "db/read_callback.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/env.h"

namespace rocksdb {

namespace log {
class Writer;
}

class Compaction;
class LogBuffer;
class LookupKey;
class MemTable;
class Version;
class VersionSet;
class WriteBufferManager;
class MergeContext;
class ColumnFamilySet;
class TableCache;
class MergeIteratorBuilder;

// Return the smallest index i such that file_level.files[i]->largest >= key.
// Return file_level.num_files if there is no such file.
// REQUIRES: "file_level.files" contains a sorted list of
// non-overlapping files.
extern int FindFile(const InternalKeyComparator& icmp,
                    const LevelFilesBrief& file_level, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, file_level.files[]
// contains disjoint ranges in sorted order.
extern bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                                  bool disjoint_sorted_files,
                                  const LevelFilesBrief& file_level,
                                  const Slice* smallest_user_key,
                                  const Slice* largest_user_key);

// Generate LevelFilesBrief from vector<FdWithKeyRange*>
// Would copy smallest_key and largest_key data to sequential memory
// arena: Arena used to allocate the memory
extern void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
                                      const std::vector<FileMetaData*>& files,
                                      Arena* arena);

class VersionStorageInfo {
 public:
  VersionStorageInfo(const InternalKeyComparator* internal_comparator,
                     const Comparator* user_comparator, int num_levels,
                     CompactionStyle compaction_style,
                     VersionStorageInfo* src_vstorage,
                     bool _force_consistency_checks);
  ~VersionStorageInfo();

  void Reserve(int level, size_t size) { files_[level].reserve(size); }

  void AddFile(int level, FileMetaData* f, Logger* info_log = nullptr);

  void SetFinalized();

  // Update num_non_empty_levels_.
  void UpdateNumNonEmptyLevels();

  void GenerateFileIndexer() {
    file_indexer_.UpdateIndex(&arena_, num_non_empty_levels_, files_);
  }

  // Update the accumulated stats from a file-meta.
  void UpdateAccumulatedStats(FileMetaData* file_meta);

  // Decrease the current stat from a to-be-deleted file-meta
  void RemoveCurrentStats(FileMetaData* file_meta);

  void ComputeCompensatedSizes();

  // Updates internal structures that keep track of compaction scores
  // We use compaction scores to figure out which compaction to do next
  // REQUIRES: db_mutex held!!
  // TODO find a better way to pass compaction_options_fifo.
  void ComputeCompactionScore(const ImmutableCFOptions& immutable_cf_options,
                              const MutableCFOptions& mutable_cf_options);

  // Estimate est_comp_needed_bytes_
  void EstimateCompactionBytesNeeded(
      const MutableCFOptions& mutable_cf_options);

  // This computes files_marked_for_compaction_ and is called by
  // ComputeCompactionScore()
  void ComputeFilesMarkedForCompaction();

  // This computes ttl_expired_files_ and is called by
  // ComputeCompactionScore()
  void ComputeExpiredTtlFiles(const ImmutableCFOptions& ioptions,
                              const uint64_t ttl);

  // This computes bottommost_files_marked_for_compaction_ and is called by
  // ComputeCompactionScore() or UpdateOldestSnapshot().
  //
  // Among bottommost files (assumes they've already been computed), marks the
  // ones that have keys that would be eliminated if recompacted, according to
  // the seqnum of the oldest existing snapshot. Must be called every time
  // oldest snapshot changes as that is when bottom-level files can become
  // eligible for compaction.
  //
  // REQUIRES: DB mutex held
  void ComputeBottommostFilesMarkedForCompaction();

  // Generate level_files_brief_ from files_
  void GenerateLevelFilesBrief();
  // Sort all files for this version based on their file size and
  // record results in files_by_compaction_pri_. The largest files are listed
  // first.
  void UpdateFilesByCompactionPri(CompactionPri compaction_pri);

  void GenerateLevel0NonOverlapping();
  bool level0_non_overlapping() const {
    return level0_non_overlapping_;
  }

  // Check whether each file in this version is bottommost (i.e., nothing in its
  // key-range could possibly exist in an older file/level).
  // REQUIRES: This version has not been saved
  void GenerateBottommostFiles();

  // Updates the oldest snapshot and related internal state, like the bottommost
  // files marked for compaction.
  // REQUIRES: DB mutex held
  void UpdateOldestSnapshot(SequenceNumber oldest_snapshot_seqnum);

  int MaxInputLevel() const;
  int MaxOutputLevel(bool allow_ingest_behind) const;

  // Return level number that has idx'th highest score
  int CompactionScoreLevel(int idx) const { return compaction_level_[idx]; }

  // Return idx'th highest score
  double CompactionScore(int idx) const { return compaction_score_[idx]; }

  void GetOverlappingInputs(
      int level, const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,               // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index = -1,        // index of overlap file
      int* file_index = nullptr,  // return index of overlap file
      bool expand_range = true,   // if set, returns files which overlap the
                                  // range and overlap each other. If false,
                                  // then just files intersecting the range
      InternalKey** next_smallest = nullptr)  // if non-null, returns the
      const;  // smallest key of next file not included
  void GetCleanInputsWithinInterval(
      int level, const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,               // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index = -1,        // index of overlap file
      int* file_index = nullptr)  // return index of overlap file
      const;

  void GetOverlappingInputsRangeBinarySearch(
      int level,                 // level > 0
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs,
      int hint_index,                // index of overlap file
      int* file_index,               // return index of overlap file
      bool within_interval = false,  // if set, force the inputs within interval
      InternalKey** next_smallest = nullptr)  // if non-null, returns the
      const;  // smallest key of next file not included

  void ExtendFileRangeOverlappingInterval(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      unsigned int index,        // start extending from this index
      int* startIndex,           // return the startIndex of input range
      int* endIndex)             // return the endIndex of input range
      const;

  void ExtendFileRangeWithinInterval(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      unsigned int index,        // start extending from this index
      int* startIndex,           // return the startIndex of input range
      int* endIndex)             // return the endIndex of input range
      const;

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Returns true iff the first or last file in inputs contains
  // an overlapping user key to the file "just outside" of it (i.e.
  // just after the last file, or just before the first file)
  // REQUIRES: "*inputs" is a sorted list of non-overlapping files
  bool HasOverlappingUserKey(const std::vector<FileMetaData*>* inputs,
                             int level);

  int num_levels() const { return num_levels_; }

  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  int num_non_empty_levels() const {
    assert(finalized_);
    return num_non_empty_levels_;
  }

  // REQUIRES: This version has been finalized.
  // (CalculateBaseBytes() is called)
  // This may or may not return number of level files. It is to keep backward
  // compatible behavior in universal compaction.
  int l0_delay_trigger_count() const { return l0_delay_trigger_count_; }

  void set_l0_delay_trigger_count(int v) { l0_delay_trigger_count_ = v; }

  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  int NumLevelFiles(int level) const {
    assert(finalized_);
    return static_cast<int>(files_[level].size());
  }

  // Return the combined file size of all files at the specified level.
  uint64_t NumLevelBytes(int level) const;

  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  const std::vector<FileMetaData*>& LevelFiles(int level) const {
    return files_[level];
  }

  const rocksdb::LevelFilesBrief& LevelFilesBrief(int level) const {
    assert(level < static_cast<int>(level_files_brief_.size()));
    return level_files_brief_[level];
  }

  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  const std::vector<int>& FilesByCompactionPri(int level) const {
    assert(finalized_);
    return files_by_compaction_pri_[level];
  }

  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>& FilesMarkedForCompaction()
      const {
    assert(finalized_);
    return files_marked_for_compaction_;
  }

  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>& ExpiredTtlFiles() const {
    assert(finalized_);
    return expired_ttl_files_;
  }

  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  // REQUIRES: DB mutex held during access
  const autovector<std::pair<int, FileMetaData*>>&
  BottommostFilesMarkedForCompaction() const {
    assert(finalized_);
    return bottommost_files_marked_for_compaction_;
  }

  int base_level() const { return base_level_; }
  double level_multiplier() const { return level_multiplier_; }

  // REQUIRES: lock is held
  // Set the index that is used to offset into files_by_compaction_pri_ to find
  // the next compaction candidate file.
  void SetNextCompactionIndex(int level, int index) {
    next_file_to_compact_by_size_[level] = index;
  }

  // REQUIRES: lock is held
  int NextCompactionIndex(int level) const {
    return next_file_to_compact_by_size_[level];
  }

  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  const FileIndexer& file_indexer() const {
    assert(finalized_);
    return file_indexer_;
  }

  // Only the first few entries of files_by_compaction_pri_ are sorted.
  // There is no need to sort all the files because it is likely
  // that on a running system, we need to look at only the first
  // few largest files because a new version is created every few
  // seconds/minutes (because of concurrent compactions).
  static const size_t kNumberFilesToSort = 50;

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[1000];
  };
  struct FileSummaryStorage {
    char buffer[3000];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;
  // Return a human-readable short (single-line) summary of files
  // in a specified level.  Uses *scratch as backing store.
  const char* LevelFileSummary(FileSummaryStorage* scratch, int level) const;

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Return a human readable string that describes this version's contents.
  std::string DebugString(bool hex = false) const;

  uint64_t GetAverageValueSize() const {
    if (accumulated_num_non_deletions_ == 0) {
      return 0;
    }
    assert(accumulated_raw_key_size_ + accumulated_raw_value_size_ > 0);
    assert(accumulated_file_size_ > 0);
    return accumulated_raw_value_size_ / accumulated_num_non_deletions_ *
           accumulated_file_size_ /
           (accumulated_raw_key_size_ + accumulated_raw_value_size_);
  }

  uint64_t GetEstimatedActiveKeys() const;

  double GetEstimatedCompressionRatioAtLevel(int level) const;

  // re-initializes the index that is used to offset into
  // files_by_compaction_pri_
  // to find the next compaction candidate file.
  void ResetNextCompactionIndex(int level) {
    next_file_to_compact_by_size_[level] = 0;
  }

  const InternalKeyComparator* InternalComparator() {
    return internal_comparator_;
  }

  // Returns maximum total bytes of data on a given level.
  uint64_t MaxBytesForLevel(int level) const;

  // Must be called after any change to MutableCFOptions.
  void CalculateBaseBytes(const ImmutableCFOptions& ioptions,
                          const MutableCFOptions& options);

  // Returns an estimate of the amount of live data in bytes.
  uint64_t EstimateLiveDataSize() const;

  uint64_t estimated_compaction_needed_bytes() const {
    return estimated_compaction_needed_bytes_;
  }

  void TEST_set_estimated_compaction_needed_bytes(uint64_t v) {
    estimated_compaction_needed_bytes_ = v;
  }

  bool force_consistency_checks() const { return force_consistency_checks_; }

  // Returns whether any key in [`smallest_key`, `largest_key`] could appear in
  // an older L0 file than `last_l0_idx` or in a greater level than `last_level`
  //
  // @param last_level Level after which we check for overlap
  // @param last_l0_idx If `last_level == 0`, index of L0 file after which we
  //    check for overlap; otherwise, must be -1
  bool RangeMightExistAfterSortedRun(const Slice& smallest_user_key,
                                     const Slice& largest_user_key,
                                     int last_level, int last_l0_idx);

 private:
  const InternalKeyComparator* internal_comparator_;
  const Comparator* user_comparator_;
  int num_levels_;            // Number of levels
  int num_non_empty_levels_;  // Number of levels. Any level larger than it
                              // is guaranteed to be empty.
  // Per-level max bytes
  std::vector<uint64_t> level_max_bytes_;

  // A short brief metadata of files per level
  autovector<rocksdb::LevelFilesBrief> level_files_brief_;
  FileIndexer file_indexer_;
  Arena arena_;  // Used to allocate space for file_levels_

  CompactionStyle compaction_style_;

  // List of files per level, files in each level are arranged
  // in increasing order of keys
  std::vector<FileMetaData*>* files_;

  // Level that L0 data should be compacted to. All levels < base_level_ should
  // be empty. -1 if it is not level-compaction so it's not applicable.
  int base_level_;

  double level_multiplier_;

  // A list for the same set of files that are stored in files_,
  // but files in each level are now sorted based on file
  // size. The file with the largest size is at the front.
  // This vector stores the index of the file from files_.
  std::vector<std::vector<int>> files_by_compaction_pri_;

  // If true, means that files in L0 have keys with non overlapping ranges
  bool level0_non_overlapping_;

  // An index into files_by_compaction_pri_ that specifies the first
  // file that is not yet compacted
  std::vector<int> next_file_to_compact_by_size_;

  // Only the first few entries of files_by_compaction_pri_ are sorted.
  // There is no need to sort all the files because it is likely
  // that on a running system, we need to look at only the first
  // few largest files because a new version is created every few
  // seconds/minutes (because of concurrent compactions).
  static const size_t number_of_files_to_sort_ = 50;

  // This vector contains list of files marked for compaction and also not
  // currently being compacted. It is protected by DB mutex. It is calculated in
  // ComputeCompactionScore()
  autovector<std::pair<int, FileMetaData*>> files_marked_for_compaction_;

  autovector<std::pair<int, FileMetaData*>> expired_ttl_files_;

  // These files are considered bottommost because none of their keys can exist
  // at lower levels. They are not necessarily all in the same level. The marked
  // ones are eligible for compaction because they contain duplicate key
  // versions that are no longer protected by snapshot. These variables are
  // protected by DB mutex and are calculated in `GenerateBottommostFiles()` and
  // `ComputeBottommostFilesMarkedForCompaction()`.
  autovector<std::pair<int, FileMetaData*>> bottommost_files_;
  autovector<std::pair<int, FileMetaData*>>
      bottommost_files_marked_for_compaction_;

  // Threshold for needing to mark another bottommost file. Maintain it so we
  // can quickly check when releasing a snapshot whether more bottommost files
  // became eligible for compaction. It's defined as the min of the max nonzero
  // seqnums of unmarked bottommost files.
  SequenceNumber bottommost_files_mark_threshold_ = kMaxSequenceNumber;

  // Monotonically increases as we release old snapshots. Zero indicates no
  // snapshots have been released yet. When no snapshots remain we set it to the
  // current seqnum, which needs to be protected as a snapshot can still be
  // created that references it.
  SequenceNumber oldest_snapshot_seqnum_ = 0;

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  // The most critical level to be compacted is listed first
  // These are used to pick the best compaction level
  std::vector<double> compaction_score_;
  std::vector<int> compaction_level_;
  int l0_delay_trigger_count_ = 0;  // Count used to trigger slow down and stop
                                    // for number of L0 files.

  // the following are the sampled temporary stats.
  // the current accumulated size of sampled files.
  uint64_t accumulated_file_size_;
  // the current accumulated size of all raw keys based on the sampled files.
  uint64_t accumulated_raw_key_size_;
  // the current accumulated size of all raw keys based on the sampled files.
  uint64_t accumulated_raw_value_size_;
  // total number of non-deletion entries
  uint64_t accumulated_num_non_deletions_;
  // total number of deletion entries
  uint64_t accumulated_num_deletions_;
  // current number of non_deletion entries
  uint64_t current_num_non_deletions_;
  // current number of deletion entries
  uint64_t current_num_deletions_;
  // current number of file samples
  uint64_t current_num_samples_;
  // Estimated bytes needed to be compacted until all levels' size is down to
  // target sizes.
  uint64_t estimated_compaction_needed_bytes_;

  bool finalized_;

  // If set to true, we will run consistency checks even if RocksDB
  // is compiled in release mode
  bool force_consistency_checks_;

  friend class Version;
  friend class VersionSet;
  // No copying allowed
  VersionStorageInfo(const VersionStorageInfo&) = delete;
  void operator=(const VersionStorageInfo&) = delete;
};

class Version {
 public:
  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  void AddIterators(const ReadOptions&, const EnvOptions& soptions,
                    MergeIteratorBuilder* merger_iter_builder,
                    RangeDelAggregator* range_del_agg);

  void AddIteratorsForLevel(const ReadOptions&, const EnvOptions& soptions,
                            MergeIteratorBuilder* merger_iter_builder,
                            int level, RangeDelAggregator* range_del_agg);

  Status OverlapWithLevelIterator(const ReadOptions&, const EnvOptions&,
                                  const Slice& smallest_user_key,
                                  const Slice& largest_user_key,
                                  int level, bool* overlap);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.
  // Uses *operands to store merge_operator operations to apply later.
  //
  // If the ReadOptions.read_tier is set to do a read-only fetch, then
  // *value_found will be set to false if it cannot be determined whether
  // this value exists without doing IO.
  //
  // If the key is Deleted, *status will be set to NotFound and
  //                        *key_exists will be set to true.
  // If no key was found, *status will be set to NotFound and
  //                      *key_exists will be set to false.
  // If seq is non-null, *seq will be set to the sequence number found
  // for the key if a key was found.
  //
  // REQUIRES: lock is not held
  void Get(const ReadOptions&, const LookupKey& key, PinnableSlice* value,
           Status* status, MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           bool* value_found = nullptr, bool* key_exists = nullptr,
           SequenceNumber* seq = nullptr, ReadCallback* callback = nullptr,
           bool* is_blob = nullptr);

  // Loads some stats information from files. Call without mutex held. It needs
  // to be called before applying the version to the version set.
  void PrepareApply(const MutableCFOptions& mutable_cf_options,
                    bool update_stats);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  // Decrease reference count. Delete the object if no reference left
  // and return true. Otherwise, return false.
  bool Unref();

  // Add all files listed in the current version to *live.
  void AddLiveFiles(std::vector<FileDescriptor>* live);

  // Return a human readable string that describes this version's contents.
  std::string DebugString(bool hex = false, bool print_stats = false) const;

  // Returns the version number of this version
  uint64_t GetVersionNumber() const { return version_number_; }

  // REQUIRES: lock is held
  // On success, "tp" will contains the table properties of the file
  // specified in "file_meta".  If the file name of "file_meta" is
  // known ahead, passing it by a non-null "fname" can save a
  // file-name conversion.
  Status GetTableProperties(std::shared_ptr<const TableProperties>* tp,
                            const FileMetaData* file_meta,
                            const std::string* fname = nullptr) const;

  // REQUIRES: lock is held
  // On success, *props will be populated with all SSTables' table properties.
  // The keys of `props` are the sst file name, the values of `props` are the
  // tables' properties, represented as std::shared_ptr.
  Status GetPropertiesOfAllTables(TablePropertiesCollection* props);
  Status GetPropertiesOfAllTables(TablePropertiesCollection* props, int level);
  Status GetPropertiesOfTablesInRange(const Range* range, std::size_t n,
                                      TablePropertiesCollection* props) const;

  // REQUIRES: lock is held
  // On success, "tp" will contains the aggregated table property among
  // the table properties of all sst files in this version.
  Status GetAggregatedTableProperties(
      std::shared_ptr<const TableProperties>* tp, int level = -1);

  uint64_t GetEstimatedActiveKeys() {
    return storage_info_.GetEstimatedActiveKeys();
  }

  size_t GetMemoryUsageByTableReaders();

  ColumnFamilyData* cfd() const { return cfd_; }

  // Return the next Version in the linked list. Used for debug only
  Version* TEST_Next() const {
    return next_;
  }

  int TEST_refs() const { return refs_; }

  VersionStorageInfo* storage_info() { return &storage_info_; }

  VersionSet* version_set() { return vset_; }

  void GetColumnFamilyMetaData(ColumnFamilyMetaData* cf_meta);

  uint64_t GetSstFilesSize();

  MutableCFOptions GetMutableCFOptions() { return mutable_cf_options_; }

 private:
  Env* env_;
  friend class VersionSet;

  const InternalKeyComparator* internal_comparator() const {
    return storage_info_.internal_comparator_;
  }
  const Comparator* user_comparator() const {
    return storage_info_.user_comparator_;
  }

  bool PrefixMayMatch(const ReadOptions& read_options,
                      InternalIterator* level_iter,
                      const Slice& internal_prefix) const;

  // Returns true if the filter blocks in the specified level will not be
  // checked during read operations. In certain cases (trivial move or preload),
  // the filter block may already be cached, but we still do not access it such
  // that it eventually expires from the cache.
  bool IsFilterSkipped(int level, bool is_file_last_in_level = false);

  // The helper function of UpdateAccumulatedStats, which may fill the missing
  // fields of file_meta from its associated TableProperties.
  // Returns true if it does initialize FileMetaData.
  bool MaybeInitializeFileMetaData(FileMetaData* file_meta);

  // Update the accumulated stats associated with the current version.
  // This accumulated stats will be used in compaction.
  void UpdateAccumulatedStats(bool update_stats);

  // Sort all files for this version based on their file size and
  // record results in files_by_compaction_pri_. The largest files are listed
  // first.
  void UpdateFilesByCompactionPri();

  ColumnFamilyData* cfd_;  // ColumnFamilyData to which this Version belongs
  Logger* info_log_;
  Statistics* db_statistics_;
  TableCache* table_cache_;
  const MergeOperator* merge_operator_;

  VersionStorageInfo storage_info_;
  VersionSet* vset_;            // VersionSet to which this Version belongs
  Version* next_;               // Next version in linked list
  Version* prev_;               // Previous version in linked list
  int refs_;                    // Number of live refs to this version
  const EnvOptions env_options_;
  const MutableCFOptions mutable_cf_options_;

  // A version number that uniquely represents this version. This is
  // used for debugging and logging purposes only.
  uint64_t version_number_;

  Version(ColumnFamilyData* cfd, VersionSet* vset, const EnvOptions& env_opt,
          MutableCFOptions mutable_cf_options, uint64_t version_number = 0);

  ~Version();

  // No copying allowed
  Version(const Version&);
  void operator=(const Version&);
};

struct ObsoleteFileInfo {
  FileMetaData* metadata;
  std::string   path;

  ObsoleteFileInfo() noexcept : metadata(nullptr) {}
  ObsoleteFileInfo(FileMetaData* f, const std::string& file_path)
      : metadata(f), path(file_path) {}

  ObsoleteFileInfo(const ObsoleteFileInfo&) = delete;
  ObsoleteFileInfo& operator=(const ObsoleteFileInfo&) = delete;

  ObsoleteFileInfo(ObsoleteFileInfo&& rhs) noexcept :
    ObsoleteFileInfo() {
      *this = std::move(rhs);
  }

  ObsoleteFileInfo& operator=(ObsoleteFileInfo&& rhs) noexcept {
    path = std::move(rhs.path);
    metadata = rhs.metadata;
    rhs.metadata = nullptr;

    return *this;
  }

  void DeleteMetadata() {
    delete metadata;
    metadata = nullptr;
  }
};

namespace {
class BaseReferencedVersionBuilder;
}

class VersionSet {
 public:
  VersionSet(const std::string& dbname, const ImmutableDBOptions* db_options,
             const EnvOptions& env_options, Cache* table_cache,
             WriteBufferManager* write_buffer_manager,
             WriteController* write_controller);
  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // column_family_options has to be set if edit is column family add
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(
      ColumnFamilyData* column_family_data,
      const MutableCFOptions& mutable_cf_options, VersionEdit* edit,
      InstrumentedMutex* mu, Directory* db_directory = nullptr,
      bool new_descriptor_log = false,
      const ColumnFamilyOptions* column_family_options = nullptr) {
    autovector<ColumnFamilyData*> cfds;
    cfds.emplace_back(column_family_data);
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(&mutable_cf_options);
    autovector<autovector<VersionEdit*>> edit_lists;
    autovector<VersionEdit*> edit_list;
    edit_list.emplace_back(edit);
    edit_lists.emplace_back(edit_list);
    return LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu,
                       db_directory, new_descriptor_log, column_family_options);
  }
  // The batch version. If edit_list.size() > 1, caller must ensure that
  // no edit in the list column family add or drop
  Status LogAndApply(
      ColumnFamilyData* column_family_data,
      const MutableCFOptions& mutable_cf_options,
      const autovector<VersionEdit*>& edit_list, InstrumentedMutex* mu,
      Directory* db_directory = nullptr, bool new_descriptor_log = false,
      const ColumnFamilyOptions* column_family_options = nullptr) {
    autovector<ColumnFamilyData*> cfds;
    cfds.emplace_back(column_family_data);
    autovector<const MutableCFOptions*> mutable_cf_options_list;
    mutable_cf_options_list.emplace_back(&mutable_cf_options);
    autovector<autovector<VersionEdit*>> edit_lists;
    edit_lists.emplace_back(edit_list);
    return LogAndApply(cfds, mutable_cf_options_list, edit_lists, mu,
                       db_directory, new_descriptor_log, column_family_options);
  }

  // The across-multi-cf batch version. If edit_lists contain more than
  // 1 version edits, caller must ensure that no edit in the []list is column
  // family manipulation.
  Status LogAndApply(
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const MutableCFOptions*>& mutable_cf_options_list,
      const autovector<autovector<VersionEdit*>>& edit_lists,
      InstrumentedMutex* mu, Directory* db_directory = nullptr,
      bool new_descriptor_log = false,
      const ColumnFamilyOptions* new_cf_options = nullptr);

  // Recover the last saved descriptor from persistent storage.
  // If read_only == true, Recover() will not complain if some column families
  // are not opened
  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool read_only = false);

  // Reads a manifest file and returns a list of column families in
  // column_families.
  static Status ListColumnFamilies(std::vector<std::string>* column_families,
                                   const std::string& dbname, Env* env);

#ifndef ROCKSDB_LITE
  // Try to reduce the number of levels. This call is valid when
  // only one level from the new max level to the old
  // max level containing files.
  // The call is static, since number of levels is immutable during
  // the lifetime of a RocksDB instance. It reduces number of levels
  // in a DB by applying changes to manifest.
  // For example, a db currently has 7 levels [0-6], and a call to
  // to reduce to 5 [0-4] can only be executed when only one level
  // among [4-6] contains files.
  static Status ReduceNumberOfLevels(const std::string& dbname,
                                     const Options* options,
                                     const EnvOptions& env_options,
                                     int new_levels);

  // printf contents (for debugging)
  Status DumpManifest(Options& options, std::string& manifestFileName,
                      bool verbose, bool hex = false, bool json = false);

#endif  // ROCKSDB_LITE

  // Return the current manifest file number
  uint64_t manifest_file_number() const { return manifest_file_number_; }

  uint64_t options_file_number() const { return options_file_number_; }

  uint64_t pending_manifest_file_number() const {
    return pending_manifest_file_number_;
  }

  uint64_t current_next_file_number() const { return next_file_number_.load(); }

  uint64_t min_log_number_to_keep_2pc() const {
    return min_log_number_to_keep_2pc_.load();
  }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1); }

  // Fetch And Add n new file number
  uint64_t FetchAddFileNumber(uint64_t n) {
    return next_file_number_.fetch_add(n);
  }

  // Return the last sequence number.
  uint64_t LastSequence() const {
    return last_sequence_.load(std::memory_order_acquire);
  }

  // Note: memory_order_acquire must be sufficient.
  uint64_t LastAllocatedSequence() const {
    return last_allocated_sequence_.load(std::memory_order_seq_cst);
  }

  // Note: memory_order_acquire must be sufficient.
  uint64_t LastPublishedSequence() const {
    return last_published_sequence_.load(std::memory_order_seq_cst);
  }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    // Last visible sequence must always be less than last written seq
    assert(!db_options_->two_write_queues || s <= last_allocated_sequence_);
    last_sequence_.store(s, std::memory_order_release);
  }

  // Note: memory_order_release must be sufficient
  void SetLastPublishedSequence(uint64_t s) {
    assert(s >= last_published_sequence_);
    last_published_sequence_.store(s, std::memory_order_seq_cst);
  }

  // Note: memory_order_release must be sufficient
  void SetLastAllocatedSequence(uint64_t s) {
    assert(s >= last_allocated_sequence_);
    last_allocated_sequence_.store(s, std::memory_order_seq_cst);
  }

  // Note: memory_order_release must be sufficient
  uint64_t FetchAddLastAllocatedSequence(uint64_t s) {
    return last_allocated_sequence_.fetch_add(s, std::memory_order_seq_cst);
  }

  // Mark the specified file number as used.
  // REQUIRED: this is only called during single-threaded recovery or repair.
  void MarkFileNumberUsed(uint64_t number);

  // Mark the specified log number as deleted
  // REQUIRED: this is only called during single-threaded recovery or repair, or
  // from ::LogAndApply where the global mutex is held.
  void MarkMinLogNumberToKeep2PC(uint64_t number);

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t prev_log_number() const { return prev_log_number_; }

  // Returns the minimum log number which still has data not flushed to any SST
  // file.
  // In non-2PC mode, all the log numbers smaller than this number can be safely
  // deleted.
  uint64_t MinLogNumberWithUnflushedData() const {
    return PreComputeMinLogNumberWithUnflushedData(nullptr);
  }
  // Returns the minimum log number which still has data not flushed to any SST
  // file, except data from `cfd_to_skip`.
  uint64_t PreComputeMinLogNumberWithUnflushedData(
      const ColumnFamilyData* cfd_to_skip) const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      if (cfd == cfd_to_skip) {
        continue;
      }
      // It's safe to ignore dropped column families here:
      // cfd->IsDropped() becomes true after the drop is persisted in MANIFEST.
      if (min_log_num > cfd->GetLogNumber() && !cfd->IsDropped()) {
        min_log_num = cfd->GetLogNumber();
      }
    }
    return min_log_num;
  }

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  InternalIterator* MakeInputIterator(
      const Compaction* c, RangeDelAggregator* range_del_agg,
      const EnvOptions& env_options_compactions);

  // Add all files listed in any live version to *live.
  void AddLiveFiles(std::vector<FileDescriptor>* live_list);

  // Return the approximate size of data to be scanned for range [start, end)
  // in levels [start_level, end_level). If end_level == 0 it will search
  // through all non-empty levels
  uint64_t ApproximateSize(Version* v, const Slice& start, const Slice& end,
                           int start_level = 0, int end_level = -1);

  // Return the size of the current manifest file
  uint64_t manifest_file_size() const { return manifest_file_size_; }

  // verify that the files that we started with for a compaction
  // still exist in the current version and in the same original level.
  // This ensures that a concurrent compaction did not erroneously
  // pick the same files to compact.
  bool VerifyCompactionFileConsistency(Compaction* c);

  Status GetMetadataForFile(uint64_t number, int* filelevel,
                            FileMetaData** metadata, ColumnFamilyData** cfd);

  // This function doesn't support leveldb SST filenames
  void GetLiveFilesMetaData(std::vector<LiveFileMetaData> *metadata);

  void GetObsoleteFiles(std::vector<ObsoleteFileInfo>* files,
                        std::vector<std::string>* manifest_filenames,
                        uint64_t min_pending_output);

  ColumnFamilySet* GetColumnFamilySet() { return column_family_set_.get(); }
  const EnvOptions& env_options() { return env_options_; }
  void ChangeEnvOptions(const MutableDBOptions& new_options) {
    env_options_.writable_file_max_buffer_size =
        new_options.writable_file_max_buffer_size;
  }

  const ImmutableDBOptions* db_options() const { return db_options_; }

  static uint64_t GetNumLiveVersions(Version* dummy_versions);

  static uint64_t GetTotalSstFilesSize(Version* dummy_versions);

 private:
  struct ManifestWriter;

  friend class Version;
  friend class DBImpl;

  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t /*bytes*/, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // ApproximateSize helper
  uint64_t ApproximateSizeLevel0(Version* v, const LevelFilesBrief& files_brief,
                                 const Slice& start, const Slice& end);

  uint64_t ApproximateSize(Version* v, const FdWithKeyRange& f,
                           const Slice& key);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(ColumnFamilyData* column_family_data, Version* v);

  ColumnFamilyData* CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                                       VersionEdit* edit);

  Status ApplyOneVersionEdit(
      VersionEdit& edit,
      const std::unordered_map<std::string, ColumnFamilyOptions>& name_to_opts,
      std::unordered_map<int, std::string>& column_families_not_found,
      std::unordered_map<uint32_t, BaseReferencedVersionBuilder*>& builders,
      bool* have_log_number, uint64_t* log_number, bool* have_prev_log_number,
      uint64_t* previous_log_number, bool* have_next_file, uint64_t* next_file,
      bool* have_last_sequence, SequenceNumber* last_sequence,
      uint64_t* min_log_number_to_keep, uint32_t* max_column_family);

  Status ProcessManifestWrites(std::deque<ManifestWriter>& writers,
                               InstrumentedMutex* mu, Directory* db_directory,
                               bool new_descriptor_log,
                               const ColumnFamilyOptions* new_cf_options);

  std::unique_ptr<ColumnFamilySet> column_family_set_;

  Env* const env_;
  const std::string dbname_;
  const ImmutableDBOptions* const db_options_;
  std::atomic<uint64_t> next_file_number_;
  // Any log number equal or lower than this should be ignored during recovery,
  // and is qualified for being deleted in 2PC mode. In non-2PC mode, this
  // number is ignored.
  std::atomic<uint64_t> min_log_number_to_keep_2pc_ = {0};
  uint64_t manifest_file_number_;
  uint64_t options_file_number_;
  uint64_t pending_manifest_file_number_;
  // The last seq visible to reads. It normally indicates the last sequence in
  // the memtable but when using two write queues it could also indicate the
  // last sequence in the WAL visible to reads.
  std::atomic<uint64_t> last_sequence_;
  // The last seq that is already allocated. It is applicable only when we have
  // two write queues. In that case seq might or might not have appreated in
  // memtable but it is expected to appear in the WAL.
  // We have last_sequence <= last_allocated_sequence_
  std::atomic<uint64_t> last_allocated_sequence_;
  // The last allocated sequence that is also published to the readers. This is
  // applicable only when last_seq_same_as_publish_seq_ is not set. Otherwise
  // last_sequence_ also indicates the last published seq.
  // We have last_sequence <= last_published_sequence_ <=
  // last_allocated_sequence_
  std::atomic<uint64_t> last_published_sequence_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  std::unique_ptr<log::Writer> descriptor_log_;

  // generates a increasing version number for every new version
  uint64_t current_version_number_;

  // Queue of writers to the manifest file
  std::deque<ManifestWriter*> manifest_writers_;

  // Current size of manifest file
  uint64_t manifest_file_size_;

  std::vector<ObsoleteFileInfo> obsolete_files_;
  std::vector<std::string> obsolete_manifests_;

  // env options for all reads and writes except compactions
  EnvOptions env_options_;

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);

  void LogAndApplyCFHelper(VersionEdit* edit);
  void LogAndApplyHelper(ColumnFamilyData* cfd, VersionBuilder* b, Version* v,
                         VersionEdit* edit, InstrumentedMutex* mu);
};

}  // namespace rocksdb
