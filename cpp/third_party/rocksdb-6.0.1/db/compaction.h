//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "db/version_set.h"
#include "options/cf_options.h"
#include "util/arena.h"
#include "util/autovector.h"

namespace rocksdb {

// Utility for comparing sstable boundary keys. Returns -1 if either a or b is
// null which provides the property that a==null indicates a key that is less
// than any key and b==null indicates a key that is greater than any key. Note
// that the comparison is performed primarily on the user-key portion of the
// key. If the user-keys compare equal, an additional test is made to sort
// range tombstone sentinel keys before other keys with the same user-key. The
// result is that 2 user-keys will compare equal if they differ purely on
// their sequence number and value, but the range tombstone sentinel for that
// user-key will compare not equal. This is necessary because the range
// tombstone sentinel key is set as the largest key for an sstable even though
// that key never appears in the database. We don't want adjacent sstables to
// be considered overlapping if they are separated by the range tombstone
// sentinel.
int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey& b);
int sstableKeyCompare(const Comparator* user_cmp, const InternalKey* a,
                      const InternalKey& b);
int sstableKeyCompare(const Comparator* user_cmp, const InternalKey& a,
                      const InternalKey* b);

// An AtomicCompactionUnitBoundary represents a range of keys [smallest,
// largest] that exactly spans one ore more neighbouring SSTs on the same
// level. Every pair of  SSTs in this range "overlap" (i.e., the largest
// user key of one file is the smallest user key of the next file). These
// boundaries are propagated down to RangeDelAggregator during compaction
// to provide safe truncation boundaries for range tombstones.
struct AtomicCompactionUnitBoundary {
  const InternalKey* smallest = nullptr;
  const InternalKey* largest = nullptr;
};

// The structure that manages compaction input files associated
// with the same physical level.
struct CompactionInputFiles {
  int level;
  std::vector<FileMetaData*> files;
  std::vector<AtomicCompactionUnitBoundary> atomic_compaction_unit_boundaries;
  inline bool empty() const { return files.empty(); }
  inline size_t size() const { return files.size(); }
  inline void clear() { files.clear(); }
  inline FileMetaData* operator[](size_t i) const { return files[i]; }
};

class Version;
class ColumnFamilyData;
class VersionStorageInfo;
class CompactionFilter;

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  Compaction(VersionStorageInfo* input_version,
             const ImmutableCFOptions& immutable_cf_options,
             const MutableCFOptions& mutable_cf_options,
             std::vector<CompactionInputFiles> inputs, int output_level,
             uint64_t target_file_size, uint64_t max_compaction_bytes,
             uint32_t output_path_id, CompressionType compression,
             CompressionOptions compression_opts, uint32_t max_subcompactions,
             std::vector<FileMetaData*> grandparents,
             bool manual_compaction = false, double score = -1,
             bool deletion_compaction = false,
             CompactionReason compaction_reason = CompactionReason::kUnknown);

  // No copying allowed
  Compaction(const Compaction&) = delete;
  void operator=(const Compaction&) = delete;

  ~Compaction();

  // Returns the level associated to the specified compaction input level.
  // If compaction_input_level is not specified, then input_level is set to 0.
  int level(size_t compaction_input_level = 0) const {
    return inputs_[compaction_input_level].level;
  }

  int start_level() const { return start_level_; }

  // Outputs will go to this level
  int output_level() const { return output_level_; }

  // Returns the number of input levels in this compaction.
  size_t num_input_levels() const { return inputs_.size(); }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // Returns the number of input files associated to the specified
  // compaction input level.
  // The function will return 0 if when "compaction_input_level" < 0
  // or "compaction_input_level" >= "num_input_levels()".
  size_t num_input_files(size_t compaction_input_level) const {
    if (compaction_input_level < inputs_.size()) {
      return inputs_[compaction_input_level].size();
    }
    return 0;
  }

  // Returns input version of the compaction
  Version* input_version() const { return input_version_; }

  // Returns the ColumnFamilyData associated with the compaction.
  ColumnFamilyData* column_family_data() const { return cfd_; }

  // Returns the file meta data of the 'i'th input file at the
  // specified compaction input level.
  // REQUIREMENT: "compaction_input_level" must be >= 0 and
  //              < "input_levels()"
  FileMetaData* input(size_t compaction_input_level, size_t i) const {
    assert(compaction_input_level < inputs_.size());
    return inputs_[compaction_input_level][i];
  }

  const std::vector<AtomicCompactionUnitBoundary>* boundaries(
      size_t compaction_input_level) const {
    assert(compaction_input_level < inputs_.size());
    return &inputs_[compaction_input_level].atomic_compaction_unit_boundaries;
  }

  // Returns the list of file meta data of the specified compaction
  // input level.
  // REQUIREMENT: "compaction_input_level" must be >= 0 and
  //              < "input_levels()"
  const std::vector<FileMetaData*>* inputs(
      size_t compaction_input_level) const {
    assert(compaction_input_level < inputs_.size());
    return &inputs_[compaction_input_level].files;
  }

  const std::vector<CompactionInputFiles>* inputs() { return &inputs_; }

  // Returns the LevelFilesBrief of the specified compaction input level.
  const LevelFilesBrief* input_levels(size_t compaction_input_level) const {
    return &input_levels_[compaction_input_level];
  }

  // Maximum size of files to build during this compaction.
  uint64_t max_output_file_size() const { return max_output_file_size_; }

  // What compression for output
  CompressionType output_compression() const { return output_compression_; }

  // What compression options for output
  CompressionOptions output_compression_opts() const {
    return output_compression_opts_;
  }

  // Whether need to write output file to second DB path.
  uint32_t output_path_id() const { return output_path_id_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // If true, then the compaction can be done by simply deleting input files.
  bool deletion_compaction() const { return deletion_compaction_; }

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the available information we have guarantees that
  // the input "user_key" does not exist in any level beyond "output_level()".
  bool KeyNotExistsBeyondOutputLevel(const Slice& user_key,
                                     std::vector<size_t>* level_ptrs) const;

  // Clear all files to indicate that they are not being compacted
  // Delete this compaction from the list of running compactions.
  //
  // Requirement: DB mutex held
  void ReleaseCompactionFiles(Status status);

  // Returns the summary of the compaction in "output" with maximum "len"
  // in bytes.  The caller is responsible for the memory management of
  // "output".
  void Summary(char* output, int len);

  // Return the score that was used to pick this compaction run.
  double score() const { return score_; }

  // Is this compaction creating a file in the bottom most level?
  bool bottommost_level() const { return bottommost_level_; }

  // Does this compaction include all sst files?
  bool is_full_compaction() const { return is_full_compaction_; }

  // Was this compaction triggered manually by the client?
  bool is_manual_compaction() const { return is_manual_compaction_; }

  // Used when allow_trivial_move option is set in
  // Universal compaction. If all the input files are
  // non overlapping, then is_trivial_move_ variable
  // will be set true, else false
  void set_is_trivial_move(bool trivial_move) {
    is_trivial_move_ = trivial_move;
  }

  // Used when allow_trivial_move option is set in
  // Universal compaction. Returns true, if the input files
  // are non-overlapping and can be trivially moved.
  bool is_trivial_move() const { return is_trivial_move_; }

  // How many total levels are there?
  int number_levels() const { return number_levels_; }

  // Return the ImmutableCFOptions that should be used throughout the compaction
  // procedure
  const ImmutableCFOptions* immutable_cf_options() const {
    return &immutable_cf_options_;
  }

  // Return the MutableCFOptions that should be used throughout the compaction
  // procedure
  const MutableCFOptions* mutable_cf_options() const {
    return &mutable_cf_options_;
  }

  // Returns the size in bytes that the output file should be preallocated to.
  // In level compaction, that is max_file_size_. In universal compaction, that
  // is the sum of all input file sizes.
  uint64_t OutputFilePreallocationSize() const;

  void SetInputVersion(Version* input_version);

  struct InputLevelSummaryBuffer {
    char buffer[128];
  };

  const char* InputLevelSummary(InputLevelSummaryBuffer* scratch) const;

  uint64_t CalculateTotalInputSize() const;

  // In case of compaction error, reset the nextIndex that is used
  // to pick up the next file to be compacted from files_by_size_
  void ResetNextCompactionIndex();

  // Create a CompactionFilter from compaction_filter_factory
  std::unique_ptr<CompactionFilter> CreateCompactionFilter() const;

  // Is the input level corresponding to output_level_ empty?
  bool IsOutputLevelEmpty() const;

  // Should this compaction be broken up into smaller ones run in parallel?
  bool ShouldFormSubcompactions() const;

  // test function to validate the functionality of IsBottommostLevel()
  // function -- determines if compaction with inputs and storage is bottommost
  static bool TEST_IsBottommostLevel(
      int output_level, VersionStorageInfo* vstorage,
      const std::vector<CompactionInputFiles>& inputs);

  TablePropertiesCollection GetOutputTableProperties() const {
    return output_table_properties_;
  }

  void SetOutputTableProperties(TablePropertiesCollection tp) {
    output_table_properties_ = std::move(tp);
  }

  Slice GetSmallestUserKey() const { return smallest_user_key_; }

  Slice GetLargestUserKey() const { return largest_user_key_; }

  int GetInputBaseLevel() const;

  CompactionReason compaction_reason() { return compaction_reason_; }

  const std::vector<FileMetaData*>& grandparents() const {
    return grandparents_;
  }

  uint64_t max_compaction_bytes() const { return max_compaction_bytes_; }

  uint32_t max_subcompactions() const { return max_subcompactions_; }

  uint64_t MaxInputFileCreationTime() const;

 private:
  // mark (or clear) all files that are being compacted
  void MarkFilesBeingCompacted(bool mark_as_compacted);

  // get the smallest and largest key present in files to be compacted
  static void GetBoundaryKeys(VersionStorageInfo* vstorage,
                              const std::vector<CompactionInputFiles>& inputs,
                              Slice* smallest_key, Slice* largest_key);

  // Get the atomic file boundaries for all files in the compaction. Necessary
  // in order to avoid the scenario described in
  // https://github.com/facebook/rocksdb/pull/4432#discussion_r221072219 and plumb
  // down appropriate key boundaries to RangeDelAggregator during compaction.
  static std::vector<CompactionInputFiles> PopulateWithAtomicBoundaries(
      VersionStorageInfo* vstorage, std::vector<CompactionInputFiles> inputs);

  // helper function to determine if compaction with inputs and storage is
  // bottommost
  static bool IsBottommostLevel(
      int output_level, VersionStorageInfo* vstorage,
      const std::vector<CompactionInputFiles>& inputs);

  static bool IsFullCompaction(VersionStorageInfo* vstorage,
                               const std::vector<CompactionInputFiles>& inputs);

  VersionStorageInfo* input_vstorage_;

  const int start_level_;    // the lowest level to be compacted
  const int output_level_;  // levels to which output files are stored
  uint64_t max_output_file_size_;
  uint64_t max_compaction_bytes_;
  uint32_t max_subcompactions_;
  const ImmutableCFOptions immutable_cf_options_;
  const MutableCFOptions mutable_cf_options_;
  Version* input_version_;
  VersionEdit edit_;
  const int number_levels_;
  ColumnFamilyData* cfd_;
  Arena arena_;          // Arena used to allocate space for file_levels_

  const uint32_t output_path_id_;
  CompressionType output_compression_;
  CompressionOptions output_compression_opts_;
  // If true, then the comaction can be done by simply deleting input files.
  const bool deletion_compaction_;

  // Compaction input files organized by level. Constant after construction
  const std::vector<CompactionInputFiles> inputs_;

  // A copy of inputs_, organized more closely in memory
  autovector<LevelFilesBrief, 2> input_levels_;

  // State used to check for number of overlapping grandparent files
  // (grandparent == "output_level_ + 1")
  std::vector<FileMetaData*> grandparents_;
  const double score_;         // score that was used to pick this compaction.

  // Is this compaction creating a file in the bottom most level?
  const bool bottommost_level_;
  // Does this compaction include all sst files?
  const bool is_full_compaction_;

  // Is this compaction requested by the client?
  const bool is_manual_compaction_;

  // True if we can do trivial move in Universal multi level
  // compaction
  bool is_trivial_move_;

  // Does input compression match the output compression?
  bool InputCompressionMatchesOutput() const;

  // table properties of output files
  TablePropertiesCollection output_table_properties_;

  // smallest user keys in compaction
  Slice smallest_user_key_;

  // largest user keys in compaction
  Slice largest_user_key_;

  // Reason for compaction
  CompactionReason compaction_reason_;
};

// Utility function
extern uint64_t TotalFileSize(const std::vector<FileMetaData*>& files);

}  // namespace rocksdb
