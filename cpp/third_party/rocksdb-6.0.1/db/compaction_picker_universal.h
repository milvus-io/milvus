//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef ROCKSDB_LITE

#include "db/compaction_picker.h"

namespace rocksdb {
class UniversalCompactionPicker : public CompactionPicker {
 public:
  UniversalCompactionPicker(const ImmutableCFOptions& ioptions,
                            const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
  virtual Compaction* PickCompaction(const std::string& cf_name,
                                     const MutableCFOptions& mutable_cf_options,
                                     VersionStorageInfo* vstorage,
                                     LogBuffer* log_buffer) override;

  virtual int MaxOutputLevel() const override { return NumberLevels() - 1; }

  virtual bool NeedsCompaction(
      const VersionStorageInfo* vstorage) const override;

 private:
  struct SortedRun {
    SortedRun(int _level, FileMetaData* _file, uint64_t _size,
              uint64_t _compensated_file_size, bool _being_compacted)
        : level(_level),
          file(_file),
          size(_size),
          compensated_file_size(_compensated_file_size),
          being_compacted(_being_compacted) {
      assert(compensated_file_size > 0);
      assert(level != 0 || file != nullptr);
    }

    void Dump(char* out_buf, size_t out_buf_size,
              bool print_path = false) const;

    // sorted_run_count is added into the string to print
    void DumpSizeInfo(char* out_buf, size_t out_buf_size,
                      size_t sorted_run_count) const;

    int level;
    // `file` Will be null for level > 0. For level = 0, the sorted run is
    // for this file.
    FileMetaData* file;
    // For level > 0, `size` and `compensated_file_size` are sum of sizes all
    // files in the level. `being_compacted` should be the same for all files
    // in a non-zero level. Use the value here.
    uint64_t size;
    uint64_t compensated_file_size;
    bool being_compacted;
  };

  // Pick Universal compaction to limit read amplification
  Compaction* PickCompactionToReduceSortedRuns(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score, unsigned int ratio,
      unsigned int num_files, const std::vector<SortedRun>& sorted_runs,
      LogBuffer* log_buffer);

  // Pick Universal compaction to limit space amplification.
  Compaction* PickCompactionToReduceSizeAmp(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score,
      const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer);

  Compaction* PickDeleteTriggeredCompaction(
      const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage, double score,
      const std::vector<SortedRun>& sorted_runs, LogBuffer* log_buffer);

  // Used in universal compaction when the enabled_trivial_move
  // option is set. Checks whether there are any overlapping files
  // in the input. Returns true if the input files are non
  // overlapping.
  bool IsInputFilesNonOverlapping(Compaction* c);

  static std::vector<SortedRun> CalculateSortedRuns(
      const VersionStorageInfo& vstorage, const ImmutableCFOptions& ioptions,
      const MutableCFOptions& mutable_cf_options);

  // Pick a path ID to place a newly generated file, with its estimated file
  // size.
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            uint64_t file_size);
};
}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
