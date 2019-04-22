//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
#pragma once
#include "rocksdb/env.h"
#include "rocksdb/slice_transform.h"

namespace rocksdb {

class TableCache;
class VersionStorageInfo;
class VersionEdit;
struct FileMetaData;
class InternalStats;

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionBuilder {
 public:
  VersionBuilder(const EnvOptions& env_options, TableCache* table_cache,
                 VersionStorageInfo* base_vstorage, Logger* info_log = nullptr);
  ~VersionBuilder();
  void CheckConsistency(VersionStorageInfo* vstorage);
  void CheckConsistencyForDeletes(VersionEdit* edit, uint64_t number,
                                  int level);
  bool CheckConsistencyForNumLevels();
  void Apply(VersionEdit* edit);
  void SaveTo(VersionStorageInfo* vstorage);
  void LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                         bool prefetch_index_and_filter_in_cache,
                         bool is_initial_load,
                         const SliceTransform* prefix_extractor);
  void MaybeAddFile(VersionStorageInfo* vstorage, int level, FileMetaData* f);

 private:
  class Rep;
  Rep* rep_;
};

extern bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b);
}  // namespace rocksdb
