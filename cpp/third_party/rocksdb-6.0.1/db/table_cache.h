//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#pragma once
#include <string>
#include <vector>
#include <stdint.h>

#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/table_reader.h"

namespace rocksdb {

class Env;
class Arena;
struct FileDescriptor;
class GetContext;
class HistogramImpl;

class TableCache {
 public:
  TableCache(const ImmutableCFOptions& ioptions,
             const EnvOptions& storage_options, Cache* cache);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-nullptr, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or nullptr if no Table object underlies
  // the returned iterator.  The returned "*tableptr" object is owned by
  // the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  // @param range_del_agg If non-nullptr, adds range deletions to the
  //    aggregator. If an error occurs, returns it in a NewErrorInternalIterator
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  InternalIterator* NewIterator(
      const ReadOptions& options, const EnvOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileMetaData& file_meta, RangeDelAggregator* range_del_agg,
      const SliceTransform* prefix_extractor = nullptr,
      TableReader** table_reader_ptr = nullptr,
      HistogramImpl* file_read_hist = nullptr, bool for_compaction = false,
      Arena* arena = nullptr, bool skip_filters = false, int level = -1,
      const InternalKey* smallest_compaction_key = nullptr,
      const InternalKey* largest_compaction_key = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value) repeatedly until
  // it returns false.
  // @param get_context State for get operation. If its range_del_agg() returns
  //    non-nullptr, adds range deletions to the aggregator. If an error occurs,
  //    returns non-ok status.
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  Status Get(const ReadOptions& options,
             const InternalKeyComparator& internal_comparator,
             const FileMetaData& file_meta, const Slice& k,
             GetContext* get_context,
             const SliceTransform* prefix_extractor = nullptr,
             HistogramImpl* file_read_hist = nullptr, bool skip_filters = false,
             int level = -1);

  // Evict any entry for the specified file number
  static void Evict(Cache* cache, uint64_t file_number);

  // Clean table handle and erase it from the table cache
  // Used in DB close, or the file is not live anymore.
  void EraseHandle(const FileDescriptor& fd, Cache::Handle* handle);

  // Find table reader
  // @param skip_filters Disables loading/accessing the filter block
  // @param level == -1 means not specified
  Status FindTable(const EnvOptions& toptions,
                   const InternalKeyComparator& internal_comparator,
                   const FileDescriptor& file_fd, Cache::Handle**,
                   const SliceTransform* prefix_extractor = nullptr,
                   const bool no_io = false, bool record_read_stats = true,
                   HistogramImpl* file_read_hist = nullptr,
                   bool skip_filters = false, int level = -1,
                   bool prefetch_index_and_filter_in_cache = true);

  // Get TableReader from a cache handle.
  TableReader* GetTableReaderFromHandle(Cache::Handle* handle);

  // Get the table properties of a given table.
  // @no_io: indicates if we should load table to the cache if it is not present
  //         in table cache yet.
  // @returns: `properties` will be reset on success. Please note that we will
  //            return Status::Incomplete() if table is not present in cache and
  //            we set `no_io` to be true.
  Status GetTableProperties(const EnvOptions& toptions,
                            const InternalKeyComparator& internal_comparator,
                            const FileDescriptor& file_meta,
                            std::shared_ptr<const TableProperties>* properties,
                            const SliceTransform* prefix_extractor = nullptr,
                            bool no_io = false);

  // Return total memory usage of the table reader of the file.
  // 0 if table reader of the file is not loaded.
  size_t GetMemoryUsageByTableReader(
      const EnvOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileDescriptor& fd,
      const SliceTransform* prefix_extractor = nullptr);

  // Release the handle from a cache
  void ReleaseHandle(Cache::Handle* handle);

  Cache* get_cache() const { return cache_; }

  // Capacity of the backing Cache that indicates inifinite TableCache capacity.
  // For example when max_open_files is -1 we set the backing Cache to this.
  static const int kInfiniteCapacity = 0x400000;

  // The tables opened with this TableCache will be immortal, i.e., their
  // lifetime is as long as that of the DB.
  void SetTablesAreImmortal() {
    if (cache_->GetCapacity() >= kInfiniteCapacity) {
      immortal_tables_ = true;
    }
  }

 private:
  // Build a table reader
  Status GetTableReader(const EnvOptions& env_options,
                        const InternalKeyComparator& internal_comparator,
                        const FileDescriptor& fd, bool sequential_mode,
                        size_t readahead, bool record_read_stats,
                        HistogramImpl* file_read_hist,
                        std::unique_ptr<TableReader>* table_reader,
                        const SliceTransform* prefix_extractor = nullptr,
                        bool skip_filters = false, int level = -1,
                        bool prefetch_index_and_filter_in_cache = true,
                        bool for_compaction = false);

  const ImmutableCFOptions& ioptions_;
  const EnvOptions& env_options_;
  Cache* const cache_;
  std::string row_cache_id_;
  bool immortal_tables_;
};

}  // namespace rocksdb
