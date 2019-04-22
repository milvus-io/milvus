// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "rocksdb/options.h"

namespace rocksdb {

struct ImmutableDBOptions {
  ImmutableDBOptions();
  explicit ImmutableDBOptions(const DBOptions& options);

  void Dump(Logger* log) const;

  bool create_if_missing;
  bool create_missing_column_families;
  bool error_if_exists;
  bool paranoid_checks;
  Env* env;
  std::shared_ptr<RateLimiter> rate_limiter;
  std::shared_ptr<SstFileManager> sst_file_manager;
  std::shared_ptr<Logger> info_log;
  InfoLogLevel info_log_level;
  int max_file_opening_threads;
  std::shared_ptr<Statistics> statistics;
  bool use_fsync;
  std::vector<DbPath> db_paths;
  std::string db_log_dir;
  std::string wal_dir;
  uint32_t max_subcompactions;
  int max_background_flushes;
  size_t max_log_file_size;
  size_t log_file_time_to_roll;
  size_t keep_log_file_num;
  size_t recycle_log_file_num;
  uint64_t max_manifest_file_size;
  int table_cache_numshardbits;
  uint64_t wal_ttl_seconds;
  uint64_t wal_size_limit_mb;
  size_t manifest_preallocation_size;
  bool allow_mmap_reads;
  bool allow_mmap_writes;
  bool use_direct_reads;
  bool use_direct_io_for_flush_and_compaction;
  bool allow_fallocate;
  bool is_fd_close_on_exec;
  bool advise_random_on_open;
  size_t db_write_buffer_size;
  std::shared_ptr<WriteBufferManager> write_buffer_manager;
  DBOptions::AccessHint access_hint_on_compaction_start;
  bool new_table_reader_for_compaction_inputs;
  size_t random_access_max_buffer_size;
  bool use_adaptive_mutex;
  std::vector<std::shared_ptr<EventListener>> listeners;
  bool enable_thread_tracking;
  bool enable_pipelined_write;
  bool allow_concurrent_memtable_write;
  bool enable_write_thread_adaptive_yield;
  uint64_t write_thread_max_yield_usec;
  uint64_t write_thread_slow_yield_usec;
  bool skip_stats_update_on_db_open;
  WALRecoveryMode wal_recovery_mode;
  bool allow_2pc;
  std::shared_ptr<Cache> row_cache;
#ifndef ROCKSDB_LITE
  WalFilter* wal_filter;
#endif  // ROCKSDB_LITE
  bool fail_if_options_file_error;
  bool dump_malloc_stats;
  bool avoid_flush_during_recovery;
  bool allow_ingest_behind;
  bool preserve_deletes;
  bool two_write_queues;
  bool manual_wal_flush;
  bool atomic_flush;
};

struct MutableDBOptions {
  MutableDBOptions();
  explicit MutableDBOptions(const MutableDBOptions& options) = default;
  explicit MutableDBOptions(const DBOptions& options);

  void Dump(Logger* log) const;

  int max_background_jobs;
  int base_background_compactions;
  int max_background_compactions;
  bool avoid_flush_during_shutdown;
  size_t writable_file_max_buffer_size;
  uint64_t delayed_write_rate;
  uint64_t max_total_wal_size;
  uint64_t delete_obsolete_files_period_micros;
  unsigned int stats_dump_period_sec;
  unsigned int stats_persist_period_sec;
  size_t stats_history_buffer_size;
  int max_open_files;
  uint64_t bytes_per_sync;
  uint64_t wal_bytes_per_sync;
  size_t compaction_readahead_size;
};

}  // namespace rocksdb
