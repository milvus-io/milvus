// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/db_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/wal_filter.h"
#include "util/logging.h"

namespace rocksdb {

ImmutableDBOptions::ImmutableDBOptions() : ImmutableDBOptions(Options()) {}

ImmutableDBOptions::ImmutableDBOptions(const DBOptions& options)
    : create_if_missing(options.create_if_missing),
      create_missing_column_families(options.create_missing_column_families),
      error_if_exists(options.error_if_exists),
      paranoid_checks(options.paranoid_checks),
      env(options.env),
      rate_limiter(options.rate_limiter),
      sst_file_manager(options.sst_file_manager),
      info_log(options.info_log),
      info_log_level(options.info_log_level),
      max_file_opening_threads(options.max_file_opening_threads),
      statistics(options.statistics),
      use_fsync(options.use_fsync),
      db_paths(options.db_paths),
      db_log_dir(options.db_log_dir),
      wal_dir(options.wal_dir),
      max_subcompactions(options.max_subcompactions),
      max_background_flushes(options.max_background_flushes),
      max_log_file_size(options.max_log_file_size),
      log_file_time_to_roll(options.log_file_time_to_roll),
      keep_log_file_num(options.keep_log_file_num),
      recycle_log_file_num(options.recycle_log_file_num),
      max_manifest_file_size(options.max_manifest_file_size),
      table_cache_numshardbits(options.table_cache_numshardbits),
      wal_ttl_seconds(options.WAL_ttl_seconds),
      wal_size_limit_mb(options.WAL_size_limit_MB),
      manifest_preallocation_size(options.manifest_preallocation_size),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      use_direct_reads(options.use_direct_reads),
      use_direct_io_for_flush_and_compaction(
          options.use_direct_io_for_flush_and_compaction),
      allow_fallocate(options.allow_fallocate),
      is_fd_close_on_exec(options.is_fd_close_on_exec),
      advise_random_on_open(options.advise_random_on_open),
      db_write_buffer_size(options.db_write_buffer_size),
      write_buffer_manager(options.write_buffer_manager),
      access_hint_on_compaction_start(options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          options.new_table_reader_for_compaction_inputs),
      random_access_max_buffer_size(options.random_access_max_buffer_size),
      use_adaptive_mutex(options.use_adaptive_mutex),
      listeners(options.listeners),
      enable_thread_tracking(options.enable_thread_tracking),
      enable_pipelined_write(options.enable_pipelined_write),
      allow_concurrent_memtable_write(options.allow_concurrent_memtable_write),
      enable_write_thread_adaptive_yield(
          options.enable_write_thread_adaptive_yield),
      write_thread_max_yield_usec(options.write_thread_max_yield_usec),
      write_thread_slow_yield_usec(options.write_thread_slow_yield_usec),
      skip_stats_update_on_db_open(options.skip_stats_update_on_db_open),
      wal_recovery_mode(options.wal_recovery_mode),
      allow_2pc(options.allow_2pc),
      row_cache(options.row_cache),
#ifndef ROCKSDB_LITE
      wal_filter(options.wal_filter),
#endif  // ROCKSDB_LITE
      fail_if_options_file_error(options.fail_if_options_file_error),
      dump_malloc_stats(options.dump_malloc_stats),
      avoid_flush_during_recovery(options.avoid_flush_during_recovery),
      allow_ingest_behind(options.allow_ingest_behind),
      preserve_deletes(options.preserve_deletes),
      two_write_queues(options.two_write_queues),
      manual_wal_flush(options.manual_wal_flush),
      atomic_flush(options.atomic_flush) {
}

void ImmutableDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "                        Options.error_if_exists: %d",
                   error_if_exists);
  ROCKS_LOG_HEADER(log, "                      Options.create_if_missing: %d",
                   create_if_missing);
  ROCKS_LOG_HEADER(log, "                        Options.paranoid_checks: %d",
                   paranoid_checks);
  ROCKS_LOG_HEADER(log, "                                    Options.env: %p",
                   env);
  ROCKS_LOG_HEADER(log, "                               Options.info_log: %p",
                   info_log.get());
  ROCKS_LOG_HEADER(log, "               Options.max_file_opening_threads: %d",
                   max_file_opening_threads);
  ROCKS_LOG_HEADER(log, "                             Options.statistics: %p",
                   statistics.get());
  ROCKS_LOG_HEADER(log, "                              Options.use_fsync: %d",
                   use_fsync);
  ROCKS_LOG_HEADER(
      log, "                      Options.max_log_file_size: %" ROCKSDB_PRIszt,
      max_log_file_size);
  ROCKS_LOG_HEADER(log,
                   "                 Options.max_manifest_file_size: %" PRIu64,
                   max_manifest_file_size);
  ROCKS_LOG_HEADER(
      log, "                  Options.log_file_time_to_roll: %" ROCKSDB_PRIszt,
      log_file_time_to_roll);
  ROCKS_LOG_HEADER(
      log, "                      Options.keep_log_file_num: %" ROCKSDB_PRIszt,
      keep_log_file_num);
  ROCKS_LOG_HEADER(
      log, "                   Options.recycle_log_file_num: %" ROCKSDB_PRIszt,
      recycle_log_file_num);
  ROCKS_LOG_HEADER(log, "                        Options.allow_fallocate: %d",
                   allow_fallocate);
  ROCKS_LOG_HEADER(log, "                       Options.allow_mmap_reads: %d",
                   allow_mmap_reads);
  ROCKS_LOG_HEADER(log, "                      Options.allow_mmap_writes: %d",
                   allow_mmap_writes);
  ROCKS_LOG_HEADER(log, "                       Options.use_direct_reads: %d",
                   use_direct_reads);
  ROCKS_LOG_HEADER(log,
                   "                       "
                   "Options.use_direct_io_for_flush_and_compaction: %d",
                   use_direct_io_for_flush_and_compaction);
  ROCKS_LOG_HEADER(log, "         Options.create_missing_column_families: %d",
                   create_missing_column_families);
  ROCKS_LOG_HEADER(log, "                             Options.db_log_dir: %s",
                   db_log_dir.c_str());
  ROCKS_LOG_HEADER(log, "                                Options.wal_dir: %s",
                   wal_dir.c_str());
  ROCKS_LOG_HEADER(log, "               Options.table_cache_numshardbits: %d",
                   table_cache_numshardbits);
  ROCKS_LOG_HEADER(log,
                   "                     Options.max_subcompactions: %" PRIu32,
                   max_subcompactions);
  ROCKS_LOG_HEADER(log, "                 Options.max_background_flushes: %d",
                   max_background_flushes);
  ROCKS_LOG_HEADER(log,
                   "                        Options.WAL_ttl_seconds: %" PRIu64,
                   wal_ttl_seconds);
  ROCKS_LOG_HEADER(log,
                   "                      Options.WAL_size_limit_MB: %" PRIu64,
                   wal_size_limit_mb);
  ROCKS_LOG_HEADER(
      log, "            Options.manifest_preallocation_size: %" ROCKSDB_PRIszt,
      manifest_preallocation_size);
  ROCKS_LOG_HEADER(log, "                    Options.is_fd_close_on_exec: %d",
                   is_fd_close_on_exec);
  ROCKS_LOG_HEADER(log, "                  Options.advise_random_on_open: %d",
                   advise_random_on_open);
  ROCKS_LOG_HEADER(
      log, "                   Options.db_write_buffer_size: %" ROCKSDB_PRIszt,
      db_write_buffer_size);
  ROCKS_LOG_HEADER(log, "                   Options.write_buffer_manager: %p",
                   write_buffer_manager.get());
  ROCKS_LOG_HEADER(log, "        Options.access_hint_on_compaction_start: %d",
                   static_cast<int>(access_hint_on_compaction_start));
  ROCKS_LOG_HEADER(log, " Options.new_table_reader_for_compaction_inputs: %d",
                   new_table_reader_for_compaction_inputs);
  ROCKS_LOG_HEADER(
      log, "          Options.random_access_max_buffer_size: %" ROCKSDB_PRIszt,
      random_access_max_buffer_size);
  ROCKS_LOG_HEADER(log, "                     Options.use_adaptive_mutex: %d",
                   use_adaptive_mutex);
  ROCKS_LOG_HEADER(log, "                           Options.rate_limiter: %p",
                   rate_limiter.get());
  Header(
      log, "    Options.sst_file_manager.rate_bytes_per_sec: %" PRIi64,
      sst_file_manager ? sst_file_manager->GetDeleteRateBytesPerSecond() : 0);
  ROCKS_LOG_HEADER(log, "                      Options.wal_recovery_mode: %d",
                   wal_recovery_mode);
  ROCKS_LOG_HEADER(log, "                 Options.enable_thread_tracking: %d",
                   enable_thread_tracking);
  ROCKS_LOG_HEADER(log, "                 Options.enable_pipelined_write: %d",
                   enable_pipelined_write);
  ROCKS_LOG_HEADER(log, "        Options.allow_concurrent_memtable_write: %d",
                   allow_concurrent_memtable_write);
  ROCKS_LOG_HEADER(log, "     Options.enable_write_thread_adaptive_yield: %d",
                   enable_write_thread_adaptive_yield);
  ROCKS_LOG_HEADER(log,
                   "            Options.write_thread_max_yield_usec: %" PRIu64,
                   write_thread_max_yield_usec);
  ROCKS_LOG_HEADER(log,
                   "           Options.write_thread_slow_yield_usec: %" PRIu64,
                   write_thread_slow_yield_usec);
  if (row_cache) {
    ROCKS_LOG_HEADER(
        log, "                              Options.row_cache: %" PRIu64,
        row_cache->GetCapacity());
  } else {
    ROCKS_LOG_HEADER(log,
                     "                              Options.row_cache: None");
  }
#ifndef ROCKSDB_LITE
  ROCKS_LOG_HEADER(log, "                             Options.wal_filter: %s",
                   wal_filter ? wal_filter->Name() : "None");
#endif  // ROCKDB_LITE

  ROCKS_LOG_HEADER(log, "            Options.avoid_flush_during_recovery: %d",
                   avoid_flush_during_recovery);
  ROCKS_LOG_HEADER(log, "            Options.allow_ingest_behind: %d",
                   allow_ingest_behind);
  ROCKS_LOG_HEADER(log, "            Options.preserve_deletes: %d",
                   preserve_deletes);
  ROCKS_LOG_HEADER(log, "            Options.two_write_queues: %d",
                   two_write_queues);
  ROCKS_LOG_HEADER(log, "            Options.manual_wal_flush: %d",
                   manual_wal_flush);
}

MutableDBOptions::MutableDBOptions()
    : max_background_jobs(2),
      base_background_compactions(-1),
      max_background_compactions(-1),
      avoid_flush_during_shutdown(false),
      writable_file_max_buffer_size(1024 * 1024),
      delayed_write_rate(2 * 1024U * 1024U),
      max_total_wal_size(0),
      delete_obsolete_files_period_micros(6ULL * 60 * 60 * 1000000),
      stats_dump_period_sec(600),
      stats_persist_period_sec(600),
      stats_history_buffer_size(1024 * 1024),
      max_open_files(-1),
      bytes_per_sync(0),
      wal_bytes_per_sync(0),
      compaction_readahead_size(0) {}

MutableDBOptions::MutableDBOptions(const DBOptions& options)
    : max_background_jobs(options.max_background_jobs),
      base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions),
      avoid_flush_during_shutdown(options.avoid_flush_during_shutdown),
      writable_file_max_buffer_size(options.writable_file_max_buffer_size),
      delayed_write_rate(options.delayed_write_rate),
      max_total_wal_size(options.max_total_wal_size),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      stats_dump_period_sec(options.stats_dump_period_sec),
      stats_persist_period_sec(options.stats_persist_period_sec),
      stats_history_buffer_size(options.stats_history_buffer_size),
      max_open_files(options.max_open_files),
      bytes_per_sync(options.bytes_per_sync),
      wal_bytes_per_sync(options.wal_bytes_per_sync),
      compaction_readahead_size(options.compaction_readahead_size) {}

void MutableDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(log, "            Options.max_background_jobs: %d",
                   max_background_jobs);
  ROCKS_LOG_HEADER(log, "            Options.max_background_compactions: %d",
                   max_background_compactions);
  ROCKS_LOG_HEADER(log, "            Options.avoid_flush_during_shutdown: %d",
                   avoid_flush_during_shutdown);
  ROCKS_LOG_HEADER(
      log, "          Options.writable_file_max_buffer_size: %" ROCKSDB_PRIszt,
      writable_file_max_buffer_size);
  ROCKS_LOG_HEADER(log, "            Options.delayed_write_rate : %" PRIu64,
                   delayed_write_rate);
  ROCKS_LOG_HEADER(log, "            Options.max_total_wal_size: %" PRIu64,
                   max_total_wal_size);
  ROCKS_LOG_HEADER(
      log, "            Options.delete_obsolete_files_period_micros: %" PRIu64,
      delete_obsolete_files_period_micros);
  ROCKS_LOG_HEADER(log, "                  Options.stats_dump_period_sec: %u",
                   stats_dump_period_sec);
  ROCKS_LOG_HEADER(log, "                Options.stats_persist_period_sec: %d",
                   stats_persist_period_sec);
  ROCKS_LOG_HEADER(log, "                Options.stats_history_buffer_size: %d",
                   stats_history_buffer_size);
  ROCKS_LOG_HEADER(log, "                         Options.max_open_files: %d",
                   max_open_files);
  ROCKS_LOG_HEADER(log,
                   "                         Options.bytes_per_sync: %" PRIu64,
                   bytes_per_sync);
  ROCKS_LOG_HEADER(log,
                   "                     Options.wal_bytes_per_sync: %" PRIu64,
                   wal_bytes_per_sync);
  ROCKS_LOG_HEADER(log,
                   "      Options.compaction_readahead_size: %" ROCKSDB_PRIszt,
                   compaction_readahead_size);
}

}  // namespace rocksdb
