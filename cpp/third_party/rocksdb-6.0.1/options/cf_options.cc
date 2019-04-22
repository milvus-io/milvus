//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "options/cf_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <cassert>
#include <limits>
#include <string>
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/concurrent_task_limiter.h"

namespace rocksdb {

ImmutableCFOptions::ImmutableCFOptions(const Options& options)
    : ImmutableCFOptions(ImmutableDBOptions(options), options) {}

ImmutableCFOptions::ImmutableCFOptions(const ImmutableDBOptions& db_options,
                                       const ColumnFamilyOptions& cf_options)
    : compaction_style(cf_options.compaction_style),
      compaction_pri(cf_options.compaction_pri),
      user_comparator(cf_options.comparator),
      internal_comparator(InternalKeyComparator(cf_options.comparator)),
      merge_operator(cf_options.merge_operator.get()),
      compaction_filter(cf_options.compaction_filter),
      compaction_filter_factory(cf_options.compaction_filter_factory.get()),
      min_write_buffer_number_to_merge(
          cf_options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          cf_options.max_write_buffer_number_to_maintain),
      inplace_update_support(cf_options.inplace_update_support),
      inplace_callback(cf_options.inplace_callback),
      info_log(db_options.info_log.get()),
      statistics(db_options.statistics.get()),
      rate_limiter(db_options.rate_limiter.get()),
      info_log_level(db_options.info_log_level),
      env(db_options.env),
      allow_mmap_reads(db_options.allow_mmap_reads),
      allow_mmap_writes(db_options.allow_mmap_writes),
      db_paths(db_options.db_paths),
      memtable_factory(cf_options.memtable_factory.get()),
      table_factory(cf_options.table_factory.get()),
      table_properties_collector_factories(
          cf_options.table_properties_collector_factories),
      advise_random_on_open(db_options.advise_random_on_open),
      bloom_locality(cf_options.bloom_locality),
      purge_redundant_kvs_while_flush(
          cf_options.purge_redundant_kvs_while_flush),
      use_fsync(db_options.use_fsync),
      compression_per_level(cf_options.compression_per_level),
      bottommost_compression(cf_options.bottommost_compression),
      bottommost_compression_opts(cf_options.bottommost_compression_opts),
      compression_opts(cf_options.compression_opts),
      level_compaction_dynamic_level_bytes(
          cf_options.level_compaction_dynamic_level_bytes),
      access_hint_on_compaction_start(
          db_options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          db_options.new_table_reader_for_compaction_inputs),
      num_levels(cf_options.num_levels),
      optimize_filters_for_hits(cf_options.optimize_filters_for_hits),
      force_consistency_checks(cf_options.force_consistency_checks),
      allow_ingest_behind(db_options.allow_ingest_behind),
      preserve_deletes(db_options.preserve_deletes),
      listeners(db_options.listeners),
      row_cache(db_options.row_cache),
      max_subcompactions(db_options.max_subcompactions),
      memtable_insert_with_hint_prefix_extractor(
          cf_options.memtable_insert_with_hint_prefix_extractor.get()),
      cf_paths(cf_options.cf_paths),
      compaction_thread_limiter(cf_options.compaction_thread_limiter) {}

// Multiple two operands. If they overflow, return op1.
uint64_t MultiplyCheckOverflow(uint64_t op1, double op2) {
  if (op1 == 0 || op2 <= 0) {
    return 0;
  }
  if (port::kMaxUint64 / op1 < op2) {
    return op1;
  }
  return static_cast<uint64_t>(op1 * op2);
}

// when level_compaction_dynamic_level_bytes is true and leveled compaction
// is used, the base level is not always L1, so precomupted max_file_size can
// no longer be used. Recompute file_size_for_level from base level.
uint64_t MaxFileSizeForLevel(const MutableCFOptions& cf_options,
    int level, CompactionStyle compaction_style, int base_level,
    bool level_compaction_dynamic_level_bytes) {
  if (!level_compaction_dynamic_level_bytes || level < base_level ||
      compaction_style != kCompactionStyleLevel) {
    assert(level >= 0);
    assert(level < (int)cf_options.max_file_size.size());
    return cf_options.max_file_size[level];
  } else {
    assert(level >= 0 && base_level >= 0);
    assert(level - base_level < (int)cf_options.max_file_size.size());
    return cf_options.max_file_size[level - base_level];
  }
}

void MutableCFOptions::RefreshDerivedOptions(int num_levels,
                                             CompactionStyle compaction_style) {
  max_file_size.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    if (i == 0 && compaction_style == kCompactionStyleUniversal) {
      max_file_size[i] = ULLONG_MAX;
    } else if (i > 1) {
      max_file_size[i] = MultiplyCheckOverflow(max_file_size[i - 1],
                                               target_file_size_multiplier);
    } else {
      max_file_size[i] = target_file_size_base;
    }
  }
}

void MutableCFOptions::Dump(Logger* log) const {
  // Memtable related options
  ROCKS_LOG_INFO(log,
                 "                        write_buffer_size: %" ROCKSDB_PRIszt,
                 write_buffer_size);
  ROCKS_LOG_INFO(log, "                  max_write_buffer_number: %d",
                 max_write_buffer_number);
  ROCKS_LOG_INFO(log,
                 "                         arena_block_size: %" ROCKSDB_PRIszt,
                 arena_block_size);
  ROCKS_LOG_INFO(log, "              memtable_prefix_bloom_ratio: %f",
                 memtable_prefix_bloom_size_ratio);
  ROCKS_LOG_INFO(log, "              memtable_whole_key_filtering: %d",
                 memtable_whole_key_filtering);
  ROCKS_LOG_INFO(log,
                 "                  memtable_huge_page_size: %" ROCKSDB_PRIszt,
                 memtable_huge_page_size);
  ROCKS_LOG_INFO(log,
                 "                    max_successive_merges: %" ROCKSDB_PRIszt,
                 max_successive_merges);
  ROCKS_LOG_INFO(log,
                 "                 inplace_update_num_locks: %" ROCKSDB_PRIszt,
                 inplace_update_num_locks);
  ROCKS_LOG_INFO(
      log, "                         prefix_extractor: %s",
      prefix_extractor == nullptr ? "nullptr" : prefix_extractor->Name());
  ROCKS_LOG_INFO(log, "                 disable_auto_compactions: %d",
                 disable_auto_compactions);
  ROCKS_LOG_INFO(log, "      soft_pending_compaction_bytes_limit: %" PRIu64,
                 soft_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "      hard_pending_compaction_bytes_limit: %" PRIu64,
                 hard_pending_compaction_bytes_limit);
  ROCKS_LOG_INFO(log, "       level0_file_num_compaction_trigger: %d",
                 level0_file_num_compaction_trigger);
  ROCKS_LOG_INFO(log, "           level0_slowdown_writes_trigger: %d",
                 level0_slowdown_writes_trigger);
  ROCKS_LOG_INFO(log, "               level0_stop_writes_trigger: %d",
                 level0_stop_writes_trigger);
  ROCKS_LOG_INFO(log, "                     max_compaction_bytes: %" PRIu64,
                 max_compaction_bytes);
  ROCKS_LOG_INFO(log, "                    target_file_size_base: %" PRIu64,
                 target_file_size_base);
  ROCKS_LOG_INFO(log, "              target_file_size_multiplier: %d",
                 target_file_size_multiplier);
  ROCKS_LOG_INFO(log, "                 max_bytes_for_level_base: %" PRIu64,
                 max_bytes_for_level_base);
  ROCKS_LOG_INFO(log, "           max_bytes_for_level_multiplier: %f",
                 max_bytes_for_level_multiplier);
  ROCKS_LOG_INFO(log, "                                      ttl: %" PRIu64,
                 ttl);
  std::string result;
  char buf[10];
  for (const auto m : max_bytes_for_level_multiplier_additional) {
    snprintf(buf, sizeof(buf), "%d, ", m);
    result += buf;
  }
  if (result.size() >= 2) {
    result.resize(result.size() - 2);
  } else {
    result = "";
  }

  ROCKS_LOG_INFO(log, "max_bytes_for_level_multiplier_additional: %s",
                 result.c_str());
  ROCKS_LOG_INFO(log, "        max_sequential_skip_in_iterations: %" PRIu64,
                 max_sequential_skip_in_iterations);
  ROCKS_LOG_INFO(log, "                     paranoid_file_checks: %d",
                 paranoid_file_checks);
  ROCKS_LOG_INFO(log, "                       report_bg_io_stats: %d",
                 report_bg_io_stats);
  ROCKS_LOG_INFO(log, "                              compression: %d",
                 static_cast<int>(compression));

  // Universal Compaction Options
  ROCKS_LOG_INFO(log, "compaction_options_universal.size_ratio : %d",
                 compaction_options_universal.size_ratio);
  ROCKS_LOG_INFO(log, "compaction_options_universal.min_merge_width : %d",
                 compaction_options_universal.min_merge_width);
  ROCKS_LOG_INFO(log, "compaction_options_universal.max_merge_width : %d",
                 compaction_options_universal.max_merge_width);
  ROCKS_LOG_INFO(
      log, "compaction_options_universal.max_size_amplification_percent : %d",
      compaction_options_universal.max_size_amplification_percent);
  ROCKS_LOG_INFO(log,
                 "compaction_options_universal.compression_size_percent : %d",
                 compaction_options_universal.compression_size_percent);
  ROCKS_LOG_INFO(log, "compaction_options_universal.stop_style : %d",
                 compaction_options_universal.stop_style);
  ROCKS_LOG_INFO(
      log, "compaction_options_universal.allow_trivial_move : %d",
      static_cast<int>(compaction_options_universal.allow_trivial_move));

  // FIFO Compaction Options
  ROCKS_LOG_INFO(log, "compaction_options_fifo.max_table_files_size : %" PRIu64,
                 compaction_options_fifo.max_table_files_size);
  ROCKS_LOG_INFO(log, "compaction_options_fifo.allow_compaction : %d",
                 compaction_options_fifo.allow_compaction);
}

MutableCFOptions::MutableCFOptions(const Options& options)
    : MutableCFOptions(ColumnFamilyOptions(options)) {}

}  // namespace rocksdb
