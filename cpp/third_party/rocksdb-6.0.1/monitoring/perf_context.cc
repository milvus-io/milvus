//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <sstream>
#include "monitoring/perf_context_imp.h"

namespace rocksdb {

#if defined(NPERF_CONTEXT) || !defined(ROCKSDB_SUPPORT_THREAD_LOCAL)
PerfContext perf_context;
#else
#if defined(OS_SOLARIS)
__thread PerfContext perf_context_;
#else
thread_local PerfContext perf_context;
#endif
#endif

PerfContext* get_perf_context() {
#if defined(NPERF_CONTEXT) || !defined(ROCKSDB_SUPPORT_THREAD_LOCAL)
  return &perf_context;
#else
#if defined(OS_SOLARIS)
  return &perf_context_;
#else
  return &perf_context;
#endif
#endif
}

PerfContext::~PerfContext() {
#if !defined(NPERF_CONTEXT) && defined(ROCKSDB_SUPPORT_THREAD_LOCAL) && !defined(OS_SOLARIS)
  ClearPerLevelPerfContext();
#endif
}

PerfContext::PerfContext(const PerfContext& other) {
#ifndef NPERF_CONTEXT
  user_key_comparison_count = other.user_key_comparison_count;
  block_cache_hit_count = other.block_cache_hit_count;
  block_read_count = other.block_read_count;
  block_read_byte = other.block_read_byte;
  block_read_time = other.block_read_time;
  block_cache_index_hit_count = other.block_cache_index_hit_count;
  index_block_read_count = other.index_block_read_count;
  block_cache_filter_hit_count = other.block_cache_filter_hit_count;
  filter_block_read_count = other.filter_block_read_count;
  compression_dict_block_read_count = other.compression_dict_block_read_count;
  block_checksum_time = other.block_checksum_time;
  block_decompress_time = other.block_decompress_time;
  get_read_bytes = other.get_read_bytes;
  multiget_read_bytes = other.multiget_read_bytes;
  iter_read_bytes = other.iter_read_bytes;
  internal_key_skipped_count = other.internal_key_skipped_count;
  internal_delete_skipped_count = other.internal_delete_skipped_count;
  internal_recent_skipped_count = other.internal_recent_skipped_count;
  internal_merge_count = other.internal_merge_count;
  write_wal_time = other.write_wal_time;
  get_snapshot_time = other.get_snapshot_time;
  get_from_memtable_time = other.get_from_memtable_time;
  get_from_memtable_count = other.get_from_memtable_count;
  get_post_process_time = other.get_post_process_time;
  get_from_output_files_time = other.get_from_output_files_time;
  seek_on_memtable_time = other.seek_on_memtable_time;
  seek_on_memtable_count = other.seek_on_memtable_count;
  next_on_memtable_count = other.next_on_memtable_count;
  prev_on_memtable_count = other.prev_on_memtable_count;
  seek_child_seek_time = other.seek_child_seek_time;
  seek_child_seek_count = other.seek_child_seek_count;
  seek_min_heap_time = other.seek_min_heap_time;
  seek_internal_seek_time = other.seek_internal_seek_time;
  find_next_user_entry_time = other.find_next_user_entry_time;
  write_pre_and_post_process_time = other.write_pre_and_post_process_time;
  write_memtable_time = other.write_memtable_time;
  write_delay_time = other.write_delay_time;
  write_thread_wait_nanos = other.write_thread_wait_nanos;
  write_scheduling_flushes_compactions_time =
      other.write_scheduling_flushes_compactions_time;
  db_mutex_lock_nanos = other.db_mutex_lock_nanos;
  db_condition_wait_nanos = other.db_condition_wait_nanos;
  merge_operator_time_nanos = other.merge_operator_time_nanos;
  read_index_block_nanos = other.read_index_block_nanos;
  read_filter_block_nanos = other.read_filter_block_nanos;
  new_table_block_iter_nanos = other.new_table_block_iter_nanos;
  new_table_iterator_nanos = other.new_table_iterator_nanos;
  block_seek_nanos = other.block_seek_nanos;
  find_table_nanos = other.find_table_nanos;
  bloom_memtable_hit_count = other.bloom_memtable_hit_count;
  bloom_memtable_miss_count = other.bloom_memtable_miss_count;
  bloom_sst_hit_count = other.bloom_sst_hit_count;
  bloom_sst_miss_count = other.bloom_sst_miss_count;
  key_lock_wait_time = other.key_lock_wait_time;
  key_lock_wait_count = other.key_lock_wait_count;

  env_new_sequential_file_nanos = other.env_new_sequential_file_nanos;
  env_new_random_access_file_nanos = other.env_new_random_access_file_nanos;
  env_new_writable_file_nanos = other.env_new_writable_file_nanos;
  env_reuse_writable_file_nanos = other.env_reuse_writable_file_nanos;
  env_new_random_rw_file_nanos = other.env_new_random_rw_file_nanos;
  env_new_directory_nanos = other.env_new_directory_nanos;
  env_file_exists_nanos = other.env_file_exists_nanos;
  env_get_children_nanos = other.env_get_children_nanos;
  env_get_children_file_attributes_nanos =
      other.env_get_children_file_attributes_nanos;
  env_delete_file_nanos = other.env_delete_file_nanos;
  env_create_dir_nanos = other.env_create_dir_nanos;
  env_create_dir_if_missing_nanos = other.env_create_dir_if_missing_nanos;
  env_delete_dir_nanos = other.env_delete_dir_nanos;
  env_get_file_size_nanos = other.env_get_file_size_nanos;
  env_get_file_modification_time_nanos =
      other.env_get_file_modification_time_nanos;
  env_rename_file_nanos = other.env_rename_file_nanos;
  env_link_file_nanos = other.env_link_file_nanos;
  env_lock_file_nanos = other.env_lock_file_nanos;
  env_unlock_file_nanos = other.env_unlock_file_nanos;
  env_new_logger_nanos = other.env_new_logger_nanos;
  get_cpu_nanos = other.get_cpu_nanos;
  if (per_level_perf_context_enabled && level_to_perf_context != nullptr) {
    ClearPerLevelPerfContext();
  }
  if (other.level_to_perf_context != nullptr) {
    level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
    *level_to_perf_context = *other.level_to_perf_context;
  }
  per_level_perf_context_enabled = other.per_level_perf_context_enabled;
#endif
}

PerfContext::PerfContext(PerfContext&& other) noexcept {
#ifndef NPERF_CONTEXT
  user_key_comparison_count = other.user_key_comparison_count;
  block_cache_hit_count = other.block_cache_hit_count;
  block_read_count = other.block_read_count;
  block_read_byte = other.block_read_byte;
  block_read_time = other.block_read_time;
  block_cache_index_hit_count = other.block_cache_index_hit_count;
  index_block_read_count = other.index_block_read_count;
  block_cache_filter_hit_count = other.block_cache_filter_hit_count;
  filter_block_read_count = other.filter_block_read_count;
  compression_dict_block_read_count = other.compression_dict_block_read_count;
  block_checksum_time = other.block_checksum_time;
  block_decompress_time = other.block_decompress_time;
  get_read_bytes = other.get_read_bytes;
  multiget_read_bytes = other.multiget_read_bytes;
  iter_read_bytes = other.iter_read_bytes;
  internal_key_skipped_count = other.internal_key_skipped_count;
  internal_delete_skipped_count = other.internal_delete_skipped_count;
  internal_recent_skipped_count = other.internal_recent_skipped_count;
  internal_merge_count = other.internal_merge_count;
  write_wal_time = other.write_wal_time;
  get_snapshot_time = other.get_snapshot_time;
  get_from_memtable_time = other.get_from_memtable_time;
  get_from_memtable_count = other.get_from_memtable_count;
  get_post_process_time = other.get_post_process_time;
  get_from_output_files_time = other.get_from_output_files_time;
  seek_on_memtable_time = other.seek_on_memtable_time;
  seek_on_memtable_count = other.seek_on_memtable_count;
  next_on_memtable_count = other.next_on_memtable_count;
  prev_on_memtable_count = other.prev_on_memtable_count;
  seek_child_seek_time = other.seek_child_seek_time;
  seek_child_seek_count = other.seek_child_seek_count;
  seek_min_heap_time = other.seek_min_heap_time;
  seek_internal_seek_time = other.seek_internal_seek_time;
  find_next_user_entry_time = other.find_next_user_entry_time;
  write_pre_and_post_process_time = other.write_pre_and_post_process_time;
  write_memtable_time = other.write_memtable_time;
  write_delay_time = other.write_delay_time;
  write_thread_wait_nanos = other.write_thread_wait_nanos;
  write_scheduling_flushes_compactions_time =
      other.write_scheduling_flushes_compactions_time;
  db_mutex_lock_nanos = other.db_mutex_lock_nanos;
  db_condition_wait_nanos = other.db_condition_wait_nanos;
  merge_operator_time_nanos = other.merge_operator_time_nanos;
  read_index_block_nanos = other.read_index_block_nanos;
  read_filter_block_nanos = other.read_filter_block_nanos;
  new_table_block_iter_nanos = other.new_table_block_iter_nanos;
  new_table_iterator_nanos = other.new_table_iterator_nanos;
  block_seek_nanos = other.block_seek_nanos;
  find_table_nanos = other.find_table_nanos;
  bloom_memtable_hit_count = other.bloom_memtable_hit_count;
  bloom_memtable_miss_count = other.bloom_memtable_miss_count;
  bloom_sst_hit_count = other.bloom_sst_hit_count;
  bloom_sst_miss_count = other.bloom_sst_miss_count;
  key_lock_wait_time = other.key_lock_wait_time;
  key_lock_wait_count = other.key_lock_wait_count;

  env_new_sequential_file_nanos = other.env_new_sequential_file_nanos;
  env_new_random_access_file_nanos = other.env_new_random_access_file_nanos;
  env_new_writable_file_nanos = other.env_new_writable_file_nanos;
  env_reuse_writable_file_nanos = other.env_reuse_writable_file_nanos;
  env_new_random_rw_file_nanos = other.env_new_random_rw_file_nanos;
  env_new_directory_nanos = other.env_new_directory_nanos;
  env_file_exists_nanos = other.env_file_exists_nanos;
  env_get_children_nanos = other.env_get_children_nanos;
  env_get_children_file_attributes_nanos =
      other.env_get_children_file_attributes_nanos;
  env_delete_file_nanos = other.env_delete_file_nanos;
  env_create_dir_nanos = other.env_create_dir_nanos;
  env_create_dir_if_missing_nanos = other.env_create_dir_if_missing_nanos;
  env_delete_dir_nanos = other.env_delete_dir_nanos;
  env_get_file_size_nanos = other.env_get_file_size_nanos;
  env_get_file_modification_time_nanos =
      other.env_get_file_modification_time_nanos;
  env_rename_file_nanos = other.env_rename_file_nanos;
  env_link_file_nanos = other.env_link_file_nanos;
  env_lock_file_nanos = other.env_lock_file_nanos;
  env_unlock_file_nanos = other.env_unlock_file_nanos;
  env_new_logger_nanos = other.env_new_logger_nanos;
  get_cpu_nanos = other.get_cpu_nanos;
  if (per_level_perf_context_enabled && level_to_perf_context != nullptr) {
    ClearPerLevelPerfContext();
  }
  if (other.level_to_perf_context != nullptr) {
    level_to_perf_context = other.level_to_perf_context;
    other.level_to_perf_context = nullptr;
  }
  per_level_perf_context_enabled = other.per_level_perf_context_enabled;
#endif
}

// TODO(Zhongyi): reduce code duplication between copy constructor and
// assignment operator
PerfContext& PerfContext::operator=(const PerfContext& other) {
#ifndef NPERF_CONTEXT
  user_key_comparison_count = other.user_key_comparison_count;
  block_cache_hit_count = other.block_cache_hit_count;
  block_read_count = other.block_read_count;
  block_read_byte = other.block_read_byte;
  block_read_time = other.block_read_time;
  block_cache_index_hit_count = other.block_cache_index_hit_count;
  index_block_read_count = other.index_block_read_count;
  block_cache_filter_hit_count = other.block_cache_filter_hit_count;
  filter_block_read_count = other.filter_block_read_count;
  compression_dict_block_read_count = other.compression_dict_block_read_count;
  block_checksum_time = other.block_checksum_time;
  block_decompress_time = other.block_decompress_time;
  get_read_bytes = other.get_read_bytes;
  multiget_read_bytes = other.multiget_read_bytes;
  iter_read_bytes = other.iter_read_bytes;
  internal_key_skipped_count = other.internal_key_skipped_count;
  internal_delete_skipped_count = other.internal_delete_skipped_count;
  internal_recent_skipped_count = other.internal_recent_skipped_count;
  internal_merge_count = other.internal_merge_count;
  write_wal_time = other.write_wal_time;
  get_snapshot_time = other.get_snapshot_time;
  get_from_memtable_time = other.get_from_memtable_time;
  get_from_memtable_count = other.get_from_memtable_count;
  get_post_process_time = other.get_post_process_time;
  get_from_output_files_time = other.get_from_output_files_time;
  seek_on_memtable_time = other.seek_on_memtable_time;
  seek_on_memtable_count = other.seek_on_memtable_count;
  next_on_memtable_count = other.next_on_memtable_count;
  prev_on_memtable_count = other.prev_on_memtable_count;
  seek_child_seek_time = other.seek_child_seek_time;
  seek_child_seek_count = other.seek_child_seek_count;
  seek_min_heap_time = other.seek_min_heap_time;
  seek_internal_seek_time = other.seek_internal_seek_time;
  find_next_user_entry_time = other.find_next_user_entry_time;
  write_pre_and_post_process_time = other.write_pre_and_post_process_time;
  write_memtable_time = other.write_memtable_time;
  write_delay_time = other.write_delay_time;
  write_thread_wait_nanos = other.write_thread_wait_nanos;
  write_scheduling_flushes_compactions_time =
      other.write_scheduling_flushes_compactions_time;
  db_mutex_lock_nanos = other.db_mutex_lock_nanos;
  db_condition_wait_nanos = other.db_condition_wait_nanos;
  merge_operator_time_nanos = other.merge_operator_time_nanos;
  read_index_block_nanos = other.read_index_block_nanos;
  read_filter_block_nanos = other.read_filter_block_nanos;
  new_table_block_iter_nanos = other.new_table_block_iter_nanos;
  new_table_iterator_nanos = other.new_table_iterator_nanos;
  block_seek_nanos = other.block_seek_nanos;
  find_table_nanos = other.find_table_nanos;
  bloom_memtable_hit_count = other.bloom_memtable_hit_count;
  bloom_memtable_miss_count = other.bloom_memtable_miss_count;
  bloom_sst_hit_count = other.bloom_sst_hit_count;
  bloom_sst_miss_count = other.bloom_sst_miss_count;
  key_lock_wait_time = other.key_lock_wait_time;
  key_lock_wait_count = other.key_lock_wait_count;

  env_new_sequential_file_nanos = other.env_new_sequential_file_nanos;
  env_new_random_access_file_nanos = other.env_new_random_access_file_nanos;
  env_new_writable_file_nanos = other.env_new_writable_file_nanos;
  env_reuse_writable_file_nanos = other.env_reuse_writable_file_nanos;
  env_new_random_rw_file_nanos = other.env_new_random_rw_file_nanos;
  env_new_directory_nanos = other.env_new_directory_nanos;
  env_file_exists_nanos = other.env_file_exists_nanos;
  env_get_children_nanos = other.env_get_children_nanos;
  env_get_children_file_attributes_nanos =
      other.env_get_children_file_attributes_nanos;
  env_delete_file_nanos = other.env_delete_file_nanos;
  env_create_dir_nanos = other.env_create_dir_nanos;
  env_create_dir_if_missing_nanos = other.env_create_dir_if_missing_nanos;
  env_delete_dir_nanos = other.env_delete_dir_nanos;
  env_get_file_size_nanos = other.env_get_file_size_nanos;
  env_get_file_modification_time_nanos =
      other.env_get_file_modification_time_nanos;
  env_rename_file_nanos = other.env_rename_file_nanos;
  env_link_file_nanos = other.env_link_file_nanos;
  env_lock_file_nanos = other.env_lock_file_nanos;
  env_unlock_file_nanos = other.env_unlock_file_nanos;
  env_new_logger_nanos = other.env_new_logger_nanos;
  get_cpu_nanos = other.get_cpu_nanos;
  if (per_level_perf_context_enabled && level_to_perf_context != nullptr) {
    ClearPerLevelPerfContext();
  }
  if (other.level_to_perf_context != nullptr) {
    level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
    *level_to_perf_context = *other.level_to_perf_context;
  }
  per_level_perf_context_enabled = other.per_level_perf_context_enabled;
#endif
  return *this;
}

void PerfContext::Reset() {
#ifndef NPERF_CONTEXT
  user_key_comparison_count = 0;
  block_cache_hit_count = 0;
  block_read_count = 0;
  block_read_byte = 0;
  block_read_time = 0;
  block_cache_index_hit_count = 0;
  index_block_read_count = 0;
  block_cache_filter_hit_count = 0;
  filter_block_read_count = 0;
  compression_dict_block_read_count = 0;
  block_checksum_time = 0;
  block_decompress_time = 0;
  get_read_bytes = 0;
  multiget_read_bytes = 0;
  iter_read_bytes = 0;
  internal_key_skipped_count = 0;
  internal_delete_skipped_count = 0;
  internal_recent_skipped_count = 0;
  internal_merge_count = 0;
  write_wal_time = 0;

  get_snapshot_time = 0;
  get_from_memtable_time = 0;
  get_from_memtable_count = 0;
  get_post_process_time = 0;
  get_from_output_files_time = 0;
  seek_on_memtable_time = 0;
  seek_on_memtable_count = 0;
  next_on_memtable_count = 0;
  prev_on_memtable_count = 0;
  seek_child_seek_time = 0;
  seek_child_seek_count = 0;
  seek_min_heap_time = 0;
  seek_internal_seek_time = 0;
  find_next_user_entry_time = 0;
  write_pre_and_post_process_time = 0;
  write_memtable_time = 0;
  write_delay_time = 0;
  write_thread_wait_nanos = 0;
  write_scheduling_flushes_compactions_time = 0;
  db_mutex_lock_nanos = 0;
  db_condition_wait_nanos = 0;
  merge_operator_time_nanos = 0;
  read_index_block_nanos = 0;
  read_filter_block_nanos = 0;
  new_table_block_iter_nanos = 0;
  new_table_iterator_nanos = 0;
  block_seek_nanos = 0;
  find_table_nanos = 0;
  bloom_memtable_hit_count = 0;
  bloom_memtable_miss_count = 0;
  bloom_sst_hit_count = 0;
  bloom_sst_miss_count = 0;
  key_lock_wait_time = 0;
  key_lock_wait_count = 0;

  env_new_sequential_file_nanos = 0;
  env_new_random_access_file_nanos = 0;
  env_new_writable_file_nanos = 0;
  env_reuse_writable_file_nanos = 0;
  env_new_random_rw_file_nanos = 0;
  env_new_directory_nanos = 0;
  env_file_exists_nanos = 0;
  env_get_children_nanos = 0;
  env_get_children_file_attributes_nanos = 0;
  env_delete_file_nanos = 0;
  env_create_dir_nanos = 0;
  env_create_dir_if_missing_nanos = 0;
  env_delete_dir_nanos = 0;
  env_get_file_size_nanos = 0;
  env_get_file_modification_time_nanos = 0;
  env_rename_file_nanos = 0;
  env_link_file_nanos = 0;
  env_lock_file_nanos = 0;
  env_unlock_file_nanos = 0;
  env_new_logger_nanos = 0;
  get_cpu_nanos = 0;
  if (per_level_perf_context_enabled && level_to_perf_context) {
    for (auto& kv : *level_to_perf_context) {
      kv.second.Reset();
    }
  }
#endif
}

#define PERF_CONTEXT_OUTPUT(counter)             \
  if (!exclude_zero_counters || (counter > 0)) { \
    ss << #counter << " = " << counter << ", ";  \
  }

#define PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER(counter)         \
  if (per_level_perf_context_enabled && \
      level_to_perf_context) {                                    \
    ss << #counter << " = ";                                      \
    for (auto& kv : *level_to_perf_context) {                     \
      if (!exclude_zero_counters || (kv.second.counter > 0)) {    \
        ss << kv.second.counter << "@level" << kv.first << ", ";  \
      }                                                           \
    }                                                             \
  }

void PerfContextByLevel::Reset() {
#ifndef NPERF_CONTEXT
  bloom_filter_useful = 0;
  bloom_filter_full_positive = 0;
  bloom_filter_full_true_positive = 0;
  block_cache_hit_count = 0;
  block_cache_miss_count = 0;
#endif
}

std::string PerfContext::ToString(bool exclude_zero_counters) const {
#ifdef NPERF_CONTEXT
  return "";
#else
  std::ostringstream ss;
  PERF_CONTEXT_OUTPUT(user_key_comparison_count);
  PERF_CONTEXT_OUTPUT(block_cache_hit_count);
  PERF_CONTEXT_OUTPUT(block_read_count);
  PERF_CONTEXT_OUTPUT(block_read_byte);
  PERF_CONTEXT_OUTPUT(block_read_time);
  PERF_CONTEXT_OUTPUT(block_cache_index_hit_count);
  PERF_CONTEXT_OUTPUT(index_block_read_count);
  PERF_CONTEXT_OUTPUT(block_cache_filter_hit_count);
  PERF_CONTEXT_OUTPUT(filter_block_read_count);
  PERF_CONTEXT_OUTPUT(compression_dict_block_read_count);
  PERF_CONTEXT_OUTPUT(block_checksum_time);
  PERF_CONTEXT_OUTPUT(block_decompress_time);
  PERF_CONTEXT_OUTPUT(get_read_bytes);
  PERF_CONTEXT_OUTPUT(multiget_read_bytes);
  PERF_CONTEXT_OUTPUT(iter_read_bytes);
  PERF_CONTEXT_OUTPUT(internal_key_skipped_count);
  PERF_CONTEXT_OUTPUT(internal_delete_skipped_count);
  PERF_CONTEXT_OUTPUT(internal_recent_skipped_count);
  PERF_CONTEXT_OUTPUT(internal_merge_count);
  PERF_CONTEXT_OUTPUT(write_wal_time);
  PERF_CONTEXT_OUTPUT(get_snapshot_time);
  PERF_CONTEXT_OUTPUT(get_from_memtable_time);
  PERF_CONTEXT_OUTPUT(get_from_memtable_count);
  PERF_CONTEXT_OUTPUT(get_post_process_time);
  PERF_CONTEXT_OUTPUT(get_from_output_files_time);
  PERF_CONTEXT_OUTPUT(seek_on_memtable_time);
  PERF_CONTEXT_OUTPUT(seek_on_memtable_count);
  PERF_CONTEXT_OUTPUT(next_on_memtable_count);
  PERF_CONTEXT_OUTPUT(prev_on_memtable_count);
  PERF_CONTEXT_OUTPUT(seek_child_seek_time);
  PERF_CONTEXT_OUTPUT(seek_child_seek_count);
  PERF_CONTEXT_OUTPUT(seek_min_heap_time);
  PERF_CONTEXT_OUTPUT(seek_internal_seek_time);
  PERF_CONTEXT_OUTPUT(find_next_user_entry_time);
  PERF_CONTEXT_OUTPUT(write_pre_and_post_process_time);
  PERF_CONTEXT_OUTPUT(write_memtable_time);
  PERF_CONTEXT_OUTPUT(write_thread_wait_nanos);
  PERF_CONTEXT_OUTPUT(write_scheduling_flushes_compactions_time);
  PERF_CONTEXT_OUTPUT(db_mutex_lock_nanos);
  PERF_CONTEXT_OUTPUT(db_condition_wait_nanos);
  PERF_CONTEXT_OUTPUT(merge_operator_time_nanos);
  PERF_CONTEXT_OUTPUT(write_delay_time);
  PERF_CONTEXT_OUTPUT(read_index_block_nanos);
  PERF_CONTEXT_OUTPUT(read_filter_block_nanos);
  PERF_CONTEXT_OUTPUT(new_table_block_iter_nanos);
  PERF_CONTEXT_OUTPUT(new_table_iterator_nanos);
  PERF_CONTEXT_OUTPUT(block_seek_nanos);
  PERF_CONTEXT_OUTPUT(find_table_nanos);
  PERF_CONTEXT_OUTPUT(bloom_memtable_hit_count);
  PERF_CONTEXT_OUTPUT(bloom_memtable_miss_count);
  PERF_CONTEXT_OUTPUT(bloom_sst_hit_count);
  PERF_CONTEXT_OUTPUT(bloom_sst_miss_count);
  PERF_CONTEXT_OUTPUT(key_lock_wait_time);
  PERF_CONTEXT_OUTPUT(key_lock_wait_count);
  PERF_CONTEXT_OUTPUT(env_new_sequential_file_nanos);
  PERF_CONTEXT_OUTPUT(env_new_random_access_file_nanos);
  PERF_CONTEXT_OUTPUT(env_new_writable_file_nanos);
  PERF_CONTEXT_OUTPUT(env_reuse_writable_file_nanos);
  PERF_CONTEXT_OUTPUT(env_new_random_rw_file_nanos);
  PERF_CONTEXT_OUTPUT(env_new_directory_nanos);
  PERF_CONTEXT_OUTPUT(env_file_exists_nanos);
  PERF_CONTEXT_OUTPUT(env_get_children_nanos);
  PERF_CONTEXT_OUTPUT(env_get_children_file_attributes_nanos);
  PERF_CONTEXT_OUTPUT(env_delete_file_nanos);
  PERF_CONTEXT_OUTPUT(env_create_dir_nanos);
  PERF_CONTEXT_OUTPUT(env_create_dir_if_missing_nanos);
  PERF_CONTEXT_OUTPUT(env_delete_dir_nanos);
  PERF_CONTEXT_OUTPUT(env_get_file_size_nanos);
  PERF_CONTEXT_OUTPUT(env_get_file_modification_time_nanos);
  PERF_CONTEXT_OUTPUT(env_rename_file_nanos);
  PERF_CONTEXT_OUTPUT(env_link_file_nanos);
  PERF_CONTEXT_OUTPUT(env_lock_file_nanos);
  PERF_CONTEXT_OUTPUT(env_unlock_file_nanos);
  PERF_CONTEXT_OUTPUT(env_new_logger_nanos);
  PERF_CONTEXT_OUTPUT(get_cpu_nanos);
  PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER(bloom_filter_useful);
  PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER(bloom_filter_full_positive);
  PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER(bloom_filter_full_true_positive);
  PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER(block_cache_hit_count);
  PERF_CONTEXT_BY_LEVEL_OUTPUT_ONE_COUNTER(block_cache_miss_count);
  return ss.str();
#endif
}

void PerfContext::EnablePerLevelPerfContext() {
  if (level_to_perf_context == nullptr) {
    level_to_perf_context = new std::map<uint32_t, PerfContextByLevel>();
  }
  per_level_perf_context_enabled = true;
}

void PerfContext::DisablePerLevelPerfContext(){
  per_level_perf_context_enabled = false;
}

void PerfContext::ClearPerLevelPerfContext(){
  if (level_to_perf_context != nullptr) {
    level_to_perf_context->clear();
    delete level_to_perf_context;
    level_to_perf_context = nullptr;
  }
  per_level_perf_context_enabled = false;
}

}
