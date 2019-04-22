// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/compaction_job_stats.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE

void CompactionJobStats::Reset() {
  elapsed_micros = 0;
  cpu_micros = 0;

  num_input_records = 0;
  num_input_files = 0;
  num_input_files_at_output_level = 0;

  num_output_records = 0;
  num_output_files = 0;

  is_manual_compaction = 0;

  total_input_bytes = 0;
  total_output_bytes = 0;

  num_records_replaced = 0;

  total_input_raw_key_bytes = 0;
  total_input_raw_value_bytes = 0;

  num_input_deletion_records = 0;
  num_expired_deletion_records = 0;

  num_corrupt_keys = 0;

  file_write_nanos = 0;
  file_range_sync_nanos = 0;
  file_fsync_nanos = 0;
  file_prepare_write_nanos = 0;

  num_single_del_fallthru = 0;
  num_single_del_mismatch = 0;
}

void CompactionJobStats::Add(const CompactionJobStats& stats) {
  elapsed_micros += stats.elapsed_micros;
  cpu_micros += stats.cpu_micros;

  num_input_records += stats.num_input_records;
  num_input_files += stats.num_input_files;
  num_input_files_at_output_level += stats.num_input_files_at_output_level;

  num_output_records += stats.num_output_records;
  num_output_files += stats.num_output_files;

  total_input_bytes += stats.total_input_bytes;
  total_output_bytes += stats.total_output_bytes;

  num_records_replaced += stats.num_records_replaced;

  total_input_raw_key_bytes += stats.total_input_raw_key_bytes;
  total_input_raw_value_bytes += stats.total_input_raw_value_bytes;

  num_input_deletion_records += stats.num_input_deletion_records;
  num_expired_deletion_records += stats.num_expired_deletion_records;

  num_corrupt_keys += stats.num_corrupt_keys;

  file_write_nanos += stats.file_write_nanos;
  file_range_sync_nanos += stats.file_range_sync_nanos;
  file_fsync_nanos += stats.file_fsync_nanos;
  file_prepare_write_nanos += stats.file_prepare_write_nanos;

  num_single_del_fallthru += stats.num_single_del_fallthru;
  num_single_del_mismatch += stats.num_single_del_mismatch;
}

#else

void CompactionJobStats::Reset() {}

void CompactionJobStats::Add(const CompactionJobStats& /*stats*/) {}

#endif  // !ROCKSDB_LITE

}  // namespace rocksdb
