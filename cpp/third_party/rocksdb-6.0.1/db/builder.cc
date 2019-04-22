//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "db/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/block_based_table_builder.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"

namespace rocksdb {

class TableFactory;

TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& ioptions, const MutableCFOptions& moptions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    WritableFileWriter* file, const CompressionType compression_type,
    const CompressionOptions& compression_opts, int level,
    const bool skip_filters, const uint64_t creation_time,
    const uint64_t oldest_key_time, const uint64_t target_file_size) {
  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  return ioptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, internal_comparator,
                          int_tbl_prop_collector_factories, compression_type,
                          compression_opts, skip_filters, column_family_name,
                          level, creation_time, oldest_key_time,
                          target_file_size),
      column_family_id, file);
}

Status BuildTable(
    const std::string& dbname, Env* env, const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const EnvOptions& env_options,
    TableCache* table_cache, InternalIterator* iter,
    std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
        range_del_iters,
    FileMetaData* meta, const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    SnapshotChecker* snapshot_checker, const CompressionType compression,
    const CompressionOptions& compression_opts, bool paranoid_file_checks,
    InternalStats* internal_stats, TableFileCreationReason reason,
    EventLogger* event_logger, int job_id, const Env::IOPriority io_priority,
    TableProperties* table_properties, int level, const uint64_t creation_time,
    const uint64_t oldest_key_time, Env::WriteLifeTimeHint write_hint) {
  assert((column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) ==
         column_family_name.empty());
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  Status s;
  meta->fd.file_size = 0;
  iter->SeekToFirst();
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&internal_comparator, snapshots));
  for (auto& range_del_iter : range_del_iters) {
    range_del_agg->AddTombstones(std::move(range_del_iter));
  }

  std::string fname = TableFileName(ioptions.cf_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
#ifndef ROCKSDB_LITE
  EventHelpers::NotifyTableFileCreationStarted(
      ioptions.listeners, dbname, column_family_name, fname, job_id, reason);
#endif  // !ROCKSDB_LITE
  TableProperties tp;

  if (iter->Valid() || !range_del_agg->IsEmpty()) {
    TableBuilder* builder;
    std::unique_ptr<WritableFileWriter> file_writer;
    // Currently we only enable dictionary compression during compaction to the
    // bottommost level.
    CompressionOptions compression_opts_for_flush(compression_opts);
    compression_opts_for_flush.max_dict_bytes = 0;
    compression_opts_for_flush.zstd_max_train_bytes = 0;
    {
      std::unique_ptr<WritableFile> file;
#ifndef NDEBUG
      bool use_direct_writes = env_options.use_direct_writes;
      TEST_SYNC_POINT_CALLBACK("BuildTable:create_file", &use_direct_writes);
#endif  // !NDEBUG
      s = NewWritableFile(env, fname, &file, env_options);
      if (!s.ok()) {
        EventHelpers::LogAndNotifyTableFileCreationFinished(
            event_logger, ioptions.listeners, dbname, column_family_name, fname,
            job_id, meta->fd, tp, reason, s);
        return s;
      }
      file->SetIOPriority(io_priority);
      file->SetWriteLifeTimeHint(write_hint);

      file_writer.reset(
          new WritableFileWriter(std::move(file), fname, env_options, env,
                                 ioptions.statistics, ioptions.listeners));
      builder = NewTableBuilder(
          ioptions, mutable_cf_options, internal_comparator,
          int_tbl_prop_collector_factories, column_family_id,
          column_family_name, file_writer.get(), compression,
          compression_opts_for_flush, level, false /* skip_filters */,
          creation_time, oldest_key_time);
    }

    MergeHelper merge(env, internal_comparator.user_comparator(),
                      ioptions.merge_operator, nullptr, ioptions.info_log,
                      true /* internal key corruption is not ok */,
                      snapshots.empty() ? 0 : snapshots.back(),
                      snapshot_checker);

    CompactionIterator c_iter(
        iter, internal_comparator.user_comparator(), &merge, kMaxSequenceNumber,
        &snapshots, earliest_write_conflict_snapshot, snapshot_checker, env,
        ShouldReportDetailedTime(env, ioptions.statistics),
        true /* internal key corruption is not ok */, range_del_agg.get());
    c_iter.SeekToFirst();
    for (; c_iter.Valid(); c_iter.Next()) {
      const Slice& key = c_iter.key();
      const Slice& value = c_iter.value();
      builder->Add(key, value);
      meta->UpdateBoundaries(key, c_iter.ikey().sequence);

      // TODO(noetzli): Update stats after flush, too.
      if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
    }

    auto range_del_it = range_del_agg->NewIterator();
    for (range_del_it->SeekToFirst(); range_del_it->Valid();
         range_del_it->Next()) {
      auto tombstone = range_del_it->Tombstone();
      auto kv = tombstone.Serialize();
      builder->Add(kv.first.Encode(), kv.second);
      meta->UpdateBoundariesForRange(kv.first, tombstone.SerializeEndKey(),
                                     tombstone.seq_, internal_comparator);
    }

    // Finish and check for builder errors
    tp = builder->GetTableProperties();
    bool empty = builder->NumEntries() == 0 && tp.num_range_deletions == 0;
    s = c_iter.status();
    if (!s.ok() || empty) {
      builder->Abandon();
    } else {
      s = builder->Finish();
    }

    if (s.ok() && !empty) {
      uint64_t file_size = builder->FileSize();
      meta->fd.file_size = file_size;
      meta->marked_for_compaction = builder->NeedCompact();
      assert(meta->fd.GetFileSize() > 0);
      tp = builder->GetTableProperties(); // refresh now that builder is finished
      if (table_properties) {
        *table_properties = tp;
      }
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok() && !empty) {
      StopWatch sw(env, ioptions.statistics, TABLE_SYNC_MICROS);
      s = file_writer->Sync(ioptions.use_fsync);
    }
    if (s.ok() && !empty) {
      s = file_writer->Close();
    }

    if (s.ok() && !empty) {
      // Verify that the table is usable
      // We set for_compaction to false and don't OptimizeForCompactionTableRead
      // here because this is a special case after we finish the table building
      // No matter whether use_direct_io_for_flush_and_compaction is true,
      // we will regrad this verification as user reads since the goal is
      // to cache it here for further user reads
      std::unique_ptr<InternalIterator> it(table_cache->NewIterator(
          ReadOptions(), env_options, internal_comparator, *meta,
          nullptr /* range_del_agg */,
          mutable_cf_options.prefix_extractor.get(), nullptr,
          (internal_stats == nullptr) ? nullptr
                                      : internal_stats->GetFileReadHist(0),
          false /* for_compaction */, nullptr /* arena */,
          false /* skip_filter */, level));
      s = it->status();
      if (s.ok() && paranoid_file_checks) {
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
        }
        s = it->status();
      }
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (!s.ok() || meta->fd.GetFileSize() == 0) {
    env->DeleteFile(fname);
  }

  // Output to event logger and fire events.
  EventHelpers::LogAndNotifyTableFileCreationFinished(
      event_logger, ioptions.listeners, dbname, column_family_name, fname,
      job_id, meta->fd, tp, reason, s);

  return s;
}

}  // namespace rocksdb
