//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Repairer does best effort recovery to recover as much data as possible after
// a disaster without compromising consistency. It does not guarantee bringing
// the database to a time consistent state.
//
// Repair process is broken into 4 phases:
// (a) Find files
// (b) Convert logs to tables
// (c) Extract metadata
// (d) Write Descriptor
//
// (a) Find files
//
// The repairer goes through all the files in the directory, and classifies them
// based on their file name. Any file that cannot be identified by name will be
// ignored.
//
// (b) Convert logs to table
//
// Every log file that is active is replayed. All sections of the file where the
// checksum does not match is skipped over. We intentionally give preference to
// data consistency.
//
// (c) Extract metadata
//
// We scan every table to compute
// (1) smallest/largest for the table
// (2) largest sequence number in the table
//
// If we are unable to scan the file, then we ignore the table.
//
// (d) Write Descriptor
//
// We generate descriptor contents:
//  - log number is set to zero
//  - next-file-number is set to 1 + largest file number we found
//  - last-sequence-number is set to largest sequence# found across
//    all tables (see 2c)
//  - compaction pointers are cleared
//  - every table file is added at level 0
//
// Possible optimization 1:
//   (a) Compute total size and use to pick appropriate max-level M
//   (b) Sort tables by largest sequence# in the table
//   (c) For each table: if it overlaps earlier table, place in level-0,
//       else place in level-M.
//   (d) We can provide options for time consistent recovery and unsafe recovery
//       (ignore checksum failure when applicable)
// Possible optimization 2:
//   Store per-table metadata (smallest, largest, largest-seq#, ...)
//   in the table's meta section to speed up ScanTable.

#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include "db/builder.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/scoped_arena_iterator.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/string_util.h"

namespace rocksdb {

namespace {

class Repairer {
 public:
  Repairer(const std::string& dbname, const DBOptions& db_options,
           const std::vector<ColumnFamilyDescriptor>& column_families,
           const ColumnFamilyOptions& default_cf_opts,
           const ColumnFamilyOptions& unknown_cf_opts, bool create_unknown_cfs)
      : dbname_(dbname),
        env_(db_options.env),
        env_options_(),
        db_options_(SanitizeOptions(dbname_, db_options)),
        immutable_db_options_(ImmutableDBOptions(db_options_)),
        icmp_(default_cf_opts.comparator),
        default_cf_opts_(
            SanitizeOptions(immutable_db_options_, default_cf_opts)),
        default_cf_iopts_(
            ImmutableCFOptions(immutable_db_options_, default_cf_opts_)),
        unknown_cf_opts_(
            SanitizeOptions(immutable_db_options_, unknown_cf_opts)),
        create_unknown_cfs_(create_unknown_cfs),
        raw_table_cache_(
            // TableCache can be small since we expect each table to be opened
            // once.
            NewLRUCache(10, db_options_.table_cache_numshardbits)),
        table_cache_(new TableCache(default_cf_iopts_, env_options_,
                                    raw_table_cache_.get())),
        wb_(db_options_.db_write_buffer_size),
        wc_(db_options_.delayed_write_rate),
        vset_(dbname_, &immutable_db_options_, env_options_,
              raw_table_cache_.get(), &wb_, &wc_),
        next_file_number_(1),
        db_lock_(nullptr) {
    for (const auto& cfd : column_families) {
      cf_name_to_opts_[cfd.name] = cfd.options;
    }
  }

  const ColumnFamilyOptions* GetColumnFamilyOptions(
      const std::string& cf_name) {
    if (cf_name_to_opts_.find(cf_name) == cf_name_to_opts_.end()) {
      if (create_unknown_cfs_) {
        return &unknown_cf_opts_;
      }
      return nullptr;
    }
    return &cf_name_to_opts_[cf_name];
  }

  // Adds a column family to the VersionSet with cf_options_ and updates
  // manifest.
  Status AddColumnFamily(const std::string& cf_name, uint32_t cf_id) {
    const auto* cf_opts = GetColumnFamilyOptions(cf_name);
    if (cf_opts == nullptr) {
      return Status::Corruption("Encountered unknown column family with name=" +
                                cf_name + ", id=" + ToString(cf_id));
    }
    Options opts(db_options_, *cf_opts);
    MutableCFOptions mut_cf_opts(opts);

    VersionEdit edit;
    edit.SetComparatorName(opts.comparator->Name());
    edit.SetLogNumber(0);
    edit.SetColumnFamily(cf_id);
    ColumnFamilyData* cfd;
    cfd = nullptr;
    edit.AddColumnFamily(cf_name);

    mutex_.Lock();
    Status status = vset_.LogAndApply(cfd, mut_cf_opts, &edit, &mutex_,
                                      nullptr /* db_directory */,
                                      false /* new_descriptor_log */, cf_opts);
    mutex_.Unlock();
    return status;
  }

  ~Repairer() {
    if (db_lock_ != nullptr) {
      env_->UnlockFile(db_lock_);
    }
    delete table_cache_;
  }

  Status Run() {
    Status status = env_->LockFile(LockFileName(dbname_), &db_lock_);
    if (!status.ok()) {
      return status;
    }
    status = FindFiles();
    if (status.ok()) {
      // Discard older manifests and start a fresh one
      for (size_t i = 0; i < manifests_.size(); i++) {
        ArchiveFile(dbname_ + "/" + manifests_[i]);
      }
      // Just create a DBImpl temporarily so we can reuse NewDB()
      DBImpl* db_impl = new DBImpl(db_options_, dbname_);
      status = db_impl->NewDB();
      delete db_impl;
    }

    if (status.ok()) {
      // Recover using the fresh manifest created by NewDB()
      status =
          vset_.Recover({{kDefaultColumnFamilyName, default_cf_opts_}}, false);
    }
    if (status.ok()) {
      // Need to scan existing SST files first so the column families are
      // created before we process WAL files
      ExtractMetaData();

      // ExtractMetaData() uses table_fds_ to know which SST files' metadata to
      // extract -- we need to clear it here since metadata for existing SST
      // files has been extracted already
      table_fds_.clear();
      ConvertLogFilesToTables();
      ExtractMetaData();
      status = AddTables();
    }
    if (status.ok()) {
      uint64_t bytes = 0;
      for (size_t i = 0; i < tables_.size(); i++) {
        bytes += tables_[i].meta.fd.GetFileSize();
      }
      ROCKS_LOG_WARN(db_options_.info_log,
                     "**** Repaired rocksdb %s; "
                     "recovered %" ROCKSDB_PRIszt " files; %" PRIu64
                     " bytes. "
                     "Some data may have been lost. "
                     "****",
                     dbname_.c_str(), tables_.size(), bytes);
    }
    return status;
  }

 private:
  struct TableInfo {
    FileMetaData meta;
    uint32_t column_family_id;
    std::string column_family_name;
    SequenceNumber min_sequence;
    SequenceNumber max_sequence;
  };

  std::string const dbname_;
  Env* const env_;
  const EnvOptions env_options_;
  const DBOptions db_options_;
  const ImmutableDBOptions immutable_db_options_;
  const InternalKeyComparator icmp_;
  const ColumnFamilyOptions default_cf_opts_;
  const ImmutableCFOptions default_cf_iopts_;  // table_cache_ holds reference
  const ColumnFamilyOptions unknown_cf_opts_;
  const bool create_unknown_cfs_;
  std::shared_ptr<Cache> raw_table_cache_;
  TableCache* table_cache_;
  WriteBufferManager wb_;
  WriteController wc_;
  VersionSet vset_;
  std::unordered_map<std::string, ColumnFamilyOptions> cf_name_to_opts_;
  InstrumentedMutex mutex_;

  std::vector<std::string> manifests_;
  std::vector<FileDescriptor> table_fds_;
  std::vector<uint64_t> logs_;
  std::vector<TableInfo> tables_;
  uint64_t next_file_number_;
  // Lock over the persistent DB state. Non-nullptr iff successfully
  // acquired.
  FileLock* db_lock_;

  Status FindFiles() {
    std::vector<std::string> filenames;
    bool found_file = false;
    std::vector<std::string> to_search_paths;

    for (size_t path_id = 0; path_id < db_options_.db_paths.size(); path_id++) {
        to_search_paths.push_back(db_options_.db_paths[path_id].path);
    }

    // search wal_dir if user uses a customize wal_dir
    bool same = false;
    Status status = env_->AreFilesSame(db_options_.wal_dir, dbname_, &same);
    if (status.IsNotSupported()) {
      same = db_options_.wal_dir == dbname_;
      status = Status::OK();
    } else if (!status.ok()) {
      return status;
    }

    if (!same) {
      to_search_paths.push_back(db_options_.wal_dir);
    }

    for (size_t path_id = 0; path_id < to_search_paths.size(); path_id++) {
      status = env_->GetChildren(to_search_paths[path_id], &filenames);
      if (!status.ok()) {
        return status;
      }
      if (!filenames.empty()) {
        found_file = true;
      }

      uint64_t number;
      FileType type;
      for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
          if (type == kDescriptorFile) {
            manifests_.push_back(filenames[i]);
          } else {
            if (number + 1 > next_file_number_) {
              next_file_number_ = number + 1;
            }
            if (type == kLogFile) {
              logs_.push_back(number);
            } else if (type == kTableFile) {
              table_fds_.emplace_back(number, static_cast<uint32_t>(path_id),
                                      0);
            } else {
              // Ignore other files
            }
          }
        }
      }
    }
    if (!found_file) {
      return Status::Corruption(dbname_, "repair found no files");
    }
    return Status::OK();
  }

  void ConvertLogFilesToTables() {
    for (size_t i = 0; i < logs_.size(); i++) {
      // we should use LogFileName(wal_dir, logs_[i]) here. user might uses wal_dir option.
      std::string logname = LogFileName(db_options_.wal_dir, logs_[i]);
      Status status = ConvertLogToTable(logs_[i]);
      if (!status.ok()) {
        ROCKS_LOG_WARN(db_options_.info_log,
                       "Log #%" PRIu64 ": ignoring conversion error: %s",
                       logs_[i], status.ToString().c_str());
      }
      ArchiveFile(logname);
    }
  }

  Status ConvertLogToTable(uint64_t log) {
    struct LogReporter : public log::Reader::Reporter {
      Env* env;
      std::shared_ptr<Logger> info_log;
      uint64_t lognum;
      void Corruption(size_t bytes, const Status& s) override {
        // We print error messages for corruption, but continue repairing.
        ROCKS_LOG_ERROR(info_log, "Log #%" PRIu64 ": dropping %d bytes; %s",
                        lognum, static_cast<int>(bytes), s.ToString().c_str());
      }
    };

    // Open the log file
    std::string logname = LogFileName(db_options_.wal_dir, log);
    std::unique_ptr<SequentialFile> lfile;
    Status status = env_->NewSequentialFile(
        logname, &lfile, env_->OptimizeForLogRead(env_options_));
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<SequentialFileReader> lfile_reader(
        new SequentialFileReader(std::move(lfile), logname));

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = db_options_.info_log;
    reporter.lognum = log;
    // We intentionally make log::Reader do checksumming so that
    // corruptions cause entire commits to be skipped instead of
    // propagating bad information (like overly large sequence
    // numbers).
    log::Reader reader(db_options_.info_log, std::move(lfile_reader), &reporter,
                       true /*enable checksum*/, log,
                       false /* retry_after_eof */);

    // Initialize per-column family memtables
    for (auto* cfd : *vset_.GetColumnFamilySet()) {
      cfd->CreateNewMemtable(*cfd->GetLatestMutableCFOptions(),
                             kMaxSequenceNumber);
    }
    auto cf_mems = new ColumnFamilyMemTablesImpl(vset_.GetColumnFamilySet());

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int counter = 0;
    while (reader.ReadRecord(&record, &scratch)) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter.Corruption(
            record.size(), Status::Corruption("log record too small"));
        continue;
      }
      WriteBatchInternal::SetContents(&batch, record);
      status = WriteBatchInternal::InsertInto(&batch, cf_mems, nullptr);
      if (status.ok()) {
        counter += WriteBatchInternal::Count(&batch);
      } else {
        ROCKS_LOG_WARN(db_options_.info_log, "Log #%" PRIu64 ": ignoring %s",
                       log, status.ToString().c_str());
        status = Status::OK();  // Keep going with rest of file
      }
    }

    // Dump a table for each column family with entries in this log file.
    for (auto* cfd : *vset_.GetColumnFamilySet()) {
      // Do not record a version edit for this conversion to a Table
      // since ExtractMetaData() will also generate edits.
      MemTable* mem = cfd->mem();
      if (mem->IsEmpty()) {
        continue;
      }

      FileMetaData meta;
      meta.fd = FileDescriptor(next_file_number_++, 0, 0);
      ReadOptions ro;
      ro.total_order_seek = true;
      Arena arena;
      ScopedArenaIterator iter(mem->NewIterator(ro, &arena));
      int64_t _current_time = 0;
      status = env_->GetCurrentTime(&_current_time);  // ignore error
      const uint64_t current_time = static_cast<uint64_t>(_current_time);
      SnapshotChecker* snapshot_checker = DisableGCSnapshotChecker::Instance();

      auto write_hint = cfd->CalculateSSTWriteHint(0);
      std::vector<std::unique_ptr<FragmentedRangeTombstoneIterator>>
          range_del_iters;
      auto range_del_iter =
          mem->NewRangeTombstoneIterator(ro, kMaxSequenceNumber);
      if (range_del_iter != nullptr) {
        range_del_iters.emplace_back(range_del_iter);
      }
      status = BuildTable(
          dbname_, env_, *cfd->ioptions(), *cfd->GetLatestMutableCFOptions(),
          env_options_, table_cache_, iter.get(), std::move(range_del_iters),
          &meta, cfd->internal_comparator(),
          cfd->int_tbl_prop_collector_factories(), cfd->GetID(), cfd->GetName(),
          {}, kMaxSequenceNumber, snapshot_checker, kNoCompression,
          CompressionOptions(), false, nullptr /* internal_stats */,
          TableFileCreationReason::kRecovery, nullptr /* event_logger */,
          0 /* job_id */, Env::IO_HIGH, nullptr /* table_properties */,
          -1 /* level */, current_time, write_hint);
      ROCKS_LOG_INFO(db_options_.info_log,
                     "Log #%" PRIu64 ": %d ops saved to Table #%" PRIu64 " %s",
                     log, counter, meta.fd.GetNumber(),
                     status.ToString().c_str());
      if (status.ok()) {
        if (meta.fd.GetFileSize() > 0) {
          table_fds_.push_back(meta.fd);
        }
      } else {
        break;
      }
    }
    delete cf_mems;
    return status;
  }

  void ExtractMetaData() {
    for (size_t i = 0; i < table_fds_.size(); i++) {
      TableInfo t;
      t.meta.fd = table_fds_[i];
      Status status = ScanTable(&t);
      if (!status.ok()) {
        std::string fname = TableFileName(
            db_options_.db_paths, t.meta.fd.GetNumber(), t.meta.fd.GetPathId());
        char file_num_buf[kFormatFileNumberBufSize];
        FormatFileNumber(t.meta.fd.GetNumber(), t.meta.fd.GetPathId(),
                         file_num_buf, sizeof(file_num_buf));
        ROCKS_LOG_WARN(db_options_.info_log, "Table #%s: ignoring %s",
                       file_num_buf, status.ToString().c_str());
        ArchiveFile(fname);
      } else {
        tables_.push_back(t);
      }
    }
  }

  Status ScanTable(TableInfo* t) {
    std::string fname = TableFileName(
        db_options_.db_paths, t->meta.fd.GetNumber(), t->meta.fd.GetPathId());
    int counter = 0;
    uint64_t file_size;
    Status status = env_->GetFileSize(fname, &file_size);
    t->meta.fd = FileDescriptor(t->meta.fd.GetNumber(), t->meta.fd.GetPathId(),
                                file_size);
    std::shared_ptr<const TableProperties> props;
    if (status.ok()) {
      status = table_cache_->GetTableProperties(env_options_, icmp_, t->meta.fd,
                                                &props);
    }
    if (status.ok()) {
      t->column_family_id = static_cast<uint32_t>(props->column_family_id);
      if (t->column_family_id ==
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) {
        ROCKS_LOG_WARN(
            db_options_.info_log,
            "Table #%" PRIu64
            ": column family unknown (probably due to legacy format); "
            "adding to default column family id 0.",
            t->meta.fd.GetNumber());
        t->column_family_id = 0;
      }

      if (vset_.GetColumnFamilySet()->GetColumnFamily(t->column_family_id) ==
          nullptr) {
        status =
            AddColumnFamily(props->column_family_name, t->column_family_id);
      }
    }
    ColumnFamilyData* cfd = nullptr;
    if (status.ok()) {
      cfd = vset_.GetColumnFamilySet()->GetColumnFamily(t->column_family_id);
      if (cfd->GetName() != props->column_family_name) {
        ROCKS_LOG_ERROR(
            db_options_.info_log,
            "Table #%" PRIu64
            ": inconsistent column family name '%s'; expected '%s' for column "
            "family id %" PRIu32 ".",
            t->meta.fd.GetNumber(), props->column_family_name.c_str(),
            cfd->GetName().c_str(), t->column_family_id);
        status = Status::Corruption(dbname_, "inconsistent column family name");
      }
    }
    if (status.ok()) {
      InternalIterator* iter = table_cache_->NewIterator(
          ReadOptions(), env_options_, cfd->internal_comparator(), t->meta,
          nullptr /* range_del_agg */,
          cfd->GetLatestMutableCFOptions()->prefix_extractor.get());
      bool empty = true;
      ParsedInternalKey parsed;
      t->min_sequence = 0;
      t->max_sequence = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        if (!ParseInternalKey(key, &parsed)) {
          ROCKS_LOG_ERROR(db_options_.info_log,
                          "Table #%" PRIu64 ": unparsable key %s",
                          t->meta.fd.GetNumber(), EscapeString(key).c_str());
          continue;
        }

        counter++;
        if (empty) {
          empty = false;
          t->meta.smallest.DecodeFrom(key);
          t->min_sequence = parsed.sequence;
        }
        t->meta.largest.DecodeFrom(key);
        if (parsed.sequence < t->min_sequence) {
          t->min_sequence = parsed.sequence;
        }
        if (parsed.sequence > t->max_sequence) {
          t->max_sequence = parsed.sequence;
        }
      }
      if (!iter->status().ok()) {
        status = iter->status();
      }
      delete iter;

      ROCKS_LOG_INFO(db_options_.info_log, "Table #%" PRIu64 ": %d entries %s",
                     t->meta.fd.GetNumber(), counter,
                     status.ToString().c_str());
    }
    return status;
  }

  Status AddTables() {
    std::unordered_map<uint32_t, std::vector<const TableInfo*>> cf_id_to_tables;
    SequenceNumber max_sequence = 0;
    for (size_t i = 0; i < tables_.size(); i++) {
      cf_id_to_tables[tables_[i].column_family_id].push_back(&tables_[i]);
      if (max_sequence < tables_[i].max_sequence) {
        max_sequence = tables_[i].max_sequence;
      }
    }
    vset_.SetLastAllocatedSequence(max_sequence);
    vset_.SetLastPublishedSequence(max_sequence);
    vset_.SetLastSequence(max_sequence);

    for (const auto& cf_id_and_tables : cf_id_to_tables) {
      auto* cfd =
          vset_.GetColumnFamilySet()->GetColumnFamily(cf_id_and_tables.first);
      VersionEdit edit;
      edit.SetComparatorName(cfd->user_comparator()->Name());
      edit.SetLogNumber(0);
      edit.SetNextFile(next_file_number_);
      edit.SetColumnFamily(cfd->GetID());

      // TODO(opt): separate out into multiple levels
      for (const auto* table : cf_id_and_tables.second) {
        edit.AddFile(0, table->meta.fd.GetNumber(), table->meta.fd.GetPathId(),
                     table->meta.fd.GetFileSize(), table->meta.smallest,
                     table->meta.largest, table->min_sequence,
                     table->max_sequence, table->meta.marked_for_compaction);
      }
      assert(next_file_number_ > 0);
      vset_.MarkFileNumberUsed(next_file_number_ - 1);
      mutex_.Lock();
      Status status = vset_.LogAndApply(
          cfd, *cfd->GetLatestMutableCFOptions(), &edit, &mutex_,
          nullptr /* db_directory */, false /* new_descriptor_log */);
      mutex_.Unlock();
      if (!status.ok()) {
        return status;
      }
    }
    return Status::OK();
  }

  void ArchiveFile(const std::string& fname) {
    // Move into another directory.  E.g., for
    //    dir/foo
    // rename to
    //    dir/lost/foo
    const char* slash = strrchr(fname.c_str(), '/');
    std::string new_dir;
    if (slash != nullptr) {
      new_dir.assign(fname.data(), slash - fname.data());
    }
    new_dir.append("/lost");
    env_->CreateDir(new_dir);  // Ignore error
    std::string new_file = new_dir;
    new_file.append("/");
    new_file.append((slash == nullptr) ? fname.c_str() : slash + 1);
    Status s = env_->RenameFile(fname, new_file);
    ROCKS_LOG_INFO(db_options_.info_log, "Archiving %s: %s\n", fname.c_str(),
                   s.ToString().c_str());
  }
};

Status GetDefaultCFOptions(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    ColumnFamilyOptions* res) {
  assert(res != nullptr);
  auto iter = std::find_if(column_families.begin(), column_families.end(),
                           [](const ColumnFamilyDescriptor& cfd) {
                             return cfd.name == kDefaultColumnFamilyName;
                           });
  if (iter == column_families.end()) {
    return Status::InvalidArgument(
        "column_families", "Must contain entry for default column family");
  }
  *res = iter->options;
  return Status::OK();
}
}  // anonymous namespace

Status RepairDB(const std::string& dbname, const DBOptions& db_options,
                const std::vector<ColumnFamilyDescriptor>& column_families
                ) {
  ColumnFamilyOptions default_cf_opts;
  Status status = GetDefaultCFOptions(column_families, &default_cf_opts);
  if (status.ok()) {
    Repairer repairer(dbname, db_options, column_families,
                      default_cf_opts,
                      ColumnFamilyOptions() /* unknown_cf_opts */,
                      false /* create_unknown_cfs */);
    status = repairer.Run();
  }
  return status;
}

Status RepairDB(const std::string& dbname, const DBOptions& db_options,
                const std::vector<ColumnFamilyDescriptor>& column_families,
                const ColumnFamilyOptions& unknown_cf_opts) {
  ColumnFamilyOptions default_cf_opts;
  Status status = GetDefaultCFOptions(column_families, &default_cf_opts);
  if (status.ok()) {
    Repairer repairer(dbname, db_options,
                      column_families, default_cf_opts,
                      unknown_cf_opts, true /* create_unknown_cfs */);
    status = repairer.Run();
  }
  return status;
}

Status RepairDB(const std::string& dbname, const Options& options) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  Repairer repairer(dbname, db_options,
                    {}, cf_options /* default_cf_opts */,
                    cf_options /* unknown_cf_opts */,
                    true /* create_unknown_cfs */);
  return repairer.Run();
}

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
