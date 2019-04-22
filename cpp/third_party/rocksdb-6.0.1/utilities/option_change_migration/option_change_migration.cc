//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/option_change_migration.h"

#ifndef ROCKSDB_LITE
#include "rocksdb/db.h"

namespace rocksdb {
namespace {
// Return a version of Options `opts` that allow us to open/write into a DB
// without triggering an automatic compaction or stalling. This is guaranteed
// by disabling automatic compactions and using huge values for stalling
// triggers.
Options GetNoCompactionOptions(const Options& opts) {
  Options ret_opts = opts;
  ret_opts.disable_auto_compactions = true;
  ret_opts.level0_slowdown_writes_trigger = 999999;
  ret_opts.level0_stop_writes_trigger = 999999;
  ret_opts.soft_pending_compaction_bytes_limit = 0;
  ret_opts.hard_pending_compaction_bytes_limit = 0;
  return ret_opts;
}

Status OpenDb(const Options& options, const std::string& dbname,
              std::unique_ptr<DB>* db) {
  db->reset();
  DB* tmpdb;
  Status s = DB::Open(options, dbname, &tmpdb);
  if (s.ok()) {
    db->reset(tmpdb);
  }
  return s;
}

Status CompactToLevel(const Options& options, const std::string& dbname,
                      int dest_level, bool need_reopen) {
  std::unique_ptr<DB> db;
  Options no_compact_opts = GetNoCompactionOptions(options);
  if (dest_level == 0) {
    // L0 has strict sequenceID requirements to files to it. It's safer
    // to only put one compacted file to there.
    // This is only used for converting to universal compaction with
    // only one level. In this case, compacting to one file is also
    // optimal.
    no_compact_opts.target_file_size_base = 999999999999999;
    no_compact_opts.max_compaction_bytes = 999999999999999;
  }
  Status s = OpenDb(no_compact_opts, dbname, &db);
  if (!s.ok()) {
    return s;
  }
  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = dest_level;
  if (dest_level == 0) {
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  }
  db->CompactRange(cro, nullptr, nullptr);

  if (need_reopen) {
    // Need to restart DB to rewrite the manifest file.
    // In order to open a DB with specific num_levels, the manifest file should
    // contain no record that mentiones any level beyond num_levels. Issuing a
    // full compaction will move all the data to a level not exceeding
    // num_levels, but the manifest may still contain previous record mentioning
    // a higher level. Reopening the DB will force the manifest to be rewritten
    // so that those records will be cleared.
    db.reset();
    s = OpenDb(no_compact_opts, dbname, &db);
  }
  return s;
}

Status MigrateToUniversal(std::string dbname, const Options& old_opts,
                          const Options& new_opts) {
  if (old_opts.num_levels <= new_opts.num_levels ||
      old_opts.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    return Status::OK();
  } else {
    bool need_compact = false;
    {
      std::unique_ptr<DB> db;
      Options opts = GetNoCompactionOptions(old_opts);
      Status s = OpenDb(opts, dbname, &db);
      if (!s.ok()) {
        return s;
      }
      ColumnFamilyMetaData metadata;
      db->GetColumnFamilyMetaData(&metadata);
      if (!metadata.levels.empty() &&
          metadata.levels.back().level >= new_opts.num_levels) {
        need_compact = true;
      }
    }
    if (need_compact) {
      return CompactToLevel(old_opts, dbname, new_opts.num_levels - 1, true);
    }
    return Status::OK();
  }
}

Status MigrateToLevelBase(std::string dbname, const Options& old_opts,
                          const Options& new_opts) {
  if (!new_opts.level_compaction_dynamic_level_bytes) {
    if (old_opts.num_levels == 1) {
      return Status::OK();
    }
    // Compact everything to level 1 to guarantee it can be safely opened.
    Options opts = old_opts;
    opts.target_file_size_base = new_opts.target_file_size_base;
    // Although sometimes we can open the DB with the new option without error,
    // We still want to compact the files to avoid the LSM tree to stuck
    // in bad shape. For example, if the user changed the level size
    // multiplier from 4 to 8, with the same data, we will have fewer
    // levels. Unless we issue a full comaction, the LSM tree may stuck
    // with more levels than needed and it won't recover automatically.
    return CompactToLevel(opts, dbname, 1, true);
  } else {
    // Compact everything to the last level to guarantee it can be safely
    // opened.
    if (old_opts.num_levels == 1) {
      return Status::OK();
    } else if (new_opts.num_levels > old_opts.num_levels) {
      // Dynamic level mode requires data to be put in the last level first.
      return CompactToLevel(new_opts, dbname, new_opts.num_levels - 1, false);
    } else {
      Options opts = old_opts;
      opts.target_file_size_base = new_opts.target_file_size_base;
      return CompactToLevel(opts, dbname, new_opts.num_levels - 1, true);
    }
  }
}
}  // namespace

Status OptionChangeMigration(std::string dbname, const Options& old_opts,
                             const Options& new_opts) {
  if (old_opts.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    // LSM generated by FIFO compation can be opened by any compaction.
    return Status::OK();
  } else if (new_opts.compaction_style ==
             CompactionStyle::kCompactionStyleUniversal) {
    return MigrateToUniversal(dbname, old_opts, new_opts);
  } else if (new_opts.compaction_style ==
             CompactionStyle::kCompactionStyleLevel) {
    return MigrateToLevelBase(dbname, old_opts, new_opts);
  } else if (new_opts.compaction_style ==
             CompactionStyle::kCompactionStyleFIFO) {
    return CompactToLevel(old_opts, dbname, 0, true);
  } else {
    return Status::NotSupported(
        "Do not how to migrate to this compaction style");
  }
}
}  // namespace rocksdb
#else
namespace rocksdb {
Status OptionChangeMigration(std::string /*dbname*/,
                             const Options& /*old_opts*/,
                             const Options& /*new_opts*/) {
  return Status::NotSupported();
}
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
