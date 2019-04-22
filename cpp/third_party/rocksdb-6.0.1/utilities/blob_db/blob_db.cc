//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/blob_db/blob_db.h"

#include <inttypes.h>
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {
namespace blob_db {

Status BlobDB::Open(const Options& options, const BlobDBOptions& bdb_options,
                    const std::string& dbname, BlobDB** blob_db) {
  *blob_db = nullptr;
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = BlobDB::Open(db_options, bdb_options, dbname, column_families,
                          &handles, blob_db);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

Status BlobDB::Open(const DBOptions& db_options,
                    const BlobDBOptions& bdb_options, const std::string& dbname,
                    const std::vector<ColumnFamilyDescriptor>& column_families,
                    std::vector<ColumnFamilyHandle*>* handles,
                    BlobDB** blob_db) {
  if (column_families.size() != 1 ||
      column_families[0].name != kDefaultColumnFamilyName) {
    return Status::NotSupported(
        "Blob DB doesn't support non-default column family.");
  }

  BlobDBImpl* blob_db_impl = new BlobDBImpl(dbname, bdb_options, db_options,
                                            column_families[0].options);
  Status s = blob_db_impl->Open(handles);
  if (s.ok()) {
    *blob_db = static_cast<BlobDB*>(blob_db_impl);
  } else {
    delete blob_db_impl;
    *blob_db = nullptr;
  }
  return s;
}

BlobDB::BlobDB() : StackableDB(nullptr) {}

void BlobDBOptions::Dump(Logger* log) const {
  ROCKS_LOG_HEADER(
      log, "                                  BlobDBOptions.blob_dir: %s",
      blob_dir.c_str());
  ROCKS_LOG_HEADER(
      log, "                             BlobDBOptions.path_relative: %d",
      path_relative);
  ROCKS_LOG_HEADER(
      log, "                                   BlobDBOptions.is_fifo: %d",
      is_fifo);
  ROCKS_LOG_HEADER(
      log, "                               BlobDBOptions.max_db_size: %" PRIu64,
      max_db_size);
  ROCKS_LOG_HEADER(
      log, "                            BlobDBOptions.ttl_range_secs: %" PRIu64,
      ttl_range_secs);
  ROCKS_LOG_HEADER(
      log, "                             BlobDBOptions.min_blob_size: %" PRIu64,
      min_blob_size);
  ROCKS_LOG_HEADER(
      log, "                            BlobDBOptions.bytes_per_sync: %" PRIu64,
      bytes_per_sync);
  ROCKS_LOG_HEADER(
      log, "                            BlobDBOptions.blob_file_size: %" PRIu64,
      blob_file_size);
  ROCKS_LOG_HEADER(
      log, "                               BlobDBOptions.compression: %d",
      static_cast<int>(compression));
  ROCKS_LOG_HEADER(
      log, "                 BlobDBOptions.enable_garbage_collection: %d",
      enable_garbage_collection);
  ROCKS_LOG_HEADER(
      log, "          BlobDBOptions.garbage_collection_interval_secs: %" PRIu64,
      garbage_collection_interval_secs);
  ROCKS_LOG_HEADER(
      log, "                  BlobDBOptions.disable_background_tasks: %d",
      disable_background_tasks);
}

}  // namespace blob_db
}  // namespace rocksdb
#endif
