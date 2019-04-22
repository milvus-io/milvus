//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/transactions/optimistic_transaction_db_impl.h"

#include <string>
#include <vector>

#include "db/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "utilities/transactions/optimistic_transaction.h"

namespace rocksdb {

Transaction* OptimisticTransactionDBImpl::BeginTransaction(
    const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options, Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new OptimisticTransaction(this, write_options, txn_options);
  }
}

Status OptimisticTransactionDB::Open(const Options& options,
                                     const std::string& dbname,
                                     OptimisticTransactionDB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = Open(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }

  return s;
}

Status OptimisticTransactionDB::Open(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    OptimisticTransactionDB** dbptr) {
  Status s;
  DB* db;

  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;

  // Enable MemTable History if not already enabled
  for (auto& column_family : column_families_copy) {
    ColumnFamilyOptions* options = &column_family.options;

    if (options->max_write_buffer_number_to_maintain == 0) {
      // Setting to -1 will set the History size to max_write_buffer_number.
      options->max_write_buffer_number_to_maintain = -1;
    }
  }

  s = DB::Open(db_options, dbname, column_families_copy, handles, &db);

  if (s.ok()) {
    *dbptr = new OptimisticTransactionDBImpl(db);
  }

  return s;
}

void OptimisticTransactionDBImpl::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options) {
  assert(dynamic_cast<OptimisticTransaction*>(txn) != nullptr);
  auto txn_impl = reinterpret_cast<OptimisticTransaction*>(txn);

  txn_impl->Reinitialize(this, write_options, txn_options);
}

}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
