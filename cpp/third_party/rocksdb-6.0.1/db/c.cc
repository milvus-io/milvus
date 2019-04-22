//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE

#include "rocksdb/c.h"

#include <stdlib.h>
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iterator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/universal_compaction.h"
#include "rocksdb/utilities/backupable_db.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/memory_util.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/perf_context.h"
#include "utilities/merge_operators.h"

#include <vector>
#include <unordered_set>
#include <map>

using rocksdb::BytewiseComparator;
using rocksdb::Cache;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::CompactionFilter;
using rocksdb::CompactionFilterFactory;
using rocksdb::CompactionFilterContext;
using rocksdb::CompactionOptionsFIFO;
using rocksdb::Comparator;
using rocksdb::CompressionType;
using rocksdb::WALRecoveryMode;
using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::DbPath;
using rocksdb::Env;
using rocksdb::EnvOptions;
using rocksdb::InfoLogLevel;
using rocksdb::FileLock;
using rocksdb::FilterPolicy;
using rocksdb::FlushOptions;
using rocksdb::IngestExternalFileOptions;
using rocksdb::Iterator;
using rocksdb::Logger;
using rocksdb::MergeOperator;
using rocksdb::MergeOperators;
using rocksdb::NewBloomFilterPolicy;
using rocksdb::NewLRUCache;
using rocksdb::Options;
using rocksdb::BlockBasedTableOptions;
using rocksdb::CuckooTableOptions;
using rocksdb::RandomAccessFile;
using rocksdb::Range;
using rocksdb::ReadOptions;
using rocksdb::SequentialFile;
using rocksdb::Slice;
using rocksdb::SliceParts;
using rocksdb::SliceTransform;
using rocksdb::Snapshot;
using rocksdb::SstFileWriter;
using rocksdb::Status;
using rocksdb::WritableFile;
using rocksdb::WriteBatch;
using rocksdb::WriteBatchWithIndex;
using rocksdb::WriteOptions;
using rocksdb::LiveFileMetaData;
using rocksdb::BackupEngine;
using rocksdb::BackupableDBOptions;
using rocksdb::BackupInfo;
using rocksdb::BackupID;
using rocksdb::RestoreOptions;
using rocksdb::CompactRangeOptions;
using rocksdb::BottommostLevelCompaction;
using rocksdb::RateLimiter;
using rocksdb::NewGenericRateLimiter;
using rocksdb::PinnableSlice;
using rocksdb::TransactionDBOptions;
using rocksdb::TransactionDB;
using rocksdb::TransactionOptions;
using rocksdb::OptimisticTransactionDB;
using rocksdb::OptimisticTransactionOptions;
using rocksdb::Transaction;
using rocksdb::Checkpoint;
using rocksdb::TransactionLogIterator;
using rocksdb::BatchResult;
using rocksdb::PerfLevel;
using rocksdb::PerfContext;
using rocksdb::MemoryUtil;

using std::shared_ptr;
using std::vector;
using std::unordered_set;
using std::map;

extern "C" {

struct rocksdb_t                 { DB*               rep; };
struct rocksdb_backup_engine_t   { BackupEngine*     rep; };
struct rocksdb_backup_engine_info_t { std::vector<BackupInfo> rep; };
struct rocksdb_restore_options_t { RestoreOptions rep; };
struct rocksdb_iterator_t        { Iterator*         rep; };
struct rocksdb_writebatch_t      { WriteBatch        rep; };
struct rocksdb_writebatch_wi_t   { WriteBatchWithIndex* rep; };
struct rocksdb_snapshot_t        { const Snapshot*   rep; };
struct rocksdb_flushoptions_t    { FlushOptions      rep; };
struct rocksdb_fifo_compaction_options_t { CompactionOptionsFIFO rep; };
struct rocksdb_readoptions_t {
   ReadOptions rep;
   // stack variables to set pointers to in ReadOptions
   Slice upper_bound;
   Slice lower_bound;
};
struct rocksdb_writeoptions_t    { WriteOptions      rep; };
struct rocksdb_options_t         { Options           rep; };
struct rocksdb_compactoptions_t {
  CompactRangeOptions rep;
};
struct rocksdb_block_based_table_options_t  { BlockBasedTableOptions rep; };
struct rocksdb_cuckoo_table_options_t  { CuckooTableOptions rep; };
struct rocksdb_seqfile_t         { SequentialFile*   rep; };
struct rocksdb_randomfile_t      { RandomAccessFile* rep; };
struct rocksdb_writablefile_t    { WritableFile*     rep; };
struct rocksdb_wal_iterator_t { TransactionLogIterator* rep; };
struct rocksdb_wal_readoptions_t { TransactionLogIterator::ReadOptions rep; };
struct rocksdb_filelock_t        { FileLock*         rep; };
struct rocksdb_logger_t {
  std::shared_ptr<Logger> rep;
};
struct rocksdb_cache_t {
  std::shared_ptr<Cache> rep;
};
struct rocksdb_livefiles_t       { std::vector<LiveFileMetaData> rep; };
struct rocksdb_column_family_handle_t  { ColumnFamilyHandle* rep; };
struct rocksdb_envoptions_t      { EnvOptions        rep; };
struct rocksdb_ingestexternalfileoptions_t  { IngestExternalFileOptions rep; };
struct rocksdb_sstfilewriter_t   { SstFileWriter*    rep; };
struct rocksdb_ratelimiter_t {
  std::shared_ptr<RateLimiter> rep;
};
struct rocksdb_perfcontext_t     { PerfContext*      rep; };
struct rocksdb_pinnableslice_t {
  PinnableSlice rep;
};
struct rocksdb_transactiondb_options_t {
  TransactionDBOptions rep;
};
struct rocksdb_transactiondb_t {
  TransactionDB* rep;
};
struct rocksdb_transaction_options_t {
  TransactionOptions rep;
};
struct rocksdb_transaction_t {
  Transaction* rep;
};
struct rocksdb_checkpoint_t {
  Checkpoint* rep;
};
struct rocksdb_optimistictransactiondb_t {
  OptimisticTransactionDB* rep;
};
struct rocksdb_optimistictransaction_options_t {
  OptimisticTransactionOptions rep;
};

struct rocksdb_compactionfiltercontext_t {
  CompactionFilter::Context rep;
};

struct rocksdb_compactionfilter_t : public CompactionFilter {
  void* state_;
  void (*destructor_)(void*);
  unsigned char (*filter_)(
      void*,
      int level,
      const char* key, size_t key_length,
      const char* existing_value, size_t value_length,
      char** new_value, size_t *new_value_length,
      unsigned char* value_changed);
  const char* (*name_)(void*);
  unsigned char ignore_snapshots_;

  ~rocksdb_compactionfilter_t() override { (*destructor_)(state_); }

  bool Filter(int level, const Slice& key, const Slice& existing_value,
              std::string* new_value, bool* value_changed) const override {
    char* c_new_value = nullptr;
    size_t new_value_length = 0;
    unsigned char c_value_changed = 0;
    unsigned char result = (*filter_)(
        state_,
        level,
        key.data(), key.size(),
        existing_value.data(), existing_value.size(),
        &c_new_value, &new_value_length, &c_value_changed);
    if (c_value_changed) {
      new_value->assign(c_new_value, new_value_length);
      *value_changed = true;
    }
    return result;
  }

  const char* Name() const override { return (*name_)(state_); }

  bool IgnoreSnapshots() const override { return ignore_snapshots_; }
};

struct rocksdb_compactionfilterfactory_t : public CompactionFilterFactory {
  void* state_;
  void (*destructor_)(void*);
  rocksdb_compactionfilter_t* (*create_compaction_filter_)(
      void*, rocksdb_compactionfiltercontext_t* context);
  const char* (*name_)(void*);

  ~rocksdb_compactionfilterfactory_t() override { (*destructor_)(state_); }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    rocksdb_compactionfiltercontext_t ccontext;
    ccontext.rep = context;
    CompactionFilter* cf = (*create_compaction_filter_)(state_, &ccontext);
    return std::unique_ptr<CompactionFilter>(cf);
  }

  const char* Name() const override { return (*name_)(state_); }
};

struct rocksdb_comparator_t : public Comparator {
  void* state_;
  void (*destructor_)(void*);
  int (*compare_)(
      void*,
      const char* a, size_t alen,
      const char* b, size_t blen);
  const char* (*name_)(void*);

  ~rocksdb_comparator_t() override { (*destructor_)(state_); }

  int Compare(const Slice& a, const Slice& b) const override {
    return (*compare_)(state_, a.data(), a.size(), b.data(), b.size());
  }

  const char* Name() const override { return (*name_)(state_); }

  // No-ops since the C binding does not support key shortening methods.
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string* /*key*/) const override {}
};

struct rocksdb_filterpolicy_t : public FilterPolicy {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*create_)(
      void*,
      const char* const* key_array, const size_t* key_length_array,
      int num_keys,
      size_t* filter_length);
  unsigned char (*key_match_)(
      void*,
      const char* key, size_t length,
      const char* filter, size_t filter_length);
  void (*delete_filter_)(
      void*,
      const char* filter, size_t filter_length);

  ~rocksdb_filterpolicy_t() override { (*destructor_)(state_); }

  const char* Name() const override { return (*name_)(state_); }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    std::vector<const char*> key_pointers(n);
    std::vector<size_t> key_sizes(n);
    for (int i = 0; i < n; i++) {
      key_pointers[i] = keys[i].data();
      key_sizes[i] = keys[i].size();
    }
    size_t len;
    char* filter = (*create_)(state_, &key_pointers[0], &key_sizes[0], n, &len);
    dst->append(filter, len);

    if (delete_filter_ != nullptr) {
      (*delete_filter_)(state_, filter, len);
    } else {
      free(filter);
    }
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
    return (*key_match_)(state_, key.data(), key.size(),
                         filter.data(), filter.size());
  }
};

struct rocksdb_mergeoperator_t : public MergeOperator {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*full_merge_)(
      void*,
      const char* key, size_t key_length,
      const char* existing_value, size_t existing_value_length,
      const char* const* operands_list, const size_t* operands_list_length,
      int num_operands,
      unsigned char* success, size_t* new_value_length);
  char* (*partial_merge_)(void*, const char* key, size_t key_length,
                          const char* const* operands_list,
                          const size_t* operands_list_length, int num_operands,
                          unsigned char* success, size_t* new_value_length);
  void (*delete_value_)(
      void*,
      const char* value, size_t value_length);

  ~rocksdb_mergeoperator_t() override { (*destructor_)(state_); }

  const char* Name() const override { return (*name_)(state_); }

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    size_t n = merge_in.operand_list.size();
    std::vector<const char*> operand_pointers(n);
    std::vector<size_t> operand_sizes(n);
    for (size_t i = 0; i < n; i++) {
      Slice operand(merge_in.operand_list[i]);
      operand_pointers[i] = operand.data();
      operand_sizes[i] = operand.size();
    }

    const char* existing_value_data = nullptr;
    size_t existing_value_len = 0;
    if (merge_in.existing_value != nullptr) {
      existing_value_data = merge_in.existing_value->data();
      existing_value_len = merge_in.existing_value->size();
    }

    unsigned char success;
    size_t new_value_len;
    char* tmp_new_value = (*full_merge_)(
        state_, merge_in.key.data(), merge_in.key.size(), existing_value_data,
        existing_value_len, &operand_pointers[0], &operand_sizes[0],
        static_cast<int>(n), &success, &new_value_len);
    merge_out->new_value.assign(tmp_new_value, new_value_len);

    if (delete_value_ != nullptr) {
      (*delete_value_)(state_, tmp_new_value, new_value_len);
    } else {
      free(tmp_new_value);
    }

    return success;
  }

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* /*logger*/) const override {
    size_t operand_count = operand_list.size();
    std::vector<const char*> operand_pointers(operand_count);
    std::vector<size_t> operand_sizes(operand_count);
    for (size_t i = 0; i < operand_count; ++i) {
      Slice operand(operand_list[i]);
      operand_pointers[i] = operand.data();
      operand_sizes[i] = operand.size();
    }

    unsigned char success;
    size_t new_value_len;
    char* tmp_new_value = (*partial_merge_)(
        state_, key.data(), key.size(), &operand_pointers[0], &operand_sizes[0],
        static_cast<int>(operand_count), &success, &new_value_len);
    new_value->assign(tmp_new_value, new_value_len);

    if (delete_value_ != nullptr) {
      (*delete_value_)(state_, tmp_new_value, new_value_len);
    } else {
      free(tmp_new_value);
    }

    return success;
  }
};

struct rocksdb_dbpath_t {
  DbPath rep;
};

struct rocksdb_env_t {
  Env* rep;
  bool is_default;
};

struct rocksdb_slicetransform_t : public SliceTransform {
  void* state_;
  void (*destructor_)(void*);
  const char* (*name_)(void*);
  char* (*transform_)(
      void*,
      const char* key, size_t length,
      size_t* dst_length);
  unsigned char (*in_domain_)(
      void*,
      const char* key, size_t length);
  unsigned char (*in_range_)(
      void*,
      const char* key, size_t length);

  ~rocksdb_slicetransform_t() override { (*destructor_)(state_); }

  const char* Name() const override { return (*name_)(state_); }

  Slice Transform(const Slice& src) const override {
    size_t len;
    char* dst = (*transform_)(state_, src.data(), src.size(), &len);
    return Slice(dst, len);
  }

  bool InDomain(const Slice& src) const override {
    return (*in_domain_)(state_, src.data(), src.size());
  }

  bool InRange(const Slice& src) const override {
    return (*in_range_)(state_, src.data(), src.size());
  }
};

struct rocksdb_universal_compaction_options_t {
  rocksdb::CompactionOptionsUniversal *rep;
};

static bool SaveError(char** errptr, const Status& s) {
  assert(errptr != nullptr);
  if (s.ok()) {
    return false;
  } else if (*errptr == nullptr) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    // TODO(sanjay): Merge with existing error?
    // This is a bug if *errptr is not created by malloc()
    free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }
  return true;
}

static char* CopyString(const std::string& str) {
  char* result = reinterpret_cast<char*>(malloc(sizeof(char) * str.size()));
  memcpy(result, str.data(), sizeof(char) * str.size());
  return result;
}

rocksdb_t* rocksdb_open(
    const rocksdb_options_t* options,
    const char* name,
    char** errptr) {
  DB* db;
  if (SaveError(errptr, DB::Open(options->rep, std::string(name), &db))) {
    return nullptr;
  }
  rocksdb_t* result = new rocksdb_t;
  result->rep = db;
  return result;
}

rocksdb_t* rocksdb_open_with_ttl(
    const rocksdb_options_t* options,
    const char* name,
    int ttl,
    char** errptr) {
  rocksdb::DBWithTTL* db;
  if (SaveError(errptr, rocksdb::DBWithTTL::Open(options->rep, std::string(name), &db, ttl))) {
    return nullptr;
  }
  rocksdb_t* result = new rocksdb_t;
  result->rep = db;
  return result;
}

rocksdb_t* rocksdb_open_for_read_only(
    const rocksdb_options_t* options,
    const char* name,
    unsigned char error_if_log_file_exist,
    char** errptr) {
  DB* db;
  if (SaveError(errptr, DB::OpenForReadOnly(options->rep, std::string(name), &db, error_if_log_file_exist))) {
    return nullptr;
  }
  rocksdb_t* result = new rocksdb_t;
  result->rep = db;
  return result;
}

rocksdb_backup_engine_t* rocksdb_backup_engine_open(
    const rocksdb_options_t* options, const char* path, char** errptr) {
  BackupEngine* be;
  if (SaveError(errptr, BackupEngine::Open(options->rep.env,
                                           BackupableDBOptions(path,
                                                               nullptr,
                                                               true,
                                                               options->rep.info_log.get()),
                                           &be))) {
    return nullptr;
  }
  rocksdb_backup_engine_t* result = new rocksdb_backup_engine_t;
  result->rep = be;
  return result;
}

void rocksdb_backup_engine_create_new_backup(rocksdb_backup_engine_t* be,
                                             rocksdb_t* db,
                                             char** errptr) {
  SaveError(errptr, be->rep->CreateNewBackup(db->rep));
}

void rocksdb_backup_engine_create_new_backup_flush(rocksdb_backup_engine_t* be,
                                                   rocksdb_t* db,
                                                   unsigned char flush_before_backup,
                                                   char** errptr) {
  SaveError(errptr, be->rep->CreateNewBackup(db->rep, flush_before_backup));
}

void rocksdb_backup_engine_purge_old_backups(rocksdb_backup_engine_t* be,
                                             uint32_t num_backups_to_keep,
                                             char** errptr) {
  SaveError(errptr, be->rep->PurgeOldBackups(num_backups_to_keep));
}

rocksdb_restore_options_t* rocksdb_restore_options_create() {
  return new rocksdb_restore_options_t;
}

void rocksdb_restore_options_destroy(rocksdb_restore_options_t* opt) {
  delete opt;
}

void rocksdb_restore_options_set_keep_log_files(rocksdb_restore_options_t* opt,
                                                int v) {
  opt->rep.keep_log_files = v;
}


void rocksdb_backup_engine_verify_backup(rocksdb_backup_engine_t* be,
    uint32_t backup_id, char** errptr) {
  SaveError(errptr, be->rep->VerifyBackup(static_cast<BackupID>(backup_id)));
}

void rocksdb_backup_engine_restore_db_from_latest_backup(
    rocksdb_backup_engine_t* be, const char* db_dir, const char* wal_dir,
    const rocksdb_restore_options_t* restore_options, char** errptr) {
  SaveError(errptr, be->rep->RestoreDBFromLatestBackup(std::string(db_dir),
                                                       std::string(wal_dir),
                                                       restore_options->rep));
}

const rocksdb_backup_engine_info_t* rocksdb_backup_engine_get_backup_info(
    rocksdb_backup_engine_t* be) {
  rocksdb_backup_engine_info_t* result = new rocksdb_backup_engine_info_t;
  be->rep->GetBackupInfo(&result->rep);
  return result;
}

int rocksdb_backup_engine_info_count(const rocksdb_backup_engine_info_t* info) {
  return static_cast<int>(info->rep.size());
}

int64_t rocksdb_backup_engine_info_timestamp(
    const rocksdb_backup_engine_info_t* info, int index) {
  return info->rep[index].timestamp;
}

uint32_t rocksdb_backup_engine_info_backup_id(
    const rocksdb_backup_engine_info_t* info, int index) {
  return info->rep[index].backup_id;
}

uint64_t rocksdb_backup_engine_info_size(
    const rocksdb_backup_engine_info_t* info, int index) {
  return info->rep[index].size;
}

uint32_t rocksdb_backup_engine_info_number_files(
    const rocksdb_backup_engine_info_t* info, int index) {
  return info->rep[index].number_files;
}

void rocksdb_backup_engine_info_destroy(
    const rocksdb_backup_engine_info_t* info) {
  delete info;
}

void rocksdb_backup_engine_close(rocksdb_backup_engine_t* be) {
  delete be->rep;
  delete be;
}

rocksdb_checkpoint_t* rocksdb_checkpoint_object_create(rocksdb_t* db,
                                                       char** errptr) {
  Checkpoint* checkpoint;
  if (SaveError(errptr, Checkpoint::Create(db->rep, &checkpoint))) {
    return nullptr;
  }
  rocksdb_checkpoint_t* result = new rocksdb_checkpoint_t;
  result->rep = checkpoint;
  return result;
}

void rocksdb_checkpoint_create(rocksdb_checkpoint_t* checkpoint,
                               const char* checkpoint_dir,
                               uint64_t log_size_for_flush, char** errptr) {
  SaveError(errptr, checkpoint->rep->CreateCheckpoint(
                        std::string(checkpoint_dir), log_size_for_flush));
}

void rocksdb_checkpoint_object_destroy(rocksdb_checkpoint_t* checkpoint) {
  delete checkpoint->rep;
  delete checkpoint;
}

void rocksdb_close(rocksdb_t* db) {
  delete db->rep;
  delete db;
}

void rocksdb_options_set_uint64add_merge_operator(rocksdb_options_t* opt) {
  opt->rep.merge_operator = rocksdb::MergeOperators::CreateUInt64AddOperator();
}

rocksdb_t* rocksdb_open_column_families(
    const rocksdb_options_t* db_options,
    const char* name,
    int num_column_families,
    const char** column_family_names,
    const rocksdb_options_t** column_family_options,
    rocksdb_column_family_handle_t** column_family_handles,
    char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, DB::Open(DBOptions(db_options->rep),
          std::string(name), column_families, &handles, &db))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    rocksdb_column_family_handle_t* c_handle = new rocksdb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  rocksdb_t* result = new rocksdb_t;
  result->rep = db;
  return result;
}

rocksdb_t* rocksdb_open_for_read_only_column_families(
    const rocksdb_options_t* db_options,
    const char* name,
    int num_column_families,
    const char** column_family_names,
    const rocksdb_options_t** column_family_options,
    rocksdb_column_family_handle_t** column_family_handles,
    unsigned char error_if_log_file_exist,
    char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  DB* db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, DB::OpenForReadOnly(DBOptions(db_options->rep),
          std::string(name), column_families, &handles, &db, error_if_log_file_exist))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    rocksdb_column_family_handle_t* c_handle = new rocksdb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  rocksdb_t* result = new rocksdb_t;
  result->rep = db;
  return result;
}

char** rocksdb_list_column_families(
    const rocksdb_options_t* options,
    const char* name,
    size_t* lencfs,
    char** errptr) {
  std::vector<std::string> fams;
  SaveError(errptr,
      DB::ListColumnFamilies(DBOptions(options->rep),
        std::string(name), &fams));

  *lencfs = fams.size();
  char** column_families = static_cast<char**>(malloc(sizeof(char*) * fams.size()));
  for (size_t i = 0; i < fams.size(); i++) {
    column_families[i] = strdup(fams[i].c_str());
  }
  return column_families;
}

void rocksdb_list_column_families_destroy(char** list, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    free(list[i]);
  }
  free(list);
}

rocksdb_column_family_handle_t* rocksdb_create_column_family(
    rocksdb_t* db,
    const rocksdb_options_t* column_family_options,
    const char* column_family_name,
    char** errptr) {
  rocksdb_column_family_handle_t* handle = new rocksdb_column_family_handle_t;
  SaveError(errptr,
      db->rep->CreateColumnFamily(ColumnFamilyOptions(column_family_options->rep),
        std::string(column_family_name), &(handle->rep)));
  return handle;
}

void rocksdb_drop_column_family(
    rocksdb_t* db,
    rocksdb_column_family_handle_t* handle,
    char** errptr) {
  SaveError(errptr, db->rep->DropColumnFamily(handle->rep));
}

void rocksdb_column_family_handle_destroy(rocksdb_column_family_handle_t* handle) {
  delete handle->rep;
  delete handle;
}

void rocksdb_put(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Put(options->rep, Slice(key, keylen), Slice(val, vallen)));
}

void rocksdb_put_cf(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Put(options->rep, column_family->rep,
              Slice(key, keylen), Slice(val, vallen)));
}

void rocksdb_delete(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    const char* key, size_t keylen,
    char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, Slice(key, keylen)));
}

void rocksdb_delete_cf(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    char** errptr) {
  SaveError(errptr, db->rep->Delete(options->rep, column_family->rep,
        Slice(key, keylen)));
}

void rocksdb_merge(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Merge(options->rep, Slice(key, keylen), Slice(val, vallen)));
}

void rocksdb_merge_cf(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    const char* val, size_t vallen,
    char** errptr) {
  SaveError(errptr,
            db->rep->Merge(options->rep, column_family->rep,
              Slice(key, keylen), Slice(val, vallen)));
}

void rocksdb_write(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_writebatch_t* batch,
    char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
}

char* rocksdb_get(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = db->rep->Get(options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* rocksdb_get_cf(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = db->rep->Get(options->rep, column_family->rep,
      Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

void rocksdb_multi_get(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    char** values_list, size_t* values_list_sizes,
    char** errs) {
  std::vector<Slice> keys(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    keys[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<std::string> values(num_keys);
  std::vector<Status> statuses = db->rep->MultiGet(options->rep, keys, &values);
  for (size_t i = 0; i < num_keys; i++) {
    if (statuses[i].ok()) {
      values_list[i] = CopyString(values[i]);
      values_list_sizes[i] = values[i].size();
      errs[i] = nullptr;
    } else {
      values_list[i] = nullptr;
      values_list_sizes[i] = 0;
      if (!statuses[i].IsNotFound()) {
        errs[i] = strdup(statuses[i].ToString().c_str());
      } else {
        errs[i] = nullptr;
      }
    }
  }
}

void rocksdb_multi_get_cf(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    const rocksdb_column_family_handle_t* const* column_families,
    size_t num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    char** values_list, size_t* values_list_sizes,
    char** errs) {
  std::vector<Slice> keys(num_keys);
  std::vector<ColumnFamilyHandle*> cfs(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    keys[i] = Slice(keys_list[i], keys_list_sizes[i]);
    cfs[i] = column_families[i]->rep;
  }
  std::vector<std::string> values(num_keys);
  std::vector<Status> statuses = db->rep->MultiGet(options->rep, cfs, keys, &values);
  for (size_t i = 0; i < num_keys; i++) {
    if (statuses[i].ok()) {
      values_list[i] = CopyString(values[i]);
      values_list_sizes[i] = values[i].size();
      errs[i] = nullptr;
    } else {
      values_list[i] = nullptr;
      values_list_sizes[i] = 0;
      if (!statuses[i].IsNotFound()) {
        errs[i] = strdup(statuses[i].ToString().c_str());
      } else {
        errs[i] = nullptr;
      }
    }
  }
}

rocksdb_iterator_t* rocksdb_create_iterator(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options) {
  rocksdb_iterator_t* result = new rocksdb_iterator_t;
  result->rep = db->rep->NewIterator(options->rep);
  return result;
}

rocksdb_wal_iterator_t* rocksdb_get_updates_since(
        rocksdb_t* db, uint64_t seq_number,
        const rocksdb_wal_readoptions_t* options,
        char** errptr) {
  std::unique_ptr<TransactionLogIterator> iter;
  TransactionLogIterator::ReadOptions ro;
  if (options!=nullptr) {
      ro = options->rep;
  }
  if (SaveError(errptr, db->rep->GetUpdatesSince(seq_number, &iter, ro))) {
    return nullptr;
  }
  rocksdb_wal_iterator_t* result = new rocksdb_wal_iterator_t;
  result->rep = iter.release();
  return result;
}

void rocksdb_wal_iter_next(rocksdb_wal_iterator_t* iter) {
    iter->rep->Next();
}

unsigned char rocksdb_wal_iter_valid(const rocksdb_wal_iterator_t* iter) {
    return iter->rep->Valid();
}

void rocksdb_wal_iter_status (const rocksdb_wal_iterator_t* iter, char** errptr) {
    SaveError(errptr, iter->rep->status());
}

void rocksdb_wal_iter_destroy (const rocksdb_wal_iterator_t* iter) {
  delete iter->rep;
  delete iter;
}

rocksdb_writebatch_t* rocksdb_wal_iter_get_batch (const rocksdb_wal_iterator_t* iter, uint64_t* seq) {
  rocksdb_writebatch_t* result = rocksdb_writebatch_create();
  BatchResult wal_batch = iter->rep->GetBatch();
  result->rep = * wal_batch.writeBatchPtr.release();
  if (seq != nullptr) {
    *seq = wal_batch.sequence;
  }
  return result;
}

uint64_t rocksdb_get_latest_sequence_number (rocksdb_t *db) {
    return db->rep->GetLatestSequenceNumber();
}

rocksdb_iterator_t* rocksdb_create_iterator_cf(
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family) {
  rocksdb_iterator_t* result = new rocksdb_iterator_t;
  result->rep = db->rep->NewIterator(options->rep, column_family->rep);
  return result;
}

void rocksdb_create_iterators(
    rocksdb_t *db,
    rocksdb_readoptions_t* opts,
    rocksdb_column_family_handle_t** column_families,
    rocksdb_iterator_t** iterators,
    size_t size,
    char** errptr) {
  std::vector<ColumnFamilyHandle*> column_families_vec;
  for (size_t i = 0; i < size; i++) {
    column_families_vec.push_back(column_families[i]->rep);
  }

  std::vector<Iterator*> res;
  Status status = db->rep->NewIterators(opts->rep, column_families_vec, &res);
  assert(res.size() == size);
  if (SaveError(errptr, status)) {
    return;
  }

  for (size_t i = 0; i < size; i++) {
    iterators[i] = new rocksdb_iterator_t;
    iterators[i]->rep = res[i];
  }
}

const rocksdb_snapshot_t* rocksdb_create_snapshot(
    rocksdb_t* db) {
  rocksdb_snapshot_t* result = new rocksdb_snapshot_t;
  result->rep = db->rep->GetSnapshot();
  return result;
}

void rocksdb_release_snapshot(
    rocksdb_t* db,
    const rocksdb_snapshot_t* snapshot) {
  db->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

char* rocksdb_property_value(
    rocksdb_t* db,
    const char* propname) {
  std::string tmp;
  if (db->rep->GetProperty(Slice(propname), &tmp)) {
    // We use strdup() since we expect human readable output.
    return strdup(tmp.c_str());
  } else {
    return nullptr;
  }
}

int rocksdb_property_int(
    rocksdb_t* db,
    const char* propname,
    uint64_t *out_val) {
  if (db->rep->GetIntProperty(Slice(propname), out_val)) {
    return 0;
  } else {
    return -1;
  }
}

char* rocksdb_property_value_cf(
    rocksdb_t* db,
    rocksdb_column_family_handle_t* column_family,
    const char* propname) {
  std::string tmp;
  if (db->rep->GetProperty(column_family->rep, Slice(propname), &tmp)) {
    // We use strdup() since we expect human readable output.
    return strdup(tmp.c_str());
  } else {
    return nullptr;
  }
}

void rocksdb_approximate_sizes(
    rocksdb_t* db,
    int num_ranges,
    const char* const* range_start_key, const size_t* range_start_key_len,
    const char* const* range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes) {
  Range* ranges = new Range[num_ranges];
  for (int i = 0; i < num_ranges; i++) {
    ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
    ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
  }
  db->rep->GetApproximateSizes(ranges, num_ranges, sizes);
  delete[] ranges;
}

void rocksdb_approximate_sizes_cf(
    rocksdb_t* db,
    rocksdb_column_family_handle_t* column_family,
    int num_ranges,
    const char* const* range_start_key, const size_t* range_start_key_len,
    const char* const* range_limit_key, const size_t* range_limit_key_len,
    uint64_t* sizes) {
  Range* ranges = new Range[num_ranges];
  for (int i = 0; i < num_ranges; i++) {
    ranges[i].start = Slice(range_start_key[i], range_start_key_len[i]);
    ranges[i].limit = Slice(range_limit_key[i], range_limit_key_len[i]);
  }
  db->rep->GetApproximateSizes(column_family->rep, ranges, num_ranges, sizes);
  delete[] ranges;
}

void rocksdb_delete_file(
    rocksdb_t* db,
    const char* name) {
  db->rep->DeleteFile(name);
}

const rocksdb_livefiles_t* rocksdb_livefiles(
    rocksdb_t* db) {
  rocksdb_livefiles_t* result = new rocksdb_livefiles_t;
  db->rep->GetLiveFilesMetaData(&result->rep);
  return result;
}

void rocksdb_compact_range(
    rocksdb_t* db,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      CompactRangeOptions(),
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void rocksdb_compact_range_cf(
    rocksdb_t* db,
    rocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len,
    const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      CompactRangeOptions(), column_family->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void rocksdb_compact_range_opt(rocksdb_t* db, rocksdb_compactoptions_t* opt,
                               const char* start_key, size_t start_key_len,
                               const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      opt->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void rocksdb_compact_range_cf_opt(rocksdb_t* db,
                                  rocksdb_column_family_handle_t* column_family,
                                  rocksdb_compactoptions_t* opt,
                                  const char* start_key, size_t start_key_len,
                                  const char* limit_key, size_t limit_key_len) {
  Slice a, b;
  db->rep->CompactRange(
      opt->rep, column_family->rep,
      // Pass nullptr Slice if corresponding "const char*" is nullptr
      (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
      (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr));
}

void rocksdb_flush(
    rocksdb_t* db,
    const rocksdb_flushoptions_t* options,
    char** errptr) {
  SaveError(errptr, db->rep->Flush(options->rep));
}

void rocksdb_disable_file_deletions(
    rocksdb_t* db,
    char** errptr) {
  SaveError(errptr, db->rep->DisableFileDeletions());
}

void rocksdb_enable_file_deletions(
    rocksdb_t* db,
    unsigned char force,
    char** errptr) {
  SaveError(errptr, db->rep->EnableFileDeletions(force));
}

void rocksdb_destroy_db(
    const rocksdb_options_t* options,
    const char* name,
    char** errptr) {
  SaveError(errptr, DestroyDB(name, options->rep));
}

void rocksdb_repair_db(
    const rocksdb_options_t* options,
    const char* name,
    char** errptr) {
  SaveError(errptr, RepairDB(name, options->rep));
}

void rocksdb_iter_destroy(rocksdb_iterator_t* iter) {
  delete iter->rep;
  delete iter;
}

unsigned char rocksdb_iter_valid(const rocksdb_iterator_t* iter) {
  return iter->rep->Valid();
}

void rocksdb_iter_seek_to_first(rocksdb_iterator_t* iter) {
  iter->rep->SeekToFirst();
}

void rocksdb_iter_seek_to_last(rocksdb_iterator_t* iter) {
  iter->rep->SeekToLast();
}

void rocksdb_iter_seek(rocksdb_iterator_t* iter, const char* k, size_t klen) {
  iter->rep->Seek(Slice(k, klen));
}

void rocksdb_iter_seek_for_prev(rocksdb_iterator_t* iter, const char* k,
                                size_t klen) {
  iter->rep->SeekForPrev(Slice(k, klen));
}

void rocksdb_iter_next(rocksdb_iterator_t* iter) {
  iter->rep->Next();
}

void rocksdb_iter_prev(rocksdb_iterator_t* iter) {
  iter->rep->Prev();
}

const char* rocksdb_iter_key(const rocksdb_iterator_t* iter, size_t* klen) {
  Slice s = iter->rep->key();
  *klen = s.size();
  return s.data();
}

const char* rocksdb_iter_value(const rocksdb_iterator_t* iter, size_t* vlen) {
  Slice s = iter->rep->value();
  *vlen = s.size();
  return s.data();
}

void rocksdb_iter_get_error(const rocksdb_iterator_t* iter, char** errptr) {
  SaveError(errptr, iter->rep->status());
}

rocksdb_writebatch_t* rocksdb_writebatch_create() {
  return new rocksdb_writebatch_t;
}

rocksdb_writebatch_t* rocksdb_writebatch_create_from(const char* rep,
                                                     size_t size) {
  rocksdb_writebatch_t* b = new rocksdb_writebatch_t;
  b->rep = WriteBatch(std::string(rep, size));
  return b;
}

void rocksdb_writebatch_destroy(rocksdb_writebatch_t* b) {
  delete b;
}

void rocksdb_writebatch_clear(rocksdb_writebatch_t* b) {
  b->rep.Clear();
}

int rocksdb_writebatch_count(rocksdb_writebatch_t* b) {
  return b->rep.Count();
}

void rocksdb_writebatch_put(
    rocksdb_writebatch_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Put(Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_put_cf(
    rocksdb_writebatch_t* b,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Put(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_putv(
    rocksdb_writebatch_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Put(SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void rocksdb_writebatch_putv_cf(
    rocksdb_writebatch_t* b,
    rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Put(column_family->rep, SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void rocksdb_writebatch_merge(
    rocksdb_writebatch_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Merge(Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_merge_cf(
    rocksdb_writebatch_t* b,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep.Merge(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_mergev(
    rocksdb_writebatch_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Merge(SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void rocksdb_writebatch_mergev_cf(
    rocksdb_writebatch_t* b,
    rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep.Merge(column_family->rep, SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void rocksdb_writebatch_delete(
    rocksdb_writebatch_t* b,
    const char* key, size_t klen) {
  b->rep.Delete(Slice(key, klen));
}

void rocksdb_writebatch_delete_cf(
    rocksdb_writebatch_t* b,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen) {
  b->rep.Delete(column_family->rep, Slice(key, klen));
}

void rocksdb_writebatch_deletev(
    rocksdb_writebatch_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep.Delete(SliceParts(key_slices.data(), num_keys));
}

void rocksdb_writebatch_deletev_cf(
    rocksdb_writebatch_t* b,
    rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep.Delete(column_family->rep, SliceParts(key_slices.data(), num_keys));
}

void rocksdb_writebatch_delete_range(rocksdb_writebatch_t* b,
                                     const char* start_key,
                                     size_t start_key_len, const char* end_key,
                                     size_t end_key_len) {
  b->rep.DeleteRange(Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void rocksdb_writebatch_delete_range_cf(
    rocksdb_writebatch_t* b, rocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* end_key,
    size_t end_key_len) {
  b->rep.DeleteRange(column_family->rep, Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void rocksdb_writebatch_delete_rangev(rocksdb_writebatch_t* b, int num_keys,
                                      const char* const* start_keys_list,
                                      const size_t* start_keys_list_sizes,
                                      const char* const* end_keys_list,
                                      const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep.DeleteRange(SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void rocksdb_writebatch_delete_rangev_cf(
    rocksdb_writebatch_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep.DeleteRange(column_family->rep,
                     SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void rocksdb_writebatch_put_log_data(
    rocksdb_writebatch_t* b,
    const char* blob, size_t len) {
  b->rep.PutLogData(Slice(blob, len));
}

class H : public WriteBatch::Handler {
 public:
  void* state_;
  void (*put_)(void*, const char* k, size_t klen, const char* v, size_t vlen);
  void (*deleted_)(void*, const char* k, size_t klen);
  void Put(const Slice& key, const Slice& value) override {
    (*put_)(state_, key.data(), key.size(), value.data(), value.size());
  }
  void Delete(const Slice& key) override {
    (*deleted_)(state_, key.data(), key.size());
  }
};

void rocksdb_writebatch_iterate(
    rocksdb_writebatch_t* b,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen)) {
  H handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.deleted_ = deleted;
  b->rep.Iterate(&handler);
}

const char* rocksdb_writebatch_data(rocksdb_writebatch_t* b, size_t* size) {
  *size = b->rep.GetDataSize();
  return b->rep.Data().c_str();
}

void rocksdb_writebatch_set_save_point(rocksdb_writebatch_t* b) {
  b->rep.SetSavePoint();
}

void rocksdb_writebatch_rollback_to_save_point(rocksdb_writebatch_t* b,
                                               char** errptr) {
  SaveError(errptr, b->rep.RollbackToSavePoint());
}

void rocksdb_writebatch_pop_save_point(rocksdb_writebatch_t* b, char** errptr) {
  SaveError(errptr, b->rep.PopSavePoint());
}

rocksdb_writebatch_wi_t* rocksdb_writebatch_wi_create(size_t reserved_bytes, unsigned char overwrite_key) {
  rocksdb_writebatch_wi_t* b = new rocksdb_writebatch_wi_t;
  b->rep = new WriteBatchWithIndex(BytewiseComparator(), reserved_bytes, overwrite_key);
  return b;
}

void rocksdb_writebatch_wi_destroy(rocksdb_writebatch_wi_t* b) {
  if (b->rep) {
    delete b->rep;
  }
  delete b;
}

void rocksdb_writebatch_wi_clear(rocksdb_writebatch_wi_t* b) {
  b->rep->Clear();
}

int rocksdb_writebatch_wi_count(rocksdb_writebatch_wi_t* b) {
  return b->rep->GetWriteBatch()->Count();
}

void rocksdb_writebatch_wi_put(
    rocksdb_writebatch_wi_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep->Put(Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_wi_put_cf(
    rocksdb_writebatch_wi_t* b,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep->Put(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_wi_putv(
    rocksdb_writebatch_wi_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Put(SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void rocksdb_writebatch_wi_putv_cf(
    rocksdb_writebatch_wi_t* b,
    rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Put(column_family->rep, SliceParts(key_slices.data(), num_keys),
             SliceParts(value_slices.data(), num_values));
}

void rocksdb_writebatch_wi_merge(
    rocksdb_writebatch_wi_t* b,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep->Merge(Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_wi_merge_cf(
    rocksdb_writebatch_wi_t* b,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen,
    const char* val, size_t vlen) {
  b->rep->Merge(column_family->rep, Slice(key, klen), Slice(val, vlen));
}

void rocksdb_writebatch_wi_mergev(
    rocksdb_writebatch_wi_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Merge(SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void rocksdb_writebatch_wi_mergev_cf(
    rocksdb_writebatch_wi_t* b,
    rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes,
    int num_values, const char* const* values_list,
    const size_t* values_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  std::vector<Slice> value_slices(num_values);
  for (int i = 0; i < num_values; i++) {
    value_slices[i] = Slice(values_list[i], values_list_sizes[i]);
  }
  b->rep->Merge(column_family->rep, SliceParts(key_slices.data(), num_keys),
               SliceParts(value_slices.data(), num_values));
}

void rocksdb_writebatch_wi_delete(
    rocksdb_writebatch_wi_t* b,
    const char* key, size_t klen) {
  b->rep->Delete(Slice(key, klen));
}

void rocksdb_writebatch_wi_delete_cf(
    rocksdb_writebatch_wi_t* b,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen) {
  b->rep->Delete(column_family->rep, Slice(key, klen));
}

void rocksdb_writebatch_wi_deletev(
    rocksdb_writebatch_wi_t* b,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep->Delete(SliceParts(key_slices.data(), num_keys));
}

void rocksdb_writebatch_wi_deletev_cf(
    rocksdb_writebatch_wi_t* b,
    rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* keys_list,
    const size_t* keys_list_sizes) {
  std::vector<Slice> key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    key_slices[i] = Slice(keys_list[i], keys_list_sizes[i]);
  }
  b->rep->Delete(column_family->rep, SliceParts(key_slices.data(), num_keys));
}

void rocksdb_writebatch_wi_delete_range(rocksdb_writebatch_wi_t* b,
                                     const char* start_key,
                                     size_t start_key_len, const char* end_key,
                                     size_t end_key_len) {
  b->rep->DeleteRange(Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void rocksdb_writebatch_wi_delete_range_cf(
    rocksdb_writebatch_wi_t* b, rocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* end_key,
    size_t end_key_len) {
  b->rep->DeleteRange(column_family->rep, Slice(start_key, start_key_len),
                     Slice(end_key, end_key_len));
}

void rocksdb_writebatch_wi_delete_rangev(rocksdb_writebatch_wi_t* b, int num_keys,
                                      const char* const* start_keys_list,
                                      const size_t* start_keys_list_sizes,
                                      const char* const* end_keys_list,
                                      const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep->DeleteRange(SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void rocksdb_writebatch_wi_delete_rangev_cf(
    rocksdb_writebatch_wi_t* b, rocksdb_column_family_handle_t* column_family,
    int num_keys, const char* const* start_keys_list,
    const size_t* start_keys_list_sizes, const char* const* end_keys_list,
    const size_t* end_keys_list_sizes) {
  std::vector<Slice> start_key_slices(num_keys);
  std::vector<Slice> end_key_slices(num_keys);
  for (int i = 0; i < num_keys; i++) {
    start_key_slices[i] = Slice(start_keys_list[i], start_keys_list_sizes[i]);
    end_key_slices[i] = Slice(end_keys_list[i], end_keys_list_sizes[i]);
  }
  b->rep->DeleteRange(column_family->rep,
                     SliceParts(start_key_slices.data(), num_keys),
                     SliceParts(end_key_slices.data(), num_keys));
}

void rocksdb_writebatch_wi_put_log_data(
    rocksdb_writebatch_wi_t* b,
    const char* blob, size_t len) {
  b->rep->PutLogData(Slice(blob, len));
}

void rocksdb_writebatch_wi_iterate(
    rocksdb_writebatch_wi_t* b,
    void* state,
    void (*put)(void*, const char* k, size_t klen, const char* v, size_t vlen),
    void (*deleted)(void*, const char* k, size_t klen)) {
  H handler;
  handler.state_ = state;
  handler.put_ = put;
  handler.deleted_ = deleted;
  b->rep->GetWriteBatch()->Iterate(&handler);
}

const char* rocksdb_writebatch_wi_data(rocksdb_writebatch_wi_t* b, size_t* size) {
  WriteBatch* wb = b->rep->GetWriteBatch();
  *size = wb->GetDataSize();
  return wb->Data().c_str();
}

void rocksdb_writebatch_wi_set_save_point(rocksdb_writebatch_wi_t* b) {
  b->rep->SetSavePoint();
}

void rocksdb_writebatch_wi_rollback_to_save_point(rocksdb_writebatch_wi_t* b,
                                               char** errptr) {
  SaveError(errptr, b->rep->RollbackToSavePoint());
}

rocksdb_iterator_t* rocksdb_writebatch_wi_create_iterator_with_base(
    rocksdb_writebatch_wi_t* wbwi,
    rocksdb_iterator_t* base_iterator) {
  rocksdb_iterator_t* result = new rocksdb_iterator_t;
  result->rep = wbwi->rep->NewIteratorWithBase(base_iterator->rep);
  delete base_iterator;
  return result;
}

rocksdb_iterator_t* rocksdb_writebatch_wi_create_iterator_with_base_cf(
    rocksdb_writebatch_wi_t* wbwi, rocksdb_iterator_t* base_iterator,
    rocksdb_column_family_handle_t* column_family) {
  rocksdb_iterator_t* result = new rocksdb_iterator_t;
  result->rep =
      wbwi->rep->NewIteratorWithBase(column_family->rep, base_iterator->rep);
  delete base_iterator;
  return result;
}

char* rocksdb_writebatch_wi_get_from_batch(
    rocksdb_writebatch_wi_t* wbwi,
    const rocksdb_options_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = wbwi->rep->GetFromBatch(options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* rocksdb_writebatch_wi_get_from_batch_cf(
    rocksdb_writebatch_wi_t* wbwi,
    const rocksdb_options_t* options,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = wbwi->rep->GetFromBatch(column_family->rep, options->rep,
      Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* rocksdb_writebatch_wi_get_from_batch_and_db(
    rocksdb_writebatch_wi_t* wbwi,
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = wbwi->rep->GetFromBatchAndDB(db->rep, options->rep, Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* rocksdb_writebatch_wi_get_from_batch_and_db_cf(
    rocksdb_writebatch_wi_t* wbwi,
    rocksdb_t* db,
    const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family,
    const char* key, size_t keylen,
    size_t* vallen,
    char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = wbwi->rep->GetFromBatchAndDB(db->rep, options->rep, column_family->rep,
      Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

void rocksdb_write_writebatch_wi(
    rocksdb_t* db,
    const rocksdb_writeoptions_t* options,
    rocksdb_writebatch_wi_t* wbwi,
    char** errptr) {
  WriteBatch* wb = wbwi->rep->GetWriteBatch();
  SaveError(errptr, db->rep->Write(options->rep, wb));
}

rocksdb_block_based_table_options_t*
rocksdb_block_based_options_create() {
  return new rocksdb_block_based_table_options_t;
}

void rocksdb_block_based_options_destroy(
    rocksdb_block_based_table_options_t* options) {
  delete options;
}

void rocksdb_block_based_options_set_block_size(
    rocksdb_block_based_table_options_t* options, size_t block_size) {
  options->rep.block_size = block_size;
}

void rocksdb_block_based_options_set_block_size_deviation(
    rocksdb_block_based_table_options_t* options, int block_size_deviation) {
  options->rep.block_size_deviation = block_size_deviation;
}

void rocksdb_block_based_options_set_block_restart_interval(
    rocksdb_block_based_table_options_t* options, int block_restart_interval) {
  options->rep.block_restart_interval = block_restart_interval;
}

void rocksdb_block_based_options_set_index_block_restart_interval(
    rocksdb_block_based_table_options_t* options, int index_block_restart_interval) {
  options->rep.index_block_restart_interval = index_block_restart_interval;
}

void rocksdb_block_based_options_set_metadata_block_size(
    rocksdb_block_based_table_options_t* options, uint64_t metadata_block_size) {
  options->rep.metadata_block_size = metadata_block_size;
}

void rocksdb_block_based_options_set_partition_filters(
    rocksdb_block_based_table_options_t* options, unsigned char partition_filters) {
  options->rep.partition_filters = partition_filters;
}

void rocksdb_block_based_options_set_use_delta_encoding(
    rocksdb_block_based_table_options_t* options, unsigned char use_delta_encoding) {
  options->rep.use_delta_encoding = use_delta_encoding;
}

void rocksdb_block_based_options_set_filter_policy(
    rocksdb_block_based_table_options_t* options,
    rocksdb_filterpolicy_t* filter_policy) {
  options->rep.filter_policy.reset(filter_policy);
}

void rocksdb_block_based_options_set_no_block_cache(
    rocksdb_block_based_table_options_t* options,
    unsigned char no_block_cache) {
  options->rep.no_block_cache = no_block_cache;
}

void rocksdb_block_based_options_set_block_cache(
    rocksdb_block_based_table_options_t* options,
    rocksdb_cache_t* block_cache) {
  if (block_cache) {
    options->rep.block_cache = block_cache->rep;
  }
}

void rocksdb_block_based_options_set_block_cache_compressed(
    rocksdb_block_based_table_options_t* options,
    rocksdb_cache_t* block_cache_compressed) {
  if (block_cache_compressed) {
    options->rep.block_cache_compressed = block_cache_compressed->rep;
  }
}

void rocksdb_block_based_options_set_whole_key_filtering(
    rocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.whole_key_filtering = v;
}

void rocksdb_block_based_options_set_format_version(
    rocksdb_block_based_table_options_t* options, int v) {
  options->rep.format_version = v;
}

void rocksdb_block_based_options_set_index_type(
    rocksdb_block_based_table_options_t* options, int v) {
  options->rep.index_type = static_cast<BlockBasedTableOptions::IndexType>(v);
}

void rocksdb_block_based_options_set_hash_index_allow_collision(
    rocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.hash_index_allow_collision = v;
}

void rocksdb_block_based_options_set_cache_index_and_filter_blocks(
    rocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.cache_index_and_filter_blocks = v;
}

void rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(
    rocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.cache_index_and_filter_blocks_with_high_priority = v;
}

void rocksdb_block_based_options_set_pin_l0_filter_and_index_blocks_in_cache(
    rocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.pin_l0_filter_and_index_blocks_in_cache = v;
}

void rocksdb_block_based_options_set_pin_top_level_index_and_filter(
    rocksdb_block_based_table_options_t* options, unsigned char v) {
  options->rep.pin_top_level_index_and_filter = v;
}

void rocksdb_options_set_block_based_table_factory(
    rocksdb_options_t *opt,
    rocksdb_block_based_table_options_t* table_options) {
  if (table_options) {
    opt->rep.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options->rep));
  }
}

rocksdb_cuckoo_table_options_t*
rocksdb_cuckoo_options_create() {
  return new rocksdb_cuckoo_table_options_t;
}

void rocksdb_cuckoo_options_destroy(
    rocksdb_cuckoo_table_options_t* options) {
  delete options;
}

void rocksdb_cuckoo_options_set_hash_ratio(
    rocksdb_cuckoo_table_options_t* options, double v) {
  options->rep.hash_table_ratio = v;
}

void rocksdb_cuckoo_options_set_max_search_depth(
    rocksdb_cuckoo_table_options_t* options, uint32_t v) {
  options->rep.max_search_depth = v;
}

void rocksdb_cuckoo_options_set_cuckoo_block_size(
    rocksdb_cuckoo_table_options_t* options, uint32_t v) {
  options->rep.cuckoo_block_size = v;
}

void rocksdb_cuckoo_options_set_identity_as_first_hash(
    rocksdb_cuckoo_table_options_t* options, unsigned char v) {
  options->rep.identity_as_first_hash = v;
}

void rocksdb_cuckoo_options_set_use_module_hash(
    rocksdb_cuckoo_table_options_t* options, unsigned char v) {
  options->rep.use_module_hash = v;
}

void rocksdb_options_set_cuckoo_table_factory(
    rocksdb_options_t *opt,
    rocksdb_cuckoo_table_options_t* table_options) {
  if (table_options) {
    opt->rep.table_factory.reset(
        rocksdb::NewCuckooTableFactory(table_options->rep));
  }
}

void rocksdb_set_options(
    rocksdb_t* db, int count, const char* const keys[], const char* const values[], char** errptr) {
        std::unordered_map<std::string, std::string> options_map;
        for (int i=0; i<count; i++)
            options_map[keys[i]] = values[i];
        SaveError(errptr,
            db->rep->SetOptions(options_map));
    }

void rocksdb_set_options_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* handle, int count, const char* const keys[], const char* const values[], char** errptr) {
        std::unordered_map<std::string, std::string> options_map;
        for (int i=0; i<count; i++)
            options_map[keys[i]] = values[i];
        SaveError(errptr,
            db->rep->SetOptions(handle->rep, options_map));
    }

rocksdb_options_t* rocksdb_options_create() {
  return new rocksdb_options_t;
}

void rocksdb_options_destroy(rocksdb_options_t* options) {
  delete options;
}

void rocksdb_options_increase_parallelism(
    rocksdb_options_t* opt, int total_threads) {
  opt->rep.IncreaseParallelism(total_threads);
}

void rocksdb_options_optimize_for_point_lookup(
    rocksdb_options_t* opt, uint64_t block_cache_size_mb) {
  opt->rep.OptimizeForPointLookup(block_cache_size_mb);
}

void rocksdb_options_optimize_level_style_compaction(
    rocksdb_options_t* opt, uint64_t memtable_memory_budget) {
  opt->rep.OptimizeLevelStyleCompaction(memtable_memory_budget);
}

void rocksdb_options_optimize_universal_style_compaction(
    rocksdb_options_t* opt, uint64_t memtable_memory_budget) {
  opt->rep.OptimizeUniversalStyleCompaction(memtable_memory_budget);
}

void rocksdb_options_set_allow_ingest_behind(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.allow_ingest_behind = v;
}

void rocksdb_options_set_compaction_filter(
    rocksdb_options_t* opt,
    rocksdb_compactionfilter_t* filter) {
  opt->rep.compaction_filter = filter;
}

void rocksdb_options_set_compaction_filter_factory(
    rocksdb_options_t* opt, rocksdb_compactionfilterfactory_t* factory) {
  opt->rep.compaction_filter_factory =
      std::shared_ptr<CompactionFilterFactory>(factory);
}

void rocksdb_options_compaction_readahead_size(
    rocksdb_options_t* opt, size_t s) {
  opt->rep.compaction_readahead_size = s;
}

void rocksdb_options_set_comparator(
    rocksdb_options_t* opt,
    rocksdb_comparator_t* cmp) {
  opt->rep.comparator = cmp;
}

void rocksdb_options_set_merge_operator(
    rocksdb_options_t* opt,
    rocksdb_mergeoperator_t* merge_operator) {
  opt->rep.merge_operator = std::shared_ptr<MergeOperator>(merge_operator);
}


void rocksdb_options_set_create_if_missing(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.create_if_missing = v;
}

void rocksdb_options_set_create_missing_column_families(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.create_missing_column_families = v;
}

void rocksdb_options_set_error_if_exists(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.error_if_exists = v;
}

void rocksdb_options_set_paranoid_checks(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.paranoid_checks = v;
}

void rocksdb_options_set_db_paths(rocksdb_options_t* opt,
                                  const rocksdb_dbpath_t** dbpath_values,
                                  size_t num_paths) {
  std::vector<DbPath> db_paths(num_paths);
  for (size_t i = 0; i < num_paths; ++i) {
    db_paths[i] = dbpath_values[i]->rep;
  }
  opt->rep.db_paths = db_paths;
}

void rocksdb_options_set_env(rocksdb_options_t* opt, rocksdb_env_t* env) {
  opt->rep.env = (env ? env->rep : nullptr);
}

void rocksdb_options_set_info_log(rocksdb_options_t* opt, rocksdb_logger_t* l) {
  if (l) {
    opt->rep.info_log = l->rep;
  }
}

void rocksdb_options_set_info_log_level(
    rocksdb_options_t* opt, int v) {
  opt->rep.info_log_level = static_cast<InfoLogLevel>(v);
}

void rocksdb_options_set_db_write_buffer_size(rocksdb_options_t* opt,
                                              size_t s) {
  opt->rep.db_write_buffer_size = s;
}

void rocksdb_options_set_write_buffer_size(rocksdb_options_t* opt, size_t s) {
  opt->rep.write_buffer_size = s;
}

void rocksdb_options_set_max_open_files(rocksdb_options_t* opt, int n) {
  opt->rep.max_open_files = n;
}

void rocksdb_options_set_max_file_opening_threads(rocksdb_options_t* opt, int n) {
  opt->rep.max_file_opening_threads = n;
}

void rocksdb_options_set_max_total_wal_size(rocksdb_options_t* opt, uint64_t n) {
  opt->rep.max_total_wal_size = n;
}

void rocksdb_options_set_target_file_size_base(
    rocksdb_options_t* opt, uint64_t n) {
  opt->rep.target_file_size_base = n;
}

void rocksdb_options_set_target_file_size_multiplier(
    rocksdb_options_t* opt, int n) {
  opt->rep.target_file_size_multiplier = n;
}

void rocksdb_options_set_max_bytes_for_level_base(
    rocksdb_options_t* opt, uint64_t n) {
  opt->rep.max_bytes_for_level_base = n;
}

void rocksdb_options_set_level_compaction_dynamic_level_bytes(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.level_compaction_dynamic_level_bytes = v;
}

void rocksdb_options_set_max_bytes_for_level_multiplier(rocksdb_options_t* opt,
                                                        double n) {
  opt->rep.max_bytes_for_level_multiplier = n;
}

void rocksdb_options_set_max_compaction_bytes(rocksdb_options_t* opt,
                                              uint64_t n) {
  opt->rep.max_compaction_bytes = n;
}

void rocksdb_options_set_max_bytes_for_level_multiplier_additional(
    rocksdb_options_t* opt, int* level_values, size_t num_levels) {
  opt->rep.max_bytes_for_level_multiplier_additional.resize(num_levels);
  for (size_t i = 0; i < num_levels; ++i) {
    opt->rep.max_bytes_for_level_multiplier_additional[i] = level_values[i];
  }
}

void rocksdb_options_enable_statistics(rocksdb_options_t* opt) {
  opt->rep.statistics = rocksdb::CreateDBStatistics();
}

void rocksdb_options_set_skip_stats_update_on_db_open(rocksdb_options_t* opt,
                                                      unsigned char val) {
  opt->rep.skip_stats_update_on_db_open = val;
}

void rocksdb_options_set_num_levels(rocksdb_options_t* opt, int n) {
  opt->rep.num_levels = n;
}

void rocksdb_options_set_level0_file_num_compaction_trigger(
    rocksdb_options_t* opt, int n) {
  opt->rep.level0_file_num_compaction_trigger = n;
}

void rocksdb_options_set_level0_slowdown_writes_trigger(
    rocksdb_options_t* opt, int n) {
  opt->rep.level0_slowdown_writes_trigger = n;
}

void rocksdb_options_set_level0_stop_writes_trigger(
    rocksdb_options_t* opt, int n) {
  opt->rep.level0_stop_writes_trigger = n;
}

void rocksdb_options_set_max_mem_compaction_level(rocksdb_options_t* /*opt*/,
                                                  int /*n*/) {}

void rocksdb_options_set_wal_recovery_mode(rocksdb_options_t* opt,int mode) {
  opt->rep.wal_recovery_mode = static_cast<WALRecoveryMode>(mode);
}

void rocksdb_options_set_compression(rocksdb_options_t* opt, int t) {
  opt->rep.compression = static_cast<CompressionType>(t);
}

void rocksdb_options_set_compression_per_level(rocksdb_options_t* opt,
                                               int* level_values,
                                               size_t num_levels) {
  opt->rep.compression_per_level.resize(num_levels);
  for (size_t i = 0; i < num_levels; ++i) {
    opt->rep.compression_per_level[i] =
      static_cast<CompressionType>(level_values[i]);
  }
}

void rocksdb_options_set_bottommost_compression_options(rocksdb_options_t* opt,
                                                        int w_bits, int level,
                                                        int strategy,
                                                        int max_dict_bytes,
                                                        bool enabled) {
  opt->rep.bottommost_compression_opts.window_bits = w_bits;
  opt->rep.bottommost_compression_opts.level = level;
  opt->rep.bottommost_compression_opts.strategy = strategy;
  opt->rep.bottommost_compression_opts.max_dict_bytes = max_dict_bytes;
  opt->rep.bottommost_compression_opts.enabled = enabled;
}

void rocksdb_options_set_compression_options(rocksdb_options_t* opt, int w_bits,
                                             int level, int strategy,
                                             int max_dict_bytes) {
  opt->rep.compression_opts.window_bits = w_bits;
  opt->rep.compression_opts.level = level;
  opt->rep.compression_opts.strategy = strategy;
  opt->rep.compression_opts.max_dict_bytes = max_dict_bytes;
}

void rocksdb_options_set_prefix_extractor(
    rocksdb_options_t* opt, rocksdb_slicetransform_t* prefix_extractor) {
  opt->rep.prefix_extractor.reset(prefix_extractor);
}

void rocksdb_options_set_use_fsync(
    rocksdb_options_t* opt, int use_fsync) {
  opt->rep.use_fsync = use_fsync;
}

void rocksdb_options_set_db_log_dir(
    rocksdb_options_t* opt, const char* db_log_dir) {
  opt->rep.db_log_dir = db_log_dir;
}

void rocksdb_options_set_wal_dir(
    rocksdb_options_t* opt, const char* v) {
  opt->rep.wal_dir = v;
}

void rocksdb_options_set_WAL_ttl_seconds(rocksdb_options_t* opt, uint64_t ttl) {
  opt->rep.WAL_ttl_seconds = ttl;
}

void rocksdb_options_set_WAL_size_limit_MB(
    rocksdb_options_t* opt, uint64_t limit) {
  opt->rep.WAL_size_limit_MB = limit;
}

void rocksdb_options_set_manifest_preallocation_size(
    rocksdb_options_t* opt, size_t v) {
  opt->rep.manifest_preallocation_size = v;
}

// noop
void rocksdb_options_set_purge_redundant_kvs_while_flush(
    rocksdb_options_t* /*opt*/, unsigned char /*v*/) {}

void rocksdb_options_set_use_direct_reads(rocksdb_options_t* opt,
                                          unsigned char v) {
  opt->rep.use_direct_reads = v;
}

void rocksdb_options_set_use_direct_io_for_flush_and_compaction(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.use_direct_io_for_flush_and_compaction = v;
}

void rocksdb_options_set_allow_mmap_reads(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.allow_mmap_reads = v;
}

void rocksdb_options_set_allow_mmap_writes(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.allow_mmap_writes = v;
}

void rocksdb_options_set_is_fd_close_on_exec(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.is_fd_close_on_exec = v;
}

void rocksdb_options_set_skip_log_error_on_recovery(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.skip_log_error_on_recovery = v;
}

void rocksdb_options_set_stats_dump_period_sec(
    rocksdb_options_t* opt, unsigned int v) {
  opt->rep.stats_dump_period_sec = v;
}

void rocksdb_options_set_advise_random_on_open(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.advise_random_on_open = v;
}

void rocksdb_options_set_access_hint_on_compaction_start(
    rocksdb_options_t* opt, int v) {
  switch(v) {
    case 0:
      opt->rep.access_hint_on_compaction_start = rocksdb::Options::NONE;
      break;
    case 1:
      opt->rep.access_hint_on_compaction_start = rocksdb::Options::NORMAL;
      break;
    case 2:
      opt->rep.access_hint_on_compaction_start = rocksdb::Options::SEQUENTIAL;
      break;
    case 3:
      opt->rep.access_hint_on_compaction_start = rocksdb::Options::WILLNEED;
      break;
  }
}

void rocksdb_options_set_use_adaptive_mutex(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.use_adaptive_mutex = v;
}

void rocksdb_options_set_wal_bytes_per_sync(
    rocksdb_options_t* opt, uint64_t v) {
  opt->rep.wal_bytes_per_sync = v;
}

void rocksdb_options_set_bytes_per_sync(
    rocksdb_options_t* opt, uint64_t v) {
  opt->rep.bytes_per_sync = v;
}

void rocksdb_options_set_writable_file_max_buffer_size(rocksdb_options_t* opt,
                                                       uint64_t v) {
  opt->rep.writable_file_max_buffer_size = static_cast<size_t>(v);
}

void rocksdb_options_set_allow_concurrent_memtable_write(rocksdb_options_t* opt,
                                                         unsigned char v) {
  opt->rep.allow_concurrent_memtable_write = v;
}

void rocksdb_options_set_enable_write_thread_adaptive_yield(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.enable_write_thread_adaptive_yield = v;
}

void rocksdb_options_set_max_sequential_skip_in_iterations(
    rocksdb_options_t* opt, uint64_t v) {
  opt->rep.max_sequential_skip_in_iterations = v;
}

void rocksdb_options_set_max_write_buffer_number(rocksdb_options_t* opt, int n) {
  opt->rep.max_write_buffer_number = n;
}

void rocksdb_options_set_min_write_buffer_number_to_merge(rocksdb_options_t* opt, int n) {
  opt->rep.min_write_buffer_number_to_merge = n;
}

void rocksdb_options_set_max_write_buffer_number_to_maintain(
    rocksdb_options_t* opt, int n) {
  opt->rep.max_write_buffer_number_to_maintain = n;
}

void rocksdb_options_set_enable_pipelined_write(rocksdb_options_t* opt,
                                                unsigned char v) {
  opt->rep.enable_pipelined_write = v;
}

void rocksdb_options_set_max_subcompactions(rocksdb_options_t* opt,
                                            uint32_t n) {
  opt->rep.max_subcompactions = n;
}

void rocksdb_options_set_max_background_jobs(rocksdb_options_t* opt, int n) {
  opt->rep.max_background_jobs = n;
}

void rocksdb_options_set_max_background_compactions(rocksdb_options_t* opt, int n) {
  opt->rep.max_background_compactions = n;
}

void rocksdb_options_set_base_background_compactions(rocksdb_options_t* opt,
                                                     int n) {
  opt->rep.base_background_compactions = n;
}

void rocksdb_options_set_max_background_flushes(rocksdb_options_t* opt, int n) {
  opt->rep.max_background_flushes = n;
}

void rocksdb_options_set_max_log_file_size(rocksdb_options_t* opt, size_t v) {
  opt->rep.max_log_file_size = v;
}

void rocksdb_options_set_log_file_time_to_roll(rocksdb_options_t* opt, size_t v) {
  opt->rep.log_file_time_to_roll = v;
}

void rocksdb_options_set_keep_log_file_num(rocksdb_options_t* opt, size_t v) {
  opt->rep.keep_log_file_num = v;
}

void rocksdb_options_set_recycle_log_file_num(rocksdb_options_t* opt,
                                              size_t v) {
  opt->rep.recycle_log_file_num = v;
}

void rocksdb_options_set_soft_rate_limit(rocksdb_options_t* opt, double v) {
  opt->rep.soft_rate_limit = v;
}

void rocksdb_options_set_hard_rate_limit(rocksdb_options_t* opt, double v) {
  opt->rep.hard_rate_limit = v;
}

void rocksdb_options_set_soft_pending_compaction_bytes_limit(rocksdb_options_t* opt, size_t v) {
  opt->rep.soft_pending_compaction_bytes_limit = v;
}

void rocksdb_options_set_hard_pending_compaction_bytes_limit(rocksdb_options_t* opt, size_t v) {
  opt->rep.hard_pending_compaction_bytes_limit = v;
}

void rocksdb_options_set_rate_limit_delay_max_milliseconds(
    rocksdb_options_t* opt, unsigned int v) {
  opt->rep.rate_limit_delay_max_milliseconds = v;
}

void rocksdb_options_set_max_manifest_file_size(
    rocksdb_options_t* opt, size_t v) {
  opt->rep.max_manifest_file_size = v;
}

void rocksdb_options_set_table_cache_numshardbits(
    rocksdb_options_t* opt, int v) {
  opt->rep.table_cache_numshardbits = v;
}

void rocksdb_options_set_table_cache_remove_scan_count_limit(
    rocksdb_options_t* /*opt*/, int /*v*/) {
  // this option is deprecated
}

void rocksdb_options_set_arena_block_size(
    rocksdb_options_t* opt, size_t v) {
  opt->rep.arena_block_size = v;
}

void rocksdb_options_set_disable_auto_compactions(rocksdb_options_t* opt, int disable) {
  opt->rep.disable_auto_compactions = disable;
}

void rocksdb_options_set_optimize_filters_for_hits(rocksdb_options_t* opt, int v) {
  opt->rep.optimize_filters_for_hits = v;
}

void rocksdb_options_set_delete_obsolete_files_period_micros(
    rocksdb_options_t* opt, uint64_t v) {
  opt->rep.delete_obsolete_files_period_micros = v;
}

void rocksdb_options_prepare_for_bulk_load(rocksdb_options_t* opt) {
  opt->rep.PrepareForBulkLoad();
}

void rocksdb_options_set_memtable_vector_rep(rocksdb_options_t *opt) {
  opt->rep.memtable_factory.reset(new rocksdb::VectorRepFactory);
}

void rocksdb_options_set_memtable_prefix_bloom_size_ratio(
    rocksdb_options_t* opt, double v) {
  opt->rep.memtable_prefix_bloom_size_ratio = v;
}

void rocksdb_options_set_memtable_huge_page_size(rocksdb_options_t* opt,
                                                 size_t v) {
  opt->rep.memtable_huge_page_size = v;
}

void rocksdb_options_set_hash_skip_list_rep(
    rocksdb_options_t *opt, size_t bucket_count,
    int32_t skiplist_height, int32_t skiplist_branching_factor) {
  rocksdb::MemTableRepFactory* factory = rocksdb::NewHashSkipListRepFactory(
      bucket_count, skiplist_height, skiplist_branching_factor);
  opt->rep.memtable_factory.reset(factory);
}

void rocksdb_options_set_hash_link_list_rep(
    rocksdb_options_t *opt, size_t bucket_count) {
  opt->rep.memtable_factory.reset(rocksdb::NewHashLinkListRepFactory(bucket_count));
}

void rocksdb_options_set_plain_table_factory(
    rocksdb_options_t *opt, uint32_t user_key_len, int bloom_bits_per_key,
    double hash_table_ratio, size_t index_sparseness) {
  rocksdb::PlainTableOptions options;
  options.user_key_len = user_key_len;
  options.bloom_bits_per_key = bloom_bits_per_key;
  options.hash_table_ratio = hash_table_ratio;
  options.index_sparseness = index_sparseness;

  rocksdb::TableFactory* factory = rocksdb::NewPlainTableFactory(options);
  opt->rep.table_factory.reset(factory);
}

void rocksdb_options_set_max_successive_merges(
    rocksdb_options_t* opt, size_t v) {
  opt->rep.max_successive_merges = v;
}

void rocksdb_options_set_bloom_locality(
    rocksdb_options_t* opt, uint32_t v) {
  opt->rep.bloom_locality = v;
}

void rocksdb_options_set_inplace_update_support(
    rocksdb_options_t* opt, unsigned char v) {
  opt->rep.inplace_update_support = v;
}

void rocksdb_options_set_inplace_update_num_locks(
    rocksdb_options_t* opt, size_t v) {
  opt->rep.inplace_update_num_locks = v;
}

void rocksdb_options_set_report_bg_io_stats(
    rocksdb_options_t* opt, int v) {
  opt->rep.report_bg_io_stats = v;
}

void rocksdb_options_set_compaction_style(rocksdb_options_t *opt, int style) {
  opt->rep.compaction_style = static_cast<rocksdb::CompactionStyle>(style);
}

void rocksdb_options_set_universal_compaction_options(rocksdb_options_t *opt, rocksdb_universal_compaction_options_t *uco) {
  opt->rep.compaction_options_universal = *(uco->rep);
}

void rocksdb_options_set_fifo_compaction_options(
    rocksdb_options_t* opt,
    rocksdb_fifo_compaction_options_t* fifo) {
  opt->rep.compaction_options_fifo = fifo->rep;
}

char *rocksdb_options_statistics_get_string(rocksdb_options_t *opt) {
  rocksdb::Statistics *statistics = opt->rep.statistics.get();
  if (statistics) {
    return strdup(statistics->ToString().c_str());
  }
  return nullptr;
}

void rocksdb_options_set_ratelimiter(rocksdb_options_t *opt, rocksdb_ratelimiter_t *limiter) {
  if (limiter) {
    opt->rep.rate_limiter = limiter->rep;
  }
}

rocksdb_ratelimiter_t* rocksdb_ratelimiter_create(
    int64_t rate_bytes_per_sec,
    int64_t refill_period_us,
    int32_t fairness) {
  rocksdb_ratelimiter_t* rate_limiter = new rocksdb_ratelimiter_t;
  rate_limiter->rep.reset(
               NewGenericRateLimiter(rate_bytes_per_sec,
                                     refill_period_us, fairness));
  return rate_limiter;
}

void rocksdb_ratelimiter_destroy(rocksdb_ratelimiter_t *limiter) {
  delete limiter;
}

void rocksdb_set_perf_level(int v) {
  PerfLevel level = static_cast<PerfLevel>(v);
  SetPerfLevel(level);
}

rocksdb_perfcontext_t* rocksdb_perfcontext_create() {
  rocksdb_perfcontext_t* context = new rocksdb_perfcontext_t;
  context->rep = rocksdb::get_perf_context();
  return context;
}

void rocksdb_perfcontext_reset(rocksdb_perfcontext_t* context) {
  context->rep->Reset();
}

char* rocksdb_perfcontext_report(rocksdb_perfcontext_t* context,
    unsigned char exclude_zero_counters) {
  return strdup(context->rep->ToString(exclude_zero_counters).c_str());
}

uint64_t rocksdb_perfcontext_metric(rocksdb_perfcontext_t* context,
    int metric) {
  PerfContext* rep = context->rep;
  switch (metric) {
    case rocksdb_user_key_comparison_count:
      return rep->user_key_comparison_count;
    case rocksdb_block_cache_hit_count:
      return rep->block_cache_hit_count;
    case rocksdb_block_read_count:
      return rep->block_read_count;
    case rocksdb_block_read_byte:
      return rep->block_read_byte;
    case rocksdb_block_read_time:
      return rep->block_read_time;
    case rocksdb_block_checksum_time:
      return rep->block_checksum_time;
    case rocksdb_block_decompress_time:
      return rep->block_decompress_time;
    case rocksdb_get_read_bytes:
      return rep->get_read_bytes;
    case rocksdb_multiget_read_bytes:
      return rep->multiget_read_bytes;
    case rocksdb_iter_read_bytes:
      return rep->iter_read_bytes;
    case rocksdb_internal_key_skipped_count:
      return rep->internal_key_skipped_count;
    case rocksdb_internal_delete_skipped_count:
      return rep->internal_delete_skipped_count;
    case rocksdb_internal_recent_skipped_count:
      return rep->internal_recent_skipped_count;
    case rocksdb_internal_merge_count:
      return rep->internal_merge_count;
    case rocksdb_get_snapshot_time:
      return rep->get_snapshot_time;
    case rocksdb_get_from_memtable_time:
      return rep->get_from_memtable_time;
    case rocksdb_get_from_memtable_count:
      return rep->get_from_memtable_count;
    case rocksdb_get_post_process_time:
      return rep->get_post_process_time;
    case rocksdb_get_from_output_files_time:
      return rep->get_from_output_files_time;
    case rocksdb_seek_on_memtable_time:
      return rep->seek_on_memtable_time;
    case rocksdb_seek_on_memtable_count:
      return rep->seek_on_memtable_count;
    case rocksdb_next_on_memtable_count:
      return rep->next_on_memtable_count;
    case rocksdb_prev_on_memtable_count:
      return rep->prev_on_memtable_count;
    case rocksdb_seek_child_seek_time:
      return rep->seek_child_seek_time;
    case rocksdb_seek_child_seek_count:
      return rep->seek_child_seek_count;
    case rocksdb_seek_min_heap_time:
      return rep->seek_min_heap_time;
    case rocksdb_seek_max_heap_time:
      return rep->seek_max_heap_time;
    case rocksdb_seek_internal_seek_time:
      return rep->seek_internal_seek_time;
    case rocksdb_find_next_user_entry_time:
      return rep->find_next_user_entry_time;
    case rocksdb_write_wal_time:
      return rep->write_wal_time;
    case rocksdb_write_memtable_time:
      return rep->write_memtable_time;
    case rocksdb_write_delay_time:
      return rep->write_delay_time;
    case rocksdb_write_pre_and_post_process_time:
      return rep->write_pre_and_post_process_time;
    case rocksdb_db_mutex_lock_nanos:
      return rep->db_mutex_lock_nanos;
    case rocksdb_db_condition_wait_nanos:
      return rep->db_condition_wait_nanos;
    case rocksdb_merge_operator_time_nanos:
      return rep->merge_operator_time_nanos;
    case rocksdb_read_index_block_nanos:
      return rep->read_index_block_nanos;
    case rocksdb_read_filter_block_nanos:
      return rep->read_filter_block_nanos;
    case rocksdb_new_table_block_iter_nanos:
      return rep->new_table_block_iter_nanos;
    case rocksdb_new_table_iterator_nanos:
      return rep->new_table_iterator_nanos;
    case rocksdb_block_seek_nanos:
      return rep->block_seek_nanos;
    case rocksdb_find_table_nanos:
      return rep->find_table_nanos;
    case rocksdb_bloom_memtable_hit_count:
      return rep->bloom_memtable_hit_count;
    case rocksdb_bloom_memtable_miss_count:
      return rep->bloom_memtable_miss_count;
    case rocksdb_bloom_sst_hit_count:
      return rep->bloom_sst_hit_count;
    case rocksdb_bloom_sst_miss_count:
      return rep->bloom_sst_miss_count;
    case rocksdb_key_lock_wait_time:
      return rep->key_lock_wait_time;
    case rocksdb_key_lock_wait_count:
      return rep->key_lock_wait_count;
    case rocksdb_env_new_sequential_file_nanos:
      return rep->env_new_sequential_file_nanos;
    case rocksdb_env_new_random_access_file_nanos:
      return rep->env_new_random_access_file_nanos;
    case rocksdb_env_new_writable_file_nanos:
      return rep->env_new_writable_file_nanos;
    case rocksdb_env_reuse_writable_file_nanos:
      return rep->env_reuse_writable_file_nanos;
    case rocksdb_env_new_random_rw_file_nanos:
      return rep->env_new_random_rw_file_nanos;
    case rocksdb_env_new_directory_nanos:
      return rep->env_new_directory_nanos;
    case rocksdb_env_file_exists_nanos:
      return rep->env_file_exists_nanos;
    case rocksdb_env_get_children_nanos:
      return rep->env_get_children_nanos;
    case rocksdb_env_get_children_file_attributes_nanos:
      return rep->env_get_children_file_attributes_nanos;
    case rocksdb_env_delete_file_nanos:
      return rep->env_delete_file_nanos;
    case rocksdb_env_create_dir_nanos:
      return rep->env_create_dir_nanos;
    case rocksdb_env_create_dir_if_missing_nanos:
      return rep->env_create_dir_if_missing_nanos;
    case rocksdb_env_delete_dir_nanos:
      return rep->env_delete_dir_nanos;
    case rocksdb_env_get_file_size_nanos:
      return rep->env_get_file_size_nanos;
    case rocksdb_env_get_file_modification_time_nanos:
      return rep->env_get_file_modification_time_nanos;
    case rocksdb_env_rename_file_nanos:
      return rep->env_rename_file_nanos;
    case rocksdb_env_link_file_nanos:
      return rep->env_link_file_nanos;
    case rocksdb_env_lock_file_nanos:
      return rep->env_lock_file_nanos;
    case rocksdb_env_unlock_file_nanos:
      return rep->env_unlock_file_nanos;
    case rocksdb_env_new_logger_nanos:
      return rep->env_new_logger_nanos;
    default:
      break;
  }
  return 0;
}

void rocksdb_perfcontext_destroy(rocksdb_perfcontext_t* context) {
  delete context;
}

/*
TODO:
DB::OpenForReadOnly
DB::KeyMayExist
DB::GetOptions
DB::GetSortedWalFiles
DB::GetLatestSequenceNumber
DB::GetUpdatesSince
DB::GetDbIdentity
DB::RunManualCompaction
custom cache
table_properties_collectors
*/

rocksdb_compactionfilter_t* rocksdb_compactionfilter_create(
    void* state,
    void (*destructor)(void*),
    unsigned char (*filter)(
        void*,
        int level,
        const char* key, size_t key_length,
        const char* existing_value, size_t value_length,
        char** new_value, size_t *new_value_length,
        unsigned char* value_changed),
    const char* (*name)(void*)) {
  rocksdb_compactionfilter_t* result = new rocksdb_compactionfilter_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->filter_ = filter;
  result->ignore_snapshots_ = true;
  result->name_ = name;
  return result;
}

void rocksdb_compactionfilter_set_ignore_snapshots(
  rocksdb_compactionfilter_t* filter,
  unsigned char whether_ignore) {
  filter->ignore_snapshots_ = whether_ignore;
}

void rocksdb_compactionfilter_destroy(rocksdb_compactionfilter_t* filter) {
  delete filter;
}

unsigned char rocksdb_compactionfiltercontext_is_full_compaction(
    rocksdb_compactionfiltercontext_t* context) {
  return context->rep.is_full_compaction;
}

unsigned char rocksdb_compactionfiltercontext_is_manual_compaction(
    rocksdb_compactionfiltercontext_t* context) {
  return context->rep.is_manual_compaction;
}

rocksdb_compactionfilterfactory_t* rocksdb_compactionfilterfactory_create(
    void* state, void (*destructor)(void*),
    rocksdb_compactionfilter_t* (*create_compaction_filter)(
        void*, rocksdb_compactionfiltercontext_t* context),
    const char* (*name)(void*)) {
  rocksdb_compactionfilterfactory_t* result =
      new rocksdb_compactionfilterfactory_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->create_compaction_filter_ = create_compaction_filter;
  result->name_ = name;
  return result;
}

void rocksdb_compactionfilterfactory_destroy(
    rocksdb_compactionfilterfactory_t* factory) {
  delete factory;
}

rocksdb_comparator_t* rocksdb_comparator_create(
    void* state,
    void (*destructor)(void*),
    int (*compare)(
        void*,
        const char* a, size_t alen,
        const char* b, size_t blen),
    const char* (*name)(void*)) {
  rocksdb_comparator_t* result = new rocksdb_comparator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->compare_ = compare;
  result->name_ = name;
  return result;
}

void rocksdb_comparator_destroy(rocksdb_comparator_t* cmp) {
  delete cmp;
}

rocksdb_filterpolicy_t* rocksdb_filterpolicy_create(
    void* state,
    void (*destructor)(void*),
    char* (*create_filter)(
        void*,
        const char* const* key_array, const size_t* key_length_array,
        int num_keys,
        size_t* filter_length),
    unsigned char (*key_may_match)(
        void*,
        const char* key, size_t length,
        const char* filter, size_t filter_length),
    void (*delete_filter)(
        void*,
        const char* filter, size_t filter_length),
    const char* (*name)(void*)) {
  rocksdb_filterpolicy_t* result = new rocksdb_filterpolicy_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->create_ = create_filter;
  result->key_match_ = key_may_match;
  result->delete_filter_ = delete_filter;
  result->name_ = name;
  return result;
}

void rocksdb_filterpolicy_destroy(rocksdb_filterpolicy_t* filter) {
  delete filter;
}

rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_bloom_format(int bits_per_key, bool original_format) {
  // Make a rocksdb_filterpolicy_t, but override all of its methods so
  // they delegate to a NewBloomFilterPolicy() instead of user
  // supplied C functions.
  struct Wrapper : public rocksdb_filterpolicy_t {
    const FilterPolicy* rep_;
    ~Wrapper() override { delete rep_; }
    const char* Name() const override { return rep_->Name(); }
    void CreateFilter(const Slice* keys, int n,
                      std::string* dst) const override {
      return rep_->CreateFilter(keys, n, dst);
    }
    bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
      return rep_->KeyMayMatch(key, filter);
    }
    static void DoNothing(void*) { }
  };
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = NewBloomFilterPolicy(bits_per_key, original_format);
  wrapper->state_ = nullptr;
  wrapper->delete_filter_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_bloom_full(int bits_per_key) {
  return rocksdb_filterpolicy_create_bloom_format(bits_per_key, false);
}

rocksdb_filterpolicy_t* rocksdb_filterpolicy_create_bloom(int bits_per_key) {
  return rocksdb_filterpolicy_create_bloom_format(bits_per_key, true);
}

rocksdb_mergeoperator_t* rocksdb_mergeoperator_create(
    void* state, void (*destructor)(void*),
    char* (*full_merge)(void*, const char* key, size_t key_length,
                        const char* existing_value,
                        size_t existing_value_length,
                        const char* const* operands_list,
                        const size_t* operands_list_length, int num_operands,
                        unsigned char* success, size_t* new_value_length),
    char* (*partial_merge)(void*, const char* key, size_t key_length,
                           const char* const* operands_list,
                           const size_t* operands_list_length, int num_operands,
                           unsigned char* success, size_t* new_value_length),
    void (*delete_value)(void*, const char* value, size_t value_length),
    const char* (*name)(void*)) {
  rocksdb_mergeoperator_t* result = new rocksdb_mergeoperator_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->full_merge_ = full_merge;
  result->partial_merge_ = partial_merge;
  result->delete_value_ = delete_value;
  result->name_ = name;
  return result;
}

void rocksdb_mergeoperator_destroy(rocksdb_mergeoperator_t* merge_operator) {
  delete merge_operator;
}

rocksdb_readoptions_t* rocksdb_readoptions_create() {
  return new rocksdb_readoptions_t;
}

void rocksdb_readoptions_destroy(rocksdb_readoptions_t* opt) {
  delete opt;
}

void rocksdb_readoptions_set_verify_checksums(
    rocksdb_readoptions_t* opt,
    unsigned char v) {
  opt->rep.verify_checksums = v;
}

void rocksdb_readoptions_set_fill_cache(
    rocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.fill_cache = v;
}

void rocksdb_readoptions_set_snapshot(
    rocksdb_readoptions_t* opt,
    const rocksdb_snapshot_t* snap) {
  opt->rep.snapshot = (snap ? snap->rep : nullptr);
}

void rocksdb_readoptions_set_iterate_upper_bound(
    rocksdb_readoptions_t* opt,
    const char* key, size_t keylen) {
  if (key == nullptr) {
    opt->upper_bound = Slice();
    opt->rep.iterate_upper_bound = nullptr;

  } else {
    opt->upper_bound = Slice(key, keylen);
    opt->rep.iterate_upper_bound = &opt->upper_bound;
  }
}

void rocksdb_readoptions_set_iterate_lower_bound(
    rocksdb_readoptions_t *opt,
    const char* key, size_t keylen) {
  if (key == nullptr) {
    opt->lower_bound = Slice();
    opt->rep.iterate_lower_bound = nullptr;
  } else {
    opt->lower_bound = Slice(key, keylen);
    opt->rep.iterate_lower_bound = &opt->lower_bound;
  }
}

void rocksdb_readoptions_set_read_tier(
    rocksdb_readoptions_t* opt, int v) {
  opt->rep.read_tier = static_cast<rocksdb::ReadTier>(v);
}

void rocksdb_readoptions_set_tailing(
    rocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.tailing = v;
}

void rocksdb_readoptions_set_managed(
    rocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.managed = v;
}

void rocksdb_readoptions_set_readahead_size(
    rocksdb_readoptions_t* opt, size_t v) {
  opt->rep.readahead_size = v;
}

void rocksdb_readoptions_set_prefix_same_as_start(
    rocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.prefix_same_as_start = v;
}

void rocksdb_readoptions_set_pin_data(rocksdb_readoptions_t* opt,
                                      unsigned char v) {
  opt->rep.pin_data = v;
}

void rocksdb_readoptions_set_total_order_seek(rocksdb_readoptions_t* opt,
                                              unsigned char v) {
  opt->rep.total_order_seek = v;
}

void rocksdb_readoptions_set_max_skippable_internal_keys(
    rocksdb_readoptions_t* opt,
    uint64_t v) {
  opt->rep.max_skippable_internal_keys = v;
}

void rocksdb_readoptions_set_background_purge_on_iterator_cleanup(
    rocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.background_purge_on_iterator_cleanup = v;
}

void rocksdb_readoptions_set_ignore_range_deletions(
    rocksdb_readoptions_t* opt, unsigned char v) {
  opt->rep.ignore_range_deletions = v;
}

rocksdb_writeoptions_t* rocksdb_writeoptions_create() {
  return new rocksdb_writeoptions_t;
}

void rocksdb_writeoptions_destroy(rocksdb_writeoptions_t* opt) {
  delete opt;
}

void rocksdb_writeoptions_set_sync(
    rocksdb_writeoptions_t* opt, unsigned char v) {
  opt->rep.sync = v;
}

void rocksdb_writeoptions_disable_WAL(rocksdb_writeoptions_t* opt, int disable) {
  opt->rep.disableWAL = disable;
}

void rocksdb_writeoptions_set_ignore_missing_column_families(
    rocksdb_writeoptions_t* opt,
    unsigned char v) {
  opt->rep.ignore_missing_column_families = v;
}

void rocksdb_writeoptions_set_no_slowdown(
    rocksdb_writeoptions_t* opt,
    unsigned char v) {
  opt->rep.no_slowdown = v;
}

void rocksdb_writeoptions_set_low_pri(
    rocksdb_writeoptions_t* opt,
    unsigned char v) {
  opt->rep.low_pri = v;
}

rocksdb_compactoptions_t* rocksdb_compactoptions_create() {
  return new rocksdb_compactoptions_t;
}

void rocksdb_compactoptions_destroy(rocksdb_compactoptions_t* opt) {
  delete opt;
}

void rocksdb_compactoptions_set_bottommost_level_compaction(
    rocksdb_compactoptions_t* opt, unsigned char v) {
  opt->rep.bottommost_level_compaction = static_cast<BottommostLevelCompaction>(v);
}

void rocksdb_compactoptions_set_exclusive_manual_compaction(
    rocksdb_compactoptions_t* opt, unsigned char v) {
  opt->rep.exclusive_manual_compaction = v;
}

void rocksdb_compactoptions_set_change_level(rocksdb_compactoptions_t* opt,
                                             unsigned char v) {
  opt->rep.change_level = v;
}

void rocksdb_compactoptions_set_target_level(rocksdb_compactoptions_t* opt,
                                             int n) {
  opt->rep.target_level = n;
}

rocksdb_flushoptions_t* rocksdb_flushoptions_create() {
  return new rocksdb_flushoptions_t;
}

void rocksdb_flushoptions_destroy(rocksdb_flushoptions_t* opt) {
  delete opt;
}

void rocksdb_flushoptions_set_wait(
    rocksdb_flushoptions_t* opt, unsigned char v) {
  opt->rep.wait = v;
}

rocksdb_cache_t* rocksdb_cache_create_lru(size_t capacity) {
  rocksdb_cache_t* c = new rocksdb_cache_t;
  c->rep = NewLRUCache(capacity);
  return c;
}

void rocksdb_cache_destroy(rocksdb_cache_t* cache) {
  delete cache;
}

void rocksdb_cache_set_capacity(rocksdb_cache_t* cache, size_t capacity) {
  cache->rep->SetCapacity(capacity);
}

size_t rocksdb_cache_get_usage(rocksdb_cache_t* cache) {
  return cache->rep->GetUsage();
}

size_t rocksdb_cache_get_pinned_usage(rocksdb_cache_t* cache) {
  return cache->rep->GetPinnedUsage();
}

rocksdb_dbpath_t* rocksdb_dbpath_create(const char* path, uint64_t target_size) {
  rocksdb_dbpath_t* result = new rocksdb_dbpath_t;
  result->rep.path = std::string(path);
  result->rep.target_size = target_size;
  return result;
}

void rocksdb_dbpath_destroy(rocksdb_dbpath_t* dbpath) {
  delete dbpath;
}

rocksdb_env_t* rocksdb_create_default_env() {
  rocksdb_env_t* result = new rocksdb_env_t;
  result->rep = Env::Default();
  result->is_default = true;
  return result;
}

rocksdb_env_t* rocksdb_create_mem_env() {
  rocksdb_env_t* result = new rocksdb_env_t;
  result->rep = rocksdb::NewMemEnv(Env::Default());
  result->is_default = false;
  return result;
}

void rocksdb_env_set_background_threads(rocksdb_env_t* env, int n) {
  env->rep->SetBackgroundThreads(n);
}

void rocksdb_env_set_high_priority_background_threads(rocksdb_env_t* env, int n) {
  env->rep->SetBackgroundThreads(n, Env::HIGH);
}

void rocksdb_env_join_all_threads(rocksdb_env_t* env) {
  env->rep->WaitForJoin();
}

void rocksdb_env_destroy(rocksdb_env_t* env) {
  if (!env->is_default) delete env->rep;
  delete env;
}

rocksdb_envoptions_t* rocksdb_envoptions_create() {
  rocksdb_envoptions_t* opt = new rocksdb_envoptions_t;
  return opt;
}

void rocksdb_envoptions_destroy(rocksdb_envoptions_t* opt) { delete opt; }

rocksdb_sstfilewriter_t* rocksdb_sstfilewriter_create(
    const rocksdb_envoptions_t* env, const rocksdb_options_t* io_options) {
  rocksdb_sstfilewriter_t* writer = new rocksdb_sstfilewriter_t;
  writer->rep = new SstFileWriter(env->rep, io_options->rep);
  return writer;
}

rocksdb_sstfilewriter_t* rocksdb_sstfilewriter_create_with_comparator(
    const rocksdb_envoptions_t* env, const rocksdb_options_t* io_options,
    const rocksdb_comparator_t* /*comparator*/) {
  rocksdb_sstfilewriter_t* writer = new rocksdb_sstfilewriter_t;
  writer->rep = new SstFileWriter(env->rep, io_options->rep);
  return writer;
}

void rocksdb_sstfilewriter_open(rocksdb_sstfilewriter_t* writer,
                                const char* name, char** errptr) {
  SaveError(errptr, writer->rep->Open(std::string(name)));
}

void rocksdb_sstfilewriter_add(rocksdb_sstfilewriter_t* writer, const char* key,
                               size_t keylen, const char* val, size_t vallen,
                               char** errptr) {
  SaveError(errptr, writer->rep->Put(Slice(key, keylen), Slice(val, vallen)));
}

void rocksdb_sstfilewriter_put(rocksdb_sstfilewriter_t* writer, const char* key,
                               size_t keylen, const char* val, size_t vallen,
                               char** errptr) {
  SaveError(errptr, writer->rep->Put(Slice(key, keylen), Slice(val, vallen)));
}

void rocksdb_sstfilewriter_merge(rocksdb_sstfilewriter_t* writer,
                                 const char* key, size_t keylen,
                                 const char* val, size_t vallen,
                                 char** errptr) {
  SaveError(errptr, writer->rep->Merge(Slice(key, keylen), Slice(val, vallen)));
}

void rocksdb_sstfilewriter_delete(rocksdb_sstfilewriter_t* writer,
                                  const char* key, size_t keylen,
                                  char** errptr) {
  SaveError(errptr, writer->rep->Delete(Slice(key, keylen)));
}

void rocksdb_sstfilewriter_finish(rocksdb_sstfilewriter_t* writer,
                                  char** errptr) {
  SaveError(errptr, writer->rep->Finish(nullptr));
}

void rocksdb_sstfilewriter_file_size(rocksdb_sstfilewriter_t* writer,
                                  uint64_t* file_size) {
  *file_size = writer->rep->FileSize();
}

void rocksdb_sstfilewriter_destroy(rocksdb_sstfilewriter_t* writer) {
  delete writer->rep;
  delete writer;
}

rocksdb_ingestexternalfileoptions_t*
rocksdb_ingestexternalfileoptions_create() {
  rocksdb_ingestexternalfileoptions_t* opt =
      new rocksdb_ingestexternalfileoptions_t;
  return opt;
}

void rocksdb_ingestexternalfileoptions_set_move_files(
    rocksdb_ingestexternalfileoptions_t* opt, unsigned char move_files) {
  opt->rep.move_files = move_files;
}

void rocksdb_ingestexternalfileoptions_set_snapshot_consistency(
    rocksdb_ingestexternalfileoptions_t* opt,
    unsigned char snapshot_consistency) {
  opt->rep.snapshot_consistency = snapshot_consistency;
}

void rocksdb_ingestexternalfileoptions_set_allow_global_seqno(
    rocksdb_ingestexternalfileoptions_t* opt,
    unsigned char allow_global_seqno) {
  opt->rep.allow_global_seqno = allow_global_seqno;
}

void rocksdb_ingestexternalfileoptions_set_allow_blocking_flush(
    rocksdb_ingestexternalfileoptions_t* opt,
    unsigned char allow_blocking_flush) {
  opt->rep.allow_blocking_flush = allow_blocking_flush;
}

void rocksdb_ingestexternalfileoptions_set_ingest_behind(
    rocksdb_ingestexternalfileoptions_t* opt,
    unsigned char ingest_behind) {
  opt->rep.ingest_behind = ingest_behind;
}

void rocksdb_ingestexternalfileoptions_destroy(
    rocksdb_ingestexternalfileoptions_t* opt) {
  delete opt;
}

void rocksdb_ingest_external_file(
    rocksdb_t* db, const char* const* file_list, const size_t list_len,
    const rocksdb_ingestexternalfileoptions_t* opt, char** errptr) {
  std::vector<std::string> files(list_len);
  for (size_t i = 0; i < list_len; ++i) {
    files[i] = std::string(file_list[i]);
  }
  SaveError(errptr, db->rep->IngestExternalFile(files, opt->rep));
}

void rocksdb_ingest_external_file_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* handle,
    const char* const* file_list, const size_t list_len,
    const rocksdb_ingestexternalfileoptions_t* opt, char** errptr) {
  std::vector<std::string> files(list_len);
  for (size_t i = 0; i < list_len; ++i) {
    files[i] = std::string(file_list[i]);
  }
  SaveError(errptr, db->rep->IngestExternalFile(handle->rep, files, opt->rep));
}

rocksdb_slicetransform_t* rocksdb_slicetransform_create(
    void* state,
    void (*destructor)(void*),
    char* (*transform)(
        void*,
        const char* key, size_t length,
        size_t* dst_length),
    unsigned char (*in_domain)(
        void*,
        const char* key, size_t length),
    unsigned char (*in_range)(
        void*,
        const char* key, size_t length),
    const char* (*name)(void*)) {
  rocksdb_slicetransform_t* result = new rocksdb_slicetransform_t;
  result->state_ = state;
  result->destructor_ = destructor;
  result->transform_ = transform;
  result->in_domain_ = in_domain;
  result->in_range_ = in_range;
  result->name_ = name;
  return result;
}

void rocksdb_slicetransform_destroy(rocksdb_slicetransform_t* st) {
  delete st;
}

struct Wrapper : public rocksdb_slicetransform_t {
  const SliceTransform* rep_;
  ~Wrapper() override { delete rep_; }
  const char* Name() const override { return rep_->Name(); }
  Slice Transform(const Slice& src) const override {
    return rep_->Transform(src);
  }
  bool InDomain(const Slice& src) const override {
    return rep_->InDomain(src);
  }
  bool InRange(const Slice& src) const override { return rep_->InRange(src); }
  static void DoNothing(void*) { }
};

rocksdb_slicetransform_t* rocksdb_slicetransform_create_fixed_prefix(size_t prefixLen) {
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = rocksdb::NewFixedPrefixTransform(prefixLen);
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

rocksdb_slicetransform_t* rocksdb_slicetransform_create_noop() {
  Wrapper* wrapper = new Wrapper;
  wrapper->rep_ = rocksdb::NewNoopTransform();
  wrapper->state_ = nullptr;
  wrapper->destructor_ = &Wrapper::DoNothing;
  return wrapper;
}

rocksdb_universal_compaction_options_t* rocksdb_universal_compaction_options_create() {
  rocksdb_universal_compaction_options_t* result = new rocksdb_universal_compaction_options_t;
  result->rep = new rocksdb::CompactionOptionsUniversal;
  return result;
}

void rocksdb_universal_compaction_options_set_size_ratio(
  rocksdb_universal_compaction_options_t* uco, int ratio) {
  uco->rep->size_ratio = ratio;
}

void rocksdb_universal_compaction_options_set_min_merge_width(
  rocksdb_universal_compaction_options_t* uco, int w) {
  uco->rep->min_merge_width = w;
}

void rocksdb_universal_compaction_options_set_max_merge_width(
  rocksdb_universal_compaction_options_t* uco, int w) {
  uco->rep->max_merge_width = w;
}

void rocksdb_universal_compaction_options_set_max_size_amplification_percent(
  rocksdb_universal_compaction_options_t* uco, int p) {
  uco->rep->max_size_amplification_percent = p;
}

void rocksdb_universal_compaction_options_set_compression_size_percent(
  rocksdb_universal_compaction_options_t* uco, int p) {
  uco->rep->compression_size_percent = p;
}

void rocksdb_universal_compaction_options_set_stop_style(
  rocksdb_universal_compaction_options_t* uco, int style) {
  uco->rep->stop_style = static_cast<rocksdb::CompactionStopStyle>(style);
}

void rocksdb_universal_compaction_options_destroy(
  rocksdb_universal_compaction_options_t* uco) {
  delete uco->rep;
  delete uco;
}

rocksdb_fifo_compaction_options_t* rocksdb_fifo_compaction_options_create() {
  rocksdb_fifo_compaction_options_t* result = new rocksdb_fifo_compaction_options_t;
  result->rep =  CompactionOptionsFIFO();
  return result;
}

void rocksdb_fifo_compaction_options_set_max_table_files_size(
    rocksdb_fifo_compaction_options_t* fifo_opts, uint64_t size) {
  fifo_opts->rep.max_table_files_size = size;
}

void rocksdb_fifo_compaction_options_destroy(
    rocksdb_fifo_compaction_options_t* fifo_opts) {
  delete fifo_opts;
}

void rocksdb_options_set_min_level_to_compress(rocksdb_options_t* opt, int level) {
  if (level >= 0) {
    assert(level <= opt->rep.num_levels);
    opt->rep.compression_per_level.resize(opt->rep.num_levels);
    for (int i = 0; i < level; i++) {
      opt->rep.compression_per_level[i] = rocksdb::kNoCompression;
    }
    for (int i = level; i < opt->rep.num_levels; i++) {
      opt->rep.compression_per_level[i] = opt->rep.compression;
    }
  }
}

int rocksdb_livefiles_count(
  const rocksdb_livefiles_t* lf) {
  return static_cast<int>(lf->rep.size());
}

const char* rocksdb_livefiles_name(
  const rocksdb_livefiles_t* lf,
  int index) {
  return lf->rep[index].name.c_str();
}

int rocksdb_livefiles_level(
  const rocksdb_livefiles_t* lf,
  int index) {
  return lf->rep[index].level;
}

size_t rocksdb_livefiles_size(
  const rocksdb_livefiles_t* lf,
  int index) {
  return lf->rep[index].size;
}

const char* rocksdb_livefiles_smallestkey(
  const rocksdb_livefiles_t* lf,
  int index,
  size_t* size) {
  *size = lf->rep[index].smallestkey.size();
  return lf->rep[index].smallestkey.data();
}

const char* rocksdb_livefiles_largestkey(
  const rocksdb_livefiles_t* lf,
  int index,
  size_t* size) {
  *size = lf->rep[index].largestkey.size();
  return lf->rep[index].largestkey.data();
}

uint64_t rocksdb_livefiles_entries(
    const rocksdb_livefiles_t* lf,
    int index) {
  return lf->rep[index].num_entries;
}

uint64_t rocksdb_livefiles_deletions(
    const rocksdb_livefiles_t* lf,
    int index) {
  return lf->rep[index].num_deletions;
}

extern void rocksdb_livefiles_destroy(
  const rocksdb_livefiles_t* lf) {
  delete lf;
}

void rocksdb_get_options_from_string(const rocksdb_options_t* base_options,
                                     const char* opts_str,
                                     rocksdb_options_t* new_options,
                                     char** errptr) {
  SaveError(errptr,
            GetOptionsFromString(base_options->rep, std::string(opts_str),
                                 &new_options->rep));
}

void rocksdb_delete_file_in_range(rocksdb_t* db, const char* start_key,
                                  size_t start_key_len, const char* limit_key,
                                  size_t limit_key_len, char** errptr) {
  Slice a, b;
  SaveError(
      errptr,
      DeleteFilesInRange(
          db->rep, db->rep->DefaultColumnFamily(),
          (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
          (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr)));
}

void rocksdb_delete_file_in_range_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    const char* start_key, size_t start_key_len, const char* limit_key,
    size_t limit_key_len, char** errptr) {
  Slice a, b;
  SaveError(
      errptr,
      DeleteFilesInRange(
          db->rep, column_family->rep,
          (start_key ? (a = Slice(start_key, start_key_len), &a) : nullptr),
          (limit_key ? (b = Slice(limit_key, limit_key_len), &b) : nullptr)));
}

rocksdb_transactiondb_options_t* rocksdb_transactiondb_options_create() {
  return new rocksdb_transactiondb_options_t;
}

void rocksdb_transactiondb_options_destroy(rocksdb_transactiondb_options_t* opt){
  delete opt;
}

void rocksdb_transactiondb_options_set_max_num_locks(
    rocksdb_transactiondb_options_t* opt, int64_t max_num_locks) {
  opt->rep.max_num_locks = max_num_locks;
}

void rocksdb_transactiondb_options_set_num_stripes(
    rocksdb_transactiondb_options_t* opt, size_t num_stripes) {
  opt->rep.num_stripes = num_stripes;
}

void rocksdb_transactiondb_options_set_transaction_lock_timeout(
    rocksdb_transactiondb_options_t* opt, int64_t txn_lock_timeout) {
  opt->rep.transaction_lock_timeout = txn_lock_timeout;
}

void rocksdb_transactiondb_options_set_default_lock_timeout(
    rocksdb_transactiondb_options_t* opt, int64_t default_lock_timeout) {
  opt->rep.default_lock_timeout = default_lock_timeout;
}

rocksdb_transaction_options_t* rocksdb_transaction_options_create() {
  return new rocksdb_transaction_options_t;
}

void rocksdb_transaction_options_destroy(rocksdb_transaction_options_t* opt) {
  delete opt;
}

void rocksdb_transaction_options_set_set_snapshot(
    rocksdb_transaction_options_t* opt, unsigned char v) {
  opt->rep.set_snapshot = v;
}

void rocksdb_transaction_options_set_deadlock_detect(
    rocksdb_transaction_options_t* opt, unsigned char v) {
  opt->rep.deadlock_detect = v;
}

void rocksdb_transaction_options_set_lock_timeout(
    rocksdb_transaction_options_t* opt, int64_t lock_timeout) {
  opt->rep.lock_timeout = lock_timeout;
}

void rocksdb_transaction_options_set_expiration(
    rocksdb_transaction_options_t* opt, int64_t expiration) {
  opt->rep.expiration = expiration;
}

void rocksdb_transaction_options_set_deadlock_detect_depth(
    rocksdb_transaction_options_t* opt, int64_t depth) {
  opt->rep.deadlock_detect_depth = depth;
}

void rocksdb_transaction_options_set_max_write_batch_size(
    rocksdb_transaction_options_t* opt, size_t size) {
  opt->rep.max_write_batch_size = size;
}

rocksdb_optimistictransaction_options_t*
rocksdb_optimistictransaction_options_create() {
  return new rocksdb_optimistictransaction_options_t;
}

void rocksdb_optimistictransaction_options_destroy(
    rocksdb_optimistictransaction_options_t* opt) {
  delete opt;
}

void rocksdb_optimistictransaction_options_set_set_snapshot(
    rocksdb_optimistictransaction_options_t* opt, unsigned char v) {
  opt->rep.set_snapshot = v;
}

rocksdb_column_family_handle_t* rocksdb_transactiondb_create_column_family(
    rocksdb_transactiondb_t* txn_db,
    const rocksdb_options_t* column_family_options,
    const char* column_family_name, char** errptr) {
  rocksdb_column_family_handle_t* handle = new rocksdb_column_family_handle_t;
  SaveError(errptr, txn_db->rep->CreateColumnFamily(
                        ColumnFamilyOptions(column_family_options->rep),
                        std::string(column_family_name), &(handle->rep)));
  return handle;
}

rocksdb_transactiondb_t* rocksdb_transactiondb_open(
    const rocksdb_options_t* options,
    const rocksdb_transactiondb_options_t* txn_db_options, const char* name,
    char** errptr) {
  TransactionDB* txn_db;
  if (SaveError(errptr, TransactionDB::Open(options->rep, txn_db_options->rep,
                                            std::string(name), &txn_db))) {
    return nullptr;
  }
  rocksdb_transactiondb_t* result = new rocksdb_transactiondb_t;
  result->rep = txn_db;
  return result;
}

const rocksdb_snapshot_t* rocksdb_transactiondb_create_snapshot(
    rocksdb_transactiondb_t* txn_db) {
  rocksdb_snapshot_t* result = new rocksdb_snapshot_t;
  result->rep = txn_db->rep->GetSnapshot();
  return result;
}

void rocksdb_transactiondb_release_snapshot(
    rocksdb_transactiondb_t* txn_db, const rocksdb_snapshot_t* snapshot) {
  txn_db->rep->ReleaseSnapshot(snapshot->rep);
  delete snapshot;
}

rocksdb_transaction_t* rocksdb_transaction_begin(
    rocksdb_transactiondb_t* txn_db,
    const rocksdb_writeoptions_t* write_options,
    const rocksdb_transaction_options_t* txn_options,
    rocksdb_transaction_t* old_txn) {
  if (old_txn == nullptr) {
    rocksdb_transaction_t* result = new rocksdb_transaction_t;
    result->rep = txn_db->rep->BeginTransaction(write_options->rep,
                                                txn_options->rep, nullptr);
    return result;
  }
  old_txn->rep = txn_db->rep->BeginTransaction(write_options->rep,
                                                txn_options->rep, old_txn->rep);
  return old_txn;
}

void rocksdb_transaction_commit(rocksdb_transaction_t* txn, char** errptr) {
  SaveError(errptr, txn->rep->Commit());
}

void rocksdb_transaction_rollback(rocksdb_transaction_t* txn, char** errptr) {
  SaveError(errptr, txn->rep->Rollback());
}

void rocksdb_transaction_set_savepoint(rocksdb_transaction_t* txn) {
  txn->rep->SetSavePoint();
}

void rocksdb_transaction_rollback_to_savepoint(rocksdb_transaction_t* txn, char** errptr) {
  SaveError(errptr, txn->rep->RollbackToSavePoint());
}

void rocksdb_transaction_destroy(rocksdb_transaction_t* txn) {
  delete txn->rep;
  delete txn;
}

const rocksdb_snapshot_t* rocksdb_transaction_get_snapshot(
    rocksdb_transaction_t* txn) {
  rocksdb_snapshot_t* result = new rocksdb_snapshot_t;
  result->rep = txn->rep->GetSnapshot();
  return result;
}

// Read a key inside a transaction
char* rocksdb_transaction_get(rocksdb_transaction_t* txn,
                              const rocksdb_readoptions_t* options,
                              const char* key, size_t klen, size_t* vlen,
                              char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = txn->rep->Get(options->rep, Slice(key, klen), &tmp);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* rocksdb_transaction_get_cf(rocksdb_transaction_t* txn,
                                 const rocksdb_readoptions_t* options,
                                 rocksdb_column_family_handle_t* column_family,
                                 const char* key, size_t klen, size_t* vlen,
                                 char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s =
      txn->rep->Get(options->rep, column_family->rep, Slice(key, klen), &tmp);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

// Read a key inside a transaction
char* rocksdb_transaction_get_for_update(rocksdb_transaction_t* txn,
                                         const rocksdb_readoptions_t* options,
                                         const char* key, size_t klen,
                                         size_t* vlen, unsigned char exclusive,
                                         char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s =
      txn->rep->GetForUpdate(options->rep, Slice(key, klen), &tmp, exclusive);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

// Read a key outside a transaction
char* rocksdb_transactiondb_get(
    rocksdb_transactiondb_t* txn_db,
    const rocksdb_readoptions_t* options,
    const char* key, size_t klen,
    size_t* vlen,
    char** errptr){
  char* result = nullptr;
  std::string tmp;
  Status s = txn_db->rep->Get(options->rep, Slice(key, klen), &tmp);
  if (s.ok()) {
    *vlen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vlen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

char* rocksdb_transactiondb_get_cf(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, size_t* vallen, char** errptr) {
  char* result = nullptr;
  std::string tmp;
  Status s = txn_db->rep->Get(options->rep, column_family->rep,
                              Slice(key, keylen), &tmp);
  if (s.ok()) {
    *vallen = tmp.size();
    result = CopyString(tmp);
  } else {
    *vallen = 0;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
  }
  return result;
}

// Put a key inside a transaction
void rocksdb_transaction_put(rocksdb_transaction_t* txn, const char* key,
                             size_t klen, const char* val, size_t vlen,
                             char** errptr) {
  SaveError(errptr, txn->rep->Put(Slice(key, klen), Slice(val, vlen)));
}

void rocksdb_transaction_put_cf(rocksdb_transaction_t* txn,
                                rocksdb_column_family_handle_t* column_family,
                                const char* key, size_t klen, const char* val,
                                size_t vlen, char** errptr) {
  SaveError(errptr, txn->rep->Put(column_family->rep, Slice(key, klen),
                                  Slice(val, vlen)));
}

// Put a key outside a transaction
void rocksdb_transactiondb_put(rocksdb_transactiondb_t* txn_db,
                               const rocksdb_writeoptions_t* options,
                               const char* key, size_t klen, const char* val,
                               size_t vlen, char** errptr) {
  SaveError(errptr,
            txn_db->rep->Put(options->rep, Slice(key, klen), Slice(val, vlen)));
}

void rocksdb_transactiondb_put_cf(rocksdb_transactiondb_t* txn_db,
                                  const rocksdb_writeoptions_t* options,
                                  rocksdb_column_family_handle_t* column_family,
                                  const char* key, size_t keylen,
                                  const char* val, size_t vallen,
                                  char** errptr) {
  SaveError(errptr, txn_db->rep->Put(options->rep, column_family->rep,
                                     Slice(key, keylen), Slice(val, vallen)));
}

// Write batch into transaction db
void rocksdb_transactiondb_write(
        rocksdb_transactiondb_t* db,
        const rocksdb_writeoptions_t* options,
        rocksdb_writebatch_t* batch,
        char** errptr) {
  SaveError(errptr, db->rep->Write(options->rep, &batch->rep));
}

// Merge a key inside a transaction
void rocksdb_transaction_merge(rocksdb_transaction_t* txn, const char* key,
                               size_t klen, const char* val, size_t vlen,
                               char** errptr) {
  SaveError(errptr, txn->rep->Merge(Slice(key, klen), Slice(val, vlen)));
}

// Merge a key outside a transaction
void rocksdb_transactiondb_merge(rocksdb_transactiondb_t* txn_db,
                                 const rocksdb_writeoptions_t* options,
                                 const char* key, size_t klen, const char* val,
                                 size_t vlen, char** errptr) {
  SaveError(errptr, txn_db->rep->Merge(options->rep, Slice(key, klen),
                                       Slice(val, vlen)));
}

// Delete a key inside a transaction
void rocksdb_transaction_delete(rocksdb_transaction_t* txn, const char* key,
                                size_t klen, char** errptr) {
  SaveError(errptr, txn->rep->Delete(Slice(key, klen)));
}

void rocksdb_transaction_delete_cf(
    rocksdb_transaction_t* txn, rocksdb_column_family_handle_t* column_family,
    const char* key, size_t klen, char** errptr) {
  SaveError(errptr, txn->rep->Delete(column_family->rep, Slice(key, klen)));
}

// Delete a key outside a transaction
void rocksdb_transactiondb_delete(rocksdb_transactiondb_t* txn_db,
                                  const rocksdb_writeoptions_t* options,
                                  const char* key, size_t klen, char** errptr) {
  SaveError(errptr, txn_db->rep->Delete(options->rep, Slice(key, klen)));
}

void rocksdb_transactiondb_delete_cf(
    rocksdb_transactiondb_t* txn_db, const rocksdb_writeoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr) {
  SaveError(errptr, txn_db->rep->Delete(options->rep, column_family->rep,
                                        Slice(key, keylen)));
}

// Create an iterator inside a transaction
rocksdb_iterator_t* rocksdb_transaction_create_iterator(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options) {
  rocksdb_iterator_t* result = new rocksdb_iterator_t;
  result->rep = txn->rep->GetIterator(options->rep);
  return result;
}

// Create an iterator inside a transaction with column family
rocksdb_iterator_t* rocksdb_transaction_create_iterator_cf(
    rocksdb_transaction_t* txn, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family) {
  rocksdb_iterator_t* result = new rocksdb_iterator_t;
  result->rep = txn->rep->GetIterator(options->rep, column_family->rep);
  return result;
}

// Create an iterator outside a transaction
rocksdb_iterator_t* rocksdb_transactiondb_create_iterator(
    rocksdb_transactiondb_t* txn_db, const rocksdb_readoptions_t* options) {
  rocksdb_iterator_t* result = new rocksdb_iterator_t;
  result->rep = txn_db->rep->NewIterator(options->rep);
  return result;
}

void rocksdb_transactiondb_close(rocksdb_transactiondb_t* txn_db) {
  delete txn_db->rep;
  delete txn_db;
}

rocksdb_checkpoint_t* rocksdb_transactiondb_checkpoint_object_create(
    rocksdb_transactiondb_t* txn_db, char** errptr) {
  Checkpoint* checkpoint;
  if (SaveError(errptr, Checkpoint::Create(txn_db->rep, &checkpoint))) {
    return nullptr;
  }
  rocksdb_checkpoint_t* result = new rocksdb_checkpoint_t;
  result->rep = checkpoint;
  return result;
}

rocksdb_optimistictransactiondb_t* rocksdb_optimistictransactiondb_open(
    const rocksdb_options_t* options, const char* name, char** errptr) {
  OptimisticTransactionDB* otxn_db;
  if (SaveError(errptr, OptimisticTransactionDB::Open(
                            options->rep, std::string(name), &otxn_db))) {
    return nullptr;
  }
  rocksdb_optimistictransactiondb_t* result =
      new rocksdb_optimistictransactiondb_t;
  result->rep = otxn_db;
  return result;
}

rocksdb_optimistictransactiondb_t*
rocksdb_optimistictransactiondb_open_column_families(
    const rocksdb_options_t* db_options, const char* name,
    int num_column_families, const char** column_family_names,
    const rocksdb_options_t** column_family_options,
    rocksdb_column_family_handle_t** column_family_handles, char** errptr) {
  std::vector<ColumnFamilyDescriptor> column_families;
  for (int i = 0; i < num_column_families; i++) {
    column_families.push_back(ColumnFamilyDescriptor(
        std::string(column_family_names[i]),
        ColumnFamilyOptions(column_family_options[i]->rep)));
  }

  OptimisticTransactionDB* otxn_db;
  std::vector<ColumnFamilyHandle*> handles;
  if (SaveError(errptr, OptimisticTransactionDB::Open(
                            DBOptions(db_options->rep), std::string(name),
                            column_families, &handles, &otxn_db))) {
    return nullptr;
  }

  for (size_t i = 0; i < handles.size(); i++) {
    rocksdb_column_family_handle_t* c_handle =
        new rocksdb_column_family_handle_t;
    c_handle->rep = handles[i];
    column_family_handles[i] = c_handle;
  }
  rocksdb_optimistictransactiondb_t* result =
      new rocksdb_optimistictransactiondb_t;
  result->rep = otxn_db;
  return result;
}

rocksdb_t* rocksdb_optimistictransactiondb_get_base_db(
    rocksdb_optimistictransactiondb_t* otxn_db) {
  DB* base_db = otxn_db->rep->GetBaseDB();

  if (base_db != nullptr) {
    rocksdb_t* result = new rocksdb_t;
    result->rep = base_db;
    return result;
  }

  return nullptr;
}

void rocksdb_optimistictransactiondb_close_base_db(rocksdb_t* base_db) {
  delete base_db;
}

rocksdb_transaction_t* rocksdb_optimistictransaction_begin(
    rocksdb_optimistictransactiondb_t* otxn_db,
    const rocksdb_writeoptions_t* write_options,
    const rocksdb_optimistictransaction_options_t* otxn_options,
    rocksdb_transaction_t* old_txn) {
  if (old_txn == nullptr) {
    rocksdb_transaction_t* result = new rocksdb_transaction_t;
    result->rep = otxn_db->rep->BeginTransaction(write_options->rep,
                                                 otxn_options->rep, nullptr);
    return result;
  }
  old_txn->rep = otxn_db->rep->BeginTransaction(
      write_options->rep, otxn_options->rep, old_txn->rep);
  return old_txn;
}

void rocksdb_optimistictransactiondb_close(
    rocksdb_optimistictransactiondb_t* otxn_db) {
  delete otxn_db->rep;
  delete otxn_db;
}

void rocksdb_free(void* ptr) { free(ptr); }

rocksdb_pinnableslice_t* rocksdb_get_pinned(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t keylen, char** errptr) {
  rocksdb_pinnableslice_t* v = new (rocksdb_pinnableslice_t);
  Status s = db->rep->Get(options->rep, db->rep->DefaultColumnFamily(),
                          Slice(key, keylen), &v->rep);
  if (!s.ok()) {
    delete (v);
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
    return nullptr;
  }
  return v;
}

rocksdb_pinnableslice_t* rocksdb_get_pinned_cf(
    rocksdb_t* db, const rocksdb_readoptions_t* options,
    rocksdb_column_family_handle_t* column_family, const char* key,
    size_t keylen, char** errptr) {
  rocksdb_pinnableslice_t* v = new (rocksdb_pinnableslice_t);
  Status s = db->rep->Get(options->rep, column_family->rep, Slice(key, keylen),
                          &v->rep);
  if (!s.ok()) {
    delete v;
    if (!s.IsNotFound()) {
      SaveError(errptr, s);
    }
    return nullptr;
  }
  return v;
}

void rocksdb_pinnableslice_destroy(rocksdb_pinnableslice_t* v) { delete v; }

const char* rocksdb_pinnableslice_value(const rocksdb_pinnableslice_t* v,
                                        size_t* vlen) {
  if (!v) {
    *vlen = 0;
    return nullptr;
  }

  *vlen = v->rep.size();
  return v->rep.data();
}

// container to keep databases and caches in order to use rocksdb::MemoryUtil
struct rocksdb_memory_consumers_t {
  std::vector<rocksdb_t*> dbs;
  std::unordered_set<rocksdb_cache_t*> caches;
};

// initializes new container of memory consumers
rocksdb_memory_consumers_t* rocksdb_memory_consumers_create() {
  return new rocksdb_memory_consumers_t;
}

// adds datatabase to the container of memory consumers
void rocksdb_memory_consumers_add_db(rocksdb_memory_consumers_t* consumers,
                                     rocksdb_t* db) {
  consumers->dbs.push_back(db);
}

// adds cache to the container of memory consumers
void rocksdb_memory_consumers_add_cache(rocksdb_memory_consumers_t* consumers,
                                        rocksdb_cache_t* cache) {
  consumers->caches.insert(cache);
}

// deletes container with memory consumers
void rocksdb_memory_consumers_destroy(rocksdb_memory_consumers_t* consumers) {
  delete consumers;
}

// contains memory usage statistics provided by rocksdb::MemoryUtil
struct rocksdb_memory_usage_t {
  uint64_t mem_table_total;
  uint64_t mem_table_unflushed;
  uint64_t mem_table_readers_total;
  uint64_t cache_total;
};

// estimates amount of memory occupied by consumers (dbs and caches)
rocksdb_memory_usage_t* rocksdb_approximate_memory_usage_create(
    rocksdb_memory_consumers_t* consumers, char** errptr) {

  vector<DB*> dbs;
  for (auto db : consumers->dbs) {
    dbs.push_back(db->rep);
  }

  unordered_set<const Cache*> cache_set;
  for (auto cache : consumers->caches) {
    cache_set.insert(const_cast<const Cache*>(cache->rep.get()));
  }

  std::map<rocksdb::MemoryUtil::UsageType, uint64_t> usage_by_type;

  auto status = MemoryUtil::GetApproximateMemoryUsageByType(dbs, cache_set,
                                                            &usage_by_type);
  if (SaveError(errptr, status)) {
    return nullptr;
  }

  auto result = new rocksdb_memory_usage_t;
  result->mem_table_total = usage_by_type[MemoryUtil::kMemTableTotal];
  result->mem_table_unflushed = usage_by_type[MemoryUtil::kMemTableUnFlushed];
  result->mem_table_readers_total = usage_by_type[MemoryUtil::kTableReadersTotal];
  result->cache_total = usage_by_type[MemoryUtil::kCacheTotal];
  return result;
}

uint64_t rocksdb_approximate_memory_usage_get_mem_table_total(
    rocksdb_memory_usage_t* memory_usage) {
  return memory_usage->mem_table_total;
}

uint64_t rocksdb_approximate_memory_usage_get_mem_table_unflushed(
    rocksdb_memory_usage_t* memory_usage) {
  return memory_usage->mem_table_unflushed;
}

uint64_t rocksdb_approximate_memory_usage_get_mem_table_readers_total(
    rocksdb_memory_usage_t* memory_usage) {
  return memory_usage->mem_table_readers_total;
}

uint64_t rocksdb_approximate_memory_usage_get_cache_total(
    rocksdb_memory_usage_t* memory_usage) {
  return memory_usage->cache_total;
}

// deletes container with memory usage estimates
void rocksdb_approximate_memory_usage_destroy(rocksdb_memory_usage_t* usage) {
  delete usage;
}

}  // end extern "C"

#endif  // !ROCKSDB_LITE
