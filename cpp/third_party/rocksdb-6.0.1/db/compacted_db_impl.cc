//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "db/compacted_db_impl.h"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "table/get_context.h"

namespace rocksdb {

extern void MarkKeyMayExist(void* arg);
extern bool SaveValue(void* arg, const ParsedInternalKey& parsed_key,
                      const Slice& v, bool hit_and_return);

CompactedDBImpl::CompactedDBImpl(
  const DBOptions& options, const std::string& dbname)
  : DBImpl(options, dbname), cfd_(nullptr), version_(nullptr),
    user_comparator_(nullptr) {
}

CompactedDBImpl::~CompactedDBImpl() {
}

size_t CompactedDBImpl::FindFile(const Slice& key) {
  size_t right = files_.num_files - 1;
  auto cmp = [&](const FdWithKeyRange& f, const Slice& k) -> bool {
    return user_comparator_->Compare(ExtractUserKey(f.largest_key), k) < 0;
  };
  return static_cast<size_t>(std::lower_bound(files_.files,
                            files_.files + right, key, cmp) - files_.files);
}

Status CompactedDBImpl::Get(const ReadOptions& options, ColumnFamilyHandle*,
                            const Slice& key, PinnableSlice* value) {
  GetContext get_context(user_comparator_, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, key, value, nullptr, nullptr,
                         nullptr, nullptr);
  LookupKey lkey(key, kMaxSequenceNumber);
  files_.files[FindFile(key)].fd.table_reader->Get(options, lkey.internal_key(),
                                                   &get_context, nullptr);
  if (get_context.State() == GetContext::kFound) {
    return Status::OK();
  }
  return Status::NotFound();
}

std::vector<Status> CompactedDBImpl::MultiGet(const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>&,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  autovector<TableReader*, 16> reader_list;
  for (const auto& key : keys) {
    const FdWithKeyRange& f = files_.files[FindFile(key)];
    if (user_comparator_->Compare(key, ExtractUserKey(f.smallest_key)) < 0) {
      reader_list.push_back(nullptr);
    } else {
      LookupKey lkey(key, kMaxSequenceNumber);
      f.fd.table_reader->Prepare(lkey.internal_key());
      reader_list.push_back(f.fd.table_reader);
    }
  }
  std::vector<Status> statuses(keys.size(), Status::NotFound());
  values->resize(keys.size());
  int idx = 0;
  for (auto* r : reader_list) {
    if (r != nullptr) {
      PinnableSlice pinnable_val;
      std::string& value = (*values)[idx];
      GetContext get_context(user_comparator_, nullptr, nullptr, nullptr,
                             GetContext::kNotFound, keys[idx], &pinnable_val,
                             nullptr, nullptr, nullptr, nullptr);
      LookupKey lkey(keys[idx], kMaxSequenceNumber);
      r->Get(options, lkey.internal_key(), &get_context, nullptr);
      value.assign(pinnable_val.data(), pinnable_val.size());
      if (get_context.State() == GetContext::kFound) {
        statuses[idx] = Status::OK();
      }
    }
    ++idx;
  }
  return statuses;
}

Status CompactedDBImpl::Init(const Options& options) {
  SuperVersionContext sv_context(/* create_superversion */ true);
  mutex_.Lock();
  ColumnFamilyDescriptor cf(kDefaultColumnFamilyName,
                            ColumnFamilyOptions(options));
  Status s = Recover({cf}, true /* read only */, false, true);
  if (s.ok()) {
    cfd_ = reinterpret_cast<ColumnFamilyHandleImpl*>(
              DefaultColumnFamily())->cfd();
    cfd_->InstallSuperVersion(&sv_context, &mutex_);
  }
  mutex_.Unlock();
  sv_context.Clean();
  if (!s.ok()) {
    return s;
  }
  NewThreadStatusCfInfo(cfd_);
  version_ = cfd_->GetSuperVersion()->current;
  user_comparator_ = cfd_->user_comparator();
  auto* vstorage = version_->storage_info();
  if (vstorage->num_non_empty_levels() == 0) {
    return Status::NotSupported("no file exists");
  }
  const LevelFilesBrief& l0 = vstorage->LevelFilesBrief(0);
  // L0 should not have files
  if (l0.num_files > 1) {
    return Status::NotSupported("L0 contain more than 1 file");
  }
  if (l0.num_files == 1) {
    if (vstorage->num_non_empty_levels() > 1) {
      return Status::NotSupported("Both L0 and other level contain files");
    }
    files_ = l0;
    return Status::OK();
  }

  for (int i = 1; i < vstorage->num_non_empty_levels() - 1; ++i) {
    if (vstorage->LevelFilesBrief(i).num_files > 0) {
      return Status::NotSupported("Other levels also contain files");
    }
  }

  int level = vstorage->num_non_empty_levels() - 1;
  if (vstorage->LevelFilesBrief(level).num_files > 0) {
    files_ = vstorage->LevelFilesBrief(level);
    return Status::OK();
  }
  return Status::NotSupported("no file exists");
}

Status CompactedDBImpl::Open(const Options& options,
                             const std::string& dbname, DB** dbptr) {
  *dbptr = nullptr;

  if (options.max_open_files != -1) {
    return Status::InvalidArgument("require max_open_files = -1");
  }
  if (options.merge_operator.get() != nullptr) {
    return Status::InvalidArgument("merge operator is not supported");
  }
  DBOptions db_options(options);
  std::unique_ptr<CompactedDBImpl> db(new CompactedDBImpl(db_options, dbname));
  Status s = db->Init(options);
  if (s.ok()) {
    db->StartTimedTasks();
    ROCKS_LOG_INFO(db->immutable_db_options_.info_log,
                   "Opened the db as fully compacted mode");
    LogFlush(db->immutable_db_options_.info_log);
    *dbptr = db.release();
  }
  return s;
}

}   // namespace rocksdb
#endif  // ROCKSDB_LITE
