//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_db_impl.h"
#include <algorithm>
#include <cinttypes>
#include <iomanip>
#include <memory>

#include "db/db_impl.h"
#include "db/write_batch_internal.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/statistics.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/utilities/stackable_db.h"
#include "rocksdb/utilities/transaction.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_builder.h"
#include "table/meta_blocks.h"
#include "util/cast_util.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/file_util.h"
#include "util/filename.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/sst_file_manager_impl.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"
#include "util/timer_queue.h"
#include "utilities/blob_db/blob_compaction_filter.h"
#include "utilities/blob_db/blob_db_iterator.h"
#include "utilities/blob_db/blob_db_listener.h"
#include "utilities/blob_db/blob_index.h"

namespace {
int kBlockBasedTableVersionFormat = 2;
}  // end namespace

namespace rocksdb {
namespace blob_db {

bool BlobFileComparator::operator()(
    const std::shared_ptr<BlobFile>& lhs,
    const std::shared_ptr<BlobFile>& rhs) const {
  return lhs->BlobFileNumber() > rhs->BlobFileNumber();
}

bool BlobFileComparatorTTL::operator()(
    const std::shared_ptr<BlobFile>& lhs,
    const std::shared_ptr<BlobFile>& rhs) const {
  assert(lhs->HasTTL() && rhs->HasTTL());
  if (lhs->expiration_range_.first < rhs->expiration_range_.first) {
    return true;
  }
  if (lhs->expiration_range_.first > rhs->expiration_range_.first) {
    return false;
  }
  return lhs->BlobFileNumber() < rhs->BlobFileNumber();
}

BlobDBImpl::BlobDBImpl(const std::string& dbname,
                       const BlobDBOptions& blob_db_options,
                       const DBOptions& db_options,
                       const ColumnFamilyOptions& cf_options)
    : BlobDB(),
      dbname_(dbname),
      db_impl_(nullptr),
      env_(db_options.env),
      bdb_options_(blob_db_options),
      db_options_(db_options),
      cf_options_(cf_options),
      env_options_(db_options),
      statistics_(db_options_.statistics.get()),
      next_file_number_(1),
      epoch_of_(0),
      closed_(true),
      open_file_count_(0),
      total_blob_size_(0),
      live_sst_size_(0),
      fifo_eviction_seq_(0),
      evict_expiration_up_to_(0),
      debug_level_(0) {
  blob_dir_ = (bdb_options_.path_relative)
                  ? dbname + "/" + bdb_options_.blob_dir
                  : bdb_options_.blob_dir;
  env_options_.bytes_per_sync = blob_db_options.bytes_per_sync;
}

BlobDBImpl::~BlobDBImpl() {
  tqueue_.shutdown();
  // CancelAllBackgroundWork(db_, true);
  Status s __attribute__((__unused__)) = Close();
  assert(s.ok());
}

Status BlobDBImpl::Close() {
  if (closed_) {
    return Status::OK();
  }
  closed_ = true;

  // Close base DB before BlobDBImpl destructs to stop event listener and
  // compaction filter call.
  Status s = db_->Close();
  // delete db_ anyway even if close failed.
  delete db_;
  // Reset pointers to avoid StackableDB delete the pointer again.
  db_ = nullptr;
  db_impl_ = nullptr;
  if (!s.ok()) {
    return s;
  }

  s = SyncBlobFiles();
  return s;
}

BlobDBOptions BlobDBImpl::GetBlobDBOptions() const { return bdb_options_; }

Status BlobDBImpl::Open(std::vector<ColumnFamilyHandle*>* handles) {
  assert(handles != nullptr);
  assert(db_ == nullptr);
  if (blob_dir_.empty()) {
    return Status::NotSupported("No blob directory in options");
  }
  if (cf_options_.compaction_filter != nullptr ||
      cf_options_.compaction_filter_factory != nullptr) {
    return Status::NotSupported("Blob DB doesn't support compaction filter.");
  }

  Status s;

  // Create info log.
  if (db_options_.info_log == nullptr) {
    s = CreateLoggerFromOptions(dbname_, db_options_, &db_options_.info_log);
    if (!s.ok()) {
      return s;
    }
  }

  ROCKS_LOG_INFO(db_options_.info_log, "Opening BlobDB...");

  // Open blob directory.
  s = env_->CreateDirIfMissing(blob_dir_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to create blob_dir %s, status: %s",
                    blob_dir_.c_str(), s.ToString().c_str());
  }
  s = env_->NewDirectory(blob_dir_, &dir_ent_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to open blob_dir %s, status: %s", blob_dir_.c_str(),
                    s.ToString().c_str());
    return s;
  }

  // Open blob files.
  s = OpenAllBlobFiles();
  if (!s.ok()) {
    return s;
  }

  // Update options
  db_options_.listeners.push_back(std::make_shared<BlobDBListener>(this));
  cf_options_.compaction_filter_factory.reset(
      new BlobIndexCompactionFilterFactory(this, env_, statistics_));

  // Open base db.
  ColumnFamilyDescriptor cf_descriptor(kDefaultColumnFamilyName, cf_options_);
  s = DB::Open(db_options_, dbname_, {cf_descriptor}, handles, &db_);
  if (!s.ok()) {
    return s;
  }
  db_impl_ = static_cast_with_check<DBImpl, DB>(db_->GetRootDB());

  // Add trash files in blob dir to file delete scheduler.
  SstFileManagerImpl* sfm = static_cast<SstFileManagerImpl*>(
      db_impl_->immutable_db_options().sst_file_manager.get());
  DeleteScheduler::CleanupDirectory(env_, sfm, blob_dir_);

  UpdateLiveSSTSize();

  // Start background jobs.
  if (!bdb_options_.disable_background_tasks) {
    StartBackgroundTasks();
  }

  ROCKS_LOG_INFO(db_options_.info_log, "BlobDB pointer %p", this);
  bdb_options_.Dump(db_options_.info_log.get());
  closed_ = false;
  return s;
}

void BlobDBImpl::StartBackgroundTasks() {
  // store a call to a member function and object
  tqueue_.add(
      kReclaimOpenFilesPeriodMillisecs,
      std::bind(&BlobDBImpl::ReclaimOpenFiles, this, std::placeholders::_1));
  tqueue_.add(static_cast<int64_t>(
                  bdb_options_.garbage_collection_interval_secs * 1000),
              std::bind(&BlobDBImpl::RunGC, this, std::placeholders::_1));
  tqueue_.add(
      kDeleteObsoleteFilesPeriodMillisecs,
      std::bind(&BlobDBImpl::DeleteObsoleteFiles, this, std::placeholders::_1));
  tqueue_.add(kSanityCheckPeriodMillisecs,
              std::bind(&BlobDBImpl::SanityCheck, this, std::placeholders::_1));
  tqueue_.add(
      kEvictExpiredFilesPeriodMillisecs,
      std::bind(&BlobDBImpl::EvictExpiredFiles, this, std::placeholders::_1));
}

Status BlobDBImpl::GetAllBlobFiles(std::set<uint64_t>* file_numbers) {
  assert(file_numbers != nullptr);
  std::vector<std::string> all_files;
  Status s = env_->GetChildren(blob_dir_, &all_files);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to get list of blob files, status: %s",
                    s.ToString().c_str());
    return s;
  }

  for (const auto& file_name : all_files) {
    uint64_t file_number;
    FileType type;
    bool success = ParseFileName(file_name, &file_number, &type);
    if (success && type == kBlobFile) {
      file_numbers->insert(file_number);
    } else {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "Skipping file in blob directory: %s", file_name.c_str());
    }
  }

  return s;
}

Status BlobDBImpl::OpenAllBlobFiles() {
  std::set<uint64_t> file_numbers;
  Status s = GetAllBlobFiles(&file_numbers);
  if (!s.ok()) {
    return s;
  }

  if (!file_numbers.empty()) {
    next_file_number_.store(*file_numbers.rbegin() + 1);
  }

  std::string blob_file_list;
  std::string obsolete_file_list;

  for (auto& file_number : file_numbers) {
    std::shared_ptr<BlobFile> blob_file = std::make_shared<BlobFile>(
        this, blob_dir_, file_number, db_options_.info_log.get());
    blob_file->MarkImmutable();

    // Read file header and footer
    Status read_metadata_status = blob_file->ReadMetadata(env_, env_options_);
    if (read_metadata_status.IsCorruption()) {
      // Remove incomplete file.
      ObsoleteBlobFile(blob_file, 0 /*obsolete_seq*/, false /*update_size*/);
      if (!obsolete_file_list.empty()) {
        obsolete_file_list.append(", ");
      }
      obsolete_file_list.append(ToString(file_number));
      continue;
    } else if (!read_metadata_status.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Unable to read metadata of blob file % " PRIu64
                      ", status: '%s'",
                      file_number, read_metadata_status.ToString().c_str());
      return read_metadata_status;
    }

    total_blob_size_ += blob_file->GetFileSize();

    blob_files_[file_number] = blob_file;
    if (!blob_file_list.empty()) {
      blob_file_list.append(", ");
    }
    blob_file_list.append(ToString(file_number));
  }

  ROCKS_LOG_INFO(db_options_.info_log,
                 "Found %" ROCKSDB_PRIszt " blob files: %s", blob_files_.size(),
                 blob_file_list.c_str());
  ROCKS_LOG_INFO(db_options_.info_log,
                 "Found %" ROCKSDB_PRIszt
                 " incomplete or corrupted blob files: %s",
                 obsolete_files_.size(), obsolete_file_list.c_str());
  return s;
}

void BlobDBImpl::CloseRandomAccessLocked(
    const std::shared_ptr<BlobFile>& bfile) {
  bfile->CloseRandomAccessLocked();
  open_file_count_--;
}

Status BlobDBImpl::GetBlobFileReader(
    const std::shared_ptr<BlobFile>& blob_file,
    std::shared_ptr<RandomAccessFileReader>* reader) {
  assert(reader != nullptr);
  bool fresh_open = false;
  Status s = blob_file->GetReader(env_, env_options_, reader, &fresh_open);
  if (s.ok() && fresh_open) {
    assert(*reader != nullptr);
    open_file_count_++;
  }
  return s;
}

std::shared_ptr<BlobFile> BlobDBImpl::NewBlobFile(const std::string& reason) {
  uint64_t file_num = next_file_number_++;
  auto bfile = std::make_shared<BlobFile>(this, blob_dir_, file_num,
                                          db_options_.info_log.get());
  ROCKS_LOG_DEBUG(db_options_.info_log, "New blob file created: %s reason='%s'",
                  bfile->PathName().c_str(), reason.c_str());
  LogFlush(db_options_.info_log);
  return bfile;
}

Status BlobDBImpl::CreateWriterLocked(const std::shared_ptr<BlobFile>& bfile) {
  std::string fpath(bfile->PathName());
  std::unique_ptr<WritableFile> wfile;

  Status s = env_->ReopenWritableFile(fpath, &wfile, env_options_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to open blob file for write: %s status: '%s'"
                    " exists: '%s'",
                    fpath.c_str(), s.ToString().c_str(),
                    env_->FileExists(fpath).ToString().c_str());
    return s;
  }

  std::unique_ptr<WritableFileWriter> fwriter;
  fwriter.reset(new WritableFileWriter(std::move(wfile), fpath, env_options_));

  uint64_t boffset = bfile->GetFileSize();
  if (debug_level_ >= 2 && boffset) {
    ROCKS_LOG_DEBUG(db_options_.info_log, "Open blob file: %s with offset: %d",
                    fpath.c_str(), boffset);
  }

  Writer::ElemType et = Writer::kEtNone;
  if (bfile->file_size_ == BlobLogHeader::kSize) {
    et = Writer::kEtFileHdr;
  } else if (bfile->file_size_ > BlobLogHeader::kSize) {
    et = Writer::kEtRecord;
  } else if (bfile->file_size_) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Open blob file: %s with wrong size: %d", fpath.c_str(),
                   boffset);
    return Status::Corruption("Invalid blob file size");
  }

  bfile->log_writer_ = std::make_shared<Writer>(
      std::move(fwriter), env_, statistics_, bfile->file_number_,
      bdb_options_.bytes_per_sync, db_options_.use_fsync, boffset);
  bfile->log_writer_->last_elem_type_ = et;

  return s;
}

std::shared_ptr<BlobFile> BlobDBImpl::FindBlobFileLocked(
    uint64_t expiration) const {
  if (open_ttl_files_.empty()) {
    return nullptr;
  }

  std::shared_ptr<BlobFile> tmp = std::make_shared<BlobFile>();
  tmp->SetHasTTL(true);
  tmp->expiration_range_ = std::make_pair(expiration, 0);
  tmp->file_number_ = std::numeric_limits<uint64_t>::max();

  auto citr = open_ttl_files_.equal_range(tmp);
  if (citr.first == open_ttl_files_.end()) {
    assert(citr.second == open_ttl_files_.end());

    std::shared_ptr<BlobFile> check = *(open_ttl_files_.rbegin());
    return (check->expiration_range_.second <= expiration) ? nullptr : check;
  }

  if (citr.first != citr.second) {
    return *(citr.first);
  }

  auto finditr = citr.second;
  if (finditr != open_ttl_files_.begin()) {
    --finditr;
  }

  bool b2 = (*finditr)->expiration_range_.second <= expiration;
  bool b1 = (*finditr)->expiration_range_.first > expiration;

  return (b1 || b2) ? nullptr : (*finditr);
}

Status BlobDBImpl::CheckOrCreateWriterLocked(
    const std::shared_ptr<BlobFile>& blob_file,
    std::shared_ptr<Writer>* writer) {
  assert(writer != nullptr);
  *writer = blob_file->GetWriter();
  if (*writer != nullptr) {
    return Status::OK();
  }
  Status s = CreateWriterLocked(blob_file);
  if (s.ok()) {
    *writer = blob_file->GetWriter();
  }
  return s;
}

Status BlobDBImpl::SelectBlobFile(std::shared_ptr<BlobFile>* blob_file) {
  assert(blob_file != nullptr);
  {
    ReadLock rl(&mutex_);
    if (open_non_ttl_file_ != nullptr) {
      *blob_file = open_non_ttl_file_;
      return Status::OK();
    }
  }

  // CHECK again
  WriteLock wl(&mutex_);
  if (open_non_ttl_file_ != nullptr) {
    *blob_file = open_non_ttl_file_;
    return Status::OK();
  }

  *blob_file = NewBlobFile("SelectBlobFile");
  assert(*blob_file != nullptr);

  // file not visible, hence no lock
  std::shared_ptr<Writer> writer;
  Status s = CheckOrCreateWriterLocked(*blob_file, &writer);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to get writer from blob file: %s, error: %s",
                    (*blob_file)->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  (*blob_file)->file_size_ = BlobLogHeader::kSize;
  (*blob_file)->header_.compression = bdb_options_.compression;
  (*blob_file)->header_.has_ttl = false;
  (*blob_file)->header_.column_family_id =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  (*blob_file)->header_valid_ = true;
  (*blob_file)->SetColumnFamilyId((*blob_file)->header_.column_family_id);
  (*blob_file)->SetHasTTL(false);
  (*blob_file)->SetCompression(bdb_options_.compression);

  s = writer->WriteHeader((*blob_file)->header_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to write header to new blob file: %s"
                    " status: '%s'",
                    (*blob_file)->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  blob_files_.insert(
      std::make_pair((*blob_file)->BlobFileNumber(), *blob_file));
  open_non_ttl_file_ = *blob_file;
  total_blob_size_ += BlobLogHeader::kSize;
  return s;
}

Status BlobDBImpl::SelectBlobFileTTL(uint64_t expiration,
                                     std::shared_ptr<BlobFile>* blob_file) {
  assert(blob_file != nullptr);
  assert(expiration != kNoExpiration);
  uint64_t epoch_read = 0;
  {
    ReadLock rl(&mutex_);
    *blob_file = FindBlobFileLocked(expiration);
    epoch_read = epoch_of_.load();
  }

  if (*blob_file != nullptr) {
    assert(!(*blob_file)->Immutable());
    return Status::OK();
  }

  uint64_t exp_low =
      (expiration / bdb_options_.ttl_range_secs) * bdb_options_.ttl_range_secs;
  uint64_t exp_high = exp_low + bdb_options_.ttl_range_secs;
  ExpirationRange expiration_range = std::make_pair(exp_low, exp_high);

  *blob_file = NewBlobFile("SelectBlobFileTTL");
  assert(*blob_file != nullptr);

  ROCKS_LOG_INFO(db_options_.info_log, "New blob file TTL range: %s %d %d",
                 (*blob_file)->PathName().c_str(), exp_low, exp_high);
  LogFlush(db_options_.info_log);

  // we don't need to take lock as no other thread is seeing bfile yet
  std::shared_ptr<Writer> writer;
  Status s = CheckOrCreateWriterLocked(*blob_file, &writer);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "Failed to get writer from blob file with TTL: %s, error: %s",
        (*blob_file)->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  (*blob_file)->header_.expiration_range = expiration_range;
  (*blob_file)->header_.compression = bdb_options_.compression;
  (*blob_file)->header_.has_ttl = true;
  (*blob_file)->header_.column_family_id =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  (*blob_file)->header_valid_ = true;
  (*blob_file)->SetColumnFamilyId((*blob_file)->header_.column_family_id);
  (*blob_file)->SetHasTTL(true);
  (*blob_file)->SetCompression(bdb_options_.compression);
  (*blob_file)->file_size_ = BlobLogHeader::kSize;

  // set the first value of the range, since that is
  // concrete at this time.  also necessary to add to open_ttl_files_
  (*blob_file)->expiration_range_ = expiration_range;

  WriteLock wl(&mutex_);
  // in case the epoch has shifted in the interim, then check
  // check condition again - should be rare.
  if (epoch_of_.load() != epoch_read) {
    std::shared_ptr<BlobFile> blob_file2 = FindBlobFileLocked(expiration);
    if (blob_file2 != nullptr) {
      *blob_file = std::move(blob_file2);
      return Status::OK();
    }
  }

  s = writer->WriteHeader((*blob_file)->header_);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to write header to new blob file: %s"
                    " status: '%s'",
                    (*blob_file)->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  blob_files_.insert(
      std::make_pair((*blob_file)->BlobFileNumber(), *blob_file));
  open_ttl_files_.insert(*blob_file);
  total_blob_size_ += BlobLogHeader::kSize;
  epoch_of_++;

  return s;
}

class BlobDBImpl::BlobInserter : public WriteBatch::Handler {
 private:
  const WriteOptions& options_;
  BlobDBImpl* blob_db_impl_;
  uint32_t default_cf_id_;
  WriteBatch batch_;

 public:
  BlobInserter(const WriteOptions& options, BlobDBImpl* blob_db_impl,
               uint32_t default_cf_id)
      : options_(options),
        blob_db_impl_(blob_db_impl),
        default_cf_id_(default_cf_id) {}

  WriteBatch* batch() { return &batch_; }

  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    Status s = blob_db_impl_->PutBlobValue(options_, key, value, kNoExpiration,
                                           &batch_);
    return s;
  }

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    Status s = WriteBatchInternal::Delete(&batch_, column_family_id, key);
    return s;
  }

  virtual Status DeleteRange(uint32_t column_family_id, const Slice& begin_key,
                             const Slice& end_key) {
    if (column_family_id != default_cf_id_) {
      return Status::NotSupported(
          "Blob DB doesn't support non-default column family.");
    }
    Status s = WriteBatchInternal::DeleteRange(&batch_, column_family_id,
                                               begin_key, end_key);
    return s;
  }

  Status SingleDeleteCF(uint32_t /*column_family_id*/,
                        const Slice& /*key*/) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  Status MergeCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                 const Slice& /*value*/) override {
    return Status::NotSupported("Not supported operation in blob db.");
  }

  void LogData(const Slice& blob) override { batch_.PutLogData(blob); }
};

Status BlobDBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  StopWatch write_sw(env_, statistics_, BLOB_DB_WRITE_MICROS);
  RecordTick(statistics_, BLOB_DB_NUM_WRITE);
  uint32_t default_cf_id =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  Status s;
  BlobInserter blob_inserter(options, this, default_cf_id);
  {
    // Release write_mutex_ before DB write to avoid race condition with
    // flush begin listener, which also require write_mutex_ to sync
    // blob files.
    MutexLock l(&write_mutex_);
    s = updates->Iterate(&blob_inserter);
  }
  if (!s.ok()) {
    return s;
  }
  return db_->Write(options, blob_inserter.batch());
}

Status BlobDBImpl::Put(const WriteOptions& options, const Slice& key,
                       const Slice& value) {
  return PutUntil(options, key, value, kNoExpiration);
}

Status BlobDBImpl::PutWithTTL(const WriteOptions& options,
                              const Slice& key, const Slice& value,
                              uint64_t ttl) {
  uint64_t now = EpochNow();
  uint64_t expiration = kNoExpiration - now > ttl ? now + ttl : kNoExpiration;
  return PutUntil(options, key, value, expiration);
}

Status BlobDBImpl::PutUntil(const WriteOptions& options, const Slice& key,
                            const Slice& value, uint64_t expiration) {
  StopWatch write_sw(env_, statistics_, BLOB_DB_WRITE_MICROS);
  RecordTick(statistics_, BLOB_DB_NUM_PUT);
  TEST_SYNC_POINT("BlobDBImpl::PutUntil:Start");
  Status s;
  WriteBatch batch;
  {
    // Release write_mutex_ before DB write to avoid race condition with
    // flush begin listener, which also require write_mutex_ to sync
    // blob files.
    MutexLock l(&write_mutex_);
    s = PutBlobValue(options, key, value, expiration, &batch);
  }
  if (s.ok()) {
    s = db_->Write(options, &batch);
  }
  TEST_SYNC_POINT("BlobDBImpl::PutUntil:Finish");
  return s;
}

Status BlobDBImpl::PutBlobValue(const WriteOptions& /*options*/,
                                const Slice& key, const Slice& value,
                                uint64_t expiration, WriteBatch* batch) {
  write_mutex_.AssertHeld();
  Status s;
  std::string index_entry;
  uint32_t column_family_id =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->GetID();
  if (value.size() < bdb_options_.min_blob_size) {
    if (expiration == kNoExpiration) {
      // Put as normal value
      s = batch->Put(key, value);
      RecordTick(statistics_, BLOB_DB_WRITE_INLINED);
    } else {
      // Inlined with TTL
      BlobIndex::EncodeInlinedTTL(&index_entry, expiration, value);
      s = WriteBatchInternal::PutBlobIndex(batch, column_family_id, key,
                                           index_entry);
      RecordTick(statistics_, BLOB_DB_WRITE_INLINED_TTL);
    }
  } else {
    std::string compression_output;
    Slice value_compressed = GetCompressedSlice(value, &compression_output);

    std::string headerbuf;
    Writer::ConstructBlobHeader(&headerbuf, key, value_compressed, expiration);

    // Check DB size limit before selecting blob file to
    // Since CheckSizeAndEvictBlobFiles() can close blob files, it needs to be
    // done before calling SelectBlobFile().
    s = CheckSizeAndEvictBlobFiles(headerbuf.size() + key.size() +
                                   value_compressed.size());
    if (!s.ok()) {
      return s;
    }

    std::shared_ptr<BlobFile> blob_file;
    if (expiration != kNoExpiration) {
      s = SelectBlobFileTTL(expiration, &blob_file);
    } else {
      s = SelectBlobFile(&blob_file);
    }
    if (s.ok()) {
      assert(blob_file != nullptr);
      assert(blob_file->compression() == bdb_options_.compression);
      s = AppendBlob(blob_file, headerbuf, key, value_compressed, expiration,
                     &index_entry);
    }
    if (s.ok()) {
      if (expiration != kNoExpiration) {
        blob_file->ExtendExpirationRange(expiration);
      }
      s = CloseBlobFileIfNeeded(blob_file);
    }
    if (s.ok()) {
      s = WriteBatchInternal::PutBlobIndex(batch, column_family_id, key,
                                           index_entry);
    }
    if (s.ok()) {
      if (expiration == kNoExpiration) {
        RecordTick(statistics_, BLOB_DB_WRITE_BLOB);
      } else {
        RecordTick(statistics_, BLOB_DB_WRITE_BLOB_TTL);
      }
    } else {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to append blob to FILE: %s: KEY: %s VALSZ: %d"
                      " status: '%s' blob_file: '%s'",
                      blob_file->PathName().c_str(), key.ToString().c_str(),
                      value.size(), s.ToString().c_str(),
                      blob_file->DumpState().c_str());
    }
  }

  RecordTick(statistics_, BLOB_DB_NUM_KEYS_WRITTEN);
  RecordTick(statistics_, BLOB_DB_BYTES_WRITTEN, key.size() + value.size());
  MeasureTime(statistics_, BLOB_DB_KEY_SIZE, key.size());
  MeasureTime(statistics_, BLOB_DB_VALUE_SIZE, value.size());

  return s;
}

Slice BlobDBImpl::GetCompressedSlice(const Slice& raw,
                                     std::string* compression_output) const {
  if (bdb_options_.compression == kNoCompression) {
    return raw;
  }
  StopWatch compression_sw(env_, statistics_, BLOB_DB_COMPRESSION_MICROS);
  CompressionType type = bdb_options_.compression;
  CompressionOptions opts;
  CompressionContext context(type);
  CompressionInfo info(opts, context, CompressionDict::GetEmptyDict(), type);
  CompressBlock(raw, info, &type, kBlockBasedTableVersionFormat,
                compression_output);
  return *compression_output;
}

void BlobDBImpl::GetCompactionContext(BlobCompactionContext* context) {
  ReadLock l(&mutex_);

  context->next_file_number = next_file_number_.load();
  context->current_blob_files.clear();
  for (auto& p : blob_files_) {
    context->current_blob_files.insert(p.first);
  }
  context->fifo_eviction_seq = fifo_eviction_seq_;
  context->evict_expiration_up_to = evict_expiration_up_to_;
}

void BlobDBImpl::UpdateLiveSSTSize() {
  uint64_t live_sst_size = 0;
  bool ok = GetIntProperty(DB::Properties::kLiveSstFilesSize, &live_sst_size);
  if (ok) {
    live_sst_size_.store(live_sst_size);
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Updated total SST file size: %" PRIu64 " bytes.",
                   live_sst_size);
  } else {
    ROCKS_LOG_ERROR(
        db_options_.info_log,
        "Failed to update total SST file size after flush or compaction.");
  }
  {
    // Trigger FIFO eviction if needed.
    MutexLock l(&write_mutex_);
    Status s = CheckSizeAndEvictBlobFiles(0, true /*force*/);
    if (s.IsNoSpace()) {
      ROCKS_LOG_WARN(db_options_.info_log,
                     "DB grow out-of-space after SST size updated. Current live"
                     " SST size: %" PRIu64
                     " , current blob files size: %" PRIu64 ".",
                     live_sst_size_.load(), total_blob_size_.load());
    }
  }
}

Status BlobDBImpl::CheckSizeAndEvictBlobFiles(uint64_t blob_size,
                                              bool force_evict) {
  write_mutex_.AssertHeld();

  uint64_t live_sst_size = live_sst_size_.load();
  if (bdb_options_.max_db_size == 0 ||
      live_sst_size + total_blob_size_.load() + blob_size <=
          bdb_options_.max_db_size) {
    return Status::OK();
  }

  if (bdb_options_.is_fifo == false ||
      (!force_evict && live_sst_size + blob_size > bdb_options_.max_db_size)) {
    // FIFO eviction is disabled, or no space to insert new blob even we evict
    // all blob files.
    return Status::NoSpace(
        "Write failed, as writing it would exceed max_db_size limit.");
  }

  std::vector<std::shared_ptr<BlobFile>> candidate_files;
  CopyBlobFiles(&candidate_files);
  std::sort(candidate_files.begin(), candidate_files.end(),
            BlobFileComparator());
  fifo_eviction_seq_ = GetLatestSequenceNumber();

  WriteLock l(&mutex_);

  while (!candidate_files.empty() &&
         live_sst_size + total_blob_size_.load() + blob_size >
             bdb_options_.max_db_size) {
    std::shared_ptr<BlobFile> blob_file = candidate_files.back();
    candidate_files.pop_back();
    WriteLock file_lock(&blob_file->mutex_);
    if (blob_file->Obsolete()) {
      // File already obsoleted by someone else.
      continue;
    }
    // FIFO eviction can evict open blob files.
    if (!blob_file->Immutable()) {
      Status s = CloseBlobFile(blob_file, false /*need_lock*/);
      if (!s.ok()) {
        return s;
      }
    }
    assert(blob_file->Immutable());
    auto expiration_range = blob_file->GetExpirationRange();
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Evict oldest blob file since DB out of space. Current "
                   "live SST file size: %" PRIu64 ", total blob size: %" PRIu64
                   ", max db size: %" PRIu64 ", evicted blob file #%" PRIu64
                   ".",
                   live_sst_size, total_blob_size_.load(),
                   bdb_options_.max_db_size, blob_file->BlobFileNumber());
    ObsoleteBlobFile(blob_file, fifo_eviction_seq_, true /*update_size*/);
    evict_expiration_up_to_ = expiration_range.first;
    RecordTick(statistics_, BLOB_DB_FIFO_NUM_FILES_EVICTED);
    RecordTick(statistics_, BLOB_DB_FIFO_NUM_KEYS_EVICTED,
               blob_file->BlobCount());
    RecordTick(statistics_, BLOB_DB_FIFO_BYTES_EVICTED,
               blob_file->GetFileSize());
    TEST_SYNC_POINT("BlobDBImpl::EvictOldestBlobFile:Evicted");
  }
  if (live_sst_size + total_blob_size_.load() + blob_size >
      bdb_options_.max_db_size) {
    return Status::NoSpace(
        "Write failed, as writing it would exceed max_db_size limit.");
  }
  return Status::OK();
}

Status BlobDBImpl::AppendBlob(const std::shared_ptr<BlobFile>& bfile,
                              const std::string& headerbuf, const Slice& key,
                              const Slice& value, uint64_t expiration,
                              std::string* index_entry) {
  Status s;
  uint64_t blob_offset = 0;
  uint64_t key_offset = 0;
  {
    WriteLock lockbfile_w(&bfile->mutex_);
    std::shared_ptr<Writer> writer;
    s = CheckOrCreateWriterLocked(bfile, &writer);
    if (!s.ok()) {
      return s;
    }

    // write the blob to the blob log.
    s = writer->EmitPhysicalRecord(headerbuf, key, value, &key_offset,
                                   &blob_offset);
  }

  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Invalid status in AppendBlob: %s status: '%s'",
                    bfile->PathName().c_str(), s.ToString().c_str());
    return s;
  }

  // increment blob count
  bfile->blob_count_++;

  uint64_t size_put = headerbuf.size() + key.size() + value.size();
  bfile->file_size_ += size_put;
  total_blob_size_ += size_put;

  if (expiration == kNoExpiration) {
    BlobIndex::EncodeBlob(index_entry, bfile->BlobFileNumber(), blob_offset,
                          value.size(), bdb_options_.compression);
  } else {
    BlobIndex::EncodeBlobTTL(index_entry, expiration, bfile->BlobFileNumber(),
                             blob_offset, value.size(),
                             bdb_options_.compression);
  }

  return s;
}

std::vector<Status> BlobDBImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  StopWatch multiget_sw(env_, statistics_, BLOB_DB_MULTIGET_MICROS);
  RecordTick(statistics_, BLOB_DB_NUM_MULTIGET);
  // Get a snapshot to avoid blob file get deleted between we
  // fetch and index entry and reading from the file.
  ReadOptions ro(read_options);
  bool snapshot_created = SetSnapshotIfNeeded(&ro);

  std::vector<Status> statuses;
  statuses.reserve(keys.size());
  values->clear();
  values->reserve(keys.size());
  PinnableSlice value;
  for (size_t i = 0; i < keys.size(); i++) {
    statuses.push_back(Get(ro, DefaultColumnFamily(), keys[i], &value));
    values->push_back(value.ToString());
    value.Reset();
  }
  if (snapshot_created) {
    db_->ReleaseSnapshot(ro.snapshot);
  }
  return statuses;
}

bool BlobDBImpl::SetSnapshotIfNeeded(ReadOptions* read_options) {
  assert(read_options != nullptr);
  if (read_options->snapshot != nullptr) {
    return false;
  }
  read_options->snapshot = db_->GetSnapshot();
  return true;
}

Status BlobDBImpl::GetBlobValue(const Slice& key, const Slice& index_entry,
                                PinnableSlice* value, uint64_t* expiration) {
  assert(value != nullptr);
  BlobIndex blob_index;
  Status s = blob_index.DecodeFrom(index_entry);
  if (!s.ok()) {
    return s;
  }
  if (blob_index.HasTTL() && blob_index.expiration() <= EpochNow()) {
    return Status::NotFound("Key expired");
  }
  if (expiration != nullptr) {
    if (blob_index.HasTTL()) {
      *expiration = blob_index.expiration();
    } else {
      *expiration = kNoExpiration;
    }
  }
  if (blob_index.IsInlined()) {
    // TODO(yiwu): If index_entry is a PinnableSlice, we can also pin the same
    // memory buffer to avoid extra copy.
    value->PinSelf(blob_index.value());
    return Status::OK();
  }
  if (blob_index.size() == 0) {
    value->PinSelf("");
    return Status::OK();
  }

  // offset has to have certain min, as we will read CRC
  // later from the Blob Header, which needs to be also a
  // valid offset.
  if (blob_index.offset() <
      (BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size())) {
    if (debug_level_ >= 2) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Invalid blob index file_number: %" PRIu64
                      " blob_offset: %" PRIu64 " blob_size: %" PRIu64
                      " key: %s",
                      blob_index.file_number(), blob_index.offset(),
                      blob_index.size(), key.data());
    }
    return Status::NotFound("Invalid blob offset");
  }

  std::shared_ptr<BlobFile> bfile;
  {
    ReadLock rl(&mutex_);
    auto hitr = blob_files_.find(blob_index.file_number());

    // file was deleted
    if (hitr == blob_files_.end()) {
      return Status::NotFound("Blob Not Found as blob file missing");
    }

    bfile = hitr->second;
  }

  if (blob_index.size() == 0 && value != nullptr) {
    value->PinSelf("");
    return Status::OK();
  }

  // takes locks when called
  std::shared_ptr<RandomAccessFileReader> reader;
  s = GetBlobFileReader(bfile, &reader);
  if (!s.ok()) {
    return s;
  }

  assert(blob_index.offset() > key.size() + sizeof(uint32_t));
  uint64_t record_offset = blob_index.offset() - key.size() - sizeof(uint32_t);
  uint64_t record_size = sizeof(uint32_t) + key.size() + blob_index.size();

  // Allocate the buffer. This is safe in C++11
  std::string buffer_str(static_cast<size_t>(record_size), static_cast<char>(0));
  char* buffer = &buffer_str[0];

  // A partial blob record contain checksum, key and value.
  Slice blob_record;
  {
    StopWatch read_sw(env_, statistics_, BLOB_DB_BLOB_FILE_READ_MICROS);
    s = reader->Read(record_offset, static_cast<size_t>(record_size), &blob_record, buffer);
    RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, blob_record.size());
  }
  if (!s.ok()) {
    ROCKS_LOG_DEBUG(db_options_.info_log,
                    "Failed to read blob from blob file %" PRIu64
                    ", blob_offset: %" PRIu64 ", blob_size: %" PRIu64
                    ", key_size: " PRIu64 ", read " PRIu64
                    " bytes, status: '%s'",
                    bfile->BlobFileNumber(), blob_index.offset(),
                    blob_index.size(), key.size(), s.ToString().c_str());
    return s;
  }
  if (blob_record.size() != record_size) {
    ROCKS_LOG_DEBUG(db_options_.info_log,
                    "Failed to read blob from blob file %" PRIu64
                    ", blob_offset: %" PRIu64 ", blob_size: %" PRIu64
                    ", key_size: " PRIu64 ", read " PRIu64
                    " bytes, status: '%s'",
                    bfile->BlobFileNumber(), blob_index.offset(),
                    blob_index.size(), key.size(), s.ToString().c_str());

    return Status::Corruption("Failed to retrieve blob from blob index.");
  }
  Slice crc_slice(blob_record.data(), sizeof(uint32_t));
  Slice blob_value(blob_record.data() + sizeof(uint32_t) + key.size(),
                   static_cast<size_t>(blob_index.size()));
  uint32_t crc_exp;
  if (!GetFixed32(&crc_slice, &crc_exp)) {
    ROCKS_LOG_DEBUG(db_options_.info_log,
                    "Unable to decode CRC from blob file %" PRIu64
                    ", blob_offset: %" PRIu64 ", blob_size: %" PRIu64
                    ", key size: %" PRIu64 ", status: '%s'",
                    bfile->BlobFileNumber(), blob_index.offset(),
                    blob_index.size(), key.size(), s.ToString().c_str());
    return Status::Corruption("Unable to decode checksum.");
  }

  uint32_t crc = crc32c::Value(blob_record.data() + sizeof(uint32_t),
                               blob_record.size() - sizeof(uint32_t));
  crc = crc32c::Mask(crc);  // Adjust for storage
  if (crc != crc_exp) {
    if (debug_level_ >= 2) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Blob crc mismatch file: %s blob_offset: %" PRIu64
                      " blob_size: %" PRIu64 " key: %s status: '%s'",
                      bfile->PathName().c_str(), blob_index.offset(),
                      blob_index.size(), key.data(), s.ToString().c_str());
    }
    return Status::Corruption("Corruption. Blob CRC mismatch");
  }

  if (bfile->compression() == kNoCompression) {
    value->PinSelf(blob_value);
  } else {
    BlockContents contents;
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily());
    {
      StopWatch decompression_sw(env_, statistics_,
                                 BLOB_DB_DECOMPRESSION_MICROS);
      UncompressionContext context(bfile->compression());
      UncompressionInfo info(context, UncompressionDict::GetEmptyDict(),
                             bfile->compression());
      s = UncompressBlockContentsForCompressionType(
          info, blob_value.data(), blob_value.size(), &contents,
          kBlockBasedTableVersionFormat, *(cfh->cfd()->ioptions()));
    }
    value->PinSelf(contents.data);
  }

  return s;
}

Status BlobDBImpl::Get(const ReadOptions& read_options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       PinnableSlice* value) {
  return Get(read_options, column_family, key, value, nullptr /*expiration*/);
}

Status BlobDBImpl::Get(const ReadOptions& read_options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       PinnableSlice* value, uint64_t* expiration) {
  StopWatch get_sw(env_, statistics_, BLOB_DB_GET_MICROS);
  RecordTick(statistics_, BLOB_DB_NUM_GET);
  return GetImpl(read_options, column_family, key, value, expiration);
}

Status BlobDBImpl::GetImpl(const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* value, uint64_t* expiration) {
  if (column_family != DefaultColumnFamily()) {
    return Status::NotSupported(
        "Blob DB doesn't support non-default column family.");
  }
  // Get a snapshot to avoid blob file get deleted between we
  // fetch and index entry and reading from the file.
  // TODO(yiwu): For Get() retry if file not found would be a simpler strategy.
  ReadOptions ro(read_options);
  bool snapshot_created = SetSnapshotIfNeeded(&ro);

  PinnableSlice index_entry;
  Status s;
  bool is_blob_index = false;
  s = db_impl_->GetImpl(ro, column_family, key, &index_entry,
                        nullptr /*value_found*/, nullptr /*read_callback*/,
                        &is_blob_index);
  TEST_SYNC_POINT("BlobDBImpl::Get:AfterIndexEntryGet:1");
  TEST_SYNC_POINT("BlobDBImpl::Get:AfterIndexEntryGet:2");
  if (expiration != nullptr) {
    *expiration = kNoExpiration;
  }
  RecordTick(statistics_, BLOB_DB_NUM_KEYS_READ);
  if (s.ok()) {
    if (is_blob_index) {
      s = GetBlobValue(key, index_entry, value, expiration);
    } else {
      // The index entry is the value itself in this case.
      value->PinSelf(index_entry);
    }
    RecordTick(statistics_, BLOB_DB_BYTES_READ, value->size());
  }
  if (snapshot_created) {
    db_->ReleaseSnapshot(ro.snapshot);
  }
  return s;
}

std::pair<bool, int64_t> BlobDBImpl::SanityCheck(bool aborted) {
  if (aborted) {
    return std::make_pair(false, -1);
  }

  ROCKS_LOG_INFO(db_options_.info_log, "Starting Sanity Check");
  ROCKS_LOG_INFO(db_options_.info_log, "Number of files %" PRIu64,
                 blob_files_.size());
  ROCKS_LOG_INFO(db_options_.info_log, "Number of open files %" PRIu64,
                 open_ttl_files_.size());

  for (auto bfile : open_ttl_files_) {
    assert(!bfile->Immutable());
  }

  uint64_t now = EpochNow();

  for (auto blob_file_pair : blob_files_) {
    auto blob_file = blob_file_pair.second;
    char buf[1000];
    int pos = snprintf(buf, sizeof(buf),
                       "Blob file %" PRIu64 ", size %" PRIu64
                       ", blob count %" PRIu64 ", immutable %d",
                       blob_file->BlobFileNumber(), blob_file->GetFileSize(),
                       blob_file->BlobCount(), blob_file->Immutable());
    if (blob_file->HasTTL()) {
      auto expiration_range = blob_file->GetExpirationRange();
      pos += snprintf(buf + pos, sizeof(buf) - pos,
                      ", expiration range (%" PRIu64 ", %" PRIu64 ")",
                      expiration_range.first, expiration_range.second);
      if (!blob_file->Obsolete()) {
        pos += snprintf(buf + pos, sizeof(buf) - pos,
                        ", expire in %" PRIu64 " seconds",
                        expiration_range.second - now);
      }
    }
    if (blob_file->Obsolete()) {
      pos += snprintf(buf + pos, sizeof(buf) - pos, ", obsolete at %" PRIu64,
                      blob_file->GetObsoleteSequence());
    }
    snprintf(buf + pos, sizeof(buf) - pos, ".");
    ROCKS_LOG_INFO(db_options_.info_log, "%s", buf);
  }

  // reschedule
  return std::make_pair(true, -1);
}

Status BlobDBImpl::CloseBlobFile(std::shared_ptr<BlobFile> bfile,
                                 bool need_lock) {
  assert(bfile != nullptr);
  write_mutex_.AssertHeld();
  Status s;
  ROCKS_LOG_INFO(db_options_.info_log,
                 "Closing blob file %" PRIu64 ". Path: %s",
                 bfile->BlobFileNumber(), bfile->PathName().c_str());
  {
    std::unique_ptr<WriteLock> lock;
    if (need_lock) {
      lock.reset(new WriteLock(&mutex_));
    }

    if (bfile->HasTTL()) {
      size_t erased __attribute__((__unused__));
      erased = open_ttl_files_.erase(bfile);
    } else if (bfile == open_non_ttl_file_) {
      open_non_ttl_file_ = nullptr;
    }
  }

  if (!bfile->closed_.load()) {
    std::unique_ptr<WriteLock> file_lock;
    if (need_lock) {
      file_lock.reset(new WriteLock(&bfile->mutex_));
    }
    s = bfile->WriteFooterAndCloseLocked();
  }

  if (s.ok()) {
    total_blob_size_ += BlobLogFooter::kSize;
  } else {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to close blob file %" PRIu64 "with error: %s",
                    bfile->BlobFileNumber(), s.ToString().c_str());
  }

  return s;
}

Status BlobDBImpl::CloseBlobFileIfNeeded(std::shared_ptr<BlobFile>& bfile) {
  // atomic read
  if (bfile->GetFileSize() < bdb_options_.blob_file_size) {
    return Status::OK();
  }
  return CloseBlobFile(bfile);
}

void BlobDBImpl::ObsoleteBlobFile(std::shared_ptr<BlobFile> blob_file,
                                  SequenceNumber obsolete_seq,
                                  bool update_size) {
  // Should hold write lock of mutex_ or during DB open.
  blob_file->MarkObsolete(obsolete_seq);
  obsolete_files_.push_back(blob_file);
  assert(total_blob_size_.load() >= blob_file->GetFileSize());
  if (update_size) {
    total_blob_size_ -= blob_file->GetFileSize();
  }
}

bool BlobDBImpl::VisibleToActiveSnapshot(
    const std::shared_ptr<BlobFile>& bfile) {
  assert(bfile->Obsolete());

  // We check whether the oldest snapshot is no less than the last sequence
  // by the time the blob file become obsolete. If so, the blob file is not
  // visible to all existing snapshots.
  //
  // If we keep track of the earliest sequence of the keys in the blob file,
  // we could instead check if there's a snapshot falls in range
  // [earliest_sequence, obsolete_sequence). But doing so will make the
  // implementation more complicated.
  SequenceNumber obsolete_sequence = bfile->GetObsoleteSequence();
  SequenceNumber oldest_snapshot = kMaxSequenceNumber;
  {
    // Need to lock DBImpl mutex before access snapshot list.
    InstrumentedMutexLock l(db_impl_->mutex());
    auto& snapshots = db_impl_->snapshots();
    if (!snapshots.empty()) {
      oldest_snapshot = snapshots.oldest()->GetSequenceNumber();
    }
  }
  bool visible = oldest_snapshot < obsolete_sequence;
  if (visible) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Obsolete blob file %" PRIu64 " (obsolete at %" PRIu64
                   ") visible to oldest snapshot %" PRIu64 ".",
                   bfile->BlobFileNumber(), obsolete_sequence, oldest_snapshot);
  }
  return visible;
}

std::pair<bool, int64_t> BlobDBImpl::EvictExpiredFiles(bool aborted) {
  if (aborted) {
    return std::make_pair(false, -1);
  }

  TEST_SYNC_POINT("BlobDBImpl::EvictExpiredFiles:0");
  TEST_SYNC_POINT("BlobDBImpl::EvictExpiredFiles:1");

  std::vector<std::shared_ptr<BlobFile>> process_files;
  uint64_t now = EpochNow();
  {
    ReadLock rl(&mutex_);
    for (auto p : blob_files_) {
      auto& blob_file = p.second;
      ReadLock file_lock(&blob_file->mutex_);
      if (blob_file->HasTTL() && !blob_file->Obsolete() &&
          blob_file->GetExpirationRange().second <= now) {
        process_files.push_back(blob_file);
      }
    }
  }

  TEST_SYNC_POINT("BlobDBImpl::EvictExpiredFiles:2");
  TEST_SYNC_POINT("BlobDBImpl::EvictExpiredFiles:3");
  TEST_SYNC_POINT_CALLBACK("BlobDBImpl::EvictExpiredFiles:cb", nullptr);

  SequenceNumber seq = GetLatestSequenceNumber();
  {
    MutexLock l(&write_mutex_);
    for (auto& blob_file : process_files) {
      WriteLock file_lock(&blob_file->mutex_);
      if (!blob_file->Immutable()) {
        CloseBlobFile(blob_file, false /*need_lock*/);
      }
      // Need to double check if the file is obsolete.
      if (!blob_file->Obsolete()) {
        ObsoleteBlobFile(blob_file, seq, true /*update_size*/);
      }
    }
  }

  return std::make_pair(true, -1);
}

Status BlobDBImpl::SyncBlobFiles() {
  MutexLock l(&write_mutex_);

  std::vector<std::shared_ptr<BlobFile>> process_files;
  {
    ReadLock rl(&mutex_);
    for (auto fitr : open_ttl_files_) {
      process_files.push_back(fitr);
    }
    if (open_non_ttl_file_ != nullptr) {
      process_files.push_back(open_non_ttl_file_);
    }
  }

  Status s;
  for (auto& blob_file : process_files) {
    s = blob_file->Fsync();
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Failed to sync blob file %" PRIu64 ", status: %s",
                      blob_file->BlobFileNumber(), s.ToString().c_str());
      return s;
    }
  }

  s = dir_ent_->Fsync();
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failed to sync blob directory, status: %s",
                    s.ToString().c_str());
  }
  return s;
}

std::pair<bool, int64_t> BlobDBImpl::ReclaimOpenFiles(bool aborted) {
  if (aborted) return std::make_pair(false, -1);

  if (open_file_count_.load() < kOpenFilesTrigger) {
    return std::make_pair(true, -1);
  }

  // in the future, we should sort by last_access_
  // instead of closing every file
  ReadLock rl(&mutex_);
  for (auto const& ent : blob_files_) {
    auto bfile = ent.second;
    if (bfile->last_access_.load() == -1) continue;

    WriteLock lockbfile_w(&bfile->mutex_);
    CloseRandomAccessLocked(bfile);
  }

  return std::make_pair(true, -1);
}

// Write callback for garbage collection to check if key has been updated
// since last read. Similar to how OptimisticTransaction works. See inline
// comment in GCFileAndUpdateLSM().
class BlobDBImpl::GarbageCollectionWriteCallback : public WriteCallback {
 public:
  GarbageCollectionWriteCallback(ColumnFamilyData* cfd, const Slice& key,
                                 SequenceNumber upper_bound)
      : cfd_(cfd), key_(key), upper_bound_(upper_bound) {}

  Status Callback(DB* db) override {
    auto* db_impl = reinterpret_cast<DBImpl*>(db);
    auto* sv = db_impl->GetAndRefSuperVersion(cfd_);
    SequenceNumber latest_seq = 0;
    bool found_record_for_key = false;
    bool is_blob_index = false;
    Status s = db_impl->GetLatestSequenceForKey(
        sv, key_, false /*cache_only*/, &latest_seq, &found_record_for_key,
        &is_blob_index);
    db_impl->ReturnAndCleanupSuperVersion(cfd_, sv);
    if (!s.ok() && !s.IsNotFound()) {
      // Error.
      assert(!s.IsBusy());
      return s;
    }
    if (s.IsNotFound()) {
      assert(!found_record_for_key);
      return Status::Busy("Key deleted");
    }
    assert(found_record_for_key);
    assert(is_blob_index);
    if (latest_seq > upper_bound_) {
      return Status::Busy("Key overwritten");
    }
    return s;
  }

  bool AllowWriteBatching() override { return false; }

 private:
  ColumnFamilyData* cfd_;
  // Key to check
  Slice key_;
  // Upper bound of sequence number to proceed.
  SequenceNumber upper_bound_;
};

// iterate over the blobs sequentially and check if the blob sequence number
// is the latest. If it is the latest, preserve it, otherwise delete it
// if it is TTL based, and the TTL has expired, then
// we can blow the entity if the key is still the latest or the Key is not
// found
// WHAT HAPPENS IF THE KEY HAS BEEN OVERRIDEN. Then we can drop the blob
// without doing anything if the earliest snapshot is not
// referring to that sequence number, i.e. it is later than the sequence number
// of the new key
//
// if it is not TTL based, then we can blow the key if the key has been
// DELETED in the LSM
Status BlobDBImpl::GCFileAndUpdateLSM(const std::shared_ptr<BlobFile>& bfptr,
                                      GCStats* gc_stats) {
  StopWatch gc_sw(env_, statistics_, BLOB_DB_GC_MICROS);
  uint64_t now = EpochNow();

  std::shared_ptr<Reader> reader =
      bfptr->OpenRandomAccessReader(env_, db_options_, env_options_);
  if (!reader) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "File sequential reader could not be opened",
                    bfptr->PathName().c_str());
    return Status::IOError("failed to create sequential reader");
  }

  BlobLogHeader header;
  Status s = reader->ReadHeader(&header);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log,
                    "Failure to read header for blob-file %s",
                    bfptr->PathName().c_str());
    return s;
  }

  auto cfh = db_impl_->DefaultColumnFamily();
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
  auto column_family_id = cfd->GetID();
  bool has_ttl = header.has_ttl;

  // this reads the key but skips the blob
  Reader::ReadLevel shallow = Reader::kReadHeaderKey;

  bool file_expired = has_ttl && now >= bfptr->GetExpirationRange().second;

  if (!file_expired) {
    // read the blob because you have to write it back to new file
    shallow = Reader::kReadHeaderKeyBlob;
  }

  BlobLogRecord record;
  std::shared_ptr<BlobFile> newfile;
  std::shared_ptr<Writer> new_writer;
  uint64_t blob_offset = 0;

  while (true) {
    assert(s.ok());

    // Read the next blob record.
    Status read_record_status =
        reader->ReadRecord(&record, shallow, &blob_offset);
    // Exit if we reach the end of blob file.
    // TODO(yiwu): properly handle ReadRecord error.
    if (!read_record_status.ok()) {
      break;
    }
    gc_stats->blob_count++;

    // Similar to OptimisticTransaction, we obtain latest_seq from
    // base DB, which is guaranteed to be no smaller than the sequence of
    // current key. We use a WriteCallback on write to check the key sequence
    // on write. If the key sequence is larger than latest_seq, we know
    // a new versions is inserted and the old blob can be disgard.
    //
    // We cannot use OptimisticTransaction because we need to pass
    // is_blob_index flag to GetImpl.
    SequenceNumber latest_seq = GetLatestSequenceNumber();
    bool is_blob_index = false;
    PinnableSlice index_entry;
    Status get_status = db_impl_->GetImpl(
        ReadOptions(), cfh, record.key, &index_entry, nullptr /*value_found*/,
        nullptr /*read_callback*/, &is_blob_index);
    TEST_SYNC_POINT("BlobDBImpl::GCFileAndUpdateLSM:AfterGetFromBaseDB");
    if (!get_status.ok() && !get_status.IsNotFound()) {
      // error
      s = get_status;
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Error while getting index entry: %s",
                      s.ToString().c_str());
      break;
    }
    if (get_status.IsNotFound() || !is_blob_index) {
      // Either the key is deleted or updated with a newer version whish is
      // inlined in LSM.
      gc_stats->num_keys_overwritten++;
      gc_stats->bytes_overwritten += record.record_size();
      continue;
    }

    BlobIndex blob_index;
    s = blob_index.DecodeFrom(index_entry);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "Error while decoding index entry: %s",
                      s.ToString().c_str());
      break;
    }
    if (blob_index.IsInlined() ||
        blob_index.file_number() != bfptr->BlobFileNumber() ||
        blob_index.offset() != blob_offset) {
      // Key has been overwritten. Drop the blob record.
      gc_stats->num_keys_overwritten++;
      gc_stats->bytes_overwritten += record.record_size();
      continue;
    }

    GarbageCollectionWriteCallback callback(cfd, record.key, latest_seq);

    // If key has expired, remove it from base DB.
    // TODO(yiwu): Blob indexes will be remove by BlobIndexCompactionFilter.
    // We can just drop the blob record.
    if (file_expired || (has_ttl && now >= record.expiration)) {
      gc_stats->num_keys_expired++;
      gc_stats->bytes_expired += record.record_size();
      TEST_SYNC_POINT("BlobDBImpl::GCFileAndUpdateLSM:BeforeDelete");
      WriteBatch delete_batch;
      Status delete_status = delete_batch.Delete(record.key);
      if (delete_status.ok()) {
        delete_status = db_impl_->WriteWithCallback(WriteOptions(),
                                                    &delete_batch, &callback);
      }
      if (!delete_status.ok() && !delete_status.IsBusy()) {
        // We hit an error.
        s = delete_status;
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "Error while deleting expired key: %s",
                        s.ToString().c_str());
        break;
      }
      // Continue to next blob record or retry.
      continue;
    }

    // Relocate the blob record to new file.
    if (!newfile) {
      // new file
      std::string reason("GC of ");
      reason += bfptr->PathName();
      newfile = NewBlobFile(reason);

      s = CheckOrCreateWriterLocked(newfile, &new_writer);
      if (!s.ok()) {
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "Failed to open file %s for writer, error: %s",
                        newfile->PathName().c_str(), s.ToString().c_str());
        break;
      }
      // Can't use header beyond this point
      newfile->header_ = std::move(header);
      newfile->header_valid_ = true;
      newfile->file_size_ = BlobLogHeader::kSize;
      newfile->SetColumnFamilyId(bfptr->column_family_id());
      newfile->SetHasTTL(bfptr->HasTTL());
      newfile->SetCompression(bfptr->compression());
      newfile->expiration_range_ = bfptr->expiration_range_;

      s = new_writer->WriteHeader(newfile->header_);
      if (!s.ok()) {
        ROCKS_LOG_ERROR(db_options_.info_log,
                        "File: %s - header writing failed",
                        newfile->PathName().c_str());
        break;
      }

      // We don't add the file to open_ttl_files_ or open_non_ttl_files_, to
      // avoid user writes writing to the file, and avoid
      // EvictExpiredFiles close the file by mistake.
      WriteLock wl(&mutex_);
      blob_files_.insert(std::make_pair(newfile->BlobFileNumber(), newfile));
    }

    std::string new_index_entry;
    uint64_t new_blob_offset = 0;
    uint64_t new_key_offset = 0;
    // write the blob to the blob log.
    s = new_writer->AddRecord(record.key, record.value, record.expiration,
                              &new_key_offset, &new_blob_offset);

    BlobIndex::EncodeBlob(&new_index_entry, newfile->BlobFileNumber(),
                          new_blob_offset, record.value.size(),
                          bdb_options_.compression);

    newfile->blob_count_++;
    newfile->file_size_ +=
        BlobLogRecord::kHeaderSize + record.key.size() + record.value.size();

    TEST_SYNC_POINT("BlobDBImpl::GCFileAndUpdateLSM:BeforeRelocate");
    WriteBatch rewrite_batch;
    Status rewrite_status = WriteBatchInternal::PutBlobIndex(
        &rewrite_batch, column_family_id, record.key, new_index_entry);
    if (rewrite_status.ok()) {
      rewrite_status = db_impl_->WriteWithCallback(WriteOptions(),
                                                   &rewrite_batch, &callback);
    }
    if (rewrite_status.ok()) {
      gc_stats->num_keys_relocated++;
      gc_stats->bytes_relocated += record.record_size();
    } else if (rewrite_status.IsBusy()) {
      // The key is overwritten in the meanwhile. Drop the blob record.
      gc_stats->num_keys_overwritten++;
      gc_stats->bytes_overwritten += record.record_size();
    } else {
      // We hit an error.
      s = rewrite_status;
      ROCKS_LOG_ERROR(db_options_.info_log, "Error while relocating key: %s",
                      s.ToString().c_str());
      break;
    }
  }  // end of ReadRecord loop

  {
    WriteLock wl(&mutex_);
    ObsoleteBlobFile(bfptr, GetLatestSequenceNumber(), true /*update_size*/);
  }

  ROCKS_LOG_INFO(
      db_options_.info_log,
      "%s blob file %" PRIu64 ". Total blob records: %" PRIu64
      ", expired: %" PRIu64 " keys/%" PRIu64
      " bytes, updated or deleted by user: %" PRIu64 " keys/%" PRIu64
      " bytes, rewrite to new file: %" PRIu64 " keys/%" PRIu64 " bytes.",
      s.ok() ? "Successfully garbage collected" : "Failed to garbage collect",
      bfptr->BlobFileNumber(), gc_stats->blob_count, gc_stats->num_keys_expired,
      gc_stats->bytes_expired, gc_stats->num_keys_overwritten,
      gc_stats->bytes_overwritten, gc_stats->num_keys_relocated,
      gc_stats->bytes_relocated);
  RecordTick(statistics_, BLOB_DB_GC_NUM_FILES);
  RecordTick(statistics_, BLOB_DB_GC_NUM_KEYS_OVERWRITTEN,
             gc_stats->num_keys_overwritten);
  RecordTick(statistics_, BLOB_DB_GC_NUM_KEYS_EXPIRED,
             gc_stats->num_keys_expired);
  RecordTick(statistics_, BLOB_DB_GC_BYTES_OVERWRITTEN,
             gc_stats->bytes_overwritten);
  RecordTick(statistics_, BLOB_DB_GC_BYTES_EXPIRED, gc_stats->bytes_expired);
  if (newfile != nullptr) {
    {
      MutexLock l(&write_mutex_);
      CloseBlobFile(newfile);
    }
    total_blob_size_ += newfile->file_size_;
    ROCKS_LOG_INFO(db_options_.info_log, "New blob file %" PRIu64 ".",
                   newfile->BlobFileNumber());
    RecordTick(statistics_, BLOB_DB_GC_NUM_NEW_FILES);
    RecordTick(statistics_, BLOB_DB_GC_NUM_KEYS_RELOCATED,
               gc_stats->num_keys_relocated);
    RecordTick(statistics_, BLOB_DB_GC_BYTES_RELOCATED,
               gc_stats->bytes_relocated);
  }
  if (!s.ok()) {
    RecordTick(statistics_, BLOB_DB_GC_FAILURES);
  }
  return s;
}

std::pair<bool, int64_t> BlobDBImpl::DeleteObsoleteFiles(bool aborted) {
  if (aborted) {
    return std::make_pair(false, -1);
  }

  MutexLock delete_file_lock(&delete_file_mutex_);
  if (disable_file_deletions_ > 0) {
    return std::make_pair(true, -1);
  }

  std::list<std::shared_ptr<BlobFile>> tobsolete;
  {
    WriteLock wl(&mutex_);
    if (obsolete_files_.empty()) {
      return std::make_pair(true, -1);
    }
    tobsolete.swap(obsolete_files_);
  }

  bool file_deleted = false;
  for (auto iter = tobsolete.begin(); iter != tobsolete.end();) {
    auto bfile = *iter;
    {
      ReadLock lockbfile_r(&bfile->mutex_);
      if (VisibleToActiveSnapshot(bfile)) {
        ROCKS_LOG_INFO(db_options_.info_log,
                       "Could not delete file due to snapshot failure %s",
                       bfile->PathName().c_str());
        ++iter;
        continue;
      }
    }
    ROCKS_LOG_INFO(db_options_.info_log,
                   "Will delete file due to snapshot success %s",
                   bfile->PathName().c_str());

    blob_files_.erase(bfile->BlobFileNumber());
    Status s = DeleteDBFile(&(db_impl_->immutable_db_options()),
                             bfile->PathName(), blob_dir_, true);
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log,
                      "File failed to be deleted as obsolete %s",
                      bfile->PathName().c_str());
      ++iter;
      continue;
    }

    file_deleted = true;
    ROCKS_LOG_INFO(db_options_.info_log,
                   "File deleted as obsolete from blob dir %s",
                   bfile->PathName().c_str());

    iter = tobsolete.erase(iter);
  }

  // directory change. Fsync
  if (file_deleted) {
    Status s = dir_ent_->Fsync();
    if (!s.ok()) {
      ROCKS_LOG_ERROR(db_options_.info_log, "Failed to sync dir %s: %s",
                      blob_dir_.c_str(), s.ToString().c_str());
    }
  }

  // put files back into obsolete if for some reason, delete failed
  if (!tobsolete.empty()) {
    WriteLock wl(&mutex_);
    for (auto bfile : tobsolete) {
      obsolete_files_.push_front(bfile);
    }
  }

  return std::make_pair(!aborted, -1);
}

void BlobDBImpl::CopyBlobFiles(
    std::vector<std::shared_ptr<BlobFile>>* bfiles_copy) {
  ReadLock rl(&mutex_);
  for (auto const& p : blob_files_) {
    bfiles_copy->push_back(p.second);
  }
}

std::pair<bool, int64_t> BlobDBImpl::RunGC(bool aborted) {
  if (aborted) {
    return std::make_pair(false, -1);
  }

  // TODO(yiwu): Garbage collection implementation.

  // reschedule
  return std::make_pair(true, -1);
}

Iterator* BlobDBImpl::NewIterator(const ReadOptions& read_options) {
  auto* cfd =
      reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->cfd();
  // Get a snapshot to avoid blob file get deleted between we
  // fetch and index entry and reading from the file.
  ManagedSnapshot* own_snapshot = nullptr;
  const Snapshot* snapshot = read_options.snapshot;
  if (snapshot == nullptr) {
    own_snapshot = new ManagedSnapshot(db_);
    snapshot = own_snapshot->snapshot();
  }
  auto* iter = db_impl_->NewIteratorImpl(
      read_options, cfd, snapshot->GetSequenceNumber(),
      nullptr /*read_callback*/, true /*allow_blob*/);
  return new BlobDBIterator(own_snapshot, iter, this, env_, statistics_);
}

Status DestroyBlobDB(const std::string& dbname, const Options& options,
                     const BlobDBOptions& bdb_options) {
  const ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;

  Status status;
  std::string blobdir;
  blobdir = (bdb_options.path_relative) ? dbname + "/" + bdb_options.blob_dir
                                        : bdb_options.blob_dir;

  std::vector<std::string> filenames;
  env->GetChildren(blobdir, &filenames);

  for (const auto& f : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kBlobFile) {
      Status del = DeleteDBFile(&soptions, blobdir + "/" + f, blobdir, true);
      if (status.ok() && !del.ok()) {
        status = del;
      }
    }
  }
  env->DeleteDir(blobdir);

  Status destroy = DestroyDB(dbname, options);
  if (status.ok() && !destroy.ok()) {
    status = destroy;
  }

  return status;
}

#ifndef NDEBUG
Status BlobDBImpl::TEST_GetBlobValue(const Slice& key, const Slice& index_entry,
                                     PinnableSlice* value) {
  return GetBlobValue(key, index_entry, value);
}

std::vector<std::shared_ptr<BlobFile>> BlobDBImpl::TEST_GetBlobFiles() const {
  ReadLock l(&mutex_);
  std::vector<std::shared_ptr<BlobFile>> blob_files;
  for (auto& p : blob_files_) {
    blob_files.emplace_back(p.second);
  }
  return blob_files;
}

std::vector<std::shared_ptr<BlobFile>> BlobDBImpl::TEST_GetObsoleteFiles()
    const {
  ReadLock l(&mutex_);
  std::vector<std::shared_ptr<BlobFile>> obsolete_files;
  for (auto& bfile : obsolete_files_) {
    obsolete_files.emplace_back(bfile);
  }
  return obsolete_files;
}

void BlobDBImpl::TEST_DeleteObsoleteFiles() {
  DeleteObsoleteFiles(false /*abort*/);
}

Status BlobDBImpl::TEST_CloseBlobFile(std::shared_ptr<BlobFile>& bfile) {
  MutexLock l(&write_mutex_);
  return CloseBlobFile(bfile);
}

void BlobDBImpl::TEST_ObsoleteBlobFile(std::shared_ptr<BlobFile>& blob_file,
                                       SequenceNumber obsolete_seq,
                                       bool update_size) {
  return ObsoleteBlobFile(blob_file, obsolete_seq, update_size);
}

Status BlobDBImpl::TEST_GCFileAndUpdateLSM(std::shared_ptr<BlobFile>& bfile,
                                           GCStats* gc_stats) {
  return GCFileAndUpdateLSM(bfile, gc_stats);
}

void BlobDBImpl::TEST_RunGC() { RunGC(false /*abort*/); }

void BlobDBImpl::TEST_EvictExpiredFiles() {
  EvictExpiredFiles(false /*abort*/);
}

uint64_t BlobDBImpl::TEST_live_sst_size() { return live_sst_size_.load(); }
#endif  //  !NDEBUG

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
