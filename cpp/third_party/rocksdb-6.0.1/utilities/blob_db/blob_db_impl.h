//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <atomic>
#include <condition_variable>
#include <limits>
#include <list>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/db_iter.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/wal_filter.h"
#include "util/mutexlock.h"
#include "util/timer_queue.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/blob_db/blob_file.h"
#include "utilities/blob_db/blob_log_format.h"
#include "utilities/blob_db/blob_log_reader.h"
#include "utilities/blob_db/blob_log_writer.h"

namespace rocksdb {

class DBImpl;
class ColumnFamilyHandle;
class ColumnFamilyData;
struct FlushJobInfo;

namespace blob_db {

struct BlobCompactionContext;
class BlobDBImpl;
class BlobFile;

// Comparator to sort "TTL" aware Blob files based on the lower value of
// TTL range.
struct BlobFileComparatorTTL {
  bool operator()(const std::shared_ptr<BlobFile>& lhs,
                  const std::shared_ptr<BlobFile>& rhs) const;
};

struct BlobFileComparator {
  bool operator()(const std::shared_ptr<BlobFile>& lhs,
                  const std::shared_ptr<BlobFile>& rhs) const;
};

struct GCStats {
  uint64_t blob_count = 0;
  uint64_t num_keys_overwritten = 0;
  uint64_t num_keys_expired = 0;
  uint64_t num_keys_relocated = 0;
  uint64_t bytes_overwritten = 0;
  uint64_t bytes_expired = 0;
  uint64_t bytes_relocated = 0;
};

/**
 * The implementation class for BlobDB. This manages the value
 * part in TTL aware sequentially written files. These files are
 * Garbage Collected.
 */
class BlobDBImpl : public BlobDB {
  friend class BlobFile;
  friend class BlobDBIterator;

 public:
  // deletions check period
  static constexpr uint32_t kDeleteCheckPeriodMillisecs = 2 * 1000;

  // gc percentage each check period
  static constexpr uint32_t kGCFilePercentage = 100;

  // gc period
  static constexpr uint32_t kGCCheckPeriodMillisecs = 60 * 1000;

  // sanity check task
  static constexpr uint32_t kSanityCheckPeriodMillisecs = 20 * 60 * 1000;

  // how many random access open files can we tolerate
  static constexpr uint32_t kOpenFilesTrigger = 100;

  // how often to schedule reclaim open files.
  static constexpr uint32_t kReclaimOpenFilesPeriodMillisecs = 1 * 1000;

  // how often to schedule delete obs files periods
  static constexpr uint32_t kDeleteObsoleteFilesPeriodMillisecs = 10 * 1000;

  // how often to schedule expired files eviction.
  static constexpr uint32_t kEvictExpiredFilesPeriodMillisecs = 10 * 1000;

  // when should oldest file be evicted:
  // on reaching 90% of blob_dir_size
  static constexpr double kEvictOldestFileAtSize = 0.9;

  using BlobDB::Put;
  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;

  using BlobDB::Get;
  Status Get(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value) override;

  Status Get(const ReadOptions& read_options, ColumnFamilyHandle* column_family,
             const Slice& key, PinnableSlice* value,
             uint64_t* expiration) override;

  using BlobDB::NewIterator;
  virtual Iterator* NewIterator(const ReadOptions& read_options) override;

  using BlobDB::NewIterators;
  virtual Status NewIterators(
      const ReadOptions& /*read_options*/,
      const std::vector<ColumnFamilyHandle*>& /*column_families*/,
      std::vector<Iterator*>* /*iterators*/) override {
    return Status::NotSupported("Not implemented");
  }

  using BlobDB::MultiGet;
  virtual std::vector<Status> MultiGet(
      const ReadOptions& read_options,
      const std::vector<Slice>& keys,
      std::vector<std::string>* values) override;

  virtual Status Write(const WriteOptions& opts, WriteBatch* updates) override;

  virtual Status Close() override;

  using BlobDB::PutWithTTL;
  Status PutWithTTL(const WriteOptions& options, const Slice& key,
                    const Slice& value, uint64_t ttl) override;

  using BlobDB::PutUntil;
  Status PutUntil(const WriteOptions& options, const Slice& key,
                  const Slice& value, uint64_t expiration) override;

  BlobDBOptions GetBlobDBOptions() const override;

  BlobDBImpl(const std::string& dbname, const BlobDBOptions& bdb_options,
             const DBOptions& db_options,
             const ColumnFamilyOptions& cf_options);

  virtual Status DisableFileDeletions() override;

  virtual Status EnableFileDeletions(bool force) override;

  virtual Status GetLiveFiles(std::vector<std::string>&,
                              uint64_t* manifest_file_size,
                              bool flush_memtable = true) override;
  virtual void GetLiveFilesMetaData(std::vector<LiveFileMetaData>*) override;

  ~BlobDBImpl();

  Status Open(std::vector<ColumnFamilyHandle*>* handles);

  Status SyncBlobFiles() override;

  void UpdateLiveSSTSize();

  void GetCompactionContext(BlobCompactionContext* context);

#ifndef NDEBUG
  Status TEST_GetBlobValue(const Slice& key, const Slice& index_entry,
                           PinnableSlice* value);

  std::vector<std::shared_ptr<BlobFile>> TEST_GetBlobFiles() const;

  std::vector<std::shared_ptr<BlobFile>> TEST_GetObsoleteFiles() const;

  Status TEST_CloseBlobFile(std::shared_ptr<BlobFile>& bfile);

  void TEST_ObsoleteBlobFile(std::shared_ptr<BlobFile>& blob_file,
                             SequenceNumber obsolete_seq = 0,
                             bool update_size = true);

  Status TEST_GCFileAndUpdateLSM(std::shared_ptr<BlobFile>& bfile,
                                 GCStats* gc_stats);

  void TEST_RunGC();

  void TEST_EvictExpiredFiles();

  void TEST_DeleteObsoleteFiles();

  uint64_t TEST_live_sst_size();

  const std::string& TEST_blob_dir() const { return blob_dir_; }
#endif  //  !NDEBUG

 private:
  class GarbageCollectionWriteCallback;
  class BlobInserter;

  // Create a snapshot if there isn't one in read options.
  // Return true if a snapshot is created.
  bool SetSnapshotIfNeeded(ReadOptions* read_options);

  Status GetImpl(const ReadOptions& read_options,
                 ColumnFamilyHandle* column_family, const Slice& key,
                 PinnableSlice* value, uint64_t* expiration = nullptr);

  Status GetBlobValue(const Slice& key, const Slice& index_entry,
                      PinnableSlice* value, uint64_t* expiration = nullptr);

  Slice GetCompressedSlice(const Slice& raw,
                           std::string* compression_output) const;

  // Close a file by appending a footer, and removes file from open files list.
  Status CloseBlobFile(std::shared_ptr<BlobFile> bfile, bool need_lock = true);

  // Close a file if its size exceeds blob_file_size
  Status CloseBlobFileIfNeeded(std::shared_ptr<BlobFile>& bfile);

  // Mark file as obsolete and move the file to obsolete file list.
  //
  // REQUIRED: hold write lock of mutex_ or during DB open.
  void ObsoleteBlobFile(std::shared_ptr<BlobFile> blob_file,
                        SequenceNumber obsolete_seq, bool update_size);

  Status PutBlobValue(const WriteOptions& options, const Slice& key,
                      const Slice& value, uint64_t expiration,
                      WriteBatch* batch);

  Status AppendBlob(const std::shared_ptr<BlobFile>& bfile,
                    const std::string& headerbuf, const Slice& key,
                    const Slice& value, uint64_t expiration,
                    std::string* index_entry);

  // find an existing blob log file based on the expiration unix epoch
  // if such a file does not exist, return nullptr
  Status SelectBlobFileTTL(uint64_t expiration,
                           std::shared_ptr<BlobFile>* blob_file);

  // find an existing blob log file to append the value to
  Status SelectBlobFile(std::shared_ptr<BlobFile>* blob_file);

  std::shared_ptr<BlobFile> FindBlobFileLocked(uint64_t expiration) const;

  // periodic sanity check. Bunch of checks
  std::pair<bool, int64_t> SanityCheck(bool aborted);

  // delete files which have been garbage collected and marked
  // obsolete. Check whether any snapshots exist which refer to
  // the same
  std::pair<bool, int64_t> DeleteObsoleteFiles(bool aborted);

  // Major task to garbage collect expired and deleted blobs
  std::pair<bool, int64_t> RunGC(bool aborted);

  // periodically check if open blob files and their TTL's has expired
  // if expired, close the sequential writer and make the file immutable
  std::pair<bool, int64_t> EvictExpiredFiles(bool aborted);

  // if the number of open files, approaches ULIMIT's this
  // task will close random readers, which are kept around for
  // efficiency
  std::pair<bool, int64_t> ReclaimOpenFiles(bool aborted);

  std::pair<bool, int64_t> RemoveTimerQ(TimerQueue* tq, bool aborted);

  // Adds the background tasks to the timer queue
  void StartBackgroundTasks();

  // add a new Blob File
  std::shared_ptr<BlobFile> NewBlobFile(const std::string& reason);

  // collect all the blob log files from the blob directory
  Status GetAllBlobFiles(std::set<uint64_t>* file_numbers);

  // Open all blob files found in blob_dir.
  Status OpenAllBlobFiles();

  Status GetBlobFileReader(const std::shared_ptr<BlobFile>& blob_file,
                           std::shared_ptr<RandomAccessFileReader>* reader);

  // hold write mutex on file and call.
  // Close the above Random Access reader
  void CloseRandomAccessLocked(const std::shared_ptr<BlobFile>& bfile);

  // hold write mutex on file and call
  // creates a sequential (append) writer for this blobfile
  Status CreateWriterLocked(const std::shared_ptr<BlobFile>& bfile);

  // returns a Writer object for the file. If writer is not
  // already present, creates one. Needs Write Mutex to be held
  Status CheckOrCreateWriterLocked(const std::shared_ptr<BlobFile>& blob_file,
                                   std::shared_ptr<Writer>* writer);

  // Iterate through keys and values on Blob and write into
  // separate file the remaining blobs and delete/update pointers
  // in LSM atomically
  Status GCFileAndUpdateLSM(const std::shared_ptr<BlobFile>& bfptr,
                            GCStats* gcstats);

  // checks if there is no snapshot which is referencing the
  // blobs
  bool VisibleToActiveSnapshot(const std::shared_ptr<BlobFile>& file);
  bool FileDeleteOk_SnapshotCheckLocked(const std::shared_ptr<BlobFile>& bfile);

  void CopyBlobFiles(std::vector<std::shared_ptr<BlobFile>>* bfiles_copy);

  uint64_t EpochNow() { return env_->NowMicros() / 1000000; }

  // Check if inserting a new blob will make DB grow out of space.
  // If is_fifo = true, FIFO eviction will be triggered to make room for the
  // new blob. If force_evict = true, FIFO eviction will evict blob files
  // even eviction will not make enough room for the new blob.
  Status CheckSizeAndEvictBlobFiles(uint64_t blob_size,
                                    bool force_evict = false);

  // name of the database directory
  std::string dbname_;

  // the base DB
  DBImpl* db_impl_;
  Env* env_;

  // the options that govern the behavior of Blob Storage
  BlobDBOptions bdb_options_;
  DBOptions db_options_;
  ColumnFamilyOptions cf_options_;
  EnvOptions env_options_;

  // Raw pointer of statistic. db_options_ has a std::shared_ptr to hold
  // ownership.
  Statistics* statistics_;

  // by default this is "blob_dir" under dbname_
  // but can be configured
  std::string blob_dir_;

  // pointer to directory
  std::unique_ptr<Directory> dir_ent_;

  // Read Write Mutex, which protects all the data structures
  // HEAVILY TRAFFICKED
  mutable port::RWMutex mutex_;

  // Writers has to hold write_mutex_ before writing.
  mutable port::Mutex write_mutex_;

  // counter for blob file number
  std::atomic<uint64_t> next_file_number_;

  // entire metadata of all the BLOB files memory
  std::map<uint64_t, std::shared_ptr<BlobFile>> blob_files_;

  // epoch or version of the open files.
  std::atomic<uint64_t> epoch_of_;

  // opened non-TTL blob file.
  std::shared_ptr<BlobFile> open_non_ttl_file_;

  // all the blob files which are currently being appended to based
  // on variety of incoming TTL's
  std::set<std::shared_ptr<BlobFile>, BlobFileComparatorTTL> open_ttl_files_;

  // Flag to check whether Close() has been called on this DB
  bool closed_;

  // timer based queue to execute tasks
  TimerQueue tqueue_;

  // number of files opened for random access/GET
  // counter is used to monitor and close excess RA files.
  std::atomic<uint32_t> open_file_count_;

  // Total size of all live blob files (i.e. exclude obsolete files).
  std::atomic<uint64_t> total_blob_size_;

  // total size of SST files.
  std::atomic<uint64_t> live_sst_size_;

  // Latest FIFO eviction timestamp
  //
  // REQUIRES: access with metex_ lock held.
  uint64_t fifo_eviction_seq_;

  // The expiration up to which latest FIFO eviction evicts.
  //
  // REQUIRES: access with metex_ lock held.
  uint64_t evict_expiration_up_to_;

  std::list<std::shared_ptr<BlobFile>> obsolete_files_;

  // DeleteObsoleteFiles, DiableFileDeletions and EnableFileDeletions block
  // on the mutex to avoid contention.
  //
  // While DeleteObsoleteFiles hold both mutex_ and delete_file_mutex_, note
  // the difference. mutex_ only needs to be held when access the
  // data-structure, and delete_file_mutex_ needs to be held the whole time
  // during DeleteObsoleteFiles to avoid being run simultaneously with
  // DisableFileDeletions.
  //
  // If both of mutex_ and delete_file_mutex_ needs to be held, it is adviced
  // to hold delete_file_mutex_ first to avoid deadlock.
  mutable port::Mutex delete_file_mutex_;

  // Each call of DisableFileDeletions will increase disable_file_deletion_
  // by 1. EnableFileDeletions will either decrease the count by 1 or reset
  // it to zeor, depending on the force flag.
  //
  // REQUIRES: access with delete_file_mutex_ held.
  int disable_file_deletions_ = 0;

  uint32_t debug_level_;
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
