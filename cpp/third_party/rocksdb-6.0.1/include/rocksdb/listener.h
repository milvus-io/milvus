// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "rocksdb/compaction_job_stats.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"

namespace rocksdb {

typedef std::unordered_map<std::string, std::shared_ptr<const TableProperties>>
    TablePropertiesCollection;

class DB;
class ColumnFamilyHandle;
class Status;
struct CompactionJobStats;
enum CompressionType : unsigned char;

enum class TableFileCreationReason {
  kFlush,
  kCompaction,
  kRecovery,
  kMisc,
};

struct TableFileCreationBriefInfo {
  // the name of the database where the file was created
  std::string db_name;
  // the name of the column family where the file was created.
  std::string cf_name;
  // the path to the created file.
  std::string file_path;
  // the id of the job (which could be flush or compaction) that
  // created the file.
  int job_id;
  // reason of creating the table.
  TableFileCreationReason reason;
};

struct TableFileCreationInfo : public TableFileCreationBriefInfo {
  TableFileCreationInfo() = default;
  explicit TableFileCreationInfo(TableProperties&& prop)
      : table_properties(prop) {}
  // the size of the file.
  uint64_t file_size;
  // Detailed properties of the created file.
  TableProperties table_properties;
  // The status indicating whether the creation was successful or not.
  Status status;
};

enum class CompactionReason : int {
  kUnknown = 0,
  // [Level] number of L0 files > level0_file_num_compaction_trigger
  kLevelL0FilesNum,
  // [Level] total size of level > MaxBytesForLevel()
  kLevelMaxLevelSize,
  // [Universal] Compacting for size amplification
  kUniversalSizeAmplification,
  // [Universal] Compacting for size ratio
  kUniversalSizeRatio,
  // [Universal] number of sorted runs > level0_file_num_compaction_trigger
  kUniversalSortedRunNum,
  // [FIFO] total size > max_table_files_size
  kFIFOMaxSize,
  // [FIFO] reduce number of files.
  kFIFOReduceNumFiles,
  // [FIFO] files with creation time < (current_time - interval)
  kFIFOTtl,
  // Manual compaction
  kManualCompaction,
  // DB::SuggestCompactRange() marked files for compaction
  kFilesMarkedForCompaction,
  // [Level] Automatic compaction within bottommost level to cleanup duplicate
  // versions of same user key, usually due to a released snapshot.
  kBottommostFiles,
  // Compaction based on TTL
  kTtl,
  // According to the comments in flush_job.cc, RocksDB treats flush as
  // a level 0 compaction in internal stats.
  kFlush,
  // Compaction caused by external sst file ingestion
  kExternalSstIngestion,
  // total number of compaction reasons, new reasons must be added above this.
  kNumOfReasons,
};

enum class FlushReason : int {
  kOthers = 0x00,
  kGetLiveFiles = 0x01,
  kShutDown = 0x02,
  kExternalFileIngestion = 0x03,
  kManualCompaction = 0x04,
  kWriteBufferManager = 0x05,
  kWriteBufferFull = 0x06,
  kTest = 0x07,
  kDeleteFiles = 0x08,
  kAutoCompaction = 0x09,
  kManualFlush = 0x0a,
  kErrorRecovery = 0xb,
};

enum class BackgroundErrorReason {
  kFlush,
  kCompaction,
  kWriteCallback,
  kMemTable,
};

enum class WriteStallCondition {
  kNormal,
  kDelayed,
  kStopped,
};

struct WriteStallInfo {
  // the name of the column family
  std::string cf_name;
  // state of the write controller
  struct {
    WriteStallCondition cur;
    WriteStallCondition prev;
  } condition;
};

#ifndef ROCKSDB_LITE

struct TableFileDeletionInfo {
  // The name of the database where the file was deleted.
  std::string db_name;
  // The path to the deleted file.
  std::string file_path;
  // The id of the job which deleted the file.
  int job_id;
  // The status indicating whether the deletion was successful or not.
  Status status;
};

struct FileOperationInfo {
  using TimePoint = std::chrono::time_point<std::chrono::system_clock,
                                            std::chrono::nanoseconds>;

  const std::string& path;
  uint64_t offset;
  size_t length;
  const TimePoint& start_timestamp;
  const TimePoint& finish_timestamp;
  Status status;
  FileOperationInfo(const std::string& _path, const TimePoint& start,
                    const TimePoint& finish)
      : path(_path), start_timestamp(start), finish_timestamp(finish) {}
};

struct FlushJobInfo {
  // the id of the column family
  uint32_t cf_id;
  // the name of the column family
  std::string cf_name;
  // the path to the newly created file
  std::string file_path;
  // the id of the thread that completed this flush job.
  uint64_t thread_id;
  // the job id, which is unique in the same thread.
  int job_id;
  // If true, then rocksdb is currently slowing-down all writes to prevent
  // creating too many Level 0 files as compaction seems not able to
  // catch up the write request speed.  This indicates that there are
  // too many files in Level 0.
  bool triggered_writes_slowdown;
  // If true, then rocksdb is currently blocking any writes to prevent
  // creating more L0 files.  This indicates that there are too many
  // files in level 0.  Compactions should try to compact L0 files down
  // to lower levels as soon as possible.
  bool triggered_writes_stop;
  // The smallest sequence number in the newly created file
  SequenceNumber smallest_seqno;
  // The largest sequence number in the newly created file
  SequenceNumber largest_seqno;
  // Table properties of the table being flushed
  TableProperties table_properties;

  FlushReason flush_reason;
};

struct CompactionJobInfo {
  CompactionJobInfo() = default;
  explicit CompactionJobInfo(const CompactionJobStats& _stats) :
      stats(_stats) {}

  // the id of the column family where the compaction happened.
  uint32_t cf_id;
  // the name of the column family where the compaction happened.
  std::string cf_name;
  // the status indicating whether the compaction was successful or not.
  Status status;
  // the id of the thread that completed this compaction job.
  uint64_t thread_id;
  // the job id, which is unique in the same thread.
  int job_id;
  // the smallest input level of the compaction.
  int base_input_level;
  // the output level of the compaction.
  int output_level;
  // the names of the compaction input files.
  std::vector<std::string> input_files;

  // the names of the compaction output files.
  std::vector<std::string> output_files;
  // Table properties for input and output tables.
  // The map is keyed by values from input_files and output_files.
  TablePropertiesCollection table_properties;

  // Reason to run the compaction
  CompactionReason compaction_reason;

  // Compression algorithm used for output files
  CompressionType compression;

  // If non-null, this variable stores detailed information
  // about this compaction.
  CompactionJobStats stats;
};

struct MemTableInfo {
  // the name of the column family to which memtable belongs
  std::string cf_name;
  // Sequence number of the first element that was inserted
  // into the memtable.
  SequenceNumber first_seqno;
  // Sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  SequenceNumber earliest_seqno;
  // Total number of entries in memtable
  uint64_t num_entries;
  // Total number of deletes in memtable
  uint64_t num_deletes;

};

struct ExternalFileIngestionInfo {
  // the name of the column family
  std::string cf_name;
  // Path of the file outside the DB
  std::string external_file_path;
  // Path of the file inside the DB
  std::string internal_file_path;
  // The global sequence number assigned to keys in this file
  SequenceNumber global_seqno;
  // Table properties of the table being flushed
  TableProperties table_properties;
};

// EventListener class contains a set of callback functions that will
// be called when specific RocksDB event happens such as flush.  It can
// be used as a building block for developing custom features such as
// stats-collector or external compaction algorithm.
//
// Note that callback functions should not run for an extended period of
// time before the function returns, otherwise RocksDB may be blocked.
// For example, it is not suggested to do DB::CompactFiles() (as it may
// run for a long while) or issue many of DB::Put() (as Put may be blocked
// in certain cases) in the same thread in the EventListener callback.
// However, doing DB::CompactFiles() and DB::Put() in another thread is
// considered safe.
//
// [Threading] All EventListener callback will be called using the
// actual thread that involves in that specific event.   For example, it
// is the RocksDB background flush thread that does the actual flush to
// call EventListener::OnFlushCompleted().
//
// [Locking] All EventListener callbacks are designed to be called without
// the current thread holding any DB mutex. This is to prevent potential
// deadlock and performance issue when using EventListener callback
// in a complex way.
class EventListener {
 public:
  // A callback function to RocksDB which will be called whenever a
  // registered RocksDB flushes a file.  The default implementation is
  // no-op.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  virtual void OnFlushCompleted(DB* /*db*/,
                                const FlushJobInfo& /*flush_job_info*/) {}

  // A callback function to RocksDB which will be called before a
  // RocksDB starts to flush memtables.  The default implementation is
  // no-op.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  virtual void OnFlushBegin(DB* /*db*/,
                            const FlushJobInfo& /*flush_job_info*/) {}

  // A callback function for RocksDB which will be called whenever
  // a SST file is deleted.  Different from OnCompactionCompleted and
  // OnFlushCompleted, this callback is designed for external logging
  // service and thus only provide string parameters instead
  // of a pointer to DB.  Applications that build logic basic based
  // on file creations and deletions is suggested to implement
  // OnFlushCompleted and OnCompactionCompleted.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from the
  // returned value.
  virtual void OnTableFileDeleted(const TableFileDeletionInfo& /*info*/) {}

  // A callback function to RocksDB which will be called before a
  // RocksDB starts to compact.  The default implementation is
  // no-op.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  virtual void OnCompactionBegin(DB* /*db*/,
                                 const CompactionJobInfo& /*ci*/) {}

  // A callback function for RocksDB which will be called whenever
  // a registered RocksDB compacts a file. The default implementation
  // is a no-op.
  //
  // Note that this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns. Otherwise, RocksDB may be blocked.
  //
  // @param db a pointer to the rocksdb instance which just compacted
  //   a file.
  // @param ci a reference to a CompactionJobInfo struct. 'ci' is released
  //  after this function is returned, and must be copied if it is needed
  //  outside of this function.
  virtual void OnCompactionCompleted(DB* /*db*/,
                                     const CompactionJobInfo& /*ci*/) {}

  // A callback function for RocksDB which will be called whenever
  // a SST file is created.  Different from OnCompactionCompleted and
  // OnFlushCompleted, this callback is designed for external logging
  // service and thus only provide string parameters instead
  // of a pointer to DB.  Applications that build logic basic based
  // on file creations and deletions is suggested to implement
  // OnFlushCompleted and OnCompactionCompleted.
  //
  // Historically it will only be called if the file is successfully created.
  // Now it will also be called on failure case. User can check info.status
  // to see if it succeeded or not.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnTableFileCreated(const TableFileCreationInfo& /*info*/) {}

  // A callback function for RocksDB which will be called before
  // a SST file is being created. It will follow by OnTableFileCreated after
  // the creation finishes.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*info*/) {}

  // A callback function for RocksDB which will be called before
  // a memtable is made immutable.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  //
  // Note that if applications would like to use the passed reference
  // outside this function call, they should make copies from these
  // returned value.
  virtual void OnMemTableSealed(
    const MemTableInfo& /*info*/) {}

  // A callback function for RocksDB which will be called before
  // a column family handle is deleted.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  // @param handle is a pointer to the column family handle to be deleted
  // which will become a dangling pointer after the deletion.
  virtual void OnColumnFamilyHandleDeletionStarted(
      ColumnFamilyHandle* /*handle*/) {}

  // A callback function for RocksDB which will be called after an external
  // file is ingested using IngestExternalFile.
  //
  // Note that the this function will run on the same thread as
  // IngestExternalFile(), if this function is blocked, IngestExternalFile()
  // will be blocked from finishing.
  virtual void OnExternalFileIngested(
      DB* /*db*/, const ExternalFileIngestionInfo& /*info*/) {}

  // A callback function for RocksDB which will be called before setting the
  // background error status to a non-OK value. The new background error status
  // is provided in `bg_error` and can be modified by the callback. E.g., a
  // callback can suppress errors by resetting it to Status::OK(), thus
  // preventing the database from entering read-only mode. We do not provide any
  // guarantee when failed flushes/compactions will be rescheduled if the user
  // suppresses an error.
  //
  // Note that this function can run on the same threads as flush, compaction,
  // and user writes. So, it is extremely important not to perform heavy
  // computations or blocking calls in this function.
  virtual void OnBackgroundError(BackgroundErrorReason /* reason */,
                                 Status* /* bg_error */) {}

  // A callback function for RocksDB which will be called whenever a change
  // of superversion triggers a change of the stall conditions.
  //
  // Note that the this function must be implemented in a way such that
  // it should not run for an extended period of time before the function
  // returns.  Otherwise, RocksDB may be blocked.
  virtual void OnStallConditionsChanged(const WriteStallInfo& /*info*/) {}

  // A callback function for RocksDB which will be called whenever a file read
  // operation finishes.
  virtual void OnFileReadFinish(const FileOperationInfo& /* info */) {}

  // A callback function for RocksDB which will be called whenever a file write
  // operation finishes.
  virtual void OnFileWriteFinish(const FileOperationInfo& /* info */) {}

  // If true, the OnFileReadFinish and OnFileWriteFinish will be called. If
  // false, then they won't be called.
  virtual bool ShouldBeNotifiedOnFileIO() { return false; }

  // A callback function for RocksDB which will be called just before
  // starting the automatic recovery process for recoverable background
  // errors, such as NoSpace(). The callback can suppress the automatic
  // recovery by setting *auto_recovery to false. The database will then
  // have to be transitioned out of read-only mode by calling DB::Resume()
  virtual void OnErrorRecoveryBegin(BackgroundErrorReason /* reason */,
                                    Status /* bg_error */,
                                    bool* /* auto_recovery */) {}

  // A callback function for RocksDB which will be called once the database
  // is recovered from read-only mode after an error. When this is called, it
  // means normal writes to the database can be issued and the user can
  // initiate any further recovery actions needed
  virtual void OnErrorRecoveryCompleted(Status /* old_bg_error */) {}

  virtual ~EventListener() {}
};

#else

class EventListener {
};

#endif  // ROCKSDB_LITE

}  // namespace rocksdb
