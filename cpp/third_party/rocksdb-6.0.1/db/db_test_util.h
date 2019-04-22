// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <fcntl.h>
#include <inttypes.h>

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "env/mock_env.h"
#include "memtable/hash_linklist_rep.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "table/block_based_table_factory.h"
#include "table/mock_table.h"
#include "table/plain_table_factory.h"
#include "table/scoped_arena_iterator.h"
#include "util/compression.h"
#include "util/filename.h"
#include "util/mock_time_env.h"
#include "util/mutexlock.h"

#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

namespace anon {
class AtomicCounter {
 public:
  explicit AtomicCounter(Env* env = NULL)
      : env_(env), cond_count_(&mu_), count_(0) {}

  void Increment() {
    MutexLock l(&mu_);
    count_++;
    cond_count_.SignalAll();
  }

  int Read() {
    MutexLock l(&mu_);
    return count_;
  }

  bool WaitFor(int count) {
    MutexLock l(&mu_);

    uint64_t start = env_->NowMicros();
    while (count_ < count) {
      uint64_t now = env_->NowMicros();
      cond_count_.TimedWait(now + /*1s*/ 1 * 1000 * 1000);
      if (env_->NowMicros() - start > /*10s*/ 10 * 1000 * 1000) {
        return false;
      }
      if (count_ < count) {
        GTEST_LOG_(WARNING) << "WaitFor is taking more time than usual";
      }
    }

    return true;
  }

  void Reset() {
    MutexLock l(&mu_);
    count_ = 0;
    cond_count_.SignalAll();
  }

 private:
  Env* env_;
  port::Mutex mu_;
  port::CondVar cond_count_;
  int count_;
};

struct OptionsOverride {
  std::shared_ptr<const FilterPolicy> filter_policy = nullptr;
  // These will be used only if filter_policy is set
  bool partition_filters = false;
  uint64_t metadata_block_size = 1024;

  // Used as a bit mask of individual enums in which to skip an XF test point
  int skip_policy = 0;
};

}  // namespace anon

enum SkipPolicy { kSkipNone = 0, kSkipNoSnapshot = 1, kSkipNoPrefix = 2 };

// A hacky skip list mem table that triggers flush after number of entries.
class SpecialMemTableRep : public MemTableRep {
 public:
  explicit SpecialMemTableRep(Allocator* allocator, MemTableRep* memtable,
                              int num_entries_flush)
      : MemTableRep(allocator),
        memtable_(memtable),
        num_entries_flush_(num_entries_flush),
        num_entries_(0) {}

  virtual KeyHandle Allocate(const size_t len, char** buf) override {
    return memtable_->Allocate(len, buf);
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  virtual void Insert(KeyHandle handle) override {
    num_entries_++;
    memtable_->Insert(handle);
  }

  // Returns true iff an entry that compares equal to key is in the list.
  virtual bool Contains(const char* key) const override {
    return memtable_->Contains(key);
  }

  virtual size_t ApproximateMemoryUsage() override {
    // Return a high memory usage when number of entries exceeds the threshold
    // to trigger a flush.
    return (num_entries_ < num_entries_flush_) ? 0 : 1024 * 1024 * 1024;
  }

  virtual void Get(const LookupKey& k, void* callback_args,
                   bool (*callback_func)(void* arg,
                                         const char* entry)) override {
    memtable_->Get(k, callback_args, callback_func);
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    return memtable_->ApproximateNumEntries(start_ikey, end_ikey);
  }

  virtual MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    return memtable_->GetIterator(arena);
  }

  virtual ~SpecialMemTableRep() override {}

 private:
  std::unique_ptr<MemTableRep> memtable_;
  int num_entries_flush_;
  int num_entries_;
};

// The factory for the hacky skip list mem table that triggers flush after
// number of entries exceeds a threshold.
class SpecialSkipListFactory : public MemTableRepFactory {
 public:
  // After number of inserts exceeds `num_entries_flush` in a mem table, trigger
  // flush.
  explicit SpecialSkipListFactory(int num_entries_flush)
      : num_entries_flush_(num_entries_flush) {}

  using MemTableRepFactory::CreateMemTableRep;
  virtual MemTableRep* CreateMemTableRep(
      const MemTableRep::KeyComparator& compare, Allocator* allocator,
      const SliceTransform* transform, Logger* /*logger*/) override {
    return new SpecialMemTableRep(
        allocator, factory_.CreateMemTableRep(compare, allocator, transform, 0),
        num_entries_flush_);
  }
  virtual const char* Name() const override { return "SkipListFactory"; }

  bool IsInsertConcurrentlySupported() const override {
    return factory_.IsInsertConcurrentlySupported();
  }

 private:
  SkipListFactory factory_;
  int num_entries_flush_;
};

// Special Env used to delay background operations
class SpecialEnv : public EnvWrapper {
 public:
  explicit SpecialEnv(Env* base);

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    class SSTableFile : public WritableFile {
     private:
      SpecialEnv* env_;
      std::unique_ptr<WritableFile> base_;

     public:
      SSTableFile(SpecialEnv* env, std::unique_ptr<WritableFile>&& base)
          : env_(env), base_(std::move(base)) {}
      Status Append(const Slice& data) override {
        if (env_->table_write_callback_) {
          (*env_->table_write_callback_)();
        }
        if (env_->drop_writes_.load(std::memory_order_acquire)) {
          // Drop writes on the floor
          return Status::OK();
        } else if (env_->no_space_.load(std::memory_order_acquire)) {
          return Status::NoSpace("No space left on device");
        } else {
          env_->bytes_written_ += data.size();
          return base_->Append(data);
        }
      }
      Status PositionedAppend(const Slice& data, uint64_t offset) override {
        if (env_->table_write_callback_) {
          (*env_->table_write_callback_)();
        }
        if (env_->drop_writes_.load(std::memory_order_acquire)) {
          // Drop writes on the floor
          return Status::OK();
        } else if (env_->no_space_.load(std::memory_order_acquire)) {
          return Status::NoSpace("No space left on device");
        } else {
          env_->bytes_written_ += data.size();
          return base_->PositionedAppend(data, offset);
        }
      }
      Status Truncate(uint64_t size) override { return base_->Truncate(size); }
      Status RangeSync(uint64_t offset, uint64_t nbytes) override {
        Status s = base_->RangeSync(offset, nbytes);
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT_CALLBACK("SpecialEnv::SStableFile::RangeSync", &s);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
        return s;
      }
      Status Close() override {
// SyncPoint is not supported in Released Windows Mode.
#if !(defined NDEBUG) || !defined(OS_WIN)
        // Check preallocation size
        // preallocation size is never passed to base file.
        size_t preallocation_size = preallocation_block_size();
        TEST_SYNC_POINT_CALLBACK("DBTestWritableFile.GetPreallocationStatus",
                                 &preallocation_size);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
        Status s = base_->Close();
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT_CALLBACK("SpecialEnv::SStableFile::Close", &s);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
        return s;
      }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        while (env_->delay_sstable_sync_.load(std::memory_order_acquire)) {
          env_->SleepForMicroseconds(100000);
        }
        Status s = base_->Sync();
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT_CALLBACK("SpecialEnv::SStableFile::Sync", &s);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
        return s;
      }
      void SetIOPriority(Env::IOPriority pri) override {
        base_->SetIOPriority(pri);
      }
      Env::IOPriority GetIOPriority() override {
        return base_->GetIOPriority();
      }
      bool use_direct_io() const override {
        return base_->use_direct_io();
      }
      Status Allocate(uint64_t offset, uint64_t len) override {
        return base_->Allocate(offset, len);
      }
    };
    class ManifestFile : public WritableFile {
     public:
      ManifestFile(SpecialEnv* env, std::unique_ptr<WritableFile>&& b)
          : env_(env), base_(std::move(b)) {}
      Status Append(const Slice& data) override {
        if (env_->manifest_write_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated writer error");
        } else {
          return base_->Append(data);
        }
      }
      Status Truncate(uint64_t size) override { return base_->Truncate(size); }
      Status Close() override { return base_->Close(); }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        if (env_->manifest_sync_error_.load(std::memory_order_acquire)) {
          return Status::IOError("simulated sync error");
        } else {
          return base_->Sync();
        }
      }
      uint64_t GetFileSize() override { return base_->GetFileSize(); }
      Status Allocate(uint64_t offset, uint64_t len) override {
        return base_->Allocate(offset, len);
      }

     private:
      SpecialEnv* env_;
      std::unique_ptr<WritableFile> base_;
    };
    class WalFile : public WritableFile {
     public:
      WalFile(SpecialEnv* env, std::unique_ptr<WritableFile>&& b)
          : env_(env), base_(std::move(b)) {
        env_->num_open_wal_file_.fetch_add(1);
      }
      virtual ~WalFile() { env_->num_open_wal_file_.fetch_add(-1); }
      Status Append(const Slice& data) override {
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT("SpecialEnv::WalFile::Append:1");
#endif
        Status s;
        if (env_->log_write_error_.load(std::memory_order_acquire)) {
          s = Status::IOError("simulated writer error");
        } else {
          int slowdown =
              env_->log_write_slowdown_.load(std::memory_order_acquire);
          if (slowdown > 0) {
            env_->SleepForMicroseconds(slowdown);
          }
          s = base_->Append(data);
        }
#if !(defined NDEBUG) || !defined(OS_WIN)
        TEST_SYNC_POINT("SpecialEnv::WalFile::Append:2");
#endif
        return s;
      }
      Status Truncate(uint64_t size) override { return base_->Truncate(size); }
      Status Close() override {
// SyncPoint is not supported in Released Windows Mode.
#if !(defined NDEBUG) || !defined(OS_WIN)
        // Check preallocation size
        // preallocation size is never passed to base file.
        size_t preallocation_size = preallocation_block_size();
        TEST_SYNC_POINT_CALLBACK("DBTestWalFile.GetPreallocationStatus",
                                 &preallocation_size);
#endif  // !(defined NDEBUG) || !defined(OS_WIN)

        return base_->Close();
      }
      Status Flush() override { return base_->Flush(); }
      Status Sync() override {
        ++env_->sync_counter_;
        return base_->Sync();
      }
      bool IsSyncThreadSafe() const override {
        return env_->is_wal_sync_thread_safe_.load();
      }
      Status Allocate(uint64_t offset, uint64_t len) override {
        return base_->Allocate(offset, len);
      }

     private:
      SpecialEnv* env_;
      std::unique_ptr<WritableFile> base_;
    };

    if (non_writeable_rate_.load(std::memory_order_acquire) > 0) {
      uint32_t random_number;
      {
        MutexLock l(&rnd_mutex_);
        random_number = rnd_.Uniform(100);
      }
      if (random_number < non_writeable_rate_.load()) {
        return Status::IOError("simulated random write error");
      }
    }

    new_writable_count_++;

    if (non_writable_count_.load() > 0) {
      non_writable_count_--;
      return Status::IOError("simulated write error");
    }

    EnvOptions optimized = soptions;
    if (strstr(f.c_str(), "MANIFEST") != nullptr ||
        strstr(f.c_str(), "log") != nullptr) {
      optimized.use_mmap_writes = false;
      optimized.use_direct_writes = false;
    }

    Status s = target()->NewWritableFile(f, r, optimized);
    if (s.ok()) {
      if (strstr(f.c_str(), ".sst") != nullptr) {
        r->reset(new SSTableFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "MANIFEST") != nullptr) {
        r->reset(new ManifestFile(this, std::move(*r)));
      } else if (strstr(f.c_str(), "log") != nullptr) {
        r->reset(new WalFile(this, std::move(*r)));
      }
    }
    return s;
  }

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& soptions) override {
    class CountingFile : public RandomAccessFile {
     public:
      CountingFile(std::unique_ptr<RandomAccessFile>&& target,
                   anon::AtomicCounter* counter,
                   std::atomic<size_t>* bytes_read)
          : target_(std::move(target)),
            counter_(counter),
            bytes_read_(bytes_read) {}
      virtual Status Read(uint64_t offset, size_t n, Slice* result,
                          char* scratch) const override {
        counter_->Increment();
        Status s = target_->Read(offset, n, result, scratch);
        *bytes_read_ += result->size();
        return s;
      }

     private:
      std::unique_ptr<RandomAccessFile> target_;
      anon::AtomicCounter* counter_;
      std::atomic<size_t>* bytes_read_;
    };

    Status s = target()->NewRandomAccessFile(f, r, soptions);
    random_file_open_counter_++;
    if (s.ok() && count_random_reads_) {
      r->reset(new CountingFile(std::move(*r), &random_read_counter_,
                                &random_read_bytes_counter_));
    }
    if (s.ok() && soptions.compaction_readahead_size > 0) {
      compaction_readahead_size_ = soptions.compaction_readahead_size;
    }
    return s;
  }

  virtual Status NewSequentialFile(const std::string& f,
                                   std::unique_ptr<SequentialFile>* r,
                                   const EnvOptions& soptions) override {
    class CountingFile : public SequentialFile {
     public:
      CountingFile(std::unique_ptr<SequentialFile>&& target,
                   anon::AtomicCounter* counter)
          : target_(std::move(target)), counter_(counter) {}
      virtual Status Read(size_t n, Slice* result, char* scratch) override {
        counter_->Increment();
        return target_->Read(n, result, scratch);
      }
      virtual Status Skip(uint64_t n) override { return target_->Skip(n); }

     private:
      std::unique_ptr<SequentialFile> target_;
      anon::AtomicCounter* counter_;
    };

    Status s = target()->NewSequentialFile(f, r, soptions);
    if (s.ok() && count_sequential_reads_) {
      r->reset(new CountingFile(std::move(*r), &sequential_read_counter_));
    }
    return s;
  }

  virtual void SleepForMicroseconds(int micros) override {
    sleep_counter_.Increment();
    if (no_slowdown_ || time_elapse_only_sleep_) {
      addon_time_.fetch_add(micros);
    }
    if (!no_slowdown_) {
      target()->SleepForMicroseconds(micros);
    }
  }

  virtual Status GetCurrentTime(int64_t* unix_time) override {
    Status s;
    if (!time_elapse_only_sleep_) {
      s = target()->GetCurrentTime(unix_time);
    }
    if (s.ok()) {
      *unix_time += addon_time_.load();
    }
    return s;
  }

  virtual uint64_t NowCPUNanos() override {
    now_cpu_count_.fetch_add(1);
    return target()->NowCPUNanos();
  }

  virtual uint64_t NowNanos() override {
    return (time_elapse_only_sleep_ ? 0 : target()->NowNanos()) +
           addon_time_.load() * 1000;
  }

  virtual uint64_t NowMicros() override {
    return (time_elapse_only_sleep_ ? 0 : target()->NowMicros()) +
           addon_time_.load();
  }

  virtual Status DeleteFile(const std::string& fname) override {
    delete_count_.fetch_add(1);
    return target()->DeleteFile(fname);
  }

  Random rnd_;
  port::Mutex rnd_mutex_;  // Lock to pretect rnd_

  // sstable Sync() calls are blocked while this pointer is non-nullptr.
  std::atomic<bool> delay_sstable_sync_;

  // Drop writes on the floor while this pointer is non-nullptr.
  std::atomic<bool> drop_writes_;

  // Simulate no-space errors while this pointer is non-nullptr.
  std::atomic<bool> no_space_;

  // Simulate non-writable file system while this pointer is non-nullptr
  std::atomic<bool> non_writable_;

  // Force sync of manifest files to fail while this pointer is non-nullptr
  std::atomic<bool> manifest_sync_error_;

  // Force write to manifest files to fail while this pointer is non-nullptr
  std::atomic<bool> manifest_write_error_;

  // Force write to log files to fail while this pointer is non-nullptr
  std::atomic<bool> log_write_error_;

  // Slow down every log write, in micro-seconds.
  std::atomic<int> log_write_slowdown_;

  // Number of WAL files that are still open for write.
  std::atomic<int> num_open_wal_file_;

  bool count_random_reads_;
  anon::AtomicCounter random_read_counter_;
  std::atomic<size_t> random_read_bytes_counter_;
  std::atomic<int> random_file_open_counter_;

  bool count_sequential_reads_;
  anon::AtomicCounter sequential_read_counter_;

  anon::AtomicCounter sleep_counter_;

  std::atomic<int64_t> bytes_written_;

  std::atomic<int> sync_counter_;

  std::atomic<uint32_t> non_writeable_rate_;

  std::atomic<uint32_t> new_writable_count_;

  std::atomic<uint32_t> non_writable_count_;

  std::function<void()>* table_write_callback_;

  std::atomic<int64_t> addon_time_;

  std::atomic<int> now_cpu_count_;

  std::atomic<int> delete_count_;

  std::atomic<bool> time_elapse_only_sleep_;

  bool no_slowdown_;

  std::atomic<bool> is_wal_sync_thread_safe_{true};

  std::atomic<size_t> compaction_readahead_size_{};
};

#ifndef ROCKSDB_LITE
class OnFileDeletionListener : public EventListener {
 public:
  OnFileDeletionListener() : matched_count_(0), expected_file_name_("") {}

  void SetExpectedFileName(const std::string file_name) {
    expected_file_name_ = file_name;
  }

  void VerifyMatchedCount(size_t expected_value) {
    ASSERT_EQ(matched_count_, expected_value);
  }

  void OnTableFileDeleted(const TableFileDeletionInfo& info) override {
    if (expected_file_name_ != "") {
      ASSERT_EQ(expected_file_name_, info.file_path);
      expected_file_name_ = "";
      matched_count_++;
    }
  }

 private:
  size_t matched_count_;
  std::string expected_file_name_;
};
#endif

// A test merge operator mimics put but also fails if one of merge operands is
// "corrupted".
class TestPutOperator : public MergeOperator {
 public:
  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    if (merge_in.existing_value != nullptr &&
        *(merge_in.existing_value) == "corrupted") {
      return false;
    }
    for (auto value : merge_in.operand_list) {
      if (value == "corrupted") {
        return false;
      }
    }
    merge_out->existing_operand = merge_in.operand_list.back();
    return true;
  }

  virtual const char* Name() const override { return "TestPutOperator"; }
};

class DBTestBase : public testing::Test {
 public:
  // Sequence of option configurations to try
  enum OptionConfig : int {
    kDefault = 0,
    kBlockBasedTableWithPrefixHashIndex = 1,
    kBlockBasedTableWithWholeKeyHashIndex = 2,
    kPlainTableFirstBytePrefix = 3,
    kPlainTableCappedPrefix = 4,
    kPlainTableCappedPrefixNonMmap = 5,
    kPlainTableAllBytesPrefix = 6,
    kVectorRep = 7,
    kHashLinkList = 8,
    kMergePut = 9,
    kFilter = 10,
    kFullFilterWithNewTableReaderForCompactions = 11,
    kUncompressed = 12,
    kNumLevel_3 = 13,
    kDBLogDir = 14,
    kWalDirAndMmapReads = 15,
    kManifestFileSize = 16,
    kPerfOptions = 17,
    kHashSkipList = 18,
    kUniversalCompaction = 19,
    kUniversalCompactionMultiLevel = 20,
    kCompressedBlockCache = 21,
    kInfiniteMaxOpenFiles = 22,
    kxxHashChecksum = 23,
    kFIFOCompaction = 24,
    kOptimizeFiltersForHits = 25,
    kRowCache = 26,
    kRecycleLogFiles = 27,
    kConcurrentSkipList = 28,
    kPipelinedWrite = 29,
    kConcurrentWALWrites = 30,
    kDirectIO,
    kLevelSubcompactions,
    kBlockBasedTableWithIndexRestartInterval,
    kBlockBasedTableWithPartitionedIndex,
    kBlockBasedTableWithPartitionedIndexFormat4,
    kPartitionedFilterWithNewTableReaderForCompactions,
    kUniversalSubcompactions,
    kxxHash64Checksum,
    // This must be the last line
    kEnd,
  };

 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  std::string alternative_db_log_dir_;
  MockEnv* mem_env_;
  Env* encrypted_env_;
  SpecialEnv* env_;
  DB* db_;
  std::vector<ColumnFamilyHandle*> handles_;

  int option_config_;
  Options last_options_;

  // Skip some options, as they may not be applicable to a specific test.
  // To add more skip constants, use values 4, 8, 16, etc.
  enum OptionSkip {
    kNoSkip = 0,
    kSkipDeletesFilterFirst = 1,
    kSkipUniversalCompaction = 2,
    kSkipMergePut = 4,
    kSkipPlainTable = 8,
    kSkipHashIndex = 16,
    kSkipNoSeekToLast = 32,
    kSkipFIFOCompaction = 128,
    kSkipMmapReads = 256,
  };

  const int kRangeDelSkipConfigs =
      // Plain tables do not support range deletions.
      kSkipPlainTable |
      // MmapReads disables the iterator pinning that RangeDelAggregator
      // requires.
      kSkipMmapReads;

  explicit DBTestBase(const std::string path);

  ~DBTestBase();

  static std::string RandomString(Random* rnd, int len) {
    std::string r;
    test::RandomString(rnd, len, &r);
    return r;
  }

  static std::string Key(int i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "key%06d", i);
    return std::string(buf);
  }

  static bool ShouldSkipOptions(int option_config, int skip_mask = kNoSkip);

  // Switch to a fresh database with the next option configuration to
  // test.  Return false if there are no more configurations to test.
  bool ChangeOptions(int skip_mask = kNoSkip);

  // Switch between different compaction styles.
  bool ChangeCompactOptions();

  // Switch between different WAL-realted options.
  bool ChangeWalOptions();

  // Switch between different filter policy
  // Jump from kDefault to kFilter to kFullFilter
  bool ChangeFilterOptions();

  // Switch between different DB options for file ingestion tests.
  bool ChangeOptionsForFileIngestionTest();

  // Return the current option configuration.
  Options CurrentOptions(const anon::OptionsOverride& options_override =
                             anon::OptionsOverride()) const;

  Options CurrentOptions(const Options& default_options,
                         const anon::OptionsOverride& options_override =
                             anon::OptionsOverride()) const;

  static Options GetDefaultOptions();

  Options GetOptions(int option_config,
                     const Options& default_options = GetDefaultOptions(),
                     const anon::OptionsOverride& options_override =
                         anon::OptionsOverride()) const;

  DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db_); }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const Options& options);

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options& options);

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<Options>& options);

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options& options);

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const std::vector<Options>& options);

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options& options);

  void Reopen(const Options& options);

  void Close();

  void DestroyAndReopen(const Options& options);

  void Destroy(const Options& options, bool delete_cf_paths = false);

  Status ReadOnlyReopen(const Options& options);

  Status TryReopen(const Options& options);

  bool IsDirectIOSupported();

  bool IsMemoryMappedAccessSupported() const;

  Status Flush(int cf = 0);

  Status Flush(const std::vector<int>& cf_ids);

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions());

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions());

  Status Merge(const Slice& k, const Slice& v,
               WriteOptions wo = WriteOptions());

  Status Merge(int cf, const Slice& k, const Slice& v,
               WriteOptions wo = WriteOptions());

  Status Delete(const std::string& k);

  Status Delete(int cf, const std::string& k);

  Status SingleDelete(const std::string& k);

  Status SingleDelete(int cf, const std::string& k);

  bool SetPreserveDeletesSequenceNumber(SequenceNumber sn);

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr);

  std::string Get(int cf, const std::string& k,
                  const Snapshot* snapshot = nullptr);

  Status Get(const std::string& k, PinnableSlice* v);

  std::vector<std::string> MultiGet(std::vector<int> cfs,
                                    const std::vector<std::string>& k,
                                    const Snapshot* snapshot = nullptr);

  uint64_t GetNumSnapshots();

  uint64_t GetTimeOldestSnapshots();

  // Return a string that contains all key,value pairs in order,
  // formatted like "(k1->v1)(k2->v2)".
  std::string Contents(int cf = 0);

  std::string AllEntriesFor(const Slice& user_key, int cf = 0);

#ifndef ROCKSDB_LITE
  int NumSortedRuns(int cf = 0);

  uint64_t TotalSize(int cf = 0);

  uint64_t SizeAtLevel(int level);

  size_t TotalLiveFiles(int cf = 0);

  size_t CountLiveFiles();

  int NumTableFilesAtLevel(int level, int cf = 0);

  double CompressionRatioAtLevel(int level, int cf = 0);

  int TotalTableFiles(int cf = 0, int levels = -1);
#endif  // ROCKSDB_LITE

  // Return spread of files per level
  std::string FilesPerLevel(int cf = 0);

  size_t CountFiles();

  uint64_t Size(const Slice& start, const Slice& limit, int cf = 0);

  void Compact(int cf, const Slice& start, const Slice& limit,
               uint32_t target_path_id);

  void Compact(int cf, const Slice& start, const Slice& limit);

  void Compact(const Slice& start, const Slice& limit);

  // Do n memtable compactions, each of which produces an sstable
  // covering the range [small,large].
  void MakeTables(int n, const std::string& small, const std::string& large,
                  int cf = 0);

  // Prevent pushing of new sstables into deeper levels by adding
  // tables that cover a specified range to all levels.
  void FillLevels(const std::string& smallest, const std::string& largest,
                  int cf);

  void MoveFilesToLevel(int level, int cf = 0);

#ifndef ROCKSDB_LITE
  void DumpFileCounts(const char* label);
#endif  // ROCKSDB_LITE

  std::string DumpSSTableList();

  static void GetSstFiles(Env* env, std::string path,
                          std::vector<std::string>* files);

  int GetSstFileCount(std::string path);

  // this will generate non-overlapping files since it keeps increasing key_idx
  void GenerateNewFile(Random* rnd, int* key_idx, bool nowait = false);

  void GenerateNewFile(int fd, Random* rnd, int* key_idx, bool nowait = false);

  static const int kNumKeysByGenerateNewRandomFile;
  static const int KNumKeysByGenerateNewFile = 100;

  void GenerateNewRandomFile(Random* rnd, bool nowait = false);

  std::string IterStatus(Iterator* iter);

  Options OptionsForLogIterTest();

  std::string DummyString(size_t len, char c = 'a');

  void VerifyIterLast(std::string expected_key, int cf = 0);

  // Used to test InplaceUpdate

  // If previous value is nullptr or delta is > than previous value,
  //   sets newValue with delta
  // If previous value is not empty,
  //   updates previous value with 'b' string of previous value size - 1.
  static UpdateStatus updateInPlaceSmallerSize(char* prevValue,
                                               uint32_t* prevSize, Slice delta,
                                               std::string* newValue);

  static UpdateStatus updateInPlaceSmallerVarintSize(char* prevValue,
                                                     uint32_t* prevSize,
                                                     Slice delta,
                                                     std::string* newValue);

  static UpdateStatus updateInPlaceLargerSize(char* prevValue,
                                              uint32_t* prevSize, Slice delta,
                                              std::string* newValue);

  static UpdateStatus updateInPlaceNoAction(char* prevValue, uint32_t* prevSize,
                                            Slice delta, std::string* newValue);

  // Utility method to test InplaceUpdate
  void validateNumberOfEntries(int numValues, int cf = 0);

  void CopyFile(const std::string& source, const std::string& destination,
                uint64_t size = 0);

  std::unordered_map<std::string, uint64_t> GetAllSSTFiles(
      uint64_t* total_size = nullptr);

  std::vector<std::uint64_t> ListTableFiles(Env* env, const std::string& path);

  void VerifyDBFromMap(
      std::map<std::string, std::string> true_data,
      size_t* total_reads_res = nullptr, bool tailing_iter = false,
      std::map<std::string, Status> status = std::map<std::string, Status>());

  void VerifyDBInternal(
      std::vector<std::pair<std::string, std::string>> true_data);

#ifndef ROCKSDB_LITE
  uint64_t GetNumberOfSstFilesForColumnFamily(DB* db,
                                              std::string column_family_name);
#endif  // ROCKSDB_LITE

  uint64_t TestGetTickerCount(const Options& options, Tickers ticker_type) {
    return options.statistics->getTickerCount(ticker_type);
  }
};

}  // namespace rocksdb
