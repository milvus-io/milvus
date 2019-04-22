//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <vector>
#include <string>
#include <thread>

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "memtable/hash_skiplist_rep.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "util/coding.h"
#include "util/fault_injection_test_env.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "utilities/merge_operators.h"

namespace rocksdb {

static const int kValueSize = 1000;

namespace {
std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}
}  // anonymous namespace

// counts how many operations were performed
class EnvCounter : public EnvWrapper {
 public:
  explicit EnvCounter(Env* base)
      : EnvWrapper(base), num_new_writable_file_(0) {}
  int GetNumberOfNewWritableFileCalls() {
    return num_new_writable_file_;
  }
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    ++num_new_writable_file_;
    return EnvWrapper::NewWritableFile(f, r, soptions);
  }

 private:
  std::atomic<int> num_new_writable_file_;
};

class ColumnFamilyTestBase : public testing::Test {
 public:
  ColumnFamilyTestBase(uint32_t format) : rnd_(139), format_(format) {
    env_ = new EnvCounter(Env::Default());
    dbname_ = test::PerThreadDBPath("column_family_test");
    db_options_.create_if_missing = true;
    db_options_.fail_if_options_file_error = true;
    db_options_.env = env_;
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
  }

  ~ColumnFamilyTestBase() override {
    std::vector<ColumnFamilyDescriptor> column_families;
    for (auto h : handles_) {
      ColumnFamilyDescriptor cfdescriptor;
      h->GetDescriptor(&cfdescriptor);
      column_families.push_back(cfdescriptor);
    }
    Close();
    rocksdb::SyncPoint::GetInstance()->DisableProcessing();
    Destroy(column_families);
    delete env_;
  }

  BlockBasedTableOptions GetBlockBasedTableOptions() {
    BlockBasedTableOptions options;
    options.format_version = format_;
    return options;
  }

  // Return the value to associate with the specified key
  Slice Value(int k, std::string* storage) {
    if (k == 0) {
      // Ugh.  Random seed of 0 used to produce no entropy.  This code
      // preserves the implementation that was in place when all of the
      // magic values in this file were picked.
      *storage = std::string(kValueSize, ' ');
      return Slice(*storage);
    } else {
      Random r(k);
      return test::RandomString(&r, kValueSize, storage);
    }
  }

  void Build(int base, int n, int flush_every = 0) {
    std::string key_space, value_space;
    WriteBatch batch;

    for (int i = 0; i < n; i++) {
      if (flush_every != 0 && i != 0 && i % flush_every == 0) {
        DBImpl* dbi = reinterpret_cast<DBImpl*>(db_);
        dbi->TEST_FlushMemTable();
      }

      int keyi = base + i;
      Slice key(DBTestBase::Key(keyi));

      batch.Clear();
      batch.Put(handles_[0], key, Value(keyi, &value_space));
      batch.Put(handles_[1], key, Value(keyi, &value_space));
      batch.Put(handles_[2], key, Value(keyi, &value_space));
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }
  }

  void CheckMissed() {
    uint64_t next_expected = 0;
    uint64_t missed = 0;
    int bad_keys = 0;
    int bad_values = 0;
    int correct = 0;
    std::string value_space;
    for (int cf = 0; cf < 3; cf++) {
      next_expected = 0;
      Iterator* iter = db_->NewIterator(ReadOptions(false, true), handles_[cf]);
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        uint64_t key;
        Slice in(iter->key());
        in.remove_prefix(3);
        if (!ConsumeDecimalNumber(&in, &key) || !in.empty() ||
            key < next_expected) {
          bad_keys++;
          continue;
        }
        missed += (key - next_expected);
        next_expected = key + 1;
        if (iter->value() != Value(static_cast<int>(key), &value_space)) {
          bad_values++;
        } else {
          correct++;
        }
      }
      delete iter;
    }

    ASSERT_EQ(0, bad_keys);
    ASSERT_EQ(0, bad_values);
    ASSERT_EQ(0, missed);
    (void)correct;
  }

  void Close() {
    for (auto h : handles_) {
      if (h) {
        db_->DestroyColumnFamilyHandle(h);
      }
    }
    handles_.clear();
    names_.clear();
    delete db_;
    db_ = nullptr;
  }

  Status TryOpen(std::vector<std::string> cf,
                 std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<ColumnFamilyDescriptor> column_families;
    names_.clear();
    for (size_t i = 0; i < cf.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(
          cf[i], options.size() == 0 ? column_family_options_ : options[i]));
      names_.push_back(cf[i]);
    }
    return DB::Open(db_options_, dbname_, column_families, &handles_, &db_);
  }

  Status OpenReadOnly(std::vector<std::string> cf,
                         std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<ColumnFamilyDescriptor> column_families;
    names_.clear();
    for (size_t i = 0; i < cf.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(
          cf[i], options.size() == 0 ? column_family_options_ : options[i]));
      names_.push_back(cf[i]);
    }
    return DB::OpenForReadOnly(db_options_, dbname_, column_families, &handles_,
                               &db_);
  }

#ifndef ROCKSDB_LITE  // ReadOnlyDB is not supported
  void AssertOpenReadOnly(std::vector<std::string> cf,
                    std::vector<ColumnFamilyOptions> options = {}) {
    ASSERT_OK(OpenReadOnly(cf, options));
  }
#endif  // !ROCKSDB_LITE


  void Open(std::vector<std::string> cf,
            std::vector<ColumnFamilyOptions> options = {}) {
    ASSERT_OK(TryOpen(cf, options));
  }

  void Open() {
    Open({"default"});
  }

  DBImpl* dbfull() { return reinterpret_cast<DBImpl*>(db_); }

  int GetProperty(int cf, std::string property) {
    std::string value;
    EXPECT_TRUE(dbfull()->GetProperty(handles_[cf], property, &value));
#ifndef CYGWIN
    return std::stoi(value);
#else
    return std::strtol(value.c_str(), 0 /* off */, 10 /* base */);
#endif
  }

  bool IsDbWriteStopped() {
#ifndef ROCKSDB_LITE
    uint64_t v;
    EXPECT_TRUE(dbfull()->GetIntProperty("rocksdb.is-write-stopped", &v));
    return (v == 1);
#else
    return dbfull()->TEST_write_controler().IsStopped();
#endif  // !ROCKSDB_LITE
  }

  uint64_t GetDbDelayedWriteRate() {
#ifndef ROCKSDB_LITE
    uint64_t v;
    EXPECT_TRUE(
        dbfull()->GetIntProperty("rocksdb.actual-delayed-write-rate", &v));
    return v;
#else
    if (!dbfull()->TEST_write_controler().NeedsDelay()) {
      return 0;
    }
    return dbfull()->TEST_write_controler().delayed_write_rate();
#endif  // !ROCKSDB_LITE
  }

  void Destroy(const std::vector<ColumnFamilyDescriptor>& column_families =
                  std::vector<ColumnFamilyDescriptor>()) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, Options(db_options_, column_family_options_),
                        column_families));
  }

  void CreateColumnFamilies(
      const std::vector<std::string>& cfs,
      const std::vector<ColumnFamilyOptions> options = {}) {
    int cfi = static_cast<int>(handles_.size());
    handles_.resize(cfi + cfs.size());
    names_.resize(cfi + cfs.size());
    for (size_t i = 0; i < cfs.size(); ++i) {
      const auto& current_cf_opt =
          options.size() == 0 ? column_family_options_ : options[i];
      ASSERT_OK(
          db_->CreateColumnFamily(current_cf_opt, cfs[i], &handles_[cfi]));
      names_[cfi] = cfs[i];

#ifndef ROCKSDB_LITE  // RocksDBLite does not support GetDescriptor
      // Verify the CF options of the returned CF handle.
      ColumnFamilyDescriptor desc;
      ASSERT_OK(handles_[cfi]->GetDescriptor(&desc));
      RocksDBOptionsParser::VerifyCFOptions(desc.options, current_cf_opt);
#endif  // !ROCKSDB_LITE
      cfi++;
    }
  }

  void Reopen(const std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<std::string> names;
    for (auto name : names_) {
      if (name != "") {
        names.push_back(name);
      }
    }
    Close();
    assert(options.size() == 0 || names.size() == options.size());
    Open(names, options);
  }

  void CreateColumnFamiliesAndReopen(const std::vector<std::string>& cfs) {
    CreateColumnFamilies(cfs);
    Reopen();
  }

  void DropColumnFamilies(const std::vector<int>& cfs) {
    for (auto cf : cfs) {
      ASSERT_OK(db_->DropColumnFamily(handles_[cf]));
      db_->DestroyColumnFamilyHandle(handles_[cf]);
      handles_[cf] = nullptr;
      names_[cf] = "";
    }
  }

  void PutRandomData(int cf, int num, int key_value_size, bool save = false) {
    if (cf >= static_cast<int>(keys_.size())) {
      keys_.resize(cf + 1);
    }
    for (int i = 0; i < num; ++i) {
      // 10 bytes for key, rest is value
      if (!save) {
        ASSERT_OK(Put(cf, test::RandomKey(&rnd_, 11),
                      RandomString(&rnd_, key_value_size - 10)));
      } else {
        std::string key = test::RandomKey(&rnd_, 11);
        keys_[cf].insert(key);
        ASSERT_OK(Put(cf, key, RandomString(&rnd_, key_value_size - 10)));
      }
    }
    db_->FlushWAL(false);
  }

#ifndef ROCKSDB_LITE  // TEST functions in DB are not supported in lite
  void WaitForFlush(int cf) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
  }

  void WaitForCompaction() {
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  uint64_t MaxTotalInMemoryState() {
    return dbfull()->TEST_MaxTotalInMemoryState();
  }

  void AssertMaxTotalInMemoryState(uint64_t value) {
    ASSERT_EQ(value, MaxTotalInMemoryState());
  }
#endif  // !ROCKSDB_LITE

  Status Put(int cf, const std::string& key, const std::string& value) {
    return db_->Put(WriteOptions(), handles_[cf], Slice(key), Slice(value));
  }
  Status Merge(int cf, const std::string& key, const std::string& value) {
    return db_->Merge(WriteOptions(), handles_[cf], Slice(key), Slice(value));
  }
  Status Flush(int cf) {
    return db_->Flush(FlushOptions(), handles_[cf]);
  }

  std::string Get(int cf, const std::string& key) {
    ReadOptions options;
    options.verify_checksums = true;
    std::string result;
    Status s = db_->Get(options, handles_[cf], Slice(key), &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  void CompactAll(int cf) {
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[cf], nullptr,
                                nullptr));
  }

  void Compact(int cf, const Slice& start, const Slice& limit) {
    ASSERT_OK(
        db_->CompactRange(CompactRangeOptions(), handles_[cf], &start, &limit));
  }

  int NumTableFilesAtLevel(int level, int cf) {
    return GetProperty(cf,
                       "rocksdb.num-files-at-level" + ToString(level));
  }

#ifndef ROCKSDB_LITE
  // Return spread of files per level
  std::string FilesPerLevel(int cf) {
    std::string result;
    int last_non_zero_offset = 0;
    for (int level = 0; level < dbfull()->NumberLevels(handles_[cf]); level++) {
      int f = NumTableFilesAtLevel(level, cf);
      char buf[100];
      snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
      result += buf;
      if (f > 0) {
        last_non_zero_offset = static_cast<int>(result.size());
      }
    }
    result.resize(last_non_zero_offset);
    return result;
  }
#endif

  void AssertFilesPerLevel(const std::string& value, int cf) {
#ifndef ROCKSDB_LITE
    ASSERT_EQ(value, FilesPerLevel(cf));
#else
    (void) value;
    (void) cf;
#endif
  }

#ifndef ROCKSDB_LITE  // GetLiveFilesMetaData is not supported
  int CountLiveFiles() {
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    return static_cast<int>(metadata.size());
  }
#endif  // !ROCKSDB_LITE

  void AssertCountLiveFiles(int expected_value) {
#ifndef ROCKSDB_LITE
    ASSERT_EQ(expected_value, CountLiveFiles());
#else
    (void) expected_value;
#endif
  }

  // Do n memtable flushes, each of which produces an sstable
  // covering the range [small,large].
  void MakeTables(int cf, int n, const std::string& small,
                  const std::string& large) {
    for (int i = 0; i < n; i++) {
      ASSERT_OK(Put(cf, small, "begin"));
      ASSERT_OK(Put(cf, large, "end"));
      ASSERT_OK(db_->Flush(FlushOptions(), handles_[cf]));
    }
  }

#ifndef ROCKSDB_LITE  // GetSortedWalFiles is not supported
  int CountLiveLogFiles() {
    int micros_wait_for_log_deletion = 20000;
    env_->SleepForMicroseconds(micros_wait_for_log_deletion);
    int ret = 0;
    VectorLogPtr wal_files;
    Status s;
    // GetSortedWalFiles is a flakey function -- it gets all the wal_dir
    // children files and then later checks for their existence. if some of the
    // log files doesn't exist anymore, it reports an error. it does all of this
    // without DB mutex held, so if a background process deletes the log file
    // while the function is being executed, it returns an error. We retry the
    // function 10 times to avoid the error failing the test
    for (int retries = 0; retries < 10; ++retries) {
      wal_files.clear();
      s = db_->GetSortedWalFiles(wal_files);
      if (s.ok()) {
        break;
      }
    }
    EXPECT_OK(s);
    for (const auto& wal : wal_files) {
      if (wal->Type() == kAliveLogFile) {
        ++ret;
      }
    }
    return ret;
    return 0;
  }
#endif  // !ROCKSDB_LITE

  void AssertCountLiveLogFiles(int value) {
#ifndef ROCKSDB_LITE  // GetSortedWalFiles is not supported
    ASSERT_EQ(value, CountLiveLogFiles());
#else
    (void) value;
#endif  // !ROCKSDB_LITE
  }

  void AssertNumberOfImmutableMemtables(std::vector<int> num_per_cf) {
    assert(num_per_cf.size() == handles_.size());

#ifndef ROCKSDB_LITE  // GetProperty is not supported in lite
    for (size_t i = 0; i < num_per_cf.size(); ++i) {
      ASSERT_EQ(num_per_cf[i], GetProperty(static_cast<int>(i),
                                           "rocksdb.num-immutable-mem-table"));
    }
#endif  // !ROCKSDB_LITE
  }

  void CopyFile(const std::string& source, const std::string& destination,
                uint64_t size = 0) {
    const EnvOptions soptions;
    std::unique_ptr<SequentialFile> srcfile;
    ASSERT_OK(env_->NewSequentialFile(source, &srcfile, soptions));
    std::unique_ptr<WritableFile> destfile;
    ASSERT_OK(env_->NewWritableFile(destination, &destfile, soptions));

    if (size == 0) {
      // default argument means copy everything
      ASSERT_OK(env_->GetFileSize(source, &size));
    }

    char buffer[4096];
    Slice slice;
    while (size > 0) {
      uint64_t one = std::min(uint64_t(sizeof(buffer)), size);
      ASSERT_OK(srcfile->Read(one, &slice, buffer));
      ASSERT_OK(destfile->Append(slice));
      size -= slice.size();
    }
    ASSERT_OK(destfile->Close());
  }

  int GetSstFileCount(std::string path) {
    std::vector<std::string> files;
    DBTestBase::GetSstFiles(env_, path, &files);
    return static_cast<int>(files.size());
  }

  void RecalculateWriteStallConditions(ColumnFamilyData* cfd,
      const MutableCFOptions& mutable_cf_options)  {
    // add lock to avoid race condition between
    // `RecalculateWriteStallConditions` which writes to CFStats and
    // background `DBImpl::DumpStats()` threads which read CFStats
    dbfull()->TEST_LockMutex();
    cfd->RecalculateWriteStallConditions(mutable_cf_options);
    dbfull()-> TEST_UnlockMutex();
  }

  std::vector<ColumnFamilyHandle*> handles_;
  std::vector<std::string> names_;
  std::vector<std::set<std::string>> keys_;
  ColumnFamilyOptions column_family_options_;
  DBOptions db_options_;
  std::string dbname_;
  DB* db_ = nullptr;
  EnvCounter* env_;
  Random rnd_;
  uint32_t format_;
};

class ColumnFamilyTest
    : public ColumnFamilyTestBase,
      virtual public ::testing::WithParamInterface<uint32_t> {
 public:
  ColumnFamilyTest() : ColumnFamilyTestBase(GetParam()) {}
};

INSTANTIATE_TEST_CASE_P(FormatDef, ColumnFamilyTest,
                        testing::Values(test::kDefaultFormatVersion));
INSTANTIATE_TEST_CASE_P(FormatLatest, ColumnFamilyTest,
                        testing::Values(test::kLatestFormatVersion));

TEST_P(ColumnFamilyTest, DontReuseColumnFamilyID) {
  for (int iter = 0; iter < 3; ++iter) {
    Open();
    CreateColumnFamilies({"one", "two", "three"});
    for (size_t i = 0; i < handles_.size(); ++i) {
      auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(i, cfh->GetID());
    }
    if (iter == 1) {
      Reopen();
    }
    DropColumnFamilies({3});
    Reopen();
    if (iter == 2) {
      // this tests if max_column_family is correctly persisted with
      // WriteSnapshot()
      Reopen();
    }
    CreateColumnFamilies({"three2"});
    // ID 3 that was used for dropped column family "three" should not be
    // reused
    auto cfh3 = reinterpret_cast<ColumnFamilyHandleImpl*>(handles_[3]);
    ASSERT_EQ(4U, cfh3->GetID());
    Close();
    Destroy();
  }
}

#ifndef ROCKSDB_LITE
TEST_P(ColumnFamilyTest, CreateCFRaceWithGetAggProperty) {
  Open();

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WriteOptionsFile:1",
        "ColumnFamilyTest.CreateCFRaceWithGetAggProperty:1"},
       {"ColumnFamilyTest.CreateCFRaceWithGetAggProperty:2",
        "DBImpl::WriteOptionsFile:2"}});
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread thread([&] { CreateColumnFamilies({"one"}); });

  TEST_SYNC_POINT("ColumnFamilyTest.CreateCFRaceWithGetAggProperty:1");
  uint64_t pv;
  db_->GetAggregatedIntProperty(DB::Properties::kEstimateTableReadersMem, &pv);
  TEST_SYNC_POINT("ColumnFamilyTest.CreateCFRaceWithGetAggProperty:2");

  thread.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
}
#endif  // !ROCKSDB_LITE

class FlushEmptyCFTestWithParam
    : public ColumnFamilyTestBase,
      virtual public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  FlushEmptyCFTestWithParam()
      : ColumnFamilyTestBase(std::get<0>(GetParam())),
        allow_2pc_(std::get<1>(GetParam())) {}

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  bool allow_2pc_;
};

TEST_P(FlushEmptyCFTestWithParam, FlushEmptyCFTest) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  db_options_.env = fault_env.get();
  db_options_.allow_2pc = allow_2pc_;
  Open();
  CreateColumnFamilies({"one", "two"});
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1

  Reopen();
  // Log file A is not dropped after reopening because default column family's
  // min log number is 0.
  // It flushes to SST file X
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 2
  ASSERT_OK(Put(1, "bar", "v2"));  // seqID 3
  // Current log file is file B now. While flushing, a new log file C is created
  // and is set to current. Boths' min log number is set to file C in memory, so
  // after flushing file B is deleted. At the same time, the min log number of
  // default CF is not written to manifest. Log file A still remains.
  // Flushed to SST file Y.
  Flush(1);
  Flush(0);
  ASSERT_OK(Put(1, "bar", "v3"));  // seqID 4
  ASSERT_OK(Put(1, "foo", "v4"));  // seqID 5
  db_->FlushWAL(false);

  // Preserve file system state up to here to simulate a crash condition.
  fault_env->SetFilesystemActive(false);
  std::vector<std::string> names;
  for (auto name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }

  Close();
  fault_env->ResetState();

  // Before opening, there are four files:
  //   Log file A contains seqID 1
  //   Log file C contains seqID 4, 5
  //   SST file X contains seqID 1
  //   SST file Y contains seqID 2, 3
  // Min log number:
  //   default CF: 0
  //   CF one, two: C
  // When opening the DB, all the seqID should be preserved.
  Open(names, {});
  ASSERT_EQ("v4", Get(1, "foo"));
  ASSERT_EQ("v3", Get(1, "bar"));
  Close();

  db_options_.env = env_;
}

TEST_P(FlushEmptyCFTestWithParam, FlushEmptyCFTest2) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  db_options_.env = fault_env.get();
  db_options_.allow_2pc = allow_2pc_;
  Open();
  CreateColumnFamilies({"one", "two"});
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1

  Reopen();
  // Log file A is not dropped after reopening because default column family's
  // min log number is 0.
  // It flushes to SST file X
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 2
  ASSERT_OK(Put(1, "bar", "v2"));  // seqID 3
  // Current log file is file B now. While flushing, a new log file C is created
  // and is set to current. Both CFs' min log number is set to file C so after
  // flushing file B is deleted. Log file A still remains.
  // Flushed to SST file Y.
  Flush(1);
  ASSERT_OK(Put(0, "bar", "v2"));  // seqID 4
  ASSERT_OK(Put(2, "bar", "v2"));  // seqID 5
  ASSERT_OK(Put(1, "bar", "v3"));  // seqID 6
  // Flushing all column families. This forces all CFs' min log to current. This
  // is written to the manifest file. Log file C is cleared.
  Flush(0);
  Flush(1);
  Flush(2);
  // Write to log file D
  ASSERT_OK(Put(1, "bar", "v4"));  // seqID 7
  ASSERT_OK(Put(1, "bar", "v5"));  // seqID 8
  db_->FlushWAL(false);
  // Preserve file system state up to here to simulate a crash condition.
  fault_env->SetFilesystemActive(false);
  std::vector<std::string> names;
  for (auto name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }

  Close();
  fault_env->ResetState();
  // Before opening, there are two logfiles:
  //   Log file A contains seqID 1
  //   Log file D contains seqID 7, 8
  // Min log number:
  //   default CF: D
  //   CF one, two: D
  // When opening the DB, log file D should be replayed using the seqID
  // specified in the file.
  Open(names, {});
  ASSERT_EQ("v1", Get(1, "foo"));
  ASSERT_EQ("v5", Get(1, "bar"));
  Close();

  db_options_.env = env_;
}

INSTANTIATE_TEST_CASE_P(
    FormatDef, FlushEmptyCFTestWithParam,
    testing::Values(std::make_tuple(test::kDefaultFormatVersion, true),
                    std::make_tuple(test::kDefaultFormatVersion, false)));
INSTANTIATE_TEST_CASE_P(
    FormatLatest, FlushEmptyCFTestWithParam,
    testing::Values(std::make_tuple(test::kLatestFormatVersion, true),
                    std::make_tuple(test::kLatestFormatVersion, false)));

TEST_P(ColumnFamilyTest, AddDrop) {
  Open();
  CreateColumnFamilies({"one", "two", "three"});
  ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
  ASSERT_EQ("NOT_FOUND", Get(2, "fodor"));
  DropColumnFamilies({2});
  ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
  CreateColumnFamilies({"four"});
  ASSERT_EQ("NOT_FOUND", Get(3, "fodor"));
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  ASSERT_EQ("NOT_FOUND", Get(3, "fodor"));
  Close();
  ASSERT_TRUE(TryOpen({"default"}).IsInvalidArgument());
  Open({"default", "one", "three", "four"});
  DropColumnFamilies({1});
  Reopen();
  Close();

  std::vector<std::string> families;
  ASSERT_OK(DB::ListColumnFamilies(db_options_, dbname_, &families));
  std::sort(families.begin(), families.end());
  ASSERT_TRUE(families ==
              std::vector<std::string>({"default", "four", "three"}));
}

TEST_P(ColumnFamilyTest, BulkAddDrop) {
  constexpr int kNumCF = 1000;
  ColumnFamilyOptions cf_options;
  WriteOptions write_options;
  Open();
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyHandle*> cf_handles;
  for (int i = 1; i <= kNumCF; i++) {
    cf_names.push_back("cf1-" + ToString(i));
  }
  ASSERT_OK(db_->CreateColumnFamilies(cf_options, cf_names, &cf_handles));
  for (int i = 1; i <= kNumCF; i++) {
    ASSERT_OK(db_->Put(write_options, cf_handles[i - 1], "foo", "bar"));
  }
  ASSERT_OK(db_->DropColumnFamilies(cf_handles));
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  for (auto* handle : cf_handles) {
    delete handle;
  }
  cf_handles.clear();
  for (int i = 1; i <= kNumCF; i++) {
    cf_descriptors.emplace_back("cf2-" + ToString(i), ColumnFamilyOptions());
  }
  ASSERT_OK(db_->CreateColumnFamilies(cf_descriptors, &cf_handles));
  for (int i = 1; i <= kNumCF; i++) {
    ASSERT_OK(db_->Put(write_options, cf_handles[i - 1], "foo", "bar"));
  }
  ASSERT_OK(db_->DropColumnFamilies(cf_handles));
  for (auto* handle : cf_handles) {
    delete handle;
  }
  Close();
  std::vector<std::string> families;
  ASSERT_OK(DB::ListColumnFamilies(db_options_, dbname_, &families));
  std::sort(families.begin(), families.end());
  ASSERT_TRUE(families == std::vector<std::string>({"default"}));
}

TEST_P(ColumnFamilyTest, DropTest) {
  // first iteration - dont reopen DB before dropping
  // second iteration - reopen DB before dropping
  for (int iter = 0; iter < 2; ++iter) {
    Open({"default"});
    CreateColumnFamiliesAndReopen({"pikachu"});
    for (int i = 0; i < 100; ++i) {
      ASSERT_OK(Put(1, ToString(i), "bar" + ToString(i)));
    }
    ASSERT_OK(Flush(1));

    if (iter == 1) {
      Reopen();
    }
    ASSERT_EQ("bar1", Get(1, "1"));

    AssertCountLiveFiles(1);
    DropColumnFamilies({1});
    // make sure that all files are deleted when we drop the column family
    AssertCountLiveFiles(0);
    Destroy();
  }
}

TEST_P(ColumnFamilyTest, WriteBatchFailure) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  WriteBatch batch;
  batch.Put(handles_[0], Slice("existing"), Slice("column-family"));
  batch.Put(handles_[1], Slice("non-existing"), Slice("column-family"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  DropColumnFamilies({1});
  WriteOptions woptions_ignore_missing_cf;
  woptions_ignore_missing_cf.ignore_missing_column_families = true;
  batch.Put(handles_[0], Slice("still here"), Slice("column-family"));
  ASSERT_OK(db_->Write(woptions_ignore_missing_cf, &batch));
  ASSERT_EQ("column-family", Get(0, "still here"));
  Status s = db_->Write(WriteOptions(), &batch);
  ASSERT_TRUE(s.IsInvalidArgument());
  Close();
}

TEST_P(ColumnFamilyTest, ReadWrite) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "bar", "v2"));
  ASSERT_OK(Put(1, "mirko", "v3"));
  ASSERT_OK(Put(0, "foo", "v2"));
  ASSERT_OK(Put(2, "fodor", "v5"));

  for (int iter = 0; iter <= 3; ++iter) {
    ASSERT_EQ("v2", Get(0, "foo"));
    ASSERT_EQ("v2", Get(0, "bar"));
    ASSERT_EQ("v3", Get(1, "mirko"));
    ASSERT_EQ("v5", Get(2, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(0, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(2, "foo"));
    if (iter <= 1) {
      Reopen();
    }
  }
  Close();
}

TEST_P(ColumnFamilyTest, IgnoreRecoveredLog) {
  std::string backup_logs = dbname_ + "/backup_logs";

  // delete old files in backup_logs directory
  ASSERT_OK(env_->CreateDirIfMissing(dbname_));
  ASSERT_OK(env_->CreateDirIfMissing(backup_logs));
  std::vector<std::string> old_files;
  env_->GetChildren(backup_logs, &old_files);
  for (auto& file : old_files) {
    if (file != "." && file != "..") {
      env_->DeleteFile(backup_logs + "/" + file);
    }
  }

  column_family_options_.merge_operator =
      MergeOperators::CreateUInt64AddOperator();
  db_options_.wal_dir = dbname_ + "/logs";
  Destroy();
  Open();
  CreateColumnFamilies({"cf1", "cf2"});

  // fill up the DB
  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Merge(0, "foo", one));
  ASSERT_OK(Merge(1, "mirko", one));
  ASSERT_OK(Merge(0, "foo", one));
  ASSERT_OK(Merge(2, "bla", one));
  ASSERT_OK(Merge(2, "fodor", one));
  ASSERT_OK(Merge(0, "bar", one));
  ASSERT_OK(Merge(2, "bla", one));
  ASSERT_OK(Merge(1, "mirko", two));
  ASSERT_OK(Merge(1, "franjo", one));

  // copy the logs to backup
  std::vector<std::string> logs;
  env_->GetChildren(db_options_.wal_dir, &logs);
  for (auto& log : logs) {
    if (log != ".." && log != ".") {
      CopyFile(db_options_.wal_dir + "/" + log, backup_logs + "/" + log);
    }
  }

  // recover the DB
  Close();

  // 1. check consistency
  // 2. copy the logs from backup back to WAL dir. if the recovery happens
  // again on the same log files, this should lead to incorrect results
  // due to applying merge operator twice
  // 3. check consistency
  for (int iter = 0; iter < 2; ++iter) {
    // assert consistency
    Open({"default", "cf1", "cf2"});
    ASSERT_EQ(two, Get(0, "foo"));
    ASSERT_EQ(one, Get(0, "bar"));
    ASSERT_EQ(three, Get(1, "mirko"));
    ASSERT_EQ(one, Get(1, "franjo"));
    ASSERT_EQ(one, Get(2, "fodor"));
    ASSERT_EQ(two, Get(2, "bla"));
    Close();

    if (iter == 0) {
      // copy the logs from backup back to wal dir
      for (auto& log : logs) {
        if (log != ".." && log != ".") {
          CopyFile(backup_logs + "/" + log, db_options_.wal_dir + "/" + log);
        }
      }
    }
  }
}

#ifndef ROCKSDB_LITE  // TEST functions used are not supported
TEST_P(ColumnFamilyTest, FlushTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "bar", "v2"));
  ASSERT_OK(Put(1, "mirko", "v3"));
  ASSERT_OK(Put(0, "foo", "v2"));
  ASSERT_OK(Put(2, "fodor", "v5"));

  for (int j = 0; j < 2; j++) {
    ReadOptions ro;
    std::vector<Iterator*> iterators;
    // Hold super version.
    if (j == 0) {
      ASSERT_OK(db_->NewIterators(ro, handles_, &iterators));
    }

    for (int i = 0; i < 3; ++i) {
      uint64_t max_total_in_memory_state =
          MaxTotalInMemoryState();
      Flush(i);
      AssertMaxTotalInMemoryState(max_total_in_memory_state);
    }
    ASSERT_OK(Put(1, "foofoo", "bar"));
    ASSERT_OK(Put(0, "foofoo", "bar"));

    for (auto* it : iterators) {
      delete it;
    }
  }
  Reopen();

  for (int iter = 0; iter <= 2; ++iter) {
    ASSERT_EQ("v2", Get(0, "foo"));
    ASSERT_EQ("v2", Get(0, "bar"));
    ASSERT_EQ("v3", Get(1, "mirko"));
    ASSERT_EQ("v5", Get(2, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(0, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(2, "foo"));
    if (iter <= 1) {
      Reopen();
    }
  }
  Close();
}

// Makes sure that obsolete log files get deleted
TEST_P(ColumnFamilyTest, LogDeletionTest) {
  db_options_.max_total_wal_size = std::numeric_limits<uint64_t>::max();
  column_family_options_.arena_block_size = 4 * 1024;
  column_family_options_.write_buffer_size = 128000;  // 128KB
  Open();
  CreateColumnFamilies({"one", "two", "three", "four"});
  // Each bracket is one log file. if number is in (), it means
  // we don't need it anymore (it's been flushed)
  // []
  AssertCountLiveLogFiles(0);
  PutRandomData(0, 1, 128);
  // [0]
  PutRandomData(1, 1, 128);
  // [0, 1]
  PutRandomData(1, 1000, 128);
  WaitForFlush(1);
  // [0, (1)] [1]
  AssertCountLiveLogFiles(2);
  PutRandomData(0, 1, 128);
  // [0, (1)] [0, 1]
  AssertCountLiveLogFiles(2);
  PutRandomData(2, 1, 128);
  // [0, (1)] [0, 1, 2]
  PutRandomData(2, 1000, 128);
  WaitForFlush(2);
  // [0, (1)] [0, 1, (2)] [2]
  AssertCountLiveLogFiles(3);
  PutRandomData(2, 1000, 128);
  WaitForFlush(2);
  // [0, (1)] [0, 1, (2)] [(2)] [2]
  AssertCountLiveLogFiles(4);
  PutRandomData(3, 1, 128);
  // [0, (1)] [0, 1, (2)] [(2)] [2, 3]
  PutRandomData(1, 1, 128);
  // [0, (1)] [0, 1, (2)] [(2)] [1, 2, 3]
  AssertCountLiveLogFiles(4);
  PutRandomData(1, 1000, 128);
  WaitForFlush(1);
  // [0, (1)] [0, (1), (2)] [(2)] [(1), 2, 3] [1]
  AssertCountLiveLogFiles(5);
  PutRandomData(0, 1000, 128);
  WaitForFlush(0);
  // [(0), (1)] [(0), (1), (2)] [(2)] [(1), 2, 3] [1, (0)] [0]
  // delete obsolete logs -->
  // [(1), 2, 3] [1, (0)] [0]
  AssertCountLiveLogFiles(3);
  PutRandomData(0, 1000, 128);
  WaitForFlush(0);
  // [(1), 2, 3] [1, (0)], [(0)] [0]
  AssertCountLiveLogFiles(4);
  PutRandomData(1, 1000, 128);
  WaitForFlush(1);
  // [(1), 2, 3] [(1), (0)] [(0)] [0, (1)] [1]
  AssertCountLiveLogFiles(5);
  PutRandomData(2, 1000, 128);
  WaitForFlush(2);
  // [(1), (2), 3] [(1), (0)] [(0)] [0, (1)] [1, (2)], [2]
  AssertCountLiveLogFiles(6);
  PutRandomData(3, 1000, 128);
  WaitForFlush(3);
  // [(1), (2), (3)] [(1), (0)] [(0)] [0, (1)] [1, (2)], [2, (3)] [3]
  // delete obsolete logs -->
  // [0, (1)] [1, (2)], [2, (3)] [3]
  AssertCountLiveLogFiles(4);
  Close();
}
#endif  // !ROCKSDB_LITE

TEST_P(ColumnFamilyTest, CrashAfterFlush) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  db_options_.env = fault_env.get();
  Open();
  CreateColumnFamilies({"one"});

  WriteBatch batch;
  batch.Put(handles_[0], Slice("foo"), Slice("bar"));
  batch.Put(handles_[1], Slice("foo"), Slice("bar"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  Flush(0);
  fault_env->SetFilesystemActive(false);

  std::vector<std::string> names;
  for (auto name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }
  Close();
  fault_env->DropUnsyncedFileData();
  fault_env->ResetState();
  Open(names, {});

  // Write batch should be atomic.
  ASSERT_EQ(Get(0, "foo"), Get(1, "foo"));

  Close();
  db_options_.env = env_;
}

TEST_P(ColumnFamilyTest, OpenNonexistentColumnFamily) {
  ASSERT_OK(TryOpen({"default"}));
  Close();
  ASSERT_TRUE(TryOpen({"default", "dne"}).IsInvalidArgument());
}

#ifndef ROCKSDB_LITE  // WaitForFlush() is not supported
// Makes sure that obsolete log files get deleted
TEST_P(ColumnFamilyTest, DifferentWriteBufferSizes) {
  // disable flushing stale column families
  db_options_.max_total_wal_size = std::numeric_limits<uint64_t>::max();
  Open();
  CreateColumnFamilies({"one", "two", "three"});
  ColumnFamilyOptions default_cf, one, two, three;
  // setup options. all column families have max_write_buffer_number setup to 10
  // "default" -> 100KB memtable, start flushing immediatelly
  // "one" -> 200KB memtable, start flushing with two immutable memtables
  // "two" -> 1MB memtable, start flushing with three immutable memtables
  // "three" -> 90KB memtable, start flushing with four immutable memtables
  default_cf.write_buffer_size = 100000;
  default_cf.arena_block_size = 4 * 4096;
  default_cf.max_write_buffer_number = 10;
  default_cf.min_write_buffer_number_to_merge = 1;
  default_cf.max_write_buffer_number_to_maintain = 0;
  one.write_buffer_size = 200000;
  one.arena_block_size = 4 * 4096;
  one.max_write_buffer_number = 10;
  one.min_write_buffer_number_to_merge = 2;
  one.max_write_buffer_number_to_maintain = 1;
  two.write_buffer_size = 1000000;
  two.arena_block_size = 4 * 4096;
  two.max_write_buffer_number = 10;
  two.min_write_buffer_number_to_merge = 3;
  two.max_write_buffer_number_to_maintain = 2;
  three.write_buffer_size = 4096 * 22;
  three.arena_block_size = 4096;
  three.max_write_buffer_number = 10;
  three.min_write_buffer_number_to_merge = 4;
  three.max_write_buffer_number_to_maintain = -1;

  Reopen({default_cf, one, two, three});

  int micros_wait_for_flush = 10000;
  PutRandomData(0, 100, 1000);
  WaitForFlush(0);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(1);
  PutRandomData(1, 200, 1000);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 0, 0});
  AssertCountLiveLogFiles(2);
  PutRandomData(2, 1000, 1000);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 1, 0});
  AssertCountLiveLogFiles(3);
  PutRandomData(2, 1000, 1000);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 2, 0});
  AssertCountLiveLogFiles(4);
  PutRandomData(3, 93, 990);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 2, 1});
  AssertCountLiveLogFiles(5);
  PutRandomData(3, 88, 990);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 2, 2});
  AssertCountLiveLogFiles(6);
  PutRandomData(3, 88, 990);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 2, 3});
  AssertCountLiveLogFiles(7);
  PutRandomData(0, 100, 1000);
  WaitForFlush(0);
  AssertNumberOfImmutableMemtables({0, 1, 2, 3});
  AssertCountLiveLogFiles(8);
  PutRandomData(2, 100, 10000);
  WaitForFlush(2);
  AssertNumberOfImmutableMemtables({0, 1, 0, 3});
  AssertCountLiveLogFiles(9);
  PutRandomData(3, 88, 990);
  WaitForFlush(3);
  AssertNumberOfImmutableMemtables({0, 1, 0, 0});
  AssertCountLiveLogFiles(10);
  PutRandomData(3, 88, 990);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 0, 1});
  AssertCountLiveLogFiles(11);
  PutRandomData(1, 200, 1000);
  WaitForFlush(1);
  AssertNumberOfImmutableMemtables({0, 0, 0, 1});
  AssertCountLiveLogFiles(5);
  PutRandomData(3, 88 * 3, 990);
  WaitForFlush(3);
  PutRandomData(3, 88 * 4, 990);
  WaitForFlush(3);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(12);
  PutRandomData(0, 100, 1000);
  WaitForFlush(0);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(12);
  PutRandomData(2, 3 * 1000, 1000);
  WaitForFlush(2);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(12);
  PutRandomData(1, 2*200, 1000);
  WaitForFlush(1);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(7);
  Close();
}
#endif  // !ROCKSDB_LITE

// The test is commented out because we want to test that snapshot is
// not created for memtables not supported it, but There isn't a memtable
// that doesn't support snapshot right now. If we have one later, we can
// re-enable the test.
//
// #ifndef ROCKSDB_LITE  // Cuckoo is not supported in lite
//   TEST_P(ColumnFamilyTest, MemtableNotSupportSnapshot) {
//   db_options_.allow_concurrent_memtable_write = false;
//   Open();
//   auto* s1 = dbfull()->GetSnapshot();
//   ASSERT_TRUE(s1 != nullptr);
//   dbfull()->ReleaseSnapshot(s1);

//   // Add a column family that doesn't support snapshot
//   ColumnFamilyOptions first;
//   first.memtable_factory.reset(new DummyMemtableNotSupportingSnapshot());
//   CreateColumnFamilies({"first"}, {first});
//   auto* s2 = dbfull()->GetSnapshot();
//   ASSERT_TRUE(s2 == nullptr);

//   // Add a column family that supports snapshot. Snapshot stays not
//   supported. ColumnFamilyOptions second; CreateColumnFamilies({"second"},
//   {second}); auto* s3 = dbfull()->GetSnapshot(); ASSERT_TRUE(s3 == nullptr);
//   Close();
// }
// #endif  // !ROCKSDB_LITE

class TestComparator : public Comparator {
  int Compare(const rocksdb::Slice& /*a*/,
              const rocksdb::Slice& /*b*/) const override {
    return 0;
  }
  const char* Name() const override { return "Test"; }
  void FindShortestSeparator(std::string* /*start*/,
                             const rocksdb::Slice& /*limit*/) const override {}
  void FindShortSuccessor(std::string* /*key*/) const override {}
};

static TestComparator third_comparator;
static TestComparator fourth_comparator;

// Test that we can retrieve the comparator from a created CF
TEST_P(ColumnFamilyTest, GetComparator) {
  Open();
  // Add a column family with no comparator specified
  CreateColumnFamilies({"first"});
  const Comparator* comp = handles_[0]->GetComparator();
  ASSERT_EQ(comp, BytewiseComparator());

  // Add three column families - one with no comparator and two
  // with comparators specified
  ColumnFamilyOptions second, third, fourth;
  second.comparator = &third_comparator;
  third.comparator = &fourth_comparator;
  CreateColumnFamilies({"second", "third", "fourth"}, {second, third, fourth});
  ASSERT_EQ(handles_[1]->GetComparator(), BytewiseComparator());
  ASSERT_EQ(handles_[2]->GetComparator(), &third_comparator);
  ASSERT_EQ(handles_[3]->GetComparator(), &fourth_comparator);
  Close();
}

TEST_P(ColumnFamilyTest, DifferentMergeOperators) {
  Open();
  CreateColumnFamilies({"first", "second"});
  ColumnFamilyOptions default_cf, first, second;
  first.merge_operator = MergeOperators::CreateUInt64AddOperator();
  second.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen({default_cf, first, second});

  std::string one, two, three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);

  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_TRUE(Merge(0, "foo", two).IsNotSupported());
  ASSERT_EQ(Get(0, "foo"), one);

  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two));
  ASSERT_EQ(Get(1, "foo"), three);

  ASSERT_OK(Put(2, "foo", two));
  ASSERT_OK(Put(2, "foo", one));
  ASSERT_OK(Merge(2, "foo", two));
  ASSERT_EQ(Get(2, "foo"), one + "," + two);
  Close();
}

#ifndef ROCKSDB_LITE  // WaitForFlush() is not supported
TEST_P(ColumnFamilyTest, DifferentCompactionStyles) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf, one, two;
  db_options_.max_open_files = 20;  // only 10 files in file cache

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = static_cast<uint64_t>(1) << 60;

  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  two.compaction_style = kCompactionStyleLevel;
  two.num_levels = 4;
  two.level0_file_num_compaction_trigger = 3;
  two.write_buffer_size = 100000;

  Reopen({default_cf, one, two});

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 1; ++i) {
    PutRandomData(1, 10, 12000);
    PutRandomData(1, 1, 10);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(i + 1), 1);
  }

  // SETUP column family "two" -- level style with 4 levels
  for (int i = 0; i < two.level0_file_num_compaction_trigger - 1; ++i) {
    PutRandomData(2, 10, 12000);
    PutRandomData(2, 1, 10);
    WaitForFlush(2);
    AssertFilesPerLevel(ToString(i + 1), 2);
  }

  // TRIGGER compaction "one"
  PutRandomData(1, 10, 12000);
  PutRandomData(1, 1, 10);

  // TRIGGER compaction "two"
  PutRandomData(2, 10, 12000);
  PutRandomData(2, 1, 10);

  // WAIT for compactions
  WaitForCompaction();

  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);

  // VERIFY compaction "two"
  AssertFilesPerLevel("0,1", 2);
  CompactAll(2);
  AssertFilesPerLevel("0,1", 2);

  Close();
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE
// Sync points not supported in RocksDB Lite

TEST_P(ColumnFamilyTest, MultipleManualCompactions) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf, one, two;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  two.compaction_style = kCompactionStyleLevel;
  two.num_levels = 4;
  two.level0_file_num_compaction_trigger = 3;
  two.write_buffer_size = 100000;

  Reopen({default_cf, one, two});

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(i + 1), 1);
  }
  bool cf_1_1 = true;
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::MultiManual:4", "ColumnFamilyTest::MultiManual:1"},
       {"ColumnFamilyTest::MultiManual:2", "ColumnFamilyTest::MultiManual:5"},
       {"ColumnFamilyTest::MultiManual:2", "ColumnFamilyTest::MultiManual:3"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1) {
          TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:4");
          cf_1_1 = false;
          TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:3");
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  std::vector<port::Thread> threads;
  threads.emplace_back([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  // SETUP column family "two" -- level style with 4 levels
  for (int i = 0; i < two.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(2, 10, 12000);
    PutRandomData(2, 1, 10);
    WaitForFlush(2);
    AssertFilesPerLevel(ToString(i + 1), 2);
  }
  threads.emplace_back([&] {
    TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:1");
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[2], nullptr, nullptr));
    TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:2");
  });

  TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:5");
  for (auto& t : threads) {
    t.join();
  }

  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);

  // VERIFY compaction "two"
  AssertFilesPerLevel("0,1", 2);
  CompactAll(2);
  AssertFilesPerLevel("0,1", 2);
  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

TEST_P(ColumnFamilyTest, AutomaticAndManualCompactions) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf, one, two;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  two.compaction_style = kCompactionStyleLevel;
  two.num_levels = 4;
  two.level0_file_num_compaction_trigger = 3;
  two.write_buffer_size = 100000;

  Reopen({default_cf, one, two});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  bool cf_1_1 = true;
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::AutoManual:4", "ColumnFamilyTest::AutoManual:1"},
       {"ColumnFamilyTest::AutoManual:2", "ColumnFamilyTest::AutoManual:5"},
       {"ColumnFamilyTest::AutoManual:2", "ColumnFamilyTest::AutoManual:3"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1) {
          cf_1_1 = false;
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:4");
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:3");
        }
      });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(i + 1), 1);
  }

  TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:1");

  // SETUP column family "two" -- level style with 4 levels
  for (int i = 0; i < two.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(2, 10, 12000);
    PutRandomData(2, 1, 10);
    WaitForFlush(2);
    AssertFilesPerLevel(ToString(i + 1), 2);
  }
  rocksdb::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[2], nullptr, nullptr));
    TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:2");
  });

  TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:5");
  threads.join();

  // WAIT for compactions
  WaitForCompaction();

  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);

  // VERIFY compaction "two"
  AssertFilesPerLevel("0,1", 2);
  CompactAll(2);
  AssertFilesPerLevel("0,1", 2);
  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, ManualAndAutomaticCompactions) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf, one, two;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  two.compaction_style = kCompactionStyleLevel;
  two.num_levels = 4;
  two.level0_file_num_compaction_trigger = 3;
  two.write_buffer_size = 100000;

  Reopen({default_cf, one, two});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(i + 1), 1);
  }
  bool cf_1_1 = true;
  bool cf_1_2 = true;
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:1"},
       {"ColumnFamilyTest::ManualAuto:5", "ColumnFamilyTest::ManualAuto:2"},
       {"ColumnFamilyTest::ManualAuto:2", "ColumnFamilyTest::ManualAuto:3"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:4");
          cf_1_1 = false;
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:3");
        } else if (cf_1_2) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:2");
          cf_1_2 = false;
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  rocksdb::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:1");

  // SETUP column family "two" -- level style with 4 levels
  for (int i = 0; i < two.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(2, 10, 12000);
    PutRandomData(2, 1, 10);
    WaitForFlush(2);
    AssertFilesPerLevel(ToString(i + 1), 2);
  }
  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:5");
  threads.join();

  // WAIT for compactions
  WaitForCompaction();

  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);

  // VERIFY compaction "two"
  AssertFilesPerLevel("0,1", 2);
  CompactAll(2);
  AssertFilesPerLevel("0,1", 2);
  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, SameCFManualManualCompactions) {
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyOptions default_cf, one;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  Reopen({default_cf, one});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(i + 1), 1);
  }
  bool cf_1_1 = true;
  bool cf_1_2 = true;
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::ManualManual:4", "ColumnFamilyTest::ManualManual:2"},
       {"ColumnFamilyTest::ManualManual:4", "ColumnFamilyTest::ManualManual:5"},
       {"ColumnFamilyTest::ManualManual:1", "ColumnFamilyTest::ManualManual:2"},
       {"ColumnFamilyTest::ManualManual:1",
        "ColumnFamilyTest::ManualManual:3"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:4");
          cf_1_1 = false;
          TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:3");
        } else if (cf_1_2) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:2");
          cf_1_2 = false;
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  rocksdb::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = true;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:5");

  WaitForFlush(1);

  // Add more L0 files and force another manual compaction
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(one.level0_file_num_compaction_trigger + i),
                        1);
  }

  rocksdb::port::Thread threads1([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:1");

  threads.join();
  threads1.join();
  WaitForCompaction();
  // VERIFY compaction "one"
  ASSERT_LE(NumTableFilesAtLevel(0, 1), 2);

  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, SameCFManualAutomaticCompactions) {
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyOptions default_cf, one;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  Reopen({default_cf, one});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(i + 1), 1);
  }
  bool cf_1_1 = true;
  bool cf_1_2 = true;
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:2"},
       {"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:5"},
       {"ColumnFamilyTest::ManualAuto:1", "ColumnFamilyTest::ManualAuto:2"},
       {"ColumnFamilyTest::ManualAuto:1", "ColumnFamilyTest::ManualAuto:3"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:4");
          cf_1_1 = false;
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:3");
        } else if (cf_1_2) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:2");
          cf_1_2 = false;
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  rocksdb::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:5");

  WaitForFlush(1);

  // Add more L0 files and force automatic compaction
  for (int i = 0; i < one.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(one.level0_file_num_compaction_trigger + i),
                        1);
  }

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:1");

  threads.join();
  WaitForCompaction();
  // VERIFY compaction "one"
  ASSERT_LE(NumTableFilesAtLevel(0, 1), 2);

  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, SameCFManualAutomaticCompactionsLevel) {
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyOptions default_cf, one;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleLevel;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 3;
  one.write_buffer_size = 120000;

  Reopen({default_cf, one});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // SETUP column family "one" -- level style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(i + 1), 1);
  }
  bool cf_1_1 = true;
  bool cf_1_2 = true;
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:2"},
       {"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:5"},
       {"ColumnFamilyTest::ManualAuto:3", "ColumnFamilyTest::ManualAuto:2"},
       {"LevelCompactionPicker::PickCompactionBySize:0",
        "ColumnFamilyTest::ManualAuto:3"},
       {"ColumnFamilyTest::ManualAuto:1", "ColumnFamilyTest::ManualAuto:3"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:4");
          cf_1_1 = false;
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:3");
        } else if (cf_1_2) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:2");
          cf_1_2 = false;
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  rocksdb::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:5");

  // Add more L0 files and force automatic compaction
  for (int i = 0; i < one.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(one.level0_file_num_compaction_trigger + i),
                        1);
  }

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:1");

  threads.join();
  WaitForCompaction();
  // VERIFY compaction "one"
  AssertFilesPerLevel("0,1", 1);

  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

// In this test, we generate enough files to trigger automatic compactions.
// The automatic compaction waits in NonTrivial:AfterRun
// We generate more files and then trigger an automatic compaction
// This will wait because the automatic compaction has files it needs.
// Once the conflict is hit, the automatic compaction starts and ends
// Then the manual will run and end.
TEST_P(ColumnFamilyTest, SameCFAutomaticManualCompactions) {
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyOptions default_cf, one;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  Reopen({default_cf, one});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  bool cf_1_1 = true;
  bool cf_1_2 = true;
  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::AutoManual:4", "ColumnFamilyTest::AutoManual:2"},
       {"ColumnFamilyTest::AutoManual:4", "ColumnFamilyTest::AutoManual:5"},
       {"CompactionPicker::CompactRange:Conflict",
        "ColumnFamilyTest::AutoManual:3"}});
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1) {
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:4");
          cf_1_1 = false;
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:3");
        } else if (cf_1_2) {
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:2");
          cf_1_2 = false;
        }
      });

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(ToString(i + 1), 1);
  }

  TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:5");

  // Add another L0 file and force automatic compaction
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
  }

  CompactRangeOptions compact_options;
  compact_options.exclusive_manual_compaction = false;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));

  TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:1");

  WaitForCompaction();
  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);
  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // Tailing iterator not supported
namespace {
std::string IterStatus(Iterator* iter) {
  std::string result;
  if (iter->Valid()) {
    result = iter->key().ToString() + "->" + iter->value().ToString();
  } else {
    result = "(invalid)";
  }
  return result;
}
}  // anonymous namespace

TEST_P(ColumnFamilyTest, NewIteratorsTest) {
  // iter == 0 -- no tailing
  // iter == 2 -- tailing
  for (int iter = 0; iter < 2; ++iter) {
    Open();
    CreateColumnFamiliesAndReopen({"one", "two"});
    ASSERT_OK(Put(0, "a", "b"));
    ASSERT_OK(Put(1, "b", "a"));
    ASSERT_OK(Put(2, "c", "m"));
    ASSERT_OK(Put(2, "v", "t"));
    std::vector<Iterator*> iterators;
    ReadOptions options;
    options.tailing = (iter == 1);
    ASSERT_OK(db_->NewIterators(options, handles_, &iterators));

    for (auto it : iterators) {
      it->SeekToFirst();
    }
    ASSERT_EQ(IterStatus(iterators[0]), "a->b");
    ASSERT_EQ(IterStatus(iterators[1]), "b->a");
    ASSERT_EQ(IterStatus(iterators[2]), "c->m");

    ASSERT_OK(Put(1, "x", "x"));

    for (auto it : iterators) {
      it->Next();
    }

    ASSERT_EQ(IterStatus(iterators[0]), "(invalid)");
    if (iter == 0) {
      // no tailing
      ASSERT_EQ(IterStatus(iterators[1]), "(invalid)");
    } else {
      // tailing
      ASSERT_EQ(IterStatus(iterators[1]), "x->x");
    }
    ASSERT_EQ(IterStatus(iterators[2]), "v->t");

    for (auto it : iterators) {
      delete it;
    }
    Destroy();
  }
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // ReadOnlyDB is not supported
TEST_P(ColumnFamilyTest, ReadOnlyDBTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two", "three", "four"});
  ASSERT_OK(Put(0, "a", "b"));
  ASSERT_OK(Put(1, "foo", "bla"));
  ASSERT_OK(Put(2, "foo", "blabla"));
  ASSERT_OK(Put(3, "foo", "blablabla"));
  ASSERT_OK(Put(4, "foo", "blablablabla"));

  DropColumnFamilies({2});
  Close();
  // open only a subset of column families
  AssertOpenReadOnly({"default", "one", "four"});
  ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  ASSERT_EQ("bla", Get(1, "foo"));
  ASSERT_EQ("blablablabla", Get(2, "foo"));


  // test newiterators
  {
    std::vector<Iterator*> iterators;
    ASSERT_OK(db_->NewIterators(ReadOptions(), handles_, &iterators));
    for (auto it : iterators) {
      it->SeekToFirst();
    }
    ASSERT_EQ(IterStatus(iterators[0]), "a->b");
    ASSERT_EQ(IterStatus(iterators[1]), "foo->bla");
    ASSERT_EQ(IterStatus(iterators[2]), "foo->blablablabla");
    for (auto it : iterators) {
      it->Next();
    }
    ASSERT_EQ(IterStatus(iterators[0]), "(invalid)");
    ASSERT_EQ(IterStatus(iterators[1]), "(invalid)");
    ASSERT_EQ(IterStatus(iterators[2]), "(invalid)");

    for (auto it : iterators) {
      delete it;
    }
  }

  Close();
  // can't open dropped column family
  Status s = OpenReadOnly({"default", "one", "two"});
  ASSERT_TRUE(!s.ok());

  // Can't open without specifying default column family
  s = OpenReadOnly({"one", "four"});
  ASSERT_TRUE(!s.ok());
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  //  WaitForFlush() is not supported in lite
TEST_P(ColumnFamilyTest, DontRollEmptyLogs) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two", "three", "four"});

  for (size_t i = 0; i < handles_.size(); ++i) {
    PutRandomData(static_cast<int>(i), 10, 100);
  }
  int num_writable_file_start = env_->GetNumberOfNewWritableFileCalls();
  // this will trigger the flushes
  for (int i = 0; i <= 4; ++i) {
    ASSERT_OK(Flush(i));
  }

  for (int i = 0; i < 4; ++i) {
    WaitForFlush(i);
  }
  int total_new_writable_files =
      env_->GetNumberOfNewWritableFileCalls() - num_writable_file_start;
  ASSERT_EQ(static_cast<size_t>(total_new_writable_files), handles_.size() + 1);
  Close();
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  //  WaitForCompaction() is not supported in lite
TEST_P(ColumnFamilyTest, FlushStaleColumnFamilies) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf, one, two;
  default_cf.write_buffer_size = 100000;  // small write buffer size
  default_cf.arena_block_size = 4096;
  default_cf.disable_auto_compactions = true;
  one.disable_auto_compactions = true;
  two.disable_auto_compactions = true;
  db_options_.max_total_wal_size = 210000;

  Reopen({default_cf, one, two});

  PutRandomData(2, 1, 10);  // 10 bytes
  for (int i = 0; i < 2; ++i) {
    PutRandomData(0, 100, 1000);  // flush
    WaitForFlush(0);

    AssertCountLiveFiles(i + 1);
  }
  // third flush. now, CF [two] should be detected as stale and flushed
  // column family 1 should not be flushed since it's empty
  PutRandomData(0, 100, 1000);  // flush
  WaitForFlush(0);
  WaitForFlush(2);
  // 3 files for default column families, 1 file for column family [two], zero
  // files for column family [one], because it's empty
  AssertCountLiveFiles(4);

  Flush(0);
  ASSERT_EQ(0, dbfull()->TEST_total_log_size());
  Close();
}
#endif  // !ROCKSDB_LITE

TEST_P(ColumnFamilyTest, CreateMissingColumnFamilies) {
  Status s = TryOpen({"one", "two"});
  ASSERT_TRUE(!s.ok());
  db_options_.create_missing_column_families = true;
  s = TryOpen({"default", "one", "two"});
  ASSERT_TRUE(s.ok());
  Close();
}

TEST_P(ColumnFamilyTest, SanitizeOptions) {
  DBOptions db_options;
  for (int s = kCompactionStyleLevel; s <= kCompactionStyleUniversal; ++s) {
    for (int l = 0; l <= 2; l++) {
      for (int i = 1; i <= 3; i++) {
        for (int j = 1; j <= 3; j++) {
          for (int k = 1; k <= 3; k++) {
            ColumnFamilyOptions original;
            original.compaction_style = static_cast<CompactionStyle>(s);
            original.num_levels = l;
            original.level0_stop_writes_trigger = i;
            original.level0_slowdown_writes_trigger = j;
            original.level0_file_num_compaction_trigger = k;
            original.write_buffer_size =
                l * 4 * 1024 * 1024 + i * 1024 * 1024 + j * 1024 + k;

            ColumnFamilyOptions result =
                SanitizeOptions(ImmutableDBOptions(db_options), original);
            ASSERT_TRUE(result.level0_stop_writes_trigger >=
                        result.level0_slowdown_writes_trigger);
            ASSERT_TRUE(result.level0_slowdown_writes_trigger >=
                        result.level0_file_num_compaction_trigger);
            ASSERT_TRUE(result.level0_file_num_compaction_trigger ==
                        original.level0_file_num_compaction_trigger);
            if (s == kCompactionStyleLevel) {
              ASSERT_GE(result.num_levels, 2);
            } else {
              ASSERT_GE(result.num_levels, 1);
              if (original.num_levels >= 1) {
                ASSERT_EQ(result.num_levels, original.num_levels);
              }
            }

            // Make sure Sanitize options sets arena_block_size to 1/8 of
            // the write_buffer_size, rounded up to a multiple of 4k.
            size_t expected_arena_block_size =
                l * 4 * 1024 * 1024 / 8 + i * 1024 * 1024 / 8;
            if (j + k != 0) {
              // not a multiple of 4k, round up 4k
              expected_arena_block_size += 4 * 1024;
            }
            ASSERT_EQ(expected_arena_block_size, result.arena_block_size);
          }
        }
      }
    }
  }
}

TEST_P(ColumnFamilyTest, ReadDroppedColumnFamily) {
  // iter 0 -- drop CF, don't reopen
  // iter 1 -- delete CF, reopen
  for (int iter = 0; iter < 2; ++iter) {
    db_options_.create_missing_column_families = true;
    db_options_.max_open_files = 20;
    // delete obsolete files always
    db_options_.delete_obsolete_files_period_micros = 0;
    Open({"default", "one", "two"});
    ColumnFamilyOptions options;
    options.level0_file_num_compaction_trigger = 100;
    options.level0_slowdown_writes_trigger = 200;
    options.level0_stop_writes_trigger = 200;
    options.write_buffer_size = 100000;  // small write buffer size
    Reopen({options, options, options});

    // 1MB should create ~10 files for each CF
    int kKeysNum = 10000;
    PutRandomData(0, kKeysNum, 100);
    PutRandomData(1, kKeysNum, 100);
    PutRandomData(2, kKeysNum, 100);

    {
      std::unique_ptr<Iterator> iterator(
          db_->NewIterator(ReadOptions(), handles_[2]));
      iterator->SeekToFirst();

      if (iter == 0) {
        // Drop CF two
        ASSERT_OK(db_->DropColumnFamily(handles_[2]));
      } else {
        // delete CF two
        db_->DestroyColumnFamilyHandle(handles_[2]);
        handles_[2] = nullptr;
      }
      // Make sure iterator created can still be used.
      int count = 0;
      for (; iterator->Valid(); iterator->Next()) {
        ASSERT_OK(iterator->status());
        ++count;
      }
      ASSERT_OK(iterator->status());
      ASSERT_EQ(count, kKeysNum);
    }

    // Add bunch more data to other CFs
    PutRandomData(0, kKeysNum, 100);
    PutRandomData(1, kKeysNum, 100);

    if (iter == 1) {
      Reopen();
    }

    // Since we didn't delete CF handle, RocksDB's contract guarantees that
    // we're still able to read dropped CF
    for (int i = 0; i < 3; ++i) {
      std::unique_ptr<Iterator> iterator(
          db_->NewIterator(ReadOptions(), handles_[i]));
      int count = 0;
      for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        ASSERT_OK(iterator->status());
        ++count;
      }
      ASSERT_OK(iterator->status());
      ASSERT_EQ(count, kKeysNum * ((i == 2) ? 1 : 2));
    }

    Close();
    Destroy();
  }
}

TEST_P(ColumnFamilyTest, FlushAndDropRaceCondition) {
  db_options_.create_missing_column_families = true;
  Open({"default", "one"});
  ColumnFamilyOptions options;
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 200;
  options.level0_stop_writes_trigger = 200;
  options.max_write_buffer_number = 20;
  options.write_buffer_size = 100000;  // small write buffer size
  Reopen({options, options});

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply::ColumnFamilyDrop:0",
        "FlushJob::WriteLevel0Table"},
       {"VersionSet::LogAndApply::ColumnFamilyDrop:1",
        "FlushJob::InstallResults"},
       {"FlushJob::InstallResults",
        "VersionSet::LogAndApply::ColumnFamilyDrop:2"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  test::SleepingBackgroundTask sleeping_task;

  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);

  // 1MB should create ~10 files for each CF
  int kKeysNum = 10000;
  PutRandomData(1, kKeysNum, 100);

  std::vector<port::Thread> threads;
  threads.emplace_back([&] { ASSERT_OK(db_->DropColumnFamily(handles_[1])); });

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
  sleeping_task.Reset();
  // now we sleep again. this is just so we're certain that flush job finished
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);
  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();

  {
    // Since we didn't delete CF handle, RocksDB's contract guarantees that
    // we're still able to read dropped CF
    std::unique_ptr<Iterator> iterator(
        db_->NewIterator(ReadOptions(), handles_[1]));
    int count = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      ASSERT_OK(iterator->status());
      ++count;
    }
    ASSERT_OK(iterator->status());
    ASSERT_EQ(count, kKeysNum);
  }
  for (auto& t : threads) {
    t.join();
  }

  Close();
  Destroy();
}

#ifndef ROCKSDB_LITE
// skipped as persisting options is not supported in ROCKSDB_LITE
namespace {
std::atomic<int> test_stage(0);
std::atomic<bool> ordered_by_writethread(false);
const int kMainThreadStartPersistingOptionsFile = 1;
const int kChildThreadFinishDroppingColumnFamily = 2;
void DropSingleColumnFamily(ColumnFamilyTest* cf_test, int cf_id,
                            std::vector<Comparator*>* comparators) {
  while (test_stage < kMainThreadStartPersistingOptionsFile &&
         !ordered_by_writethread) {
    Env::Default()->SleepForMicroseconds(100);
  }
  cf_test->DropColumnFamilies({cf_id});
  if ((*comparators)[cf_id]) {
    delete (*comparators)[cf_id];
    (*comparators)[cf_id] = nullptr;
  }
  test_stage = kChildThreadFinishDroppingColumnFamily;
}
}  // namespace

TEST_P(ColumnFamilyTest, CreateAndDropRace) {
  const int kCfCount = 5;
  std::vector<ColumnFamilyOptions> cf_opts;
  std::vector<Comparator*> comparators;
  for (int i = 0; i < kCfCount; ++i) {
    cf_opts.emplace_back();
    comparators.push_back(new test::SimpleSuffixReverseComparator());
    cf_opts.back().comparator = comparators.back();
  }
  db_options_.create_if_missing = true;
  db_options_.create_missing_column_families = true;

  auto main_thread_id = std::this_thread::get_id();

  rocksdb::SyncPoint::GetInstance()->SetCallBack("PersistRocksDBOptions:start",
                                                 [&](void* /*arg*/) {
    auto current_thread_id = std::this_thread::get_id();
    // If it's the main thread hitting this sync-point, then it
    // will be blocked until some other thread update the test_stage.
    if (main_thread_id == current_thread_id) {
      test_stage = kMainThreadStartPersistingOptionsFile;
      while (test_stage < kChildThreadFinishDroppingColumnFamily &&
             !ordered_by_writethread) {
        Env::Default()->SleepForMicroseconds(100);
      }
    }
  });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::EnterUnbatched:Wait", [&](void* /*arg*/) {
        // This means a thread doing DropColumnFamily() is waiting for
        // other thread to finish persisting options.
        // In such case, we update the test_stage to unblock the main thread.
        ordered_by_writethread = true;
      });

  // Create a database with four column families
  Open({"default", "one", "two", "three"},
       {cf_opts[0], cf_opts[1], cf_opts[2], cf_opts[3]});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Start a thread that will drop the first column family
  // and its comparator
  rocksdb::port::Thread drop_cf_thread(DropSingleColumnFamily, this, 1,
                                       &comparators);

  DropColumnFamilies({2});

  drop_cf_thread.join();
  Close();
  Destroy();
  for (auto* comparator : comparators) {
    if (comparator) {
      delete comparator;
    }
  }

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // !ROCKSDB_LITE

TEST_P(ColumnFamilyTest, WriteStallSingleColumnFamily) {
  const uint64_t kBaseRate = 800000u;
  db_options_.delayed_write_rate = kBaseRate;
  db_options_.max_background_compactions = 6;

  Open({"default"});
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();

  VersionStorageInfo* vstorage = cfd->current()->storage_info();

  MutableCFOptions mutable_cf_options(column_family_options_);

  mutable_cf_options.level0_slowdown_writes_trigger = 20;
  mutable_cf_options.level0_stop_writes_trigger = 10000;
  mutable_cf_options.soft_pending_compaction_bytes_limit = 200;
  mutable_cf_options.hard_pending_compaction_bytes_limit = 2000;
  mutable_cf_options.disable_auto_compactions = false;

  vstorage->TEST_set_estimated_compaction_needed_bytes(50);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->TEST_set_estimated_compaction_needed_bytes(201);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(400);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(500);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(450);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(205);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(202);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(201);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(198);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->TEST_set_estimated_compaction_needed_bytes(399);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(599);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(2001);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(3001);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->TEST_set_estimated_compaction_needed_bytes(390);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(100);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->set_l0_delay_trigger_count(100);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(101);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->set_l0_delay_trigger_count(0);
  vstorage->TEST_set_estimated_compaction_needed_bytes(300);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage->set_l0_delay_trigger_count(101);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(200);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage->set_l0_delay_trigger_count(0);
  vstorage->TEST_set_estimated_compaction_needed_bytes(0);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  mutable_cf_options.disable_auto_compactions = true;
  dbfull()->TEST_write_controler().set_delayed_write_rate(kBaseRate);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->set_l0_delay_trigger_count(50);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(0, GetDbDelayedWriteRate());
  ASSERT_EQ(kBaseRate, dbfull()->TEST_write_controler().delayed_write_rate());

  vstorage->set_l0_delay_trigger_count(60);
  vstorage->TEST_set_estimated_compaction_needed_bytes(300);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(0, GetDbDelayedWriteRate());
  ASSERT_EQ(kBaseRate, dbfull()->TEST_write_controler().delayed_write_rate());

  mutable_cf_options.disable_auto_compactions = false;
  vstorage->set_l0_delay_trigger_count(70);
  vstorage->TEST_set_estimated_compaction_needed_bytes(500);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->set_l0_delay_trigger_count(71);
  vstorage->TEST_set_estimated_compaction_needed_bytes(501);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());
}

TEST_P(ColumnFamilyTest, CompactionSpeedupSingleColumnFamily) {
  db_options_.max_background_compactions = 6;
  Open({"default"});
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();

  VersionStorageInfo* vstorage = cfd->current()->storage_info();

  MutableCFOptions mutable_cf_options(column_family_options_);

  // Speed up threshold = min(4 * 2, 4 + (36 - 4)/4) = 8
  mutable_cf_options.level0_file_num_compaction_trigger = 4;
  mutable_cf_options.level0_slowdown_writes_trigger = 36;
  mutable_cf_options.level0_stop_writes_trigger = 50;
  // Speedup threshold = 200 / 4 = 50
  mutable_cf_options.soft_pending_compaction_bytes_limit = 200;
  mutable_cf_options.hard_pending_compaction_bytes_limit = 2000;

  vstorage->TEST_set_estimated_compaction_needed_bytes(40);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(50);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(300);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(45);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(7);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(9);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(6);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  // Speed up threshold = min(4 * 2, 4 + (12 - 4)/4) = 6
  mutable_cf_options.level0_file_num_compaction_trigger = 4;
  mutable_cf_options.level0_slowdown_writes_trigger = 16;
  mutable_cf_options.level0_stop_writes_trigger = 30;

  vstorage->set_l0_delay_trigger_count(5);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(7);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(3);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
}

TEST_P(ColumnFamilyTest, WriteStallTwoColumnFamilies) {
  const uint64_t kBaseRate = 810000u;
  db_options_.delayed_write_rate = kBaseRate;
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  VersionStorageInfo* vstorage = cfd->current()->storage_info();

  ColumnFamilyData* cfd1 =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  VersionStorageInfo* vstorage1 = cfd1->current()->storage_info();

  MutableCFOptions mutable_cf_options(column_family_options_);
  mutable_cf_options.level0_slowdown_writes_trigger = 20;
  mutable_cf_options.level0_stop_writes_trigger = 10000;
  mutable_cf_options.soft_pending_compaction_bytes_limit = 200;
  mutable_cf_options.hard_pending_compaction_bytes_limit = 2000;

  MutableCFOptions mutable_cf_options1 = mutable_cf_options;
  mutable_cf_options1.soft_pending_compaction_bytes_limit = 500;

  vstorage->TEST_set_estimated_compaction_needed_bytes(50);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(201);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(600);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(70);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(800);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(300);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(700);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(500);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(600);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());
}

TEST_P(ColumnFamilyTest, CompactionSpeedupTwoColumnFamilies) {
  db_options_.max_background_compactions = 6;
  column_family_options_.soft_pending_compaction_bytes_limit = 200;
  column_family_options_.hard_pending_compaction_bytes_limit = 2000;
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  VersionStorageInfo* vstorage = cfd->current()->storage_info();

  ColumnFamilyData* cfd1 =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  VersionStorageInfo* vstorage1 = cfd1->current()->storage_info();

  MutableCFOptions mutable_cf_options(column_family_options_);
  // Speed up threshold = min(4 * 2, 4 + (36 - 4)/4) = 8
  mutable_cf_options.level0_file_num_compaction_trigger = 4;
  mutable_cf_options.level0_slowdown_writes_trigger = 36;
  mutable_cf_options.level0_stop_writes_trigger = 30;
  // Speedup threshold = 200 / 4 = 50
  mutable_cf_options.soft_pending_compaction_bytes_limit = 200;
  mutable_cf_options.hard_pending_compaction_bytes_limit = 2000;

  MutableCFOptions mutable_cf_options1 = mutable_cf_options;
  mutable_cf_options1.level0_slowdown_writes_trigger = 16;

  vstorage->TEST_set_estimated_compaction_needed_bytes(40);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(60);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(30);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(70);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(20);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(3);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(9);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage1->set_l0_delay_trigger_count(2);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(0);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
}

TEST_P(ColumnFamilyTest, CreateAndDestoryOptions) {
  std::unique_ptr<ColumnFamilyOptions> cfo(new ColumnFamilyOptions());
  ColumnFamilyHandle* cfh;
  Open();
  ASSERT_OK(db_->CreateColumnFamily(*(cfo.get()), "yoyo", &cfh));
  cfo.reset();
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->DropColumnFamily(cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
}

TEST_P(ColumnFamilyTest, CreateDropAndDestroy) {
  ColumnFamilyHandle* cfh;
  Open();
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "yoyo", &cfh));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->DropColumnFamily(cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
}

#ifndef ROCKSDB_LITE
TEST_P(ColumnFamilyTest, CreateDropAndDestroyWithoutFileDeletion) {
  ColumnFamilyHandle* cfh;
  Open();
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "yoyo", &cfh));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->DisableFileDeletions());
  ASSERT_OK(db_->DropColumnFamily(cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
}

TEST_P(ColumnFamilyTest, FlushCloseWALFiles) {
  SpecialEnv env(Env::Default());
  db_options_.env = &env;
  db_options_.max_background_flushes = 1;
  column_family_options_.memtable_factory.reset(new SpecialSkipListFactory(2));
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(0, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));

  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkFlush:done", "FlushCloseWALFiles:0"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  // Block flush jobs from running
  test::SleepingBackgroundTask sleeping_task;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);

  WriteOptions wo;
  wo.sync = true;
  ASSERT_OK(db_->Put(wo, handles_[1], "fodor", "mirko"));

  ASSERT_EQ(2, env.num_open_wal_file_.load());

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
  TEST_SYNC_POINT("FlushCloseWALFiles:0");
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(1, env.num_open_wal_file_.load());

  Reopen();
  ASSERT_EQ("mirko", Get(0, "fodor"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  db_options_.env = env_;
  Close();
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // WaitForFlush() is not supported
TEST_P(ColumnFamilyTest, IteratorCloseWALFile1) {
  SpecialEnv env(Env::Default());
  db_options_.env = &env;
  db_options_.max_background_flushes = 1;
  column_family_options_.memtable_factory.reset(new SpecialSkipListFactory(2));
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "fodor", "mirko"));
  // Create an iterator holding the current super version.
  Iterator* it = db_->NewIterator(ReadOptions(), handles_[1]);
  // A flush will make `it` hold the last reference of its super version.
  Flush(1);

  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(0, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));

  // Flush jobs will close previous WAL files after finishing. By
  // block flush jobs from running, we trigger a condition where
  // the iterator destructor should close the WAL files.
  test::SleepingBackgroundTask sleeping_task;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);

  WriteOptions wo;
  wo.sync = true;
  ASSERT_OK(db_->Put(wo, handles_[1], "fodor", "mirko"));

  ASSERT_EQ(2, env.num_open_wal_file_.load());
  // Deleting the iterator will clear its super version, triggering
  // closing all files
  delete it;
  ASSERT_EQ(1, env.num_open_wal_file_.load());

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
  WaitForFlush(1);

  Reopen();
  ASSERT_EQ("mirko", Get(0, "fodor"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  db_options_.env = env_;
  Close();
}

TEST_P(ColumnFamilyTest, IteratorCloseWALFile2) {
  SpecialEnv env(Env::Default());
  // Allow both of flush and purge job to schedule.
  env.SetBackgroundThreads(2, Env::HIGH);
  db_options_.env = &env;
  db_options_.max_background_flushes = 1;
  column_family_options_.memtable_factory.reset(new SpecialSkipListFactory(2));
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "fodor", "mirko"));
  // Create an iterator holding the current super version.
  ReadOptions ro;
  ro.background_purge_on_iterator_cleanup = true;
  Iterator* it = db_->NewIterator(ro, handles_[1]);
  // A flush will make `it` hold the last reference of its super version.
  Flush(1);

  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(0, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));

  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"ColumnFamilyTest::IteratorCloseWALFile2:0",
       "DBImpl::BGWorkPurge:start"},
      {"ColumnFamilyTest::IteratorCloseWALFile2:2",
       "DBImpl::BackgroundCallFlush:start"},
      {"DBImpl::BGWorkPurge:end", "ColumnFamilyTest::IteratorCloseWALFile2:1"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = true;
  ASSERT_OK(db_->Put(wo, handles_[1], "fodor", "mirko"));

  ASSERT_EQ(2, env.num_open_wal_file_.load());
  // Deleting the iterator will clear its super version, triggering
  // closing all files
  delete it;
  ASSERT_EQ(2, env.num_open_wal_file_.load());

  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:0");
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:1");
  ASSERT_EQ(1, env.num_open_wal_file_.load());
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:2");
  WaitForFlush(1);
  ASSERT_EQ(1, env.num_open_wal_file_.load());
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  Reopen();
  ASSERT_EQ("mirko", Get(0, "fodor"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  db_options_.env = env_;
  Close();
}
#endif  // !ROCKSDB_LITE

#ifndef ROCKSDB_LITE  // TEST functions are not supported in lite
TEST_P(ColumnFamilyTest, ForwardIteratorCloseWALFile) {
  SpecialEnv env(Env::Default());
  // Allow both of flush and purge job to schedule.
  env.SetBackgroundThreads(2, Env::HIGH);
  db_options_.env = &env;
  db_options_.max_background_flushes = 1;
  column_family_options_.memtable_factory.reset(new SpecialSkipListFactory(3));
  column_family_options_.level0_file_num_compaction_trigger = 2;
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodar2", "mirko"));
  Flush(1);

  // Create an iterator holding the current super version, as well as
  // the SST file just flushed.
  ReadOptions ro;
  ro.tailing = true;
  ro.background_purge_on_iterator_cleanup = true;
  Iterator* it = db_->NewIterator(ro, handles_[1]);
  // A flush will make `it` hold the last reference of its super version.

  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodar2", "mirko"));
  Flush(1);

  WaitForCompaction();

  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(0, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));

  rocksdb::SyncPoint::GetInstance()->LoadDependency({
      {"ColumnFamilyTest::IteratorCloseWALFile2:0",
       "DBImpl::BGWorkPurge:start"},
      {"ColumnFamilyTest::IteratorCloseWALFile2:2",
       "DBImpl::BackgroundCallFlush:start"},
      {"DBImpl::BGWorkPurge:end", "ColumnFamilyTest::IteratorCloseWALFile2:1"},
  });
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = true;
  ASSERT_OK(db_->Put(wo, handles_[1], "fodor", "mirko"));

  env.delete_count_.store(0);
  ASSERT_EQ(2, env.num_open_wal_file_.load());
  // Deleting the iterator will clear its super version, triggering
  // closing all files
  it->Seek("");
  ASSERT_EQ(2, env.num_open_wal_file_.load());
  ASSERT_EQ(0, env.delete_count_.load());

  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:0");
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:1");
  ASSERT_EQ(1, env.num_open_wal_file_.load());
  ASSERT_EQ(1, env.delete_count_.load());
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:2");
  WaitForFlush(1);
  ASSERT_EQ(1, env.num_open_wal_file_.load());
  ASSERT_EQ(1, env.delete_count_.load());

  delete it;
  rocksdb::SyncPoint::GetInstance()->DisableProcessing();

  Reopen();
  ASSERT_EQ("mirko", Get(0, "fodor"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  db_options_.env = env_;
  Close();
}
#endif  // !ROCKSDB_LITE

// Disable on windows because SyncWAL requires env->IsSyncThreadSafe()
// to return true which is not so in unbuffered mode.
#ifndef OS_WIN
TEST_P(ColumnFamilyTest, LogSyncConflictFlush) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});

  Put(0, "", "");
  Put(1, "foo", "bar");

  rocksdb::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::SyncWAL:BeforeMarkLogsSynced:1",
        "ColumnFamilyTest::LogSyncConflictFlush:1"},
       {"ColumnFamilyTest::LogSyncConflictFlush:2",
        "DBImpl::SyncWAL:BeforeMarkLogsSynced:2"}});

  rocksdb::SyncPoint::GetInstance()->EnableProcessing();

  rocksdb::port::Thread thread([&] { db_->SyncWAL(); });

  TEST_SYNC_POINT("ColumnFamilyTest::LogSyncConflictFlush:1");
  Flush(1);
  Put(1, "foo", "bar");
  Flush(1);

  TEST_SYNC_POINT("ColumnFamilyTest::LogSyncConflictFlush:2");

  thread.join();

  rocksdb::SyncPoint::GetInstance()->DisableProcessing();
  Close();
}
#endif

// this test is placed here, because the infrastructure for Column Family
// test is being used to ensure a roll of wal files.
// Basic idea is to test that WAL truncation is being detected and not
// ignored
TEST_P(ColumnFamilyTest, DISABLED_LogTruncationTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});

  Build(0, 100);

  // Flush the 0th column family to force a roll of the wal log
  Flush(0);

  // Add some more entries
  Build(100, 100);

  std::vector<std::string> filenames;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));

  // collect wal files
  std::vector<std::string> logfs;
  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t number;
    FileType type;
    if (!(ParseFileName(filenames[i], &number, &type))) continue;

    if (type != kLogFile) continue;

    logfs.push_back(filenames[i]);
  }

  std::sort(logfs.begin(), logfs.end());
  ASSERT_GE(logfs.size(), 2);

  // Take the last but one file, and truncate it
  std::string fpath = dbname_ + "/" + logfs[logfs.size() - 2];
  std::vector<std::string> names_save = names_;

  uint64_t fsize;
  ASSERT_OK(env_->GetFileSize(fpath, &fsize));
  ASSERT_GT(fsize, 0);

  Close();

  std::string backup_logs = dbname_ + "/backup_logs";
  std::string t_fpath = backup_logs + "/" + logfs[logfs.size() - 2];

  ASSERT_OK(env_->CreateDirIfMissing(backup_logs));
  // Not sure how easy it is to make this data driven.
  // need to read back the WAL file and truncate last 10
  // entries
  CopyFile(fpath, t_fpath, fsize - 9180);

  ASSERT_OK(env_->DeleteFile(fpath));
  ASSERT_OK(env_->RenameFile(t_fpath, fpath));

  db_options_.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  OpenReadOnly(names_save);

  CheckMissed();

  Close();

  Open(names_save);

  CheckMissed();

  Close();

  // cleanup
  env_->DeleteDir(backup_logs);
}

TEST_P(ColumnFamilyTest, DefaultCfPathsTest) {
  Open();
  // Leave cf_paths for one column families to be empty.
  // Files should be generated according to db_paths for that
  // column family.
  ColumnFamilyOptions cf_opt1, cf_opt2;
  cf_opt1.cf_paths.emplace_back(dbname_ + "_one_1",
                                std::numeric_limits<uint64_t>::max());
  CreateColumnFamilies({"one", "two"}, {cf_opt1, cf_opt2});
  Reopen({ColumnFamilyOptions(), cf_opt1, cf_opt2});

  // Fill Column family 1.
  PutRandomData(1, 100, 100);
  Flush(1);

  ASSERT_EQ(1, GetSstFileCount(cf_opt1.cf_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // Fill column family 2
  PutRandomData(2, 100, 100);
  Flush(2);

  // SST from Column family 2 should be generated in
  // db_paths which is dbname_ in this case.
  ASSERT_EQ(1, GetSstFileCount(dbname_));
}

TEST_P(ColumnFamilyTest, MultipleCFPathsTest) {
  Open();
  // Configure Column family specific paths.
  ColumnFamilyOptions cf_opt1, cf_opt2;
  cf_opt1.cf_paths.emplace_back(dbname_ + "_one_1",
                                std::numeric_limits<uint64_t>::max());
  cf_opt2.cf_paths.emplace_back(dbname_ + "_two_1",
                                std::numeric_limits<uint64_t>::max());
  CreateColumnFamilies({"one", "two"}, {cf_opt1, cf_opt2});
  Reopen({ColumnFamilyOptions(), cf_opt1, cf_opt2});

  PutRandomData(1, 100, 100, true /* save */);
  Flush(1);

  // Check that files are generated in appropriate paths.
  ASSERT_EQ(1, GetSstFileCount(cf_opt1.cf_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  PutRandomData(2, 100, 100, true /* save */);
  Flush(2);

  ASSERT_EQ(1, GetSstFileCount(cf_opt2.cf_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // Re-open and verify the keys.
  Reopen({ColumnFamilyOptions(), cf_opt1, cf_opt2});
  DBImpl* dbi = reinterpret_cast<DBImpl*>(db_);
  for (int cf = 1; cf != 3; ++cf) {
    ReadOptions read_options;
    read_options.readahead_size = 0;
    auto it = dbi->NewIterator(read_options, handles_[cf]);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      Slice key(it->key());
      ASSERT_NE(keys_[cf].end(), keys_[cf].find(key.ToString()));
    }
    delete it;

    for (const auto& key : keys_[cf]) {
      ASSERT_NE("NOT_FOUND", Get(cf, key));
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
