//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "port/port.h"
#include "rocksdb/utilities/debug.h"
#include "util/cast_util.h"
#include "util/fault_injection_test_env.h"
#include "util/file_util.h"
#include "util/random.h"
#include "util/sst_file_manager_impl.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/blob_db/blob_db_impl.h"
#include "utilities/blob_db/blob_index.h"

namespace rocksdb {
namespace blob_db {

class BlobDBTest : public testing::Test {
 public:
  const int kMaxBlobSize = 1 << 14;

  struct BlobRecord {
    std::string key;
    std::string value;
    uint64_t expiration = 0;
  };

  BlobDBTest()
      : dbname_(test::PerThreadDBPath("blob_db_test")),
        mock_env_(new MockTimeEnv(Env::Default())),
        fault_injection_env_(new FaultInjectionTestEnv(Env::Default())),
        blob_db_(nullptr) {
    Status s = DestroyBlobDB(dbname_, Options(), BlobDBOptions());
    assert(s.ok());
  }

  ~BlobDBTest() override {
    SyncPoint::GetInstance()->ClearAllCallBacks();
    Destroy();
  }

  Status TryOpen(BlobDBOptions bdb_options = BlobDBOptions(),
                 Options options = Options()) {
    options.create_if_missing = true;
    return BlobDB::Open(options, bdb_options, dbname_, &blob_db_);
  }

  void Open(BlobDBOptions bdb_options = BlobDBOptions(),
            Options options = Options()) {
    ASSERT_OK(TryOpen(bdb_options, options));
  }

  void Reopen(BlobDBOptions bdb_options = BlobDBOptions(),
              Options options = Options()) {
    assert(blob_db_ != nullptr);
    delete blob_db_;
    blob_db_ = nullptr;
    Open(bdb_options, options);
  }

  void Close() {
    assert(blob_db_ != nullptr);
    delete blob_db_;
    blob_db_ = nullptr;
  }

  void Destroy() {
    if (blob_db_) {
      Options options = blob_db_->GetOptions();
      BlobDBOptions bdb_options = blob_db_->GetBlobDBOptions();
      delete blob_db_;
      blob_db_ = nullptr;
      ASSERT_OK(DestroyBlobDB(dbname_, options, bdb_options));
    }
  }

  BlobDBImpl *blob_db_impl() {
    return reinterpret_cast<BlobDBImpl *>(blob_db_);
  }

  Status Put(const Slice &key, const Slice &value,
             std::map<std::string, std::string> *data = nullptr) {
    Status s = blob_db_->Put(WriteOptions(), key, value);
    if (data != nullptr) {
      (*data)[key.ToString()] = value.ToString();
    }
    return s;
  }

  void Delete(const std::string &key,
              std::map<std::string, std::string> *data = nullptr) {
    ASSERT_OK(blob_db_->Delete(WriteOptions(), key));
    if (data != nullptr) {
      data->erase(key);
    }
  }

  Status PutWithTTL(const Slice &key, const Slice &value, uint64_t ttl,
                    std::map<std::string, std::string> *data = nullptr) {
    Status s = blob_db_->PutWithTTL(WriteOptions(), key, value, ttl);
    if (data != nullptr) {
      (*data)[key.ToString()] = value.ToString();
    }
    return s;
  }

  Status PutUntil(const Slice &key, const Slice &value, uint64_t expiration) {
    return blob_db_->PutUntil(WriteOptions(), key, value, expiration);
  }

  void PutRandomWithTTL(const std::string &key, uint64_t ttl, Random *rnd,
                        std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(
        blob_db_->PutWithTTL(WriteOptions(), Slice(key), Slice(value), ttl));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandomUntil(const std::string &key, uint64_t expiration, Random *rnd,
                      std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(blob_db_->PutUntil(WriteOptions(), Slice(key), Slice(value),
                                 expiration));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandom(const std::string &key, Random *rnd,
                 std::map<std::string, std::string> *data = nullptr) {
    PutRandom(blob_db_, key, rnd, data);
  }

  void PutRandom(DB *db, const std::string &key, Random *rnd,
                 std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(db->Put(WriteOptions(), Slice(key), Slice(value)));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandomToWriteBatch(
      const std::string &key, Random *rnd, WriteBatch *batch,
      std::map<std::string, std::string> *data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = test::RandomHumanReadableString(rnd, len);
    ASSERT_OK(batch->Put(key, value));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  // Verify blob db contain expected data and nothing more.
  void VerifyDB(const std::map<std::string, std::string> &data) {
    VerifyDB(blob_db_, data);
  }

  void VerifyDB(DB *db, const std::map<std::string, std::string> &data) {
    // Verify normal Get
    auto* cfh = db->DefaultColumnFamily();
    for (auto &p : data) {
      PinnableSlice value_slice;
      ASSERT_OK(db->Get(ReadOptions(), cfh, p.first, &value_slice));
      ASSERT_EQ(p.second, value_slice.ToString());
      std::string value;
      ASSERT_OK(db->Get(ReadOptions(), cfh, p.first, &value));
      ASSERT_EQ(p.second, value);
    }

    // Verify iterators
    Iterator *iter = db->NewIterator(ReadOptions());
    iter->SeekToFirst();
    for (auto &p : data) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(p.first, iter->key().ToString());
      ASSERT_EQ(p.second, iter->value().ToString());
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }

  void VerifyBaseDB(
      const std::map<std::string, KeyVersion> &expected_versions) {
    auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
    DB *db = blob_db_->GetRootDB();
    const size_t kMaxKeys = 10000;
    std::vector<KeyVersion> versions;
    GetAllKeyVersions(db, "", "", kMaxKeys, &versions);
    ASSERT_EQ(expected_versions.size(), versions.size());
    size_t i = 0;
    for (auto &key_version : expected_versions) {
      const KeyVersion &expected_version = key_version.second;
      ASSERT_EQ(expected_version.user_key, versions[i].user_key);
      ASSERT_EQ(expected_version.sequence, versions[i].sequence);
      ASSERT_EQ(expected_version.type, versions[i].type);
      if (versions[i].type == kTypeValue) {
        ASSERT_EQ(expected_version.value, versions[i].value);
      } else {
        ASSERT_EQ(kTypeBlobIndex, versions[i].type);
        PinnableSlice value;
        ASSERT_OK(bdb_impl->TEST_GetBlobValue(versions[i].user_key,
                                              versions[i].value, &value));
        ASSERT_EQ(expected_version.value, value.ToString());
      }
      i++;
    }
  }

  void InsertBlobs() {
    WriteOptions wo;
    std::string value;

    Random rnd(301);
    for (size_t i = 0; i < 100000; i++) {
      uint64_t ttl = rnd.Next() % 86400;
      PutRandomWithTTL("key" + ToString(i % 500), ttl, &rnd, nullptr);
    }

    for (size_t i = 0; i < 10; i++) {
      Delete("key" + ToString(i % 500));
    }
  }

  const std::string dbname_;
  std::unique_ptr<MockTimeEnv> mock_env_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  BlobDB *blob_db_;
};  // class BlobDBTest

TEST_F(BlobDBTest, Put) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, PutWithTTL) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.min_blob_size = 0;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_env_->set_current_time(50);
  for (size_t i = 0; i < 100; i++) {
    uint64_t ttl = rnd.Next() % 100;
    PutRandomWithTTL("key" + ToString(i), ttl, &rnd,
                     (ttl <= 50 ? nullptr : &data));
  }
  mock_env_->set_current_time(100);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  GCStats gc_stats;
  ASSERT_OK(bdb_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(100 - data.size(), gc_stats.num_keys_expired);
  ASSERT_EQ(data.size(), gc_stats.num_keys_relocated);
  VerifyDB(data);
}

TEST_F(BlobDBTest, PutUntil) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.min_blob_size = 0;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_env_->set_current_time(50);
  for (size_t i = 0; i < 100; i++) {
    uint64_t expiration = rnd.Next() % 100 + 50;
    PutRandomUntil("key" + ToString(i), expiration, &rnd,
                   (expiration <= 100 ? nullptr : &data));
  }
  mock_env_->set_current_time(100);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  GCStats gc_stats;
  ASSERT_OK(bdb_impl->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(100 - data.size(), gc_stats.num_keys_expired);
  ASSERT_EQ(data.size(), gc_stats.num_keys_relocated);
  VerifyDB(data);
}

TEST_F(BlobDBTest, StackableDBGet) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i++) {
    StackableDB *db = blob_db_;
    ColumnFamilyHandle *column_family = db->DefaultColumnFamily();
    std::string key = "key" + ToString(i);
    PinnableSlice pinnable_value;
    ASSERT_OK(db->Get(ReadOptions(), column_family, key, &pinnable_value));
    std::string string_value;
    ASSERT_OK(db->Get(ReadOptions(), column_family, key, &string_value));
    ASSERT_EQ(string_value, pinnable_value.ToString());
    ASSERT_EQ(string_value, data[key]);
  }
}

TEST_F(BlobDBTest, GetExpiration) {
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  mock_env_->set_current_time(100);
  Open(bdb_options, options);
  Put("key1", "value1");
  PutWithTTL("key2", "value2", 200);
  PinnableSlice value;
  uint64_t expiration;
  ASSERT_OK(blob_db_->Get(ReadOptions(), "key1", &value, &expiration));
  ASSERT_EQ("value1", value.ToString());
  ASSERT_EQ(kNoExpiration, expiration);
  ASSERT_OK(blob_db_->Get(ReadOptions(), "key2", &value, &expiration));
  ASSERT_EQ("value2", value.ToString());
  ASSERT_EQ(300 /* = 100 + 200 */, expiration);
}

TEST_F(BlobDBTest, GetIOError) {
  Options options;
  options.env = fault_injection_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;  // Make sure value write to blob file
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  ColumnFamilyHandle *column_family = blob_db_->DefaultColumnFamily();
  PinnableSlice value;
  ASSERT_OK(Put("foo", "bar"));
  fault_injection_env_->SetFilesystemActive(false, Status::IOError());
  Status s = blob_db_->Get(ReadOptions(), column_family, "foo", &value);
  ASSERT_TRUE(s.IsIOError());
  // Reactivate file system to allow test to close DB.
  fault_injection_env_->SetFilesystemActive(true);
}

TEST_F(BlobDBTest, PutIOError) {
  Options options;
  options.env = fault_injection_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;  // Make sure value write to blob file
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  fault_injection_env_->SetFilesystemActive(false, Status::IOError());
  ASSERT_TRUE(Put("foo", "v1").IsIOError());
  fault_injection_env_->SetFilesystemActive(true, Status::IOError());
  ASSERT_OK(Put("bar", "v1"));
}

TEST_F(BlobDBTest, WriteBatch) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    WriteBatch batch;
    for (size_t j = 0; j < 10; j++) {
      PutRandomToWriteBatch("key" + ToString(j * 100 + i), &rnd, &batch, &data);
    }
    blob_db_->Write(WriteOptions(), &batch);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, Delete) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i += 5) {
    Delete("key" + ToString(i), &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, DeleteBatch) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd);
  }
  WriteBatch batch;
  for (size_t i = 0; i < 100; i++) {
    batch.Delete("key" + ToString(i));
  }
  ASSERT_OK(blob_db_->Write(WriteOptions(), &batch));
  // DB should be empty.
  VerifyDB({});
}

TEST_F(BlobDBTest, Override) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (int i = 0; i < 10000; i++) {
    PutRandom("key" + ToString(i), &rnd, nullptr);
  }
  // override all the keys
  for (int i = 0; i < 10000; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  VerifyDB(data);
}

#ifdef SNAPPY
TEST_F(BlobDBTest, Compression) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  bdb_options.compression = CompressionType::kSnappyCompression;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("put-key" + ToString(i), &rnd, &data);
  }
  for (int i = 0; i < 100; i++) {
    WriteBatch batch;
    for (size_t j = 0; j < 10; j++) {
      PutRandomToWriteBatch("write-batch-key" + ToString(j * 100 + i), &rnd,
                            &batch, &data);
    }
    blob_db_->Write(WriteOptions(), &batch);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, DecompressAfterReopen) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  bdb_options.compression = CompressionType::kSnappyCompression;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("put-key" + ToString(i), &rnd, &data);
  }
  VerifyDB(data);
  bdb_options.compression = CompressionType::kNoCompression;
  Reopen(bdb_options);
  VerifyDB(data);
}
#endif

TEST_F(BlobDBTest, MultipleWriters) {
  Open(BlobDBOptions());

  std::vector<port::Thread> workers;
  std::vector<std::map<std::string, std::string>> data_set(10);
  for (uint32_t i = 0; i < 10; i++)
    workers.push_back(port::Thread(
        [&](uint32_t id) {
          Random rnd(301 + id);
          for (int j = 0; j < 100; j++) {
            std::string key = "key" + ToString(id) + "_" + ToString(j);
            if (id < 5) {
              PutRandom(key, &rnd, &data_set[id]);
            } else {
              WriteBatch batch;
              PutRandomToWriteBatch(key, &rnd, &batch, &data_set[id]);
              blob_db_->Write(WriteOptions(), &batch);
            }
          }
        },
        i));
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 10; i++) {
    workers[i].join();
    data.insert(data_set[i].begin(), data_set[i].end());
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, GCAfterOverwriteKeys) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  DBImpl *db_impl = static_cast_with_check<DBImpl, DB>(blob_db_->GetBaseDB());
  std::map<std::string, std::string> data;
  for (int i = 0; i < 200; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[0]));
  // Test for data in SST
  size_t new_keys = 0;
  for (int i = 0; i < 100; i++) {
    if (rnd.Next() % 2 == 1) {
      new_keys++;
      PutRandom("key" + ToString(i), &rnd, &data);
    }
  }
  db_impl->TEST_FlushMemTable(true /*wait*/);
  // Test for data in memtable
  for (int i = 100; i < 200; i++) {
    if (rnd.Next() % 2 == 1) {
      new_keys++;
      PutRandom("key" + ToString(i), &rnd, &data);
    }
  }
  GCStats gc_stats;
  ASSERT_OK(blob_db_impl()->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(200, gc_stats.blob_count);
  ASSERT_EQ(0, gc_stats.num_keys_expired);
  ASSERT_EQ(200 - new_keys, gc_stats.num_keys_relocated);
  VerifyDB(data);
}

TEST_F(BlobDBTest, GCRelocateKeyWhileOverwriting) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  ASSERT_OK(blob_db_->Put(WriteOptions(), "foo", "v1"));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[0]));

  SyncPoint::GetInstance()->LoadDependency(
      {{"BlobDBImpl::GCFileAndUpdateLSM:AfterGetFromBaseDB",
        "BlobDBImpl::PutUntil:Start"},
       {"BlobDBImpl::PutUntil:Finish",
        "BlobDBImpl::GCFileAndUpdateLSM:BeforeRelocate"}});
  SyncPoint::GetInstance()->EnableProcessing();

  auto writer = port::Thread(
      [this]() { ASSERT_OK(blob_db_->Put(WriteOptions(), "foo", "v2")); });

  GCStats gc_stats;
  ASSERT_OK(blob_db_impl()->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(1, gc_stats.blob_count);
  ASSERT_EQ(0, gc_stats.num_keys_expired);
  ASSERT_EQ(1, gc_stats.num_keys_overwritten);
  ASSERT_EQ(0, gc_stats.num_keys_relocated);
  writer.join();
  VerifyDB({{"foo", "v2"}});
}

TEST_F(BlobDBTest, GCExpiredKeyWhileOverwriting) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  mock_env_->set_current_time(100);
  ASSERT_OK(blob_db_->PutUntil(WriteOptions(), "foo", "v1", 200));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[0]));
  mock_env_->set_current_time(300);

  SyncPoint::GetInstance()->LoadDependency(
      {{"BlobDBImpl::GCFileAndUpdateLSM:AfterGetFromBaseDB",
        "BlobDBImpl::PutUntil:Start"},
       {"BlobDBImpl::PutUntil:Finish",
        "BlobDBImpl::GCFileAndUpdateLSM:BeforeDelete"}});
  SyncPoint::GetInstance()->EnableProcessing();

  auto writer = port::Thread([this]() {
    ASSERT_OK(blob_db_->PutUntil(WriteOptions(), "foo", "v2", 400));
  });

  GCStats gc_stats;
  ASSERT_OK(blob_db_impl()->TEST_GCFileAndUpdateLSM(blob_files[0], &gc_stats));
  ASSERT_EQ(1, gc_stats.blob_count);
  ASSERT_EQ(1, gc_stats.num_keys_expired);
  ASSERT_EQ(0, gc_stats.num_keys_relocated);
  writer.join();
  VerifyDB({{"foo", "v2"}});
}

TEST_F(BlobDBTest, NewFileGeneratedFromGCShouldMarkAsImmutable) {
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  ASSERT_OK(Put("foo", "bar"));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  auto blob_file1 = blob_files[0];
  ASSERT_EQ(1, blob_files.size());
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_file1));
  GCStats gc_stats;
  ASSERT_OK(blob_db_impl()->TEST_GCFileAndUpdateLSM(blob_file1, &gc_stats));
  ASSERT_EQ(1, gc_stats.blob_count);
  ASSERT_EQ(1, gc_stats.num_keys_relocated);
  blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(2, blob_files.size());
  ASSERT_EQ(blob_file1, blob_files[0]);
  ASSERT_TRUE(blob_files[1]->Immutable());
}

// This test is no longer valid since we now return an error when we go
// over the configured max_db_size.
// The test needs to be re-written later in such a way that writes continue
// after a GC happens.
TEST_F(BlobDBTest, DISABLED_GCOldestSimpleBlobFileWhenOutOfSpace) {
  // Use mock env to stop wall clock.
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.max_db_size = 100;
  bdb_options.blob_file_size = 100;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::string value(100, 'v');
  ASSERT_OK(blob_db_->PutWithTTL(WriteOptions(), "key_with_ttl", value, 60));
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(blob_db_->Put(WriteOptions(), "key" + ToString(i), value));
  }
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(11, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_TRUE(blob_files[0]->Immutable());
  for (int i = 1; i <= 10; i++) {
    ASSERT_FALSE(blob_files[i]->HasTTL());
    if (i < 10) {
      ASSERT_TRUE(blob_files[i]->Immutable());
    }
  }
  blob_db_impl()->TEST_RunGC();
  // The oldest simple blob file (i.e. blob_files[1]) has been selected for GC.
  auto obsolete_files = blob_db_impl()->TEST_GetObsoleteFiles();
  ASSERT_EQ(1, obsolete_files.size());
  ASSERT_EQ(blob_files[1]->BlobFileNumber(),
            obsolete_files[0]->BlobFileNumber());
}

TEST_F(BlobDBTest, ReadWhileGC) {
  // run the same test for Get(), MultiGet() and Iterator each.
  for (int i = 0; i < 2; i++) {
    BlobDBOptions bdb_options;
    bdb_options.min_blob_size = 0;
    bdb_options.disable_background_tasks = true;
    Open(bdb_options);
    blob_db_->Put(WriteOptions(), "foo", "bar");
    auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(1, blob_files.size());
    std::shared_ptr<BlobFile> bfile = blob_files[0];
    uint64_t bfile_number = bfile->BlobFileNumber();
    ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(bfile));

    switch (i) {
      case 0:
        SyncPoint::GetInstance()->LoadDependency(
            {{"BlobDBImpl::Get:AfterIndexEntryGet:1",
              "BlobDBTest::ReadWhileGC:1"},
             {"BlobDBTest::ReadWhileGC:2",
              "BlobDBImpl::Get:AfterIndexEntryGet:2"}});
        break;
      case 1:
        SyncPoint::GetInstance()->LoadDependency(
            {{"BlobDBIterator::UpdateBlobValue:Start:1",
              "BlobDBTest::ReadWhileGC:1"},
             {"BlobDBTest::ReadWhileGC:2",
              "BlobDBIterator::UpdateBlobValue:Start:2"}});
        break;
    }
    SyncPoint::GetInstance()->EnableProcessing();

    auto reader = port::Thread([this, i]() {
      std::string value;
      std::vector<std::string> values;
      std::vector<Status> statuses;
      switch (i) {
        case 0:
          ASSERT_OK(blob_db_->Get(ReadOptions(), "foo", &value));
          ASSERT_EQ("bar", value);
          break;
        case 1:
          // VerifyDB use iterator to scan the DB.
          VerifyDB({{"foo", "bar"}});
          break;
      }
    });

    TEST_SYNC_POINT("BlobDBTest::ReadWhileGC:1");
    GCStats gc_stats;
    ASSERT_OK(blob_db_impl()->TEST_GCFileAndUpdateLSM(bfile, &gc_stats));
    ASSERT_EQ(1, gc_stats.blob_count);
    ASSERT_EQ(1, gc_stats.num_keys_relocated);
    blob_db_impl()->TEST_DeleteObsoleteFiles();
    // The file shouln't be deleted
    blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(2, blob_files.size());
    ASSERT_EQ(bfile_number, blob_files[0]->BlobFileNumber());
    auto obsolete_files = blob_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(1, obsolete_files.size());
    ASSERT_EQ(bfile_number, obsolete_files[0]->BlobFileNumber());
    TEST_SYNC_POINT("BlobDBTest::ReadWhileGC:2");
    reader.join();
    SyncPoint::GetInstance()->DisableProcessing();

    // The file is deleted this time
    blob_db_impl()->TEST_DeleteObsoleteFiles();
    blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(1, blob_files.size());
    ASSERT_NE(bfile_number, blob_files[0]->BlobFileNumber());
    ASSERT_EQ(0, blob_db_impl()->TEST_GetObsoleteFiles().size());
    VerifyDB({{"foo", "bar"}});
    Destroy();
  }
}

TEST_F(BlobDBTest, SstFileManager) {
  // run the same test for Get(), MultiGet() and Iterator each.
  std::shared_ptr<SstFileManager> sst_file_manager(
      NewSstFileManager(mock_env_.get()));
  sst_file_manager->SetDeleteRateBytesPerSecond(1);
  SstFileManagerImpl *sfm =
      static_cast<SstFileManagerImpl *>(sst_file_manager.get());

  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  Options db_options;

  int files_deleted_directly = 0;
  int files_scheduled_to_delete = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion",
      [&](void * /*arg*/) { files_scheduled_to_delete++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile",
      [&](void * /*arg*/) { files_deleted_directly++; });
  SyncPoint::GetInstance()->EnableProcessing();
  db_options.sst_file_manager = sst_file_manager;

  Open(bdb_options, db_options);

  // Create one obselete file and clean it.
  blob_db_->Put(WriteOptions(), "foo", "bar");
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  std::shared_ptr<BlobFile> bfile = blob_files[0];
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(bfile));
  GCStats gc_stats;
  ASSERT_OK(blob_db_impl()->TEST_GCFileAndUpdateLSM(bfile, &gc_stats));
  blob_db_impl()->TEST_DeleteObsoleteFiles();

  // Even if SSTFileManager is not set, DB is creating a dummy one.
  ASSERT_EQ(1, files_scheduled_to_delete);
  ASSERT_EQ(0, files_deleted_directly);
  Destroy();
  // Make sure that DestroyBlobDB() also goes through delete scheduler.
  ASSERT_GE(files_scheduled_to_delete, 2);
  ASSERT_EQ(0, files_deleted_directly);
  SyncPoint::GetInstance()->DisableProcessing();
  sfm->WaitForEmptyTrash();
}

TEST_F(BlobDBTest, SstFileManagerRestart) {
  int files_deleted_directly = 0;
  int files_scheduled_to_delete = 0;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion",
      [&](void * /*arg*/) { files_scheduled_to_delete++; });
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile",
      [&](void * /*arg*/) { files_deleted_directly++; });

  // run the same test for Get(), MultiGet() and Iterator each.
  std::shared_ptr<SstFileManager> sst_file_manager(
      NewSstFileManager(mock_env_.get()));
  sst_file_manager->SetDeleteRateBytesPerSecond(1);
  SstFileManagerImpl *sfm =
      static_cast<SstFileManagerImpl *>(sst_file_manager.get());

  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  Options db_options;

  SyncPoint::GetInstance()->EnableProcessing();
  db_options.sst_file_manager = sst_file_manager;

  Open(bdb_options, db_options);
  std::string blob_dir = blob_db_impl()->TEST_blob_dir();
  blob_db_->Put(WriteOptions(), "foo", "bar");
  Close();

  // Create 3 dummy trash files under the blob_dir
  CreateFile(db_options.env, blob_dir + "/000666.blob.trash", "", false);
  CreateFile(db_options.env, blob_dir + "/000888.blob.trash", "", true);
  CreateFile(db_options.env, blob_dir + "/something_not_match.trash", "",
             false);

  // Make sure that reopening the DB rescan the existing trash files
  Open(bdb_options, db_options);
  ASSERT_GE(files_scheduled_to_delete, 3);
  ASSERT_EQ(0, files_deleted_directly);

  sfm->WaitForEmptyTrash();

  // There should be exact one file under the blob dir now.
  std::vector<std::string> all_files;
  ASSERT_OK(db_options.env->GetChildren(blob_dir, &all_files));
  int nfiles = 0;
  for (const auto &f : all_files) {
    assert(!f.empty());
    if (f[0] == '.') {
      continue;
    }
    nfiles++;
  }
  ASSERT_EQ(nfiles, 1);

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(BlobDBTest, SnapshotAndGarbageCollection) {
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  // i = when to take snapshot
  for (int i = 0; i < 4; i++) {
    for (bool delete_key : {true, false}) {
      const Snapshot *snapshot = nullptr;
      Destroy();
      Open(bdb_options);
      // First file
      ASSERT_OK(Put("key1", "value"));
      if (i == 0) {
        snapshot = blob_db_->GetSnapshot();
      }
      auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
      ASSERT_EQ(1, blob_files.size());
      ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[0]));
      // Second file
      ASSERT_OK(Put("key2", "value"));
      if (i == 1) {
        snapshot = blob_db_->GetSnapshot();
      }
      blob_files = blob_db_impl()->TEST_GetBlobFiles();
      ASSERT_EQ(2, blob_files.size());
      auto bfile = blob_files[1];
      ASSERT_FALSE(bfile->Immutable());
      ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(bfile));
      // Third file
      ASSERT_OK(Put("key3", "value"));
      if (i == 2) {
        snapshot = blob_db_->GetSnapshot();
      }
      if (delete_key) {
        Delete("key2");
      }
      GCStats gc_stats;
      ASSERT_OK(blob_db_impl()->TEST_GCFileAndUpdateLSM(bfile, &gc_stats));
      ASSERT_TRUE(bfile->Obsolete());
      ASSERT_EQ(1, gc_stats.blob_count);
      if (delete_key) {
        ASSERT_EQ(0, gc_stats.num_keys_relocated);
      } else {
        ASSERT_EQ(1, gc_stats.num_keys_relocated);
      }
      ASSERT_EQ(blob_db_->GetLatestSequenceNumber(),
                bfile->GetObsoleteSequence());
      if (i == 3) {
        snapshot = blob_db_->GetSnapshot();
      }
      size_t num_files = delete_key ? 3 : 4;
      ASSERT_EQ(num_files, blob_db_impl()->TEST_GetBlobFiles().size());
      blob_db_impl()->TEST_DeleteObsoleteFiles();
      if (i == 3) {
        // The snapshot shouldn't see data in bfile
        ASSERT_EQ(num_files - 1, blob_db_impl()->TEST_GetBlobFiles().size());
        blob_db_->ReleaseSnapshot(snapshot);
      } else {
        // The snapshot will see data in bfile, so the file shouldn't be deleted
        ASSERT_EQ(num_files, blob_db_impl()->TEST_GetBlobFiles().size());
        blob_db_->ReleaseSnapshot(snapshot);
        blob_db_impl()->TEST_DeleteObsoleteFiles();
        ASSERT_EQ(num_files - 1, blob_db_impl()->TEST_GetBlobFiles().size());
      }
    }
  }
}

TEST_F(BlobDBTest, ColumnFamilyNotSupported) {
  Options options;
  options.env = mock_env_.get();
  mock_env_->set_current_time(0);
  Open(BlobDBOptions(), options);
  ColumnFamilyHandle *default_handle = blob_db_->DefaultColumnFamily();
  ColumnFamilyHandle *handle = nullptr;
  std::string value;
  std::vector<std::string> values;
  // The call simply pass through to base db. It should succeed.
  ASSERT_OK(
      blob_db_->CreateColumnFamily(ColumnFamilyOptions(), "foo", &handle));
  ASSERT_TRUE(blob_db_->Put(WriteOptions(), handle, "k", "v").IsNotSupported());
  ASSERT_TRUE(blob_db_->PutWithTTL(WriteOptions(), handle, "k", "v", 60)
                  .IsNotSupported());
  ASSERT_TRUE(blob_db_->PutUntil(WriteOptions(), handle, "k", "v", 100)
                  .IsNotSupported());
  WriteBatch batch;
  batch.Put("k1", "v1");
  batch.Put(handle, "k2", "v2");
  ASSERT_TRUE(blob_db_->Write(WriteOptions(), &batch).IsNotSupported());
  ASSERT_TRUE(blob_db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_TRUE(
      blob_db_->Get(ReadOptions(), handle, "k", &value).IsNotSupported());
  auto statuses = blob_db_->MultiGet(ReadOptions(), {default_handle, handle},
                                     {"k1", "k2"}, &values);
  ASSERT_EQ(2, statuses.size());
  ASSERT_TRUE(statuses[0].IsNotSupported());
  ASSERT_TRUE(statuses[1].IsNotSupported());
  ASSERT_EQ(nullptr, blob_db_->NewIterator(ReadOptions(), handle));
  delete handle;
}

TEST_F(BlobDBTest, GetLiveFilesMetaData) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.blob_dir = "blob_dir";
  bdb_options.path_relative = true;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + ToString(i), &rnd, &data);
  }
  std::vector<LiveFileMetaData> metadata;
  blob_db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(1U, metadata.size());
  // Path should be relative to db_name, but begin with slash.
  std::string filename = "/blob_dir/000001.blob";
  ASSERT_EQ(filename, metadata[0].name);
  ASSERT_EQ("default", metadata[0].column_family_name);
  std::vector<std::string> livefile;
  uint64_t mfs;
  ASSERT_OK(blob_db_->GetLiveFiles(livefile, &mfs, false));
  ASSERT_EQ(4U, livefile.size());
  ASSERT_EQ(filename, livefile[3]);
  VerifyDB(data);
}

TEST_F(BlobDBTest, MigrateFromPlainRocksDB) {
  constexpr size_t kNumKey = 20;
  constexpr size_t kNumIteration = 10;
  Random rnd(301);
  std::map<std::string, std::string> data;
  std::vector<bool> is_blob(kNumKey, false);

  // Write to plain rocksdb.
  Options options;
  options.create_if_missing = true;
  DB *db = nullptr;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  for (size_t i = 0; i < kNumIteration; i++) {
    auto key_index = rnd.Next() % kNumKey;
    std::string key = "key" + ToString(key_index);
    PutRandom(db, key, &rnd, &data);
  }
  VerifyDB(db, data);
  delete db;
  db = nullptr;

  // Open as blob db. Verify it can read existing data.
  Open();
  VerifyDB(blob_db_, data);
  for (size_t i = 0; i < kNumIteration; i++) {
    auto key_index = rnd.Next() % kNumKey;
    std::string key = "key" + ToString(key_index);
    is_blob[key_index] = true;
    PutRandom(blob_db_, key, &rnd, &data);
  }
  VerifyDB(blob_db_, data);
  delete blob_db_;
  blob_db_ = nullptr;

  // Verify plain db return error for keys written by blob db.
  ASSERT_OK(DB::Open(options, dbname_, &db));
  std::string value;
  for (size_t i = 0; i < kNumKey; i++) {
    std::string key = "key" + ToString(i);
    Status s = db->Get(ReadOptions(), key, &value);
    if (data.count(key) == 0) {
      ASSERT_TRUE(s.IsNotFound());
    } else if (is_blob[i]) {
      ASSERT_TRUE(s.IsNotSupported());
    } else {
      ASSERT_OK(s);
      ASSERT_EQ(data[key], value);
    }
  }
  delete db;
}

// Test to verify that a NoSpace IOError Status is returned on reaching
// max_db_size limit.
TEST_F(BlobDBTest, OutOfSpace) {
  // Use mock env to stop wall clock.
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.max_db_size = 200;
  bdb_options.is_fifo = false;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  // Each stored blob has an overhead of about 42 bytes currently.
  // So a small key + a 100 byte blob should take up ~150 bytes in the db.
  std::string value(100, 'v');
  ASSERT_OK(blob_db_->PutWithTTL(WriteOptions(), "key1", value, 60));

  // Putting another blob should fail as ading it would exceed the max_db_size
  // limit.
  Status s = blob_db_->PutWithTTL(WriteOptions(), "key2", value, 60);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_TRUE(s.IsNoSpace());
}

TEST_F(BlobDBTest, FIFOEviction) {
  BlobDBOptions bdb_options;
  bdb_options.max_db_size = 200;
  bdb_options.blob_file_size = 100;
  bdb_options.is_fifo = true;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  std::atomic<int> evict_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobDBImpl::EvictOldestBlobFile:Evicted",
      [&](void *) { evict_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Each stored blob has an overhead of 32 bytes currently.
  // So a 100 byte blob should take up 132 bytes.
  std::string value(100, 'v');
  ASSERT_OK(blob_db_->PutWithTTL(WriteOptions(), "key1", value, 10));
  VerifyDB({{"key1", value}});

  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());

  // Adding another 100 bytes blob would take the total size to 264 bytes
  // (2*132). max_db_size will be exceeded
  // than max_db_size and trigger FIFO eviction.
  ASSERT_OK(blob_db_->PutWithTTL(WriteOptions(), "key2", value, 60));
  ASSERT_EQ(1, evict_count);
  // key1 will exist until corresponding file be deleted.
  VerifyDB({{"key1", value}, {"key2", value}});

  // Adding another 100 bytes blob without TTL.
  ASSERT_OK(blob_db_->Put(WriteOptions(), "key3", value));
  ASSERT_EQ(2, evict_count);
  // key1 and key2 will exist until corresponding file be deleted.
  VerifyDB({{"key1", value}, {"key2", value}, {"key3", value}});

  // The fourth blob file, without TTL.
  ASSERT_OK(blob_db_->Put(WriteOptions(), "key4", value));
  ASSERT_EQ(3, evict_count);
  VerifyDB(
      {{"key1", value}, {"key2", value}, {"key3", value}, {"key4", value}});

  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(4, blob_files.size());
  ASSERT_TRUE(blob_files[0]->Obsolete());
  ASSERT_TRUE(blob_files[1]->Obsolete());
  ASSERT_TRUE(blob_files[2]->Obsolete());
  ASSERT_FALSE(blob_files[3]->Obsolete());
  auto obsolete_files = blob_db_impl()->TEST_GetObsoleteFiles();
  ASSERT_EQ(3, obsolete_files.size());
  ASSERT_EQ(blob_files[0], obsolete_files[0]);
  ASSERT_EQ(blob_files[1], obsolete_files[1]);
  ASSERT_EQ(blob_files[2], obsolete_files[2]);

  blob_db_impl()->TEST_DeleteObsoleteFiles();
  obsolete_files = blob_db_impl()->TEST_GetObsoleteFiles();
  ASSERT_TRUE(obsolete_files.empty());
  VerifyDB({{"key4", value}});
}

TEST_F(BlobDBTest, FIFOEviction_NoOldestFileToEvict) {
  Options options;
  BlobDBOptions bdb_options;
  bdb_options.max_db_size = 1000;
  bdb_options.blob_file_size = 5000;
  bdb_options.is_fifo = true;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  std::atomic<int> evict_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobDBImpl::EvictOldestBlobFile:Evicted",
      [&](void *) { evict_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string value(2000, 'v');
  ASSERT_TRUE(Put("foo", std::string(2000, 'v')).IsNoSpace());
  ASSERT_EQ(0, evict_count);
}

TEST_F(BlobDBTest, FIFOEviction_NoEnoughBlobFilesToEvict) {
  BlobDBOptions bdb_options;
  bdb_options.is_fifo = true;
  bdb_options.min_blob_size = 100;
  bdb_options.disable_background_tasks = true;
  Options options;
  // Use mock env to stop wall clock.
  options.env = mock_env_.get();
  options.disable_auto_compactions = true;
  auto statistics = CreateDBStatistics();
  options.statistics = statistics;
  Open(bdb_options, options);

  ASSERT_EQ(0, blob_db_impl()->TEST_live_sst_size());
  std::string small_value(50, 'v');
  std::map<std::string, std::string> data;
  // Insert some data into LSM tree to make sure FIFO eviction take SST
  // file size into account.
  for (int i = 0; i < 1000; i++) {
    ASSERT_OK(Put("key" + ToString(i), small_value, &data));
  }
  ASSERT_OK(blob_db_->Flush(FlushOptions()));
  uint64_t live_sst_size = 0;
  ASSERT_TRUE(blob_db_->GetIntProperty(DB::Properties::kTotalSstFilesSize,
                                       &live_sst_size));
  ASSERT_TRUE(live_sst_size > 0);
  ASSERT_EQ(live_sst_size, blob_db_impl()->TEST_live_sst_size());

  bdb_options.max_db_size = live_sst_size + 2000;
  Reopen(bdb_options, options);
  ASSERT_EQ(live_sst_size, blob_db_impl()->TEST_live_sst_size());

  std::string value_1k(1000, 'v');
  ASSERT_OK(PutWithTTL("large_key1", value_1k, 60, &data));
  ASSERT_EQ(0, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));
  VerifyDB(data);
  // large_key2 evicts large_key1
  ASSERT_OK(PutWithTTL("large_key2", value_1k, 60, &data));
  ASSERT_EQ(1, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  data.erase("large_key1");
  VerifyDB(data);
  // large_key3 get no enough space even after evicting large_key2, so it
  // instead return no space error.
  std::string value_2k(2000, 'v');
  ASSERT_TRUE(PutWithTTL("large_key3", value_2k, 60).IsNoSpace());
  ASSERT_EQ(1, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));
  // Verify large_key2 still exists.
  VerifyDB(data);
}

// Test flush or compaction will trigger FIFO eviction since they update
// total SST file size.
TEST_F(BlobDBTest, FIFOEviction_TriggerOnSSTSizeChange) {
  BlobDBOptions bdb_options;
  bdb_options.max_db_size = 1000;
  bdb_options.is_fifo = true;
  bdb_options.min_blob_size = 100;
  bdb_options.disable_background_tasks = true;
  Options options;
  // Use mock env to stop wall clock.
  options.env = mock_env_.get();
  auto statistics = CreateDBStatistics();
  options.statistics = statistics;
  options.compression = kNoCompression;
  Open(bdb_options, options);

  std::string value(800, 'v');
  ASSERT_OK(PutWithTTL("large_key", value, 60));
  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(0, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));
  VerifyDB({{"large_key", value}});

  // Insert some small keys and flush to bring DB out of space.
  std::map<std::string, std::string> data;
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("key" + ToString(i), "v", &data));
  }
  ASSERT_OK(blob_db_->Flush(FlushOptions()));

  // Verify large_key is deleted by FIFO eviction.
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(0, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(1, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));
  VerifyDB(data);
}

TEST_F(BlobDBTest, InlineSmallValues) {
  constexpr uint64_t kMaxExpiration = 1000;
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = kMaxExpiration;
  bdb_options.min_blob_size = 100;
  bdb_options.blob_file_size = 256 * 1000 * 1000;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.env = mock_env_.get();
  mock_env_->set_current_time(0);
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  std::map<std::string, KeyVersion> versions;
  for (size_t i = 0; i < 1000; i++) {
    bool is_small_value = rnd.Next() % 2;
    bool has_ttl = rnd.Next() % 2;
    uint64_t expiration = rnd.Next() % kMaxExpiration;
    int len = is_small_value ? 50 : 200;
    std::string key = "key" + ToString(i);
    std::string value = test::RandomHumanReadableString(&rnd, len);
    std::string blob_index;
    data[key] = value;
    SequenceNumber sequence = blob_db_->GetLatestSequenceNumber() + 1;
    if (!has_ttl) {
      ASSERT_OK(blob_db_->Put(WriteOptions(), key, value));
    } else {
      ASSERT_OK(blob_db_->PutUntil(WriteOptions(), key, value, expiration));
    }
    ASSERT_EQ(blob_db_->GetLatestSequenceNumber(), sequence);
    versions[key] =
        KeyVersion(key, value, sequence,
                   (is_small_value && !has_ttl) ? kTypeValue : kTypeBlobIndex);
  }
  VerifyDB(data);
  VerifyBaseDB(versions);
  auto *bdb_impl = static_cast<BlobDBImpl *>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(2, blob_files.size());
  std::shared_ptr<BlobFile> non_ttl_file;
  std::shared_ptr<BlobFile> ttl_file;
  if (blob_files[0]->HasTTL()) {
    ttl_file = blob_files[0];
    non_ttl_file = blob_files[1];
  } else {
    non_ttl_file = blob_files[0];
    ttl_file = blob_files[1];
  }
  ASSERT_FALSE(non_ttl_file->HasTTL());
  ASSERT_TRUE(ttl_file->HasTTL());
}

TEST_F(BlobDBTest, CompactionFilterNotSupported) {
  class TestCompactionFilter : public CompactionFilter {
    const char *Name() const override { return "TestCompactionFilter"; }
  };
  class TestCompactionFilterFactory : public CompactionFilterFactory {
    const char *Name() const override { return "TestCompactionFilterFactory"; }
    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context & /*context*/) override {
      return std::unique_ptr<CompactionFilter>(new TestCompactionFilter());
    }
  };
  for (int i = 0; i < 2; i++) {
    Options options;
    if (i == 0) {
      options.compaction_filter = new TestCompactionFilter();
    } else {
      options.compaction_filter_factory.reset(
          new TestCompactionFilterFactory());
    }
    ASSERT_TRUE(TryOpen(BlobDBOptions(), options).IsNotSupported());
    delete options.compaction_filter;
  }
}

// Test comapction filter should remove any expired blob index.
TEST_F(BlobDBTest, FilterExpiredBlobIndex) {
  constexpr size_t kNumKeys = 100;
  constexpr size_t kNumPuts = 1000;
  constexpr uint64_t kMaxExpiration = 1000;
  constexpr uint64_t kCompactTime = 500;
  constexpr uint64_t kMinBlobSize = 100;
  Random rnd(301);
  mock_env_->set_current_time(0);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = kMinBlobSize;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.env = mock_env_.get();
  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  std::map<std::string, std::string> data_after_compact;
  for (size_t i = 0; i < kNumPuts; i++) {
    bool is_small_value = rnd.Next() % 2;
    bool has_ttl = rnd.Next() % 2;
    uint64_t expiration = rnd.Next() % kMaxExpiration;
    int len = is_small_value ? 10 : 200;
    std::string key = "key" + ToString(rnd.Next() % kNumKeys);
    std::string value = test::RandomHumanReadableString(&rnd, len);
    if (!has_ttl) {
      if (is_small_value) {
        std::string blob_entry;
        BlobIndex::EncodeInlinedTTL(&blob_entry, expiration, value);
        // Fake blob index with TTL. See what it will do.
        ASSERT_GT(kMinBlobSize, blob_entry.size());
        value = blob_entry;
      }
      ASSERT_OK(Put(key, value));
      data_after_compact[key] = value;
    } else {
      ASSERT_OK(PutUntil(key, value, expiration));
      if (expiration <= kCompactTime) {
        data_after_compact.erase(key);
      } else {
        data_after_compact[key] = value;
      }
    }
    data[key] = value;
  }
  VerifyDB(data);

  mock_env_->set_current_time(kCompactTime);
  // Take a snapshot before compaction. Make sure expired blob indexes is
  // filtered regardless of snapshot.
  const Snapshot *snapshot = blob_db_->GetSnapshot();
  // Issue manual compaction to trigger compaction filter.
  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(),
                                   blob_db_->DefaultColumnFamily(), nullptr,
                                   nullptr));
  blob_db_->ReleaseSnapshot(snapshot);
  // Verify expired blob index are filtered.
  std::vector<KeyVersion> versions;
  const size_t kMaxKeys = 10000;
  GetAllKeyVersions(blob_db_, "", "", kMaxKeys, &versions);
  ASSERT_EQ(data_after_compact.size(), versions.size());
  for (auto &version : versions) {
    ASSERT_TRUE(data_after_compact.count(version.user_key) > 0);
  }
  VerifyDB(data_after_compact);
}

// Test compaction filter should remove any blob index where corresponding
// blob file has been removed (either by FIFO or garbage collection).
TEST_F(BlobDBTest, FilterFileNotAvailable) {
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.disable_auto_compactions = true;
  Open(bdb_options, options);

  ASSERT_OK(Put("foo", "v1"));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_EQ(1, blob_files[0]->BlobFileNumber());
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[0]));

  ASSERT_OK(Put("bar", "v2"));
  blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(2, blob_files.size());
  ASSERT_EQ(2, blob_files[1]->BlobFileNumber());
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[1]));

  const size_t kMaxKeys = 10000;

  DB *base_db = blob_db_->GetRootDB();
  std::vector<KeyVersion> versions;
  ASSERT_OK(GetAllKeyVersions(base_db, "", "", kMaxKeys, &versions));
  ASSERT_EQ(2, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  ASSERT_EQ("foo", versions[1].user_key);
  VerifyDB({{"bar", "v2"}, {"foo", "v1"}});

  ASSERT_OK(blob_db_->Flush(FlushOptions()));
  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, "", "", kMaxKeys, &versions));
  ASSERT_EQ(2, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  ASSERT_EQ("foo", versions[1].user_key);
  VerifyDB({{"bar", "v2"}, {"foo", "v1"}});

  // Remove the first blob file and compact. foo should be remove from base db.
  blob_db_impl()->TEST_ObsoleteBlobFile(blob_files[0]);
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, "", "", kMaxKeys, &versions));
  ASSERT_EQ(1, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  VerifyDB({{"bar", "v2"}});

  // Remove the second blob file and compact. bar should be remove from base db.
  blob_db_impl()->TEST_ObsoleteBlobFile(blob_files[1]);
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, "", "", kMaxKeys, &versions));
  ASSERT_EQ(0, versions.size());
  VerifyDB({});
}

// Test compaction filter should filter any inlined TTL keys that would have
// been dropped by last FIFO eviction if they are store out-of-line.
TEST_F(BlobDBTest, FilterForFIFOEviction) {
  Random rnd(215);
  BlobDBOptions bdb_options;
  bdb_options.min_blob_size = 100;
  bdb_options.ttl_range_secs = 60;
  bdb_options.max_db_size = 0;
  bdb_options.disable_background_tasks = true;
  Options options;
  // Use mock env to stop wall clock.
  mock_env_->set_current_time(0);
  options.env = mock_env_.get();
  auto statistics = CreateDBStatistics();
  options.statistics = statistics;
  options.disable_auto_compactions = true;
  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  std::map<std::string, std::string> data_after_compact;
  // Insert some small values that will be inlined.
  for (int i = 0; i < 1000; i++) {
    std::string key = "key" + ToString(i);
    std::string value = test::RandomHumanReadableString(&rnd, 50);
    uint64_t ttl = rnd.Next() % 120 + 1;
    ASSERT_OK(PutWithTTL(key, value, ttl, &data));
    if (ttl >= 60) {
      data_after_compact[key] = value;
    }
  }
  uint64_t num_keys_to_evict = data.size() - data_after_compact.size();
  ASSERT_OK(blob_db_->Flush(FlushOptions()));
  uint64_t live_sst_size = blob_db_impl()->TEST_live_sst_size();
  ASSERT_GT(live_sst_size, 0);
  VerifyDB(data);

  bdb_options.max_db_size = live_sst_size + 30000;
  bdb_options.is_fifo = true;
  Reopen(bdb_options, options);
  VerifyDB(data);

  // Put two large values, each on a different blob file.
  std::string large_value(10000, 'v');
  ASSERT_OK(PutWithTTL("large_key1", large_value, 90));
  ASSERT_OK(PutWithTTL("large_key2", large_value, 150));
  ASSERT_EQ(2, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(0, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));
  data["large_key1"] = large_value;
  data["large_key2"] = large_value;
  VerifyDB(data);

  // Put a third large value which will bring the DB out of space.
  // FIFO eviction will evict the file of large_key1.
  ASSERT_OK(PutWithTTL("large_key3", large_value, 150));
  ASSERT_EQ(1, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));
  ASSERT_EQ(2, blob_db_impl()->TEST_GetBlobFiles().size());
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
  data.erase("large_key1");
  data["large_key3"] = large_value;
  VerifyDB(data);

  // Putting some more small values. These values shouldn't be evicted by
  // compaction filter since they are inserted after FIFO eviction.
  ASSERT_OK(PutWithTTL("foo", "v", 30, &data_after_compact));
  ASSERT_OK(PutWithTTL("bar", "v", 30, &data_after_compact));

  // FIFO eviction doesn't trigger again since there enough room for the flush.
  ASSERT_OK(blob_db_->Flush(FlushOptions()));
  ASSERT_EQ(1, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));

  // Manual compact and check if compaction filter evict those keys with
  // expiration < 60.
  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // All keys with expiration < 60, plus large_key1 is filtered by
  // compaction filter.
  ASSERT_EQ(num_keys_to_evict + 1,
            statistics->getTickerCount(BLOB_DB_BLOB_INDEX_EVICTED_COUNT));
  ASSERT_EQ(1, statistics->getTickerCount(BLOB_DB_FIFO_NUM_FILES_EVICTED));
  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
  data_after_compact["large_key2"] = large_value;
  data_after_compact["large_key3"] = large_value;
  VerifyDB(data_after_compact);
}

// File should be evicted after expiration.
TEST_F(BlobDBTest, EvictExpiredFile) {
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 100;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.env = mock_env_.get();
  Open(bdb_options, options);
  mock_env_->set_current_time(50);
  std::map<std::string, std::string> data;
  ASSERT_OK(PutWithTTL("foo", "bar", 100, &data));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  auto blob_file = blob_files[0];
  ASSERT_FALSE(blob_file->Immutable());
  ASSERT_FALSE(blob_file->Obsolete());
  VerifyDB(data);
  mock_env_->set_current_time(250);
  // The key should expired now.
  blob_db_impl()->TEST_EvictExpiredFiles();
  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(1, blob_db_impl()->TEST_GetObsoleteFiles().size());
  ASSERT_TRUE(blob_file->Immutable());
  ASSERT_TRUE(blob_file->Obsolete());
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(0, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(0, blob_db_impl()->TEST_GetObsoleteFiles().size());
  // Make sure we don't return garbage value after blob file being evicted,
  // but the blob index still exists in the LSM tree.
  std::string val = "";
  ASSERT_TRUE(blob_db_->Get(ReadOptions(), "foo", &val).IsNotFound());
  ASSERT_EQ("", val);
}

TEST_F(BlobDBTest, DisableFileDeletions) {
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (bool force : {true, false}) {
    ASSERT_OK(Put("foo", "v", &data));
    auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(1, blob_files.size());
    auto blob_file = blob_files[0];
    ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_file));
    blob_db_impl()->TEST_ObsoleteBlobFile(blob_file);
    ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
    ASSERT_EQ(1, blob_db_impl()->TEST_GetObsoleteFiles().size());
    // Call DisableFileDeletions twice.
    ASSERT_OK(blob_db_->DisableFileDeletions());
    ASSERT_OK(blob_db_->DisableFileDeletions());
    // File deletions should be disabled.
    blob_db_impl()->TEST_DeleteObsoleteFiles();
    ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
    ASSERT_EQ(1, blob_db_impl()->TEST_GetObsoleteFiles().size());
    VerifyDB(data);
    // Enable file deletions once. If force=true, file deletion is enabled.
    // Otherwise it needs to enable it for a second time.
    ASSERT_OK(blob_db_->EnableFileDeletions(force));
    blob_db_impl()->TEST_DeleteObsoleteFiles();
    if (!force) {
      ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
      ASSERT_EQ(1, blob_db_impl()->TEST_GetObsoleteFiles().size());
      VerifyDB(data);
      // Call EnableFileDeletions a second time.
      ASSERT_OK(blob_db_->EnableFileDeletions(false));
      blob_db_impl()->TEST_DeleteObsoleteFiles();
    }
    // Regardless of value of `force`, file should be deleted by now.
    ASSERT_EQ(0, blob_db_impl()->TEST_GetBlobFiles().size());
    ASSERT_EQ(0, blob_db_impl()->TEST_GetObsoleteFiles().size());
    VerifyDB({});
  }
}

TEST_F(BlobDBTest, ShutdownWait) {
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 100;
  bdb_options.min_blob_size = 0;
  bdb_options.disable_background_tasks = false;
  Options options;
  options.env = mock_env_.get();

  SyncPoint::GetInstance()->LoadDependency({
      {"BlobDBImpl::EvictExpiredFiles:0", "BlobDBTest.ShutdownWait:0"},
      {"BlobDBTest.ShutdownWait:1", "BlobDBImpl::EvictExpiredFiles:1"},
      {"BlobDBImpl::EvictExpiredFiles:2", "BlobDBTest.ShutdownWait:2"},
      {"BlobDBTest.ShutdownWait:3", "BlobDBImpl::EvictExpiredFiles:3"},
  });
  // Force all tasks to be scheduled immediately.
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "TimeQueue::Add:item.end", [&](void *arg) {
        std::chrono::steady_clock::time_point *tp =
            static_cast<std::chrono::steady_clock::time_point *>(arg);
        *tp =
            std::chrono::steady_clock::now() - std::chrono::milliseconds(10000);
      });

  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "BlobDBImpl::EvictExpiredFiles:cb", [&](void * /*arg*/) {
        // Sleep 3 ms to increase the chance of data race.
        // We've synced up the code so that EvictExpiredFiles()
        // is called concurrently with ~BlobDBImpl().
        // ~BlobDBImpl() is supposed to wait for all background
        // task to shutdown before doing anything else. In order
        // to use the same test to reproduce a bug of the waiting
        // logic, we wait a little bit here, so that TSAN can
        // catch the data race.
        // We should improve the test if we find a better way.
        Env::Default()->SleepForMicroseconds(3000);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  Open(bdb_options, options);
  mock_env_->set_current_time(50);
  std::map<std::string, std::string> data;
  ASSERT_OK(PutWithTTL("foo", "bar", 100, &data));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  auto blob_file = blob_files[0];
  ASSERT_FALSE(blob_file->Immutable());
  ASSERT_FALSE(blob_file->Obsolete());
  VerifyDB(data);

  TEST_SYNC_POINT("BlobDBTest.ShutdownWait:0");
  mock_env_->set_current_time(250);
  // The key should expired now.
  TEST_SYNC_POINT("BlobDBTest.ShutdownWait:1");

  TEST_SYNC_POINT("BlobDBTest.ShutdownWait:2");
  TEST_SYNC_POINT("BlobDBTest.ShutdownWait:3");
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
}

}  //  namespace blob_db
}  //  namespace rocksdb

// A black-box test for the ttl wrapper around rocksdb
int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "SKIPPED as BlobDB is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
