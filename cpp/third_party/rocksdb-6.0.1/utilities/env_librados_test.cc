//  Copyright (c) 2016, Red Hat, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/env_librados.h"
#include <rados/librados.hpp>
#include "env/mock_env.h"
#include "util/testharness.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "util/random.h"
#include <chrono>
#include <ostream>
#include "rocksdb/utilities/transaction_db.h"

class Timer {
  typedef std::chrono::high_resolution_clock high_resolution_clock;
  typedef std::chrono::milliseconds milliseconds;
public:
  explicit Timer(bool run = false)
  {
    if (run)
      Reset();
  }
  void Reset()
  {
    _start = high_resolution_clock::now();
  }
  milliseconds Elapsed() const
  {
    return std::chrono::duration_cast<milliseconds>(high_resolution_clock::now() - _start);
  }
  template <typename T, typename Traits>
  friend std::basic_ostream<T, Traits>& operator<<(std::basic_ostream<T, Traits>& out, const Timer& timer)
  {
    return out << timer.Elapsed().count();
  }
private:
  high_resolution_clock::time_point _start;
};

namespace rocksdb {

class EnvLibradosTest : public testing::Test {
public:
  // we will use all of these below
  const std::string db_name = "env_librados_test_db";
  const std::string db_pool = db_name + "_pool";
  const char *keyring = "admin";
  const char *config = "../ceph/src/ceph.conf";

  EnvLibrados* env_;
  const EnvOptions soptions_;

  EnvLibradosTest()
    : env_(new EnvLibrados(db_name, config, db_pool)) {
  }
  ~EnvLibradosTest() {
    delete env_;
    librados::Rados rados;
    int ret = 0;
    do {
      ret = rados.init("admin"); // just use the client.admin keyring
      if (ret < 0) { // let's handle any error that might have come back
        std::cerr << "couldn't initialize rados! error " << ret << std::endl;
        ret = EXIT_FAILURE;
        break;
      }

      ret = rados.conf_read_file(config);
      if (ret < 0) {
        // This could fail if the config file is malformed, but it'd be hard.
        std::cerr << "failed to parse config file " << config
                  << "! error" << ret << std::endl;
        ret = EXIT_FAILURE;
        break;
      }

      /*
       * next, we actually connect to the cluster
       */

      ret = rados.connect();
      if (ret < 0) {
        std::cerr << "couldn't connect to cluster! error " << ret << std::endl;
        ret = EXIT_FAILURE;
        break;
      }

      /*
       * And now we're done, so let's remove our pool and then
       * shut down the connection gracefully.
       */
      int delete_ret = rados.pool_delete(db_pool.c_str());
      if (delete_ret < 0) {
        // be careful not to
        std::cerr << "We failed to delete our test pool!" << db_pool << delete_ret << std::endl;
        ret = EXIT_FAILURE;
      }
    } while (0);
  }
};

TEST_F(EnvLibradosTest, Basics) {
  uint64_t file_size;
  std::unique_ptr<WritableFile> writable_file;
  std::vector<std::string> children;

  ASSERT_OK(env_->CreateDir("/dir"));
  // Check that the directory is empty.
  ASSERT_EQ(Status::NotFound(), env_->FileExists("/dir/non_existent"));
  ASSERT_TRUE(!env_->GetFileSize("/dir/non_existent", &file_size).ok());
  ASSERT_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(0U, children.size());

  // Create a file.
  ASSERT_OK(env_->NewWritableFile("/dir/f", &writable_file, soptions_));
  writable_file.reset();

  // Check that the file exists.
  ASSERT_OK(env_->FileExists("/dir/f"));
  ASSERT_OK(env_->GetFileSize("/dir/f", &file_size));
  ASSERT_EQ(0U, file_size);
  ASSERT_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(1U, children.size());
  ASSERT_EQ("f", children[0]);

  // Write to the file.
  ASSERT_OK(env_->NewWritableFile("/dir/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("abc"));
  writable_file.reset();


  // Check for expected size.
  ASSERT_OK(env_->GetFileSize("/dir/f", &file_size));
  ASSERT_EQ(3U, file_size);


  // Check that renaming works.
  ASSERT_TRUE(!env_->RenameFile("/dir/non_existent", "/dir/g").ok());
  ASSERT_OK(env_->RenameFile("/dir/f", "/dir/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists("/dir/f"));
  ASSERT_OK(env_->FileExists("/dir/g"));
  ASSERT_OK(env_->GetFileSize("/dir/g", &file_size));
  ASSERT_EQ(3U, file_size);

  // Check that opening non-existent file fails.
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  ASSERT_TRUE(
    !env_->NewSequentialFile("/dir/non_existent", &seq_file, soptions_).ok());
  ASSERT_TRUE(!seq_file);
  ASSERT_TRUE(!env_->NewRandomAccessFile("/dir/non_existent", &rand_file,
                                         soptions_).ok());
  ASSERT_TRUE(!rand_file);

  // Check that deleting works.
  ASSERT_TRUE(!env_->DeleteFile("/dir/non_existent").ok());
  ASSERT_OK(env_->DeleteFile("/dir/g"));
  ASSERT_EQ(Status::NotFound(), env_->FileExists("/dir/g"));
  ASSERT_OK(env_->GetChildren("/dir", &children));
  ASSERT_EQ(0U, children.size());
  ASSERT_OK(env_->DeleteDir("/dir"));
}

TEST_F(EnvLibradosTest, ReadWrite) {
  std::unique_ptr<WritableFile> writable_file;
  std::unique_ptr<SequentialFile> seq_file;
  std::unique_ptr<RandomAccessFile> rand_file;
  Slice result;
  char scratch[100];

  ASSERT_OK(env_->CreateDir("/dir"));

  ASSERT_OK(env_->NewWritableFile("/dir/f", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("hello "));
  ASSERT_OK(writable_file->Append("world"));
  writable_file.reset();

  // Read sequentially.
  ASSERT_OK(env_->NewSequentialFile("/dir/f", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(seq_file->Skip(1));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(seq_file->Read(1000, &result, scratch));  // Try reading past EOF.
  ASSERT_EQ(0U, result.size());
  ASSERT_OK(seq_file->Skip(100));  // Try to skip past end of file.
  ASSERT_OK(seq_file->Read(1000, &result, scratch));
  ASSERT_EQ(0U, result.size());

  // Random reads.
  ASSERT_OK(env_->NewRandomAccessFile("/dir/f", &rand_file, soptions_));
  ASSERT_OK(rand_file->Read(6, 5, &result, scratch));  // Read "world".
  ASSERT_EQ(0, result.compare("world"));
  ASSERT_OK(rand_file->Read(0, 5, &result, scratch));  // Read "hello".
  ASSERT_EQ(0, result.compare("hello"));
  ASSERT_OK(rand_file->Read(10, 100, &result, scratch));  // Read "d".
  ASSERT_EQ(0, result.compare("d"));

  // Too high offset.
  ASSERT_OK(rand_file->Read(1000, 5, &result, scratch));
}

TEST_F(EnvLibradosTest, Locks) {
  FileLock* lock = nullptr;
  std::unique_ptr<WritableFile> writable_file;

  ASSERT_OK(env_->CreateDir("/dir"));

  ASSERT_OK(env_->NewWritableFile("/dir/f", &writable_file, soptions_));

  // These are no-ops, but we test they return success.
  ASSERT_OK(env_->LockFile("some file", &lock));
  ASSERT_OK(env_->UnlockFile(lock));

  ASSERT_OK(env_->LockFile("/dir/f", &lock));
  ASSERT_OK(env_->UnlockFile(lock));
}

TEST_F(EnvLibradosTest, Misc) {
  std::string test_dir;
  ASSERT_OK(env_->GetTestDirectory(&test_dir));
  ASSERT_TRUE(!test_dir.empty());

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_TRUE(!env_->NewWritableFile("/a/b", &writable_file, soptions_).ok());

  ASSERT_OK(env_->NewWritableFile("/a", &writable_file, soptions_));
  // These are no-ops, but we test they return success.
  ASSERT_OK(writable_file->Sync());
  ASSERT_OK(writable_file->Flush());
  ASSERT_OK(writable_file->Close());
  writable_file.reset();
}

TEST_F(EnvLibradosTest, LargeWrite) {
  const size_t kWriteSize = 300 * 1024;
  char* scratch = new char[kWriteSize * 2];

  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, 'h');
  }

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->CreateDir("/dir"));
  ASSERT_OK(env_->NewWritableFile("/dir/g", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("foo"));
  ASSERT_OK(writable_file->Append(write_data));
  writable_file.reset();

  std::unique_ptr<SequentialFile> seq_file;
  Slice result;
  ASSERT_OK(env_->NewSequentialFile("/dir/g", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(3, &result, scratch));  // Read "foo".
  ASSERT_EQ(0, result.compare("foo"));

  size_t read = 0;
  std::string read_data;
  while (read < kWriteSize) {
    ASSERT_OK(seq_file->Read(kWriteSize - read, &result, scratch));
    read_data.append(result.data(), result.size());
    read += result.size();
  }
  ASSERT_TRUE(write_data == read_data);
  delete[] scratch;
}

TEST_F(EnvLibradosTest, FrequentlySmallWrite) {
  const size_t kWriteSize = 1 << 10;
  char* scratch = new char[kWriteSize * 2];

  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, 'h');
  }

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->CreateDir("/dir"));
  ASSERT_OK(env_->NewWritableFile("/dir/g", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append("foo"));

  for (size_t i = 0; i < kWriteSize; ++i) {
    ASSERT_OK(writable_file->Append("h"));
  }
  writable_file.reset();

  std::unique_ptr<SequentialFile> seq_file;
  Slice result;
  ASSERT_OK(env_->NewSequentialFile("/dir/g", &seq_file, soptions_));
  ASSERT_OK(seq_file->Read(3, &result, scratch));  // Read "foo".
  ASSERT_EQ(0, result.compare("foo"));

  size_t read = 0;
  std::string read_data;
  while (read < kWriteSize) {
    ASSERT_OK(seq_file->Read(kWriteSize - read, &result, scratch));
    read_data.append(result.data(), result.size());
    read += result.size();
  }
  ASSERT_TRUE(write_data == read_data);
  delete[] scratch;
}

TEST_F(EnvLibradosTest, Truncate) {
  const size_t kWriteSize = 300 * 1024;
  const size_t truncSize = 1024;
  std::string write_data;
  for (size_t i = 0; i < kWriteSize; ++i) {
    write_data.append(1, 'h');
  }

  std::unique_ptr<WritableFile> writable_file;
  ASSERT_OK(env_->CreateDir("/dir"));
  ASSERT_OK(env_->NewWritableFile("/dir/g", &writable_file, soptions_));
  ASSERT_OK(writable_file->Append(write_data));
  ASSERT_EQ(writable_file->GetFileSize(), kWriteSize);
  ASSERT_OK(writable_file->Truncate(truncSize));
  ASSERT_EQ(writable_file->GetFileSize(), truncSize);
  writable_file.reset();
}

TEST_F(EnvLibradosTest, DBBasics) {
  std::string kDBPath = "/tmp/DBBasics";
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.env = env_;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  delete db;
}

TEST_F(EnvLibradosTest, DBLoadKeysInRandomOrder) {
  char key[20] = {0}, value[20] = {0};
  int max_loop = 1 << 10;
  Timer timer(false);
  std::cout << "Test size : loop(" << max_loop << ")" << std::endl;
  /**********************************
            use default env
  ***********************************/
  std::string kDBPath1 = "/tmp/DBLoadKeysInRandomOrder1";
  DB* db1;
  Options options1;
  // Optimize Rocksdb. This is the easiest way to get RocksDB to perform well
  options1.IncreaseParallelism();
  options1.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options1.create_if_missing = true;

  // open DB
  Status s1 = DB::Open(options1, kDBPath1, &db1);
  assert(s1.ok());

  rocksdb::Random64 r1(time(nullptr));

  timer.Reset();
  for (int i = 0; i < max_loop; ++i) {
    snprintf(key,
             20,
             "%16lx",
             (unsigned long)r1.Uniform(std::numeric_limits<uint64_t>::max()));
    snprintf(value,
             20,
             "%16lx",
             (unsigned long)r1.Uniform(std::numeric_limits<uint64_t>::max()));
    // Put key-value
    s1 = db1->Put(WriteOptions(), key, value);
    assert(s1.ok());
  }
  std::cout << "Time by default : " << timer << "ms" << std::endl;
  delete db1;

  /**********************************
            use librados env
  ***********************************/
  std::string kDBPath2 = "/tmp/DBLoadKeysInRandomOrder2";
  DB* db2;
  Options options2;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options2.IncreaseParallelism();
  options2.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options2.create_if_missing = true;
  options2.env = env_;

  // open DB
  Status s2 = DB::Open(options2, kDBPath2, &db2);
  assert(s2.ok());

  rocksdb::Random64 r2(time(nullptr));

  timer.Reset();
  for (int i = 0; i < max_loop; ++i) {
    snprintf(key,
             20,
             "%16lx",
             (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
    snprintf(value,
             20,
             "%16lx",
             (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
    // Put key-value
    s2 = db2->Put(WriteOptions(), key, value);
    assert(s2.ok());
  }
  std::cout << "Time by librados : " << timer << "ms" << std::endl;
  delete db2;
}

TEST_F(EnvLibradosTest, DBBulkLoadKeysInRandomOrder) {
  char key[20] = {0}, value[20] = {0};
  int max_loop = 1 << 6;
  int bulk_size = 1 << 15;
  Timer timer(false);
  std::cout << "Test size : loop(" << max_loop << "); bulk_size(" << bulk_size << ")" << std::endl;
  /**********************************
            use default env
  ***********************************/
  std::string kDBPath1 = "/tmp/DBBulkLoadKeysInRandomOrder1";
  DB* db1;
  Options options1;
  // Optimize Rocksdb. This is the easiest way to get RocksDB to perform well
  options1.IncreaseParallelism();
  options1.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options1.create_if_missing = true;

  // open DB
  Status s1 = DB::Open(options1, kDBPath1, &db1);
  assert(s1.ok());

  rocksdb::Random64 r1(time(nullptr));

  timer.Reset();
  for (int i = 0; i < max_loop; ++i) {
    WriteBatch batch;
    for (int j = 0; j < bulk_size; ++j) {
      snprintf(key,
               20,
               "%16lx",
               (unsigned long)r1.Uniform(std::numeric_limits<uint64_t>::max()));
      snprintf(value,
               20,
               "%16lx",
               (unsigned long)r1.Uniform(std::numeric_limits<uint64_t>::max()));
      batch.Put(key, value);
    }
    s1 = db1->Write(WriteOptions(), &batch);
    assert(s1.ok());
  }
  std::cout << "Time by default : " << timer << "ms" << std::endl;
  delete db1;

  /**********************************
            use librados env
  ***********************************/
  std::string kDBPath2 = "/tmp/DBBulkLoadKeysInRandomOrder2";
  DB* db2;
  Options options2;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options2.IncreaseParallelism();
  options2.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options2.create_if_missing = true;
  options2.env = env_;

  // open DB
  Status s2 = DB::Open(options2, kDBPath2, &db2);
  assert(s2.ok());

  rocksdb::Random64 r2(time(nullptr));

  timer.Reset();
  for (int i = 0; i < max_loop; ++i) {
    WriteBatch batch;
    for (int j = 0; j < bulk_size; ++j) {
      snprintf(key,
               20,
               "%16lx",
               (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
      snprintf(value,
               20,
               "%16lx",
               (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
      batch.Put(key, value);
    }
    s2 = db2->Write(WriteOptions(), &batch);
    assert(s2.ok());
  }
  std::cout << "Time by librados : " << timer << "ms" << std::endl;
  delete db2;
}

TEST_F(EnvLibradosTest, DBBulkLoadKeysInSequentialOrder) {
  char key[20] = {0}, value[20] = {0};
  int max_loop = 1 << 6;
  int bulk_size = 1 << 15;
  Timer timer(false);
  std::cout << "Test size : loop(" << max_loop << "); bulk_size(" << bulk_size << ")" << std::endl;
  /**********************************
            use default env
  ***********************************/
  std::string kDBPath1 = "/tmp/DBBulkLoadKeysInSequentialOrder1";
  DB* db1;
  Options options1;
  // Optimize Rocksdb. This is the easiest way to get RocksDB to perform well
  options1.IncreaseParallelism();
  options1.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options1.create_if_missing = true;

  // open DB
  Status s1 = DB::Open(options1, kDBPath1, &db1);
  assert(s1.ok());

  rocksdb::Random64 r1(time(nullptr));

  timer.Reset();
  for (int i = 0; i < max_loop; ++i) {
    WriteBatch batch;
    for (int j = 0; j < bulk_size; ++j) {
      snprintf(key,
               20,
               "%019lld",
               (long long)(i * bulk_size + j));
      snprintf(value,
               20,
               "%16lx",
               (unsigned long)r1.Uniform(std::numeric_limits<uint64_t>::max()));
      batch.Put(key, value);
    }
    s1 = db1->Write(WriteOptions(), &batch);
    assert(s1.ok());
  }
  std::cout << "Time by default : " << timer << "ms" << std::endl;
  delete db1;

  /**********************************
            use librados env
  ***********************************/
  std::string kDBPath2 = "/tmp/DBBulkLoadKeysInSequentialOrder2";
  DB* db2;
  Options options2;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options2.IncreaseParallelism();
  options2.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options2.create_if_missing = true;
  options2.env = env_;

  // open DB
  Status s2 = DB::Open(options2, kDBPath2, &db2);
  assert(s2.ok());

  rocksdb::Random64 r2(time(nullptr));

  timer.Reset();
  for (int i = 0; i < max_loop; ++i) {
    WriteBatch batch;
    for (int j = 0; j < bulk_size; ++j) {
      snprintf(key,
               20,
               "%16lx",
               (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
      snprintf(value,
               20,
               "%16lx",
               (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
      batch.Put(key, value);
    }
    s2 = db2->Write(WriteOptions(), &batch);
    assert(s2.ok());
  }
  std::cout << "Time by librados : " << timer << "ms" << std::endl;
  delete db2;
}

TEST_F(EnvLibradosTest, DBRandomRead) {
  char key[20] = {0}, value[20] = {0};
  int max_loop = 1 << 6;
  int bulk_size = 1 << 10;
  int read_loop = 1 << 20;
  Timer timer(false);
  std::cout << "Test size : keys_num(" << max_loop << ", " << bulk_size << "); read_loop(" << read_loop << ")" << std::endl;
  /**********************************
            use default env
  ***********************************/
  std::string kDBPath1 = "/tmp/DBRandomRead1";
  DB* db1;
  Options options1;
  // Optimize Rocksdb. This is the easiest way to get RocksDB to perform well
  options1.IncreaseParallelism();
  options1.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options1.create_if_missing = true;

  // open DB
  Status s1 = DB::Open(options1, kDBPath1, &db1);
  assert(s1.ok());

  rocksdb::Random64 r1(time(nullptr));


  for (int i = 0; i < max_loop; ++i) {
    WriteBatch batch;
    for (int j = 0; j < bulk_size; ++j) {
      snprintf(key,
               20,
               "%019lld",
               (long long)(i * bulk_size + j));
      snprintf(value,
               20,
               "%16lx",
               (unsigned long)r1.Uniform(std::numeric_limits<uint64_t>::max()));
      batch.Put(key, value);
    }
    s1 = db1->Write(WriteOptions(), &batch);
    assert(s1.ok());
  }
  timer.Reset();
  int base1 = 0, offset1 = 0;
  for (int i = 0; i < read_loop; ++i) {
    base1 = r1.Uniform(max_loop);
    offset1 = r1.Uniform(bulk_size);
    std::string value1;
    snprintf(key,
             20,
             "%019lld",
             (long long)(base1 * bulk_size + offset1));
    s1 = db1->Get(ReadOptions(), key, &value1);
    assert(s1.ok());
  }
  std::cout << "Time by default : " << timer << "ms" << std::endl;
  delete db1;

  /**********************************
            use librados env
  ***********************************/
  std::string kDBPath2 = "/tmp/DBRandomRead2";
  DB* db2;
  Options options2;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options2.IncreaseParallelism();
  options2.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options2.create_if_missing = true;
  options2.env = env_;

  // open DB
  Status s2 = DB::Open(options2, kDBPath2, &db2);
  assert(s2.ok());

  rocksdb::Random64 r2(time(nullptr));

  for (int i = 0; i < max_loop; ++i) {
    WriteBatch batch;
    for (int j = 0; j < bulk_size; ++j) {
      snprintf(key,
               20,
               "%019lld",
               (long long)(i * bulk_size + j));
      snprintf(value,
               20,
               "%16lx",
               (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
      batch.Put(key, value);
    }
    s2 = db2->Write(WriteOptions(), &batch);
    assert(s2.ok());
  }

  timer.Reset();
  int base2 = 0, offset2 = 0;
  for (int i = 0; i < read_loop; ++i) {
    base2 = r2.Uniform(max_loop);
    offset2 = r2.Uniform(bulk_size);
    std::string value2;
    snprintf(key,
             20,
             "%019lld",
             (long long)(base2 * bulk_size + offset2));
    s2 = db2->Get(ReadOptions(), key, &value2);
    if (!s2.ok()) {
      std::cout << s2.ToString() << std::endl;
    }
    assert(s2.ok());
  }
  std::cout << "Time by librados : " << timer << "ms" << std::endl;
  delete db2;
}

class EnvLibradosMutipoolTest : public testing::Test {
public:
  // we will use all of these below
  const std::string client_name = "client.admin";
  const std::string cluster_name = "ceph";
  const uint64_t flags = 0;
  const std::string db_name = "env_librados_test_db";
  const std::string db_pool = db_name + "_pool";
  const std::string wal_dir = "/wal";
  const std::string wal_pool = db_name + "_wal_pool";
  const size_t write_buffer_size = 1 << 20;
  const char *keyring = "admin";
  const char *config = "../ceph/src/ceph.conf";

  EnvLibrados* env_;
  const EnvOptions soptions_;

  EnvLibradosMutipoolTest() {
    env_ = new EnvLibrados(client_name, cluster_name, flags, db_name, config, db_pool, wal_dir, wal_pool, write_buffer_size);
  }
  ~EnvLibradosMutipoolTest() {
    delete env_;
    librados::Rados rados;
    int ret = 0;
    do {
      ret = rados.init("admin"); // just use the client.admin keyring
      if (ret < 0) { // let's handle any error that might have come back
        std::cerr << "couldn't initialize rados! error " << ret << std::endl;
        ret = EXIT_FAILURE;
        break;
      }

      ret = rados.conf_read_file(config);
      if (ret < 0) {
        // This could fail if the config file is malformed, but it'd be hard.
        std::cerr << "failed to parse config file " << config
                  << "! error" << ret << std::endl;
        ret = EXIT_FAILURE;
        break;
      }

      /*
       * next, we actually connect to the cluster
       */

      ret = rados.connect();
      if (ret < 0) {
        std::cerr << "couldn't connect to cluster! error " << ret << std::endl;
        ret = EXIT_FAILURE;
        break;
      }

      /*
       * And now we're done, so let's remove our pool and then
       * shut down the connection gracefully.
       */
      int delete_ret = rados.pool_delete(db_pool.c_str());
      if (delete_ret < 0) {
        // be careful not to
        std::cerr << "We failed to delete our test pool!" << db_pool << delete_ret << std::endl;
        ret = EXIT_FAILURE;
      }
      delete_ret = rados.pool_delete(wal_pool.c_str());
      if (delete_ret < 0) {
        // be careful not to
        std::cerr << "We failed to delete our test pool!" << wal_pool << delete_ret << std::endl;
        ret = EXIT_FAILURE;
      }
    } while (0);
  }
};

TEST_F(EnvLibradosMutipoolTest, Basics) {
  uint64_t file_size;
  std::unique_ptr<WritableFile> writable_file;
  std::vector<std::string> children;
  std::vector<std::string> v = {"/tmp/dir1", "/tmp/dir2", "/tmp/dir3", "/tmp/dir4", "dir"};

  for (size_t i = 0; i < v.size(); ++i) {
    std::string dir = v[i];
    std::string dir_non_existent = dir + "/non_existent";
    std::string dir_f = dir + "/f";
    std::string dir_g = dir + "/g";

    ASSERT_OK(env_->CreateDir(dir.c_str()));
    // Check that the directory is empty.
    ASSERT_EQ(Status::NotFound(), env_->FileExists(dir_non_existent.c_str()));
    ASSERT_TRUE(!env_->GetFileSize(dir_non_existent.c_str(), &file_size).ok());
    ASSERT_OK(env_->GetChildren(dir.c_str(), &children));
    ASSERT_EQ(0U, children.size());

    // Create a file.
    ASSERT_OK(env_->NewWritableFile(dir_f.c_str(), &writable_file, soptions_));
    writable_file.reset();

    // Check that the file exists.
    ASSERT_OK(env_->FileExists(dir_f.c_str()));
    ASSERT_OK(env_->GetFileSize(dir_f.c_str(), &file_size));
    ASSERT_EQ(0U, file_size);
    ASSERT_OK(env_->GetChildren(dir.c_str(), &children));
    ASSERT_EQ(1U, children.size());
    ASSERT_EQ("f", children[0]);

    // Write to the file.
    ASSERT_OK(env_->NewWritableFile(dir_f.c_str(), &writable_file, soptions_));
    ASSERT_OK(writable_file->Append("abc"));
    writable_file.reset();


    // Check for expected size.
    ASSERT_OK(env_->GetFileSize(dir_f.c_str(), &file_size));
    ASSERT_EQ(3U, file_size);


    // Check that renaming works.
    ASSERT_TRUE(!env_->RenameFile(dir_non_existent.c_str(), dir_g.c_str()).ok());
    ASSERT_OK(env_->RenameFile(dir_f.c_str(), dir_g.c_str()));
    ASSERT_EQ(Status::NotFound(), env_->FileExists(dir_f.c_str()));
    ASSERT_OK(env_->FileExists(dir_g.c_str()));
    ASSERT_OK(env_->GetFileSize(dir_g.c_str(), &file_size));
    ASSERT_EQ(3U, file_size);

    // Check that opening non-existent file fails.
    std::unique_ptr<SequentialFile> seq_file;
    std::unique_ptr<RandomAccessFile> rand_file;
    ASSERT_TRUE(
      !env_->NewSequentialFile(dir_non_existent.c_str(), &seq_file, soptions_).ok());
    ASSERT_TRUE(!seq_file);
    ASSERT_TRUE(!env_->NewRandomAccessFile(dir_non_existent.c_str(), &rand_file,
                                           soptions_).ok());
    ASSERT_TRUE(!rand_file);

    // Check that deleting works.
    ASSERT_TRUE(!env_->DeleteFile(dir_non_existent.c_str()).ok());
    ASSERT_OK(env_->DeleteFile(dir_g.c_str()));
    ASSERT_EQ(Status::NotFound(), env_->FileExists(dir_g.c_str()));
    ASSERT_OK(env_->GetChildren(dir.c_str(), &children));
    ASSERT_EQ(0U, children.size());
    ASSERT_OK(env_->DeleteDir(dir.c_str()));
  }
}

TEST_F(EnvLibradosMutipoolTest, DBBasics) {
  std::string kDBPath = "/tmp/DBBasics";
  std::string walPath = "/tmp/wal";
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.env = env_;
  options.wal_dir = walPath;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // Put key-value
  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  {
    WriteBatch batch;
    batch.Delete("key1");
    batch.Put("key2", value);
    s = db->Write(WriteOptions(), &batch);
  }

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  delete db;
}

TEST_F(EnvLibradosMutipoolTest, DBBulkLoadKeysInRandomOrder) {
  char key[20] = {0}, value[20] = {0};
  int max_loop = 1 << 6;
  int bulk_size = 1 << 15;
  Timer timer(false);
  std::cout << "Test size : loop(" << max_loop << "); bulk_size(" << bulk_size << ")" << std::endl;
  /**********************************
            use default env
  ***********************************/
  std::string kDBPath1 = "/tmp/DBBulkLoadKeysInRandomOrder1";
  std::string walPath = "/tmp/wal";
  DB* db1;
  Options options1;
  // Optimize Rocksdb. This is the easiest way to get RocksDB to perform well
  options1.IncreaseParallelism();
  options1.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options1.create_if_missing = true;

  // open DB
  Status s1 = DB::Open(options1, kDBPath1, &db1);
  assert(s1.ok());

  rocksdb::Random64 r1(time(nullptr));

  timer.Reset();
  for (int i = 0; i < max_loop; ++i) {
    WriteBatch batch;
    for (int j = 0; j < bulk_size; ++j) {
      snprintf(key,
               20,
               "%16lx",
               (unsigned long)r1.Uniform(std::numeric_limits<uint64_t>::max()));
      snprintf(value,
               20,
               "%16lx",
               (unsigned long)r1.Uniform(std::numeric_limits<uint64_t>::max()));
      batch.Put(key, value);
    }
    s1 = db1->Write(WriteOptions(), &batch);
    assert(s1.ok());
  }
  std::cout << "Time by default : " << timer << "ms" << std::endl;
  delete db1;

  /**********************************
            use librados env
  ***********************************/
  std::string kDBPath2 = "/tmp/DBBulkLoadKeysInRandomOrder2";
  DB* db2;
  Options options2;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options2.IncreaseParallelism();
  options2.OptimizeLevelStyleCompaction();
  // create the DB if it's not already present
  options2.create_if_missing = true;
  options2.env = env_;
  options2.wal_dir = walPath;

  // open DB
  Status s2 = DB::Open(options2, kDBPath2, &db2);
  if (!s2.ok()) {
    std::cerr << s2.ToString() << std::endl;
  }
  assert(s2.ok());

  rocksdb::Random64 r2(time(nullptr));

  timer.Reset();
  for (int i = 0; i < max_loop; ++i) {
    WriteBatch batch;
    for (int j = 0; j < bulk_size; ++j) {
      snprintf(key,
               20,
               "%16lx",
               (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
      snprintf(value,
               20,
               "%16lx",
               (unsigned long)r2.Uniform(std::numeric_limits<uint64_t>::max()));
      batch.Put(key, value);
    }
    s2 = db2->Write(WriteOptions(), &batch);
    assert(s2.ok());
  }
  std::cout << "Time by librados : " << timer << "ms" << std::endl;
  delete db2;
}

TEST_F(EnvLibradosMutipoolTest, DBTransactionDB) {
  std::string kDBPath = "/tmp/DBTransactionDB";
  // open DB
  Options options;
  TransactionDBOptions txn_db_options;
  options.create_if_missing = true;
  options.env = env_;
  TransactionDB* txn_db;

  Status s = TransactionDB::Open(options, txn_db_options, kDBPath, &txn_db);
  assert(s.ok());

  WriteOptions write_options;
  ReadOptions read_options;
  TransactionOptions txn_options;
  std::string value;

  ////////////////////////////////////////////////////////
  //
  // Simple OptimisticTransaction Example ("Read Committed")
  //
  ////////////////////////////////////////////////////////

  // Start a transaction
  Transaction* txn = txn_db->BeginTransaction(write_options);
  assert(txn);

  // Read a key in this transaction
  s = txn->Get(read_options, "abc", &value);
  assert(s.IsNotFound());

  // Write a key in this transaction
  s = txn->Put("abc", "def");
  assert(s.ok());

  // Read a key OUTSIDE this transaction. Does not affect txn.
  s = txn_db->Get(read_options, "abc", &value);

  // Write a key OUTSIDE of this transaction.
  // Does not affect txn since this is an unrelated key.  If we wrote key 'abc'
  // here, the transaction would fail to commit.
  s = txn_db->Put(write_options, "xyz", "zzz");

  // Commit transaction
  s = txn->Commit();
  assert(s.ok());
  delete txn;

  ////////////////////////////////////////////////////////
  //
  // "Repeatable Read" (Snapshot Isolation) Example
  //   -- Using a single Snapshot
  //
  ////////////////////////////////////////////////////////

  // Set a snapshot at start of transaction by setting set_snapshot=true
  txn_options.set_snapshot = true;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  const Snapshot* snapshot = txn->GetSnapshot();

  // Write a key OUTSIDE of transaction
  s = txn_db->Put(write_options, "abc", "xyz");
  assert(s.ok());

  // Attempt to read a key using the snapshot.  This will fail since
  // the previous write outside this txn conflicts with this read.
  read_options.snapshot = snapshot;
  s = txn->GetForUpdate(read_options, "abc", &value);
  assert(s.IsBusy());

  txn->Rollback();

  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;
  snapshot = nullptr;

  ////////////////////////////////////////////////////////
  //
  // "Read Committed" (Monotonic Atomic Views) Example
  //   --Using multiple Snapshots
  //
  ////////////////////////////////////////////////////////

  // In this example, we set the snapshot multiple times.  This is probably
  // only necessary if you have very strict isolation requirements to
  // implement.

  // Set a snapshot at start of transaction
  txn_options.set_snapshot = true;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  // Do some reads and writes to key "x"
  read_options.snapshot = txn_db->GetSnapshot();
  s = txn->Get(read_options, "x", &value);
  txn->Put("x", "x");

  // Do a write outside of the transaction to key "y"
  s = txn_db->Put(write_options, "y", "y");

  // Set a new snapshot in the transaction
  txn->SetSnapshot();
  txn->SetSavePoint();
  read_options.snapshot = txn_db->GetSnapshot();

  // Do some reads and writes to key "y"
  // Since the snapshot was advanced, the write done outside of the
  // transaction does not conflict.
  s = txn->GetForUpdate(read_options, "y", &value);
  txn->Put("y", "y");

  // Decide we want to revert the last write from this transaction.
  txn->RollbackToSavePoint();

  // Commit.
  s = txn->Commit();
  assert(s.ok());
  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;

  // Cleanup
  delete txn_db;
  DestroyDB(kDBPath, options);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as EnvMirror is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
