//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/persistent_cache_test.h"

#include <functional>
#include <memory>
#include <thread>

#include "utilities/persistent_cache/block_cache_tier.h"

namespace rocksdb {

static const double kStressFactor = .125;

#ifdef OS_LINUX
static void OnOpenForRead(void* arg) {
  int* val = static_cast<int*>(arg);
  *val &= ~O_DIRECT;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "NewRandomAccessFile:O_DIRECT",
      std::bind(OnOpenForRead, std::placeholders::_1));
}

static void OnOpenForWrite(void* arg) {
  int* val = static_cast<int*>(arg);
  *val &= ~O_DIRECT;
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "NewWritableFile:O_DIRECT",
      std::bind(OnOpenForWrite, std::placeholders::_1));
}
#endif

static void RemoveDirectory(const std::string& folder) {
  std::vector<std::string> files;
  Status status = Env::Default()->GetChildren(folder, &files);
  if (!status.ok()) {
    // we assume the directory does not exist
    return;
  }

  // cleanup files with the patter :digi:.rc
  for (auto file : files) {
    if (file == "." || file == "..") {
      continue;
    }
    status = Env::Default()->DeleteFile(folder + "/" + file);
    assert(status.ok());
  }

  status = Env::Default()->DeleteDir(folder);
  assert(status.ok());
}

static void OnDeleteDir(void* arg) {
  char* dir = static_cast<char*>(arg);
  RemoveDirectory(std::string(dir));
}

//
// Simple logger that prints message on stdout
//
class ConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  ConsoleLogger() : Logger(InfoLogLevel::ERROR_LEVEL) {}

  void Logv(const char* format, va_list ap) override {
    MutexLock _(&lock_);
    vprintf(format, ap);
    printf("\n");
  }

  port::Mutex lock_;
};

// construct a tiered RAM+Block cache
std::unique_ptr<PersistentTieredCache> NewTieredCache(
    const size_t mem_size, const PersistentCacheConfig& opt) {
  std::unique_ptr<PersistentTieredCache> tcache(new PersistentTieredCache());
  // create primary tier
  assert(mem_size);
  auto pcache = std::shared_ptr<PersistentCacheTier>(new VolatileCacheTier(
      /*is_compressed*/ true, mem_size));
  tcache->AddTier(pcache);
  // create secondary tier
  auto scache = std::shared_ptr<PersistentCacheTier>(new BlockCacheTier(opt));
  tcache->AddTier(scache);

  Status s = tcache->Open();
  assert(s.ok());
  return tcache;
}

// create block cache
std::unique_ptr<PersistentCacheTier> NewBlockCache(
    Env* env, const std::string& path,
    const uint64_t max_size = std::numeric_limits<uint64_t>::max(),
    const bool enable_direct_writes = false) {
  const uint32_t max_file_size = static_cast<uint32_t>(12 * 1024 * 1024 * kStressFactor);
  auto log = std::make_shared<ConsoleLogger>();
  PersistentCacheConfig opt(env, path, max_size, log);
  opt.cache_file_size = max_file_size;
  opt.max_write_pipeline_backlog_size = std::numeric_limits<uint64_t>::max();
  opt.enable_direct_writes = enable_direct_writes;
  std::unique_ptr<PersistentCacheTier> scache(new BlockCacheTier(opt));
  Status s = scache->Open();
  assert(s.ok());
  return scache;
}

// create a new cache tier
std::unique_ptr<PersistentTieredCache> NewTieredCache(
    Env* env, const std::string& path, const uint64_t max_volatile_cache_size,
    const uint64_t max_block_cache_size =
        std::numeric_limits<uint64_t>::max()) {
  const uint32_t max_file_size = static_cast<uint32_t>(12 * 1024 * 1024 * kStressFactor);
  auto log = std::make_shared<ConsoleLogger>();
  auto opt = PersistentCacheConfig(env, path, max_block_cache_size, log);
  opt.cache_file_size = max_file_size;
  opt.max_write_pipeline_backlog_size = std::numeric_limits<uint64_t>::max();
  // create tier out of the two caches
  auto cache = NewTieredCache(max_volatile_cache_size, opt);
  return cache;
}

PersistentCacheTierTest::PersistentCacheTierTest()
    : path_(test::PerThreadDBPath("cache_test")) {
#ifdef OS_LINUX
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  rocksdb::SyncPoint::GetInstance()->SetCallBack("NewRandomAccessFile:O_DIRECT",
                                                 OnOpenForRead);
  rocksdb::SyncPoint::GetInstance()->SetCallBack("NewWritableFile:O_DIRECT",
                                                 OnOpenForWrite);
#endif
}

// Block cache tests
TEST_F(PersistentCacheTierTest, DISABLED_BlockCacheInsertWithFileCreateError) {
  cache_ = NewBlockCache(Env::Default(), path_,
                         /*size=*/std::numeric_limits<uint64_t>::max(),
                         /*direct_writes=*/ false);
  rocksdb::SyncPoint::GetInstance()->SetCallBack( 
    "BlockCacheTier::NewCacheFile:DeleteDir", OnDeleteDir);

  RunNegativeInsertTest(/*nthreads=*/ 1,
                        /*max_keys*/
                          static_cast<size_t>(10 * 1024 * kStressFactor));

  rocksdb::SyncPoint::GetInstance()->ClearAllCallBacks();
}

#if defined(TRAVIS) || defined(ROCKSDB_VALGRIND_RUN)
// Travis is unable to handle the normal version of the tests running out of
// fds, out of space and timeouts. This is an easier version of the test
// specifically written for Travis
TEST_F(PersistentCacheTierTest, BasicTest) {
  cache_ = std::make_shared<VolatileCacheTier>();
  RunInsertTest(/*nthreads=*/1, /*max_keys=*/1024);

  cache_ = NewBlockCache(Env::Default(), path_,
                         /*size=*/std::numeric_limits<uint64_t>::max(),
                         /*direct_writes=*/true);
  RunInsertTest(/*nthreads=*/1, /*max_keys=*/1024);

  cache_ = NewTieredCache(Env::Default(), path_,
                          /*memory_size=*/static_cast<size_t>(1 * 1024 * 1024));
  RunInsertTest(/*nthreads=*/1, /*max_keys=*/1024);
}
#else
// Volatile cache tests
TEST_F(PersistentCacheTierTest, VolatileCacheInsert) {
  for (auto nthreads : {1, 5}) {
    for (auto max_keys :
         {10 * 1024 * kStressFactor, 1 * 1024 * 1024 * kStressFactor}) {
      cache_ = std::make_shared<VolatileCacheTier>();
      RunInsertTest(nthreads, static_cast<size_t>(max_keys));
    }
  }
}

TEST_F(PersistentCacheTierTest, VolatileCacheInsertWithEviction) {
  for (auto nthreads : {1, 5}) {
    for (auto max_keys : {1 * 1024 * 1024 * kStressFactor}) {
      cache_ = std::make_shared<VolatileCacheTier>(
          /*compressed=*/true, /*size=*/static_cast<size_t>(1 * 1024 * 1024 * kStressFactor));
      RunInsertTestWithEviction(nthreads, static_cast<size_t>(max_keys));
    }
  }
}

// Block cache tests
TEST_F(PersistentCacheTierTest, BlockCacheInsert) {
  for (auto direct_writes : {true, false}) {
    for (auto nthreads : {1, 5}) {
      for (auto max_keys :
           {10 * 1024 * kStressFactor, 1 * 1024 * 1024 * kStressFactor}) {
        cache_ = NewBlockCache(Env::Default(), path_,
                               /*size=*/std::numeric_limits<uint64_t>::max(),
                               direct_writes);
        RunInsertTest(nthreads, static_cast<size_t>(max_keys));
      }
    }
  }
}

TEST_F(PersistentCacheTierTest, BlockCacheInsertWithEviction) {
  for (auto nthreads : {1, 5}) {
    for (auto max_keys : {1 * 1024 * 1024 * kStressFactor}) {
      cache_ = NewBlockCache(Env::Default(), path_,
                             /*max_size=*/static_cast<size_t>(200 * 1024 * 1024 * kStressFactor));
      RunInsertTestWithEviction(nthreads, static_cast<size_t>(max_keys));
    }
  }
}

// Tiered cache tests
TEST_F(PersistentCacheTierTest, TieredCacheInsert) {
  for (auto nthreads : {1, 5}) {
    for (auto max_keys :
         {10 * 1024 * kStressFactor, 1 * 1024 * 1024 * kStressFactor}) {
      cache_ = NewTieredCache(Env::Default(), path_,
                              /*memory_size=*/static_cast<size_t>(1 * 1024 * 1024 * kStressFactor));
      RunInsertTest(nthreads, static_cast<size_t>(max_keys));
    }
  }
}

// the tests causes a lot of file deletions which Travis limited testing
// environment cannot handle
TEST_F(PersistentCacheTierTest, TieredCacheInsertWithEviction) {
  for (auto nthreads : {1, 5}) {
    for (auto max_keys : {1 * 1024 * 1024 * kStressFactor}) {
      cache_ = NewTieredCache(
          Env::Default(), path_,
          /*memory_size=*/static_cast<size_t>(1 * 1024 * 1024 * kStressFactor),
          /*block_cache_size*/ static_cast<size_t>(200 * 1024 * 1024 * kStressFactor));
      RunInsertTestWithEviction(nthreads, static_cast<size_t>(max_keys));
    }
  }
}
#endif

std::shared_ptr<PersistentCacheTier> MakeVolatileCache(
    const std::string& /*dbname*/) {
  return std::make_shared<VolatileCacheTier>();
}

std::shared_ptr<PersistentCacheTier> MakeBlockCache(const std::string& dbname) {
  return NewBlockCache(Env::Default(), dbname);
}

std::shared_ptr<PersistentCacheTier> MakeTieredCache(
    const std::string& dbname) {
  const auto memory_size = 1 * 1024 * 1024 * kStressFactor;
  return NewTieredCache(Env::Default(), dbname, static_cast<size_t>(memory_size));
}

#ifdef OS_LINUX
static void UniqueIdCallback(void* arg) {
  int* result = reinterpret_cast<int*>(arg);
  if (*result == -1) {
    *result = 0;
  }

  rocksdb::SyncPoint::GetInstance()->ClearTrace();
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "GetUniqueIdFromFile:FS_IOC_GETVERSION", UniqueIdCallback);
}
#endif

TEST_F(PersistentCacheTierTest, FactoryTest) {
  for (auto nvm_opt : {true, false}) {
    ASSERT_FALSE(cache_);
    auto log = std::make_shared<ConsoleLogger>();
    std::shared_ptr<PersistentCache> cache;
    ASSERT_OK(NewPersistentCache(Env::Default(), path_,
                                 /*size=*/1 * 1024 * 1024 * 1024, log, nvm_opt,
                                 &cache));
    ASSERT_TRUE(cache);
    ASSERT_EQ(cache->Stats().size(), 1);
    ASSERT_TRUE(cache->Stats()[0].size());
    cache.reset();
  }
}

PersistentCacheDBTest::PersistentCacheDBTest() : DBTestBase("/cache_test") {
#ifdef OS_LINUX
  rocksdb::SyncPoint::GetInstance()->EnableProcessing();
  rocksdb::SyncPoint::GetInstance()->SetCallBack(
      "GetUniqueIdFromFile:FS_IOC_GETVERSION", UniqueIdCallback);
  rocksdb::SyncPoint::GetInstance()->SetCallBack("NewRandomAccessFile:O_DIRECT",
                                                 OnOpenForRead);
#endif
}

// test template
void PersistentCacheDBTest::RunTest(
    const std::function<std::shared_ptr<PersistentCacheTier>(bool)>& new_pcache,
    const size_t max_keys = 100 * 1024, const size_t max_usecase = 5) {
  if (!Snappy_Supported()) {
    return;
  }

  // number of insertion interations
  int num_iter = static_cast<int>(max_keys * kStressFactor);

  for (size_t iter = 0; iter < max_usecase; iter++) {
    Options options;
    options.write_buffer_size =
      static_cast<size_t>(64 * 1024 * kStressFactor);  // small write buffer
    options.statistics = rocksdb::CreateDBStatistics();
    options = CurrentOptions(options);

    // setup page cache
    std::shared_ptr<PersistentCacheTier> pcache;
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;

    const size_t size_max = std::numeric_limits<size_t>::max();

    switch (iter) {
      case 0:
        // page cache, block cache, no-compressed cache
        pcache = new_pcache(/*is_compressed=*/true);
        table_options.persistent_cache = pcache;
        table_options.block_cache = NewLRUCache(size_max);
        table_options.block_cache_compressed = nullptr;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 1:
        // page cache, block cache, compressed cache
        pcache = new_pcache(/*is_compressed=*/true);
        table_options.persistent_cache = pcache;
        table_options.block_cache = NewLRUCache(size_max);
        table_options.block_cache_compressed = NewLRUCache(size_max);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 2:
        // page cache, block cache, compressed cache + KNoCompression
        // both block cache and compressed cache, but DB is not compressed
        // also, make block cache sizes bigger, to trigger block cache hits
        pcache = new_pcache(/*is_compressed=*/true);
        table_options.persistent_cache = pcache;
        table_options.block_cache = NewLRUCache(size_max);
        table_options.block_cache_compressed = NewLRUCache(size_max);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        options.compression = kNoCompression;
        break;
      case 3:
        // page cache, no block cache, no compressed cache
        pcache = new_pcache(/*is_compressed=*/false);
        table_options.persistent_cache = pcache;
        table_options.block_cache = nullptr;
        table_options.block_cache_compressed = nullptr;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 4:
        // page cache, no block cache, no compressed cache
        // Page cache caches compressed blocks
        pcache = new_pcache(/*is_compressed=*/true);
        table_options.persistent_cache = pcache;
        table_options.block_cache = nullptr;
        table_options.block_cache_compressed = nullptr;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      default:
        FAIL();
    }

    std::vector<std::string> values;
    // insert data
    Insert(options, table_options, num_iter, &values);
    // flush all data in cache to device
    pcache->TEST_Flush();
    // verify data
    Verify(num_iter, values);

    auto block_miss = TestGetTickerCount(options, BLOCK_CACHE_MISS);
    auto compressed_block_hit =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT);
    auto compressed_block_miss =
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS);
    auto page_hit = TestGetTickerCount(options, PERSISTENT_CACHE_HIT);
    auto page_miss = TestGetTickerCount(options, PERSISTENT_CACHE_MISS);

    // check that we triggered the appropriate code paths in the cache
    switch (iter) {
      case 0:
        // page cache, block cache, no-compressed cache
        ASSERT_GT(page_miss, 0);
        ASSERT_GT(page_hit, 0);
        ASSERT_GT(block_miss, 0);
        ASSERT_EQ(compressed_block_miss, 0);
        ASSERT_EQ(compressed_block_hit, 0);
        break;
      case 1:
        // page cache, block cache, compressed cache
        ASSERT_GT(page_miss, 0);
        ASSERT_GT(block_miss, 0);
        ASSERT_GT(compressed_block_miss, 0);
        break;
      case 2:
        // page cache, block cache, compressed cache + KNoCompression
        ASSERT_GT(page_miss, 0);
        ASSERT_GT(page_hit, 0);
        ASSERT_GT(block_miss, 0);
        ASSERT_GT(compressed_block_miss, 0);
        // remember kNoCompression
        ASSERT_EQ(compressed_block_hit, 0);
        break;
      case 3:
      case 4:
        // page cache, no block cache, no compressed cache
        ASSERT_GT(page_miss, 0);
        ASSERT_GT(page_hit, 0);
        ASSERT_EQ(compressed_block_hit, 0);
        ASSERT_EQ(compressed_block_miss, 0);
        break;
      default:
        FAIL();
    }

    options.create_if_missing = true;
    DestroyAndReopen(options);

    pcache->Close();
  }
}

#if defined(TRAVIS) || defined(ROCKSDB_VALGRIND_RUN)
// Travis is unable to handle the normal version of the tests running out of
// fds, out of space and timeouts. This is an easier version of the test
// specifically written for Travis
TEST_F(PersistentCacheDBTest, BasicTest) {
  RunTest(std::bind(&MakeBlockCache, dbname_), /*max_keys=*/1024,
          /*max_usecase=*/1);
}
#else
// test table with block page cache
TEST_F(PersistentCacheDBTest, BlockCacheTest) {
  RunTest(std::bind(&MakeBlockCache, dbname_));
}

// test table with volatile page cache
TEST_F(PersistentCacheDBTest, VolatileCacheTest) {
  RunTest(std::bind(&MakeVolatileCache, dbname_));
}

// test table with tiered page cache
TEST_F(PersistentCacheDBTest, TieredCacheTest) {
  RunTest(std::bind(&MakeTieredCache, dbname_));
}
#endif

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#else
int main() { return 0; }
#endif
