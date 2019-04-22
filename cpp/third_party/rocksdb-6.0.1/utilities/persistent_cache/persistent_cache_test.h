//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#ifndef ROCKSDB_LITE

#include <functional>
#include <limits>
#include <list>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "rocksdb/cache.h"
#include "table/block_builder.h"
#include "port/port.h"
#include "util/arena.h"
#include "util/testharness.h"
#include "utilities/persistent_cache/volatile_tier_impl.h"

namespace rocksdb {

//
// Unit tests for testing PersistentCacheTier
//
class PersistentCacheTierTest : public testing::Test {
 public:
  PersistentCacheTierTest();
  virtual ~PersistentCacheTierTest() {
    if (cache_) {
      Status s = cache_->Close();
      assert(s.ok());
    }
  }

 protected:
  // Flush cache
  void Flush() {
    if (cache_) {
      cache_->TEST_Flush();
    }
  }

  // create threaded workload
  template <class T>
  std::list<port::Thread> SpawnThreads(const size_t n, const T& fn) {
    std::list<port::Thread> threads;
    for (size_t i = 0; i < n; i++) {
      port::Thread th(fn);
      threads.push_back(std::move(th));
    }
    return threads;
  }

  // Wait for threads to join
  void Join(std::list<port::Thread>&& threads) {
    for (auto& th : threads) {
      th.join();
    }
    threads.clear();
  }

  // Run insert workload in threads
  void Insert(const size_t nthreads, const size_t max_keys) {
    key_ = 0;
    max_keys_ = max_keys;
    // spawn threads
    auto fn = std::bind(&PersistentCacheTierTest::InsertImpl, this);
    auto threads = SpawnThreads(nthreads, fn);
    // join with threads
    Join(std::move(threads));
    // Flush cache
    Flush();
  }

  // Run verification on the cache
  void Verify(const size_t nthreads = 1, const bool eviction_enabled = false) {
    stats_verify_hits_ = 0;
    stats_verify_missed_ = 0;
    key_ = 0;
    // spawn threads
    auto fn =
        std::bind(&PersistentCacheTierTest::VerifyImpl, this, eviction_enabled);
    auto threads = SpawnThreads(nthreads, fn);
    // join with threads
    Join(std::move(threads));
  }

  // pad 0 to numbers
  std::string PaddedNumber(const size_t data, const size_t pad_size) {
    assert(pad_size);
    char* ret = new char[pad_size];
    int pos = static_cast<int>(pad_size) - 1;
    size_t count = 0;
    size_t t = data;
    // copy numbers
    while (t) {
      count++;
      ret[pos--] = '0' + t % 10;
      t = t / 10;
    }
    // copy 0s
    while (pos >= 0) {
      ret[pos--] = '0';
    }
    // post condition
    assert(count <= pad_size);
    assert(pos == -1);
    std::string result(ret, pad_size);
    delete[] ret;
    return result;
  }

  // Insert workload implementation
  void InsertImpl() {
    const std::string prefix = "key_prefix_";

    while (true) {
      size_t i = key_++;
      if (i >= max_keys_) {
        break;
      }

      char data[4 * 1024];
      memset(data, '0' + (i % 10), sizeof(data));
      auto k = prefix + PaddedNumber(i, /*count=*/8);
      Slice key(k);
      while (true) {
        Status status = cache_->Insert(key, data, sizeof(data));
        if (status.ok()) {
          break;
        }
        ASSERT_TRUE(status.IsTryAgain());
        Env::Default()->SleepForMicroseconds(1 * 1000 * 1000);
      }
    }
  }

  // Verification implementation
  void VerifyImpl(const bool eviction_enabled = false) {
    const std::string prefix = "key_prefix_";
    while (true) {
      size_t i = key_++;
      if (i >= max_keys_) {
        break;
      }

      char edata[4 * 1024];
      memset(edata, '0' + (i % 10), sizeof(edata));
      auto k = prefix + PaddedNumber(i, /*count=*/8);
      Slice key(k);
      std::unique_ptr<char[]> block;
      size_t block_size;

      if (eviction_enabled) {
        if (!cache_->Lookup(key, &block, &block_size).ok()) {
          // assume that the key is evicted
          stats_verify_missed_++;
          continue;
        }
      }

      ASSERT_OK(cache_->Lookup(key, &block, &block_size));
      ASSERT_EQ(block_size, sizeof(edata));
      ASSERT_EQ(memcmp(edata, block.get(), sizeof(edata)), 0);
      stats_verify_hits_++;
    }
  }

  // template for insert test
  void RunInsertTest(const size_t nthreads, const size_t max_keys) {
    Insert(nthreads, max_keys);
    Verify(nthreads);
    ASSERT_EQ(stats_verify_hits_, max_keys);
    ASSERT_EQ(stats_verify_missed_, 0);

    cache_->Close();
    cache_.reset();
  }

  // template for negative insert test
  void RunNegativeInsertTest(const size_t nthreads, const size_t max_keys) {
    Insert(nthreads, max_keys);
    Verify(nthreads, /*eviction_enabled=*/true);
    ASSERT_LT(stats_verify_hits_, max_keys);
    ASSERT_GT(stats_verify_missed_, 0);

    cache_->Close();
    cache_.reset();
  }

  // template for insert with eviction test
  void RunInsertTestWithEviction(const size_t nthreads, const size_t max_keys) {
    Insert(nthreads, max_keys);
    Verify(nthreads, /*eviction_enabled=*/true);
    ASSERT_EQ(stats_verify_hits_ + stats_verify_missed_, max_keys);
    ASSERT_GT(stats_verify_hits_, 0);
    ASSERT_GT(stats_verify_missed_, 0);

    cache_->Close();
    cache_.reset();
  }

  const std::string path_;
  std::shared_ptr<Logger> log_;
  std::shared_ptr<PersistentCacheTier> cache_;
  std::atomic<size_t> key_{0};
  size_t max_keys_ = 0;
  std::atomic<size_t> stats_verify_hits_{0};
  std::atomic<size_t> stats_verify_missed_{0};
};

//
// RocksDB tests
//
class PersistentCacheDBTest : public DBTestBase {
 public:
  PersistentCacheDBTest();

  static uint64_t TestGetTickerCount(const Options& options,
                                     Tickers ticker_type) {
    return static_cast<uint32_t>(
        options.statistics->getTickerCount(ticker_type));
  }

  // insert data to table
  void Insert(const Options& options,
              const BlockBasedTableOptions& /*table_options*/,
              const int num_iter, std::vector<std::string>* values) {
    CreateAndReopenWithCF({"pikachu"}, options);
    // default column family doesn't have block cache
    Options no_block_cache_opts;
    no_block_cache_opts.statistics = options.statistics;
    no_block_cache_opts = CurrentOptions(no_block_cache_opts);
    BlockBasedTableOptions table_options_no_bc;
    table_options_no_bc.no_block_cache = true;
    no_block_cache_opts.table_factory.reset(
        NewBlockBasedTableFactory(table_options_no_bc));
    ReopenWithColumnFamilies(
        {"default", "pikachu"},
        std::vector<Options>({no_block_cache_opts, options}));

    Random rnd(301);

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    std::string str;
    for (int i = 0; i < num_iter; i++) {
      if (i % 4 == 0) {  // high compression ratio
        str = RandomString(&rnd, 1000);
      }
      values->push_back(str);
      ASSERT_OK(Put(1, Key(i), (*values)[i]));
    }

    // flush all data from memtable so that reads are from block cache
    ASSERT_OK(Flush(1));
  }

  // verify data
  void Verify(const int num_iter, const std::vector<std::string>& values) {
    for (int j = 0; j < 2; ++j) {
      for (int i = 0; i < num_iter; i++) {
        ASSERT_EQ(Get(1, Key(i)), values[i]);
      }
    }
  }

  // test template
  void RunTest(const std::function<std::shared_ptr<PersistentCacheTier>(bool)>&
                   new_pcache,
               const size_t max_keys, const size_t max_usecase);
};

}  // namespace rocksdb

#endif
