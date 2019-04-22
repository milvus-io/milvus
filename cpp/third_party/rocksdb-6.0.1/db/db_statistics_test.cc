//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>

#include "db/db_test_util.h"
#include "monitoring/thread_status_util.h"
#include "port/stack_trace.h"
#include "rocksdb/statistics.h"

namespace rocksdb {

class DBStatisticsTest : public DBTestBase {
 public:
  DBStatisticsTest() : DBTestBase("/db_statistics_test") {}
};

TEST_F(DBStatisticsTest, CompressionStatsTest) {
  CompressionType type;

  if (Snappy_Supported()) {
    type = kSnappyCompression;
    fprintf(stderr, "using snappy\n");
  } else if (Zlib_Supported()) {
    type = kZlibCompression;
    fprintf(stderr, "using zlib\n");
  } else if (BZip2_Supported()) {
    type = kBZip2Compression;
    fprintf(stderr, "using bzip2\n");
  } else if (LZ4_Supported()) {
    type = kLZ4Compression;
    fprintf(stderr, "using lz4\n");
  } else if (XPRESS_Supported()) {
    type = kXpressCompression;
    fprintf(stderr, "using xpress\n");
  } else if (ZSTD_Supported()) {
    type = kZSTD;
    fprintf(stderr, "using ZSTD\n");
  } else {
    fprintf(stderr, "skipping test, compression disabled\n");
    return;
  }

  Options options = CurrentOptions();
  options.compression = type;
  options.statistics = rocksdb::CreateDBStatistics();
  options.statistics->stats_level_ = StatsLevel::kExceptTimeForMutex;
  DestroyAndReopen(options);

  int kNumKeysWritten = 100000;

  // Check that compressions occur and are counted when compression is turned on
  Random rnd(301);
  for (int i = 0; i < kNumKeysWritten; ++i) {
    // compressible string
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 128) + std::string(128, 'a')));
  }
  ASSERT_OK(Flush());
  ASSERT_GT(options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED), 0);

  for (int i = 0; i < kNumKeysWritten; ++i) {
    auto r = Get(Key(i));
  }
  ASSERT_GT(options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED), 0);

  options.compression = kNoCompression;
  DestroyAndReopen(options);
  uint64_t currentCompressions =
            options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
  uint64_t currentDecompressions =
            options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED);

  // Check that compressions do not occur when turned off
  for (int i = 0; i < kNumKeysWritten; ++i) {
    // compressible string
    ASSERT_OK(Put(Key(i), RandomString(&rnd, 128) + std::string(128, 'a')));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED)
            - currentCompressions, 0);

  for (int i = 0; i < kNumKeysWritten; ++i) {
    auto r = Get(Key(i));
  }
  ASSERT_EQ(options.statistics->getTickerCount(NUMBER_BLOCK_DECOMPRESSED)
            - currentDecompressions, 0);
}

TEST_F(DBStatisticsTest, MutexWaitStatsDisabledByDefault) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_EQ(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), 0);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBStatisticsTest, MutexWaitStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  options.statistics->stats_level_ = StatsLevel::kAll;
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_GE(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), kMutexWaitDelay);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBStatisticsTest, ResetStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = rocksdb::CreateDBStatistics();
  DestroyAndReopen(options);
  for (int i = 0; i < 2; ++i) {
    // pick arbitrary ticker and histogram. On first iteration they're zero
    // because db is unused. On second iteration they're zero due to Reset().
    ASSERT_EQ(0, TestGetTickerCount(options, NUMBER_KEYS_WRITTEN));
    HistogramData histogram_data;
    options.statistics->histogramData(DB_WRITE, &histogram_data);
    ASSERT_EQ(0.0, histogram_data.max);

    if (i == 0) {
      // The Put() makes some of the ticker/histogram stats nonzero until we
      // Reset().
      ASSERT_OK(Put("hello", "rocksdb"));
      ASSERT_EQ(1, TestGetTickerCount(options, NUMBER_KEYS_WRITTEN));
      options.statistics->histogramData(DB_WRITE, &histogram_data);
      ASSERT_GT(histogram_data.max, 0.0);
      options.statistics->Reset();
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
