//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "port/stack_trace.h"
#include "util/testharness.h"
#include "util/testutil.h"

#include "rocksdb/statistics.h"

namespace rocksdb {

class StatisticsTest : public testing::Test {};

// Sanity check to make sure that contents and order of TickersNameMap
// match Tickers enum
TEST_F(StatisticsTest, SanityTickers) {
  EXPECT_EQ(static_cast<size_t>(Tickers::TICKER_ENUM_MAX),
            TickersNameMap.size());

  for (uint32_t t = 0; t < Tickers::TICKER_ENUM_MAX; t++) {
    auto pair = TickersNameMap[static_cast<size_t>(t)];
    ASSERT_EQ(pair.first, t) << "Miss match at " << pair.second;
  }
}

// Sanity check to make sure that contents and order of HistogramsNameMap
// match Tickers enum
TEST_F(StatisticsTest, SanityHistograms) {
  EXPECT_EQ(static_cast<size_t>(Histograms::HISTOGRAM_ENUM_MAX),
            HistogramsNameMap.size());

  for (uint32_t h = 0; h < Histograms::HISTOGRAM_ENUM_MAX; h++) {
    auto pair = HistogramsNameMap[static_cast<size_t>(h)];
    ASSERT_EQ(pair.first, h) << "Miss match at " << pair.second;
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
