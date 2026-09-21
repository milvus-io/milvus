// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arrow/io/caching.h>
#include <gtest/gtest.h>
#include <parquet/properties.h>

#include <cstdint>
#include <initializer_list>

#include "storage/KeyRetriever.h"

namespace milvus::storage {
namespace {

constexpr int64_t kHoleSize = 1024 * 1024;
constexpr int64_t kRangeSize = 64 * 1024 * 1024;

// Pins the configured properties to known values and restores arrow's defaults
// afterwards, because the configuration is process-wide.
class ArrowReaderPropertiesTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        ConfigureArrowReaderProperties(kHoleSize, kRangeSize);
    }

    void
    TearDown() override {
        ConfigureArrowReaderProperties(0, 0);
    }
};

TEST_F(ArrowReaderPropertiesTest, NonPositiveEagerRangeKeepsConfigured) {
    auto configured = GetArrowReaderProperties().cache_options();
    for (int64_t eager_range_size : {int64_t{0}, int64_t{-1}}) {
        auto options =
            GetArrowReaderProperties(eager_range_size).cache_options();
        EXPECT_EQ(options.lazy, configured.lazy);
        EXPECT_EQ(options.hole_size_limit, kHoleSize);
        EXPECT_EQ(options.range_size_limit, kRangeSize);
    }
}

TEST_F(ArrowReaderPropertiesTest, EagerRangeReadsAllRangesAtOnce) {
    constexpr int64_t kEagerRangeSize = 8 * 1024 * 1024;
    auto properties = GetArrowReaderProperties(kEagerRangeSize);
    auto options = properties.cache_options();

    EXPECT_TRUE(properties.pre_buffer());
    EXPECT_FALSE(options.lazy);
    EXPECT_EQ(options.prefetch_limit, 0);
    EXPECT_EQ(options.range_size_limit, kEagerRangeSize);
    // The configured hole size already fits below the range size.
    EXPECT_EQ(options.hole_size_limit, kHoleSize);
}

TEST_F(ArrowReaderPropertiesTest, EagerRangeBelowHoleSizeShrinksHoleSize) {
    // arrow requires range_size_limit > hole_size_limit when it coalesces.
    constexpr int64_t kEagerRangeSize = 4096;
    auto options = GetArrowReaderProperties(kEagerRangeSize).cache_options();

    EXPECT_EQ(options.range_size_limit, kEagerRangeSize);
    EXPECT_LT(options.hole_size_limit, options.range_size_limit);
    EXPECT_GE(options.hole_size_limit, 0);
}

TEST_F(ArrowReaderPropertiesTest, EagerRangeDoesNotChangeConfigured) {
    auto before = GetArrowReaderProperties().cache_options();
    (void)GetArrowReaderProperties(8 * 1024 * 1024);
    auto after = GetArrowReaderProperties().cache_options();

    EXPECT_EQ(after.lazy, before.lazy);
    EXPECT_EQ(after.hole_size_limit, before.hole_size_limit);
    EXPECT_EQ(after.range_size_limit, before.range_size_limit);
}

}  // namespace
}  // namespace milvus::storage
