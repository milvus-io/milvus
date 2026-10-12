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

TEST_F(ArrowReaderPropertiesTest, WithoutEagerPrebufferKeepsConfigured) {
    auto configured = GetArrowReaderProperties().cache_options();
    auto options = GetArrowReaderProperties(false).cache_options();

    EXPECT_EQ(options.lazy, configured.lazy);
    EXPECT_EQ(options.hole_size_limit, kHoleSize);
    EXPECT_EQ(options.range_size_limit, kRangeSize);
}

TEST_F(ArrowReaderPropertiesTest, EagerPrebufferReadsAllRangesAtOnce) {
    auto properties = GetArrowReaderProperties(true);
    auto options = properties.cache_options();

    EXPECT_TRUE(properties.pre_buffer());
    EXPECT_FALSE(options.lazy);
    EXPECT_EQ(options.prefetch_limit, 0);
}

TEST_F(ArrowReaderPropertiesTest, EagerPrebufferKeepsTheConfiguredCoalescing) {
    // Only the timing of the requests changes: an eager reader must coalesce
    // exactly like every other one, or it would issue a different number of
    // object-storage requests than the operator configured for.
    auto options = GetArrowReaderProperties(true).cache_options();

    EXPECT_EQ(options.hole_size_limit, kHoleSize);
    EXPECT_EQ(options.range_size_limit, kRangeSize);
}

TEST_F(ArrowReaderPropertiesTest, EagerPrebufferDoesNotChangeConfigured) {
    auto before = GetArrowReaderProperties().cache_options();
    (void)GetArrowReaderProperties(true);
    auto after = GetArrowReaderProperties().cache_options();

    EXPECT_EQ(after.lazy, before.lazy);
    EXPECT_EQ(after.hole_size_limit, before.hole_size_limit);
    EXPECT_EQ(after.range_size_limit, before.range_size_limit);
}

}  // namespace
}  // namespace milvus::storage
