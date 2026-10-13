// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include "exec/expression/JsonNumberComparison.h"

namespace milvus::exec {

TEST(JsonNumberComparisonTest, Int64ToDoublePreservesExactEquality) {
    const std::vector<std::pair<int64_t, bool>> cases = {
        {0, true},
        {1, true},
        {-1, true},
        {9007199254740991LL, true},
        {9007199254740992LL, true},
        {9007199254740993LL, false},
        {9007199254740994LL, true},
        {-9007199254740993LL, false},
        {std::numeric_limits<int64_t>::min(), true},
        {std::numeric_limits<int64_t>::min() + 1, false},
        {std::numeric_limits<int64_t>::max(), false},
    };
    for (const auto& [value, exact] : cases) {
        SCOPED_TRACE(value);
        auto converted = ConvertJsonNumberExact<double>(value);
        ASSERT_EQ(converted.has_value(), exact);
        if (converted) {
            // Check numeric equality as well as conversion eligibility.
            EXPECT_EQ(static_cast<long double>(*converted),
                      static_cast<long double>(value));
        }
    }
}

TEST(JsonNumberComparisonTest, DoubleToInt64RejectsFractionalAndOutOfRange) {
    for (double value : {0.0, 1.0, -1.0, 0x1p53, -0x1p63}) {
        auto converted = ConvertJsonNumberExact<int64_t>(value);
        ASSERT_TRUE(converted.has_value());
        EXPECT_EQ(static_cast<double>(*converted), value);
    }
    for (double value :
         {1.5,
          -1.5,
          std::nextafter(1.0, 2.0),
          std::nextafter(-1.0, -2.0),
          std::numeric_limits<double>::denorm_min(),
          0x1p63,
          std::nextafter(-0x1p63, -std::numeric_limits<double>::infinity()),
          std::numeric_limits<double>::infinity(),
          -std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN()}) {
        SCOPED_TRACE(value);
        EXPECT_FALSE(ConvertJsonNumberExact<int64_t>(value).has_value());
    }
}

TEST(JsonNumberComparisonTest, ParsedJsonNumbersPreserveExactTermValues) {
    struct TestCase {
        const char* json;
        std::optional<int64_t> integer;
        std::optional<double> floating;
    };
    const std::vector<TestCase> cases = {
        {"9007199254740992", 9007199254740992LL, 0x1p53},
        {"9007199254740993", 9007199254740993LL, std::nullopt},
        {"-9007199254740993", -9007199254740993LL, std::nullopt},
        {"9007199254740992.0", 9007199254740992LL, 0x1p53},
        {"9223372036854775807",
         std::numeric_limits<int64_t>::max(),
         std::nullopt},
        {"-9223372036854775808", std::numeric_limits<int64_t>::min(), -0x1p63},
        {"-9223372036854775807",
         std::numeric_limits<int64_t>::min() + 1,
         std::nullopt},
        {"9223372036854775808.0", std::nullopt, 0x1p63},
        {"-9223372036854775808.0",
         std::numeric_limits<int64_t>::min(),
         -0x1p63},
        {"-9223372036854777856.0", std::nullopt, -0x1p63 - 2048.0},
        {"1.5", std::nullopt, 1.5},
        {"1.0", 1, 1.0},
        {"0.0", 0, 0.0},
        {"-0.0", 0, -0.0},
        // Keep the uint64-as-double behavior shared by JSON stats/indexes.
        {"9223372036854775809", std::nullopt, 0x1p63},
        {"18446744073709551615", std::nullopt, 0x1p64},
    };
    simdjson::ondemand::parser parser;
    for (const auto& test : cases) {
        SCOPED_TRACE(test.json);
        simdjson::padded_string json{std::string_view(test.json)};
        auto document = parser.iterate(json);
        simdjson::ondemand::number number;
        ASSERT_EQ(document.get_number().get(number), simdjson::SUCCESS);
        EXPECT_EQ(ConvertJsonNumberExact<int64_t>(number), test.integer);
        EXPECT_EQ(ConvertJsonNumberExact<double>(number), test.floating);
    }
}

TEST(JsonNumberComparisonTest, NumericTermsRequiringExactIndexFallback) {
    for (int64_t value :
         std::vector<int64_t>{9007199254740992LL,
                              9007199254740993LL,
                              -9007199254740992LL,
                              std::numeric_limits<int64_t>::min(),
                              std::numeric_limits<int64_t>::max()}) {
        proto::plan::GenericValue term;
        term.set_int64_val(value);
        EXPECT_TRUE(JsonNumericBoundRequiresPreciseInt64Comparison(term));
    }
    for (double value : {0x1p53, -0x1p53, 0x1p63, -0x1p63}) {
        proto::plan::GenericValue term;
        term.set_float_val(value);
        EXPECT_TRUE(JsonNumericBoundRequiresPreciseInt64Comparison(term));
    }
    for (double value : {1.5, 0.0, 9007199254740991.0, 0x1p64}) {
        proto::plan::GenericValue term;
        term.set_float_val(value);
        EXPECT_FALSE(JsonNumericBoundRequiresPreciseInt64Comparison(term));
    }
}

}  // namespace milvus::exec
