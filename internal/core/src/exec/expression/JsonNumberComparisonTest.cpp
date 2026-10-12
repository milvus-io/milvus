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

}  // namespace milvus::exec
