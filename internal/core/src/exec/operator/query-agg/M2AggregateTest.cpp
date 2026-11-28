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

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "common/Vector.h"
#include "exec/operator/query-agg/M2Aggregate.h"

namespace milvus::exec {
namespace {

template <typename T>
using Values = std::vector<std::optional<T>>;

// Feeds values to the accumulator in batches of batch rows, through
// addRawInput with 1 state row per key when keys are given, else through
// addSingleGroupRawInput, and returns the emitted mean or M2 per key, nullopt
// for a NULL result. A global aggregation reports under key 0.
template <typename T>
std::map<int64_t, std::optional<double>>
RunM2(DataType type,
      const Values<T>& values,
      const std::vector<int64_t>& keys,
      size_t batch,
      bool emitMean = false) {
    M2Aggregate<T> m2(emitMean);
    // Null flag in byte 0, the 32-byte accumulator from byte 8.
    m2.setOffsets(8, 0, 1, 0);
    std::map<int64_t, std::array<int64_t, 5>> rows;
    for (auto key : keys.empty() ? std::vector<int64_t>{0} : keys) {
        rows[key] = {};
    }
    std::vector<char*> groups;
    std::vector<vector_size_t> indices;
    for (auto& [key, row] : rows) {
        indices.push_back(groups.size());
        groups.push_back(reinterpret_cast<char*>(row.data()));
    }
    m2.initializeNewGroups(groups.data(), indices);

    for (size_t begin = 0; begin < values.size(); begin += batch) {
        size_t n = std::min(batch, values.size() - begin);
        auto column = std::make_shared<ColumnVector>(type, n);
        std::vector<char*> row_groups;
        for (size_t i = 0; i < n; i++) {
            if (values[begin + i].has_value()) {
                column->SetValueAt<T>(i, *values[begin + i]);
            } else {
                column->nullAt(i);
            }
            if (!keys.empty()) {
                row_groups.push_back(
                    reinterpret_cast<char*>(rows[keys[begin + i]].data()));
            }
        }
        if (keys.empty()) {
            m2.addSingleGroupRawInput(groups[0], n, {column});
        } else {
            m2.addRawInput(row_groups.data(), n, {column});
        }
    }

    VectorPtr result =
        std::make_shared<ColumnVector>(DataType::DOUBLE, groups.size());
    m2.extractValues(groups.data(), groups.size(), &result);
    auto m2s = std::dynamic_pointer_cast<ColumnVector>(result);
    std::map<int64_t, std::optional<double>> ret;
    size_t i = 0;
    for (auto& [key, row] : rows) {
        ret[key] = m2s->ValidAt(i)
                       ? std::optional<double>(m2s->ValueAt<double>(i))
                       : std::nullopt;
        i++;
    }
    return ret;
}

// Returns the 2-pass M2 of the non-NULL values converted to double, or
// nullopt when there are none.
template <typename T>
std::optional<double>
TwoPassM2(const Values<T>& values) {
    double sum = 0;
    int64_t n = 0;
    for (auto& v : values) {
        if (v.has_value()) {
            sum += static_cast<double>(*v);
            n++;
        }
    }
    if (n == 0) {
        return std::nullopt;
    }
    double m2 = 0;
    for (auto& v : values) {
        if (v.has_value()) {
            m2 += std::pow(static_cast<double>(*v) - sum / n, 2);
        }
    }
    return m2;
}

// Checks the global M2 and mean against a 2-pass reference for batches of 1,
// 2 and all rows.
template <typename T>
void
ExpectGlobalM2(DataType type, const Values<T>& values) {
    auto want = TwoPassM2(values);
    double sum = 0;
    int64_t n = 0;
    for (auto& v : values) {
        if (v.has_value()) {
            sum += static_cast<double>(*v);
            n++;
        }
    }
    for (size_t batch : {size_t(1), size_t(2), values.size() + 1}) {
        auto got = RunM2(type, values, {}, batch)[0];
        auto mean = RunM2(type, values, {}, batch, true)[0];
        ASSERT_EQ(want.has_value(), got.has_value()) << "batch " << batch;
        ASSERT_EQ(want.has_value(), mean.has_value()) << "batch " << batch;
        if (want.has_value()) {
            EXPECT_NEAR(*want, *got, 1e-9 * std::max(1.0, *want))
                << "batch " << batch;
            EXPECT_NEAR(sum / n, *mean, 1e-9 * std::max(1.0, std::abs(*mean)))
                << "batch " << batch;
        }
    }
}

TEST(M2AggregateTest, GlobalMatchesTwoPass) {
    using std::nullopt;
    ExpectGlobalM2<double>(DataType::DOUBLE, {1, 2, 3, nullopt});
    ExpectGlobalM2<double>(DataType::DOUBLE, {});
    ExpectGlobalM2<double>(DataType::DOUBLE, {nullopt, nullopt});
    ExpectGlobalM2<double>(DataType::DOUBLE, {5});
    ExpectGlobalM2<double>(DataType::DOUBLE, {7, 7, nullopt, 7});
    ExpectGlobalM2<double>(DataType::DOUBLE, {-3, -1, 0, 2, 5.5});
    ExpectGlobalM2<double>(DataType::DOUBLE, {1e9 + 1, 1e9 + 2, 1e9 + 3});
    ExpectGlobalM2<float>(DataType::FLOAT, {0.5f, nullopt, 1.5f, 2.5f});
    constexpr int64_t k2p53 = int64_t(1) << 53;
    ExpectGlobalM2<int64_t>(DataType::INT64,
                            {k2p53, k2p53 + 8, nullopt, k2p53 + 16});
    ExpectGlobalM2<int64_t>(
        DataType::INT64,
        {std::numeric_limits<int64_t>::min(), 0, int64_t(1) << 62});
    // Int64 values convert to double, so 2^53 + 1 and 2^53 + 3 round to
    // 2^53 and 2^53 + 4: M2 is 8, not 2.
    EXPECT_DOUBLE_EQ(
        *RunM2<int64_t>(DataType::INT64, {k2p53 + 1, k2p53 + 3}, {}, 2)[0], 8);
}

TEST(M2AggregateTest, GroupedMatchesTwoPass) {
    using std::nullopt;
    Values<double> values{1, 10, 2, nullopt, 3, nullopt, 4, -5};
    std::vector<int64_t> keys{1, 2, 1, 2, 1, 3, 1, 2};
    for (size_t batch : {size_t(1), size_t(3), values.size()}) {
        auto got = RunM2(DataType::DOUBLE, values, keys, batch);
        EXPECT_DOUBLE_EQ(*got[1], *TwoPassM2<double>({1, 2, 3, 4}));
        EXPECT_DOUBLE_EQ(*got[2], *TwoPassM2<double>({10, -5}));
        EXPECT_FALSE(got[3].has_value());
        auto mean = RunM2(DataType::DOUBLE, values, keys, batch, true);
        EXPECT_DOUBLE_EQ(*mean[1], 2.5);
        EXPECT_DOUBLE_EQ(*mean[2], 2.5);
        EXPECT_FALSE(mean[3].has_value());
    }
}

// The review example: 2e7 values near 1e9 with standard deviation 1e-3. The
// reference takes deviations from 1e9, which are exact in double.
TEST(M2AggregateTest, LargeOffsetSmallVariance) {
    std::mt19937_64 gen(42);
    std::normal_distribution<double> z(0, 1);
    Values<double> values(20000000);
    for (auto& v : values) {
        v = 1e9 + 1e-3 * z(gen);
    }
    double dev = 0;
    for (auto& v : values) {
        dev += *v - 1e9;
    }
    dev /= values.size();
    double want = 0;
    for (auto& v : values) {
        want += (*v - 1e9 - dev) * (*v - 1e9 - dev);
    }
    std::vector<int64_t> keys(values.size(), 1);
    for (auto grouped : {false, true}) {
        auto k = grouped ? keys : std::vector<int64_t>{};
        auto got = *RunM2(DataType::DOUBLE, values, k, 4096)[grouped];
        auto mean = *RunM2(DataType::DOUBLE, values, k, 4096, true)[grouped];
        EXPECT_NEAR(got, want, 1e-6 * want) << "grouped " << grouped;
        EXPECT_NEAR(mean, 1e9 + dev, 1e-6) << "grouped " << grouped;
    }
}

// Nanosecond epoch timestamps: the Int64 sum of 10 values overflows, the
// variance is finite and exact in double.
TEST(M2AggregateTest, Int64EpochNanoseconds) {
    Values<int64_t> values;
    for (int64_t k = 0; k < 10; k++) {
        values.push_back(int64_t(1700000000) * 1000000000 + k * 1000000000);
    }
    for (size_t batch : {size_t(1), size_t(4), values.size()}) {
        EXPECT_DOUBLE_EQ(*RunM2(DataType::INT64, values, {}, batch)[0],
                         82.5e18);
        EXPECT_DOUBLE_EQ(
            *RunM2(
                DataType::INT64, values, std::vector<int64_t>(10, 1), batch)[1],
            82.5e18);
    }
}

// A first value whose square overflows gives M2 0, and an M2 that overflows
// gives +Inf, never NaN.
TEST(M2AggregateTest, OverflowGivesInfNotNaN) {
    for (auto grouped : {false, true}) {
        auto keys = [&](size_t n) {
            return grouped ? std::vector<int64_t>(n, 1)
                           : std::vector<int64_t>{};
        };
        for (size_t batch : {size_t(1), size_t(2), size_t(4)}) {
            EXPECT_EQ(*RunM2<double>(
                          DataType::DOUBLE, {2e154}, keys(1), batch)[grouped],
                      0);
            auto m2 = *RunM2<double>(
                DataType::DOUBLE, {2e154, -2e154, 1}, keys(3), batch)[grouped];
            EXPECT_TRUE(std::isinf(m2) && m2 > 0)
                << "grouped " << grouped << " batch " << batch << " m2 " << m2;
            // delta^2 overflows, but the M2 of 0 and 1.5e154 is finite.
            m2 = *RunM2<double>(
                DataType::DOUBLE, {0, 1.5e154}, keys(2), batch)[grouped];
            EXPECT_NEAR(m2, 1.125e308, 1e-12 * 1.125e308)
                << "grouped " << grouped << " batch " << batch;
            // x - shift overflows: in 1 batch the 2-pass M2 is NaN, row by
            // row the mean turns NaN. M2 must be +Inf either way.
            m2 = *RunM2<double>(
                DataType::DOUBLE, {1e308, -1e308}, keys(2), batch)[grouped];
            EXPECT_TRUE(std::isinf(m2) && m2 > 0)
                << "grouped " << grouped << " batch " << batch << " m2 " << m2;
            m2 = *RunM2<double>(DataType::DOUBLE,
                                {1e308, -1e308, 1, 2},
                                keys(4),
                                batch)[grouped];
            EXPECT_TRUE(std::isinf(m2) && m2 > 0)
                << "grouped " << grouped << " batch " << batch << " m2 " << m2;
        }
    }
}

}  // namespace
}  // namespace milvus::exec
