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
#include <string>
#include <vector>

#include "common/Vector.h"
#include "exec/operator/query-agg/M2Aggregate.h"

namespace milvus::exec {
namespace {

template <typename T>
using Values = std::vector<std::optional<T>>;

// Feeds values to m2 in batches of batch rows, through addRawInput with 1
// state row per key when keys are given, else through addSingleGroupRawInput,
// and returns the extracted M2 per key, nullopt for a NULL result. A global
// aggregation reports under key 0.
template <typename T>
std::map<int64_t, std::optional<double>>
RunM2(DataType type,
      const Values<T>& values,
      const std::vector<int64_t>& keys,
      size_t batch) {
    M2Aggregate<T> m2;
    // Null flag in byte 0, the 24-byte accumulator from byte 8.
    m2.setOffsets(8, 0, 1, 0);
    std::map<int64_t, std::array<int64_t, 4>> rows;
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

template <typename T>
void
ExpectGlobalM2(DataType type, const Values<T>& values) {
    auto want = TwoPassM2(values);
    for (size_t batch : {size_t(1), size_t(2), values.size() + 1}) {
        auto got = RunM2(type, values, {}, batch)[0];
        ASSERT_EQ(want.has_value(), got.has_value()) << "batch " << batch;
        if (want.has_value()) {
            EXPECT_NEAR(*want, *got, 1e-9 * std::max(1.0, *want))
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
    }
}

}  // namespace
}  // namespace milvus::exec
