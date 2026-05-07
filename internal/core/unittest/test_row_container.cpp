// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/Types.h"
#include "common/Vector.h"
#include "exec/operator/query-agg/RowContainer.h"

using namespace milvus;
using namespace milvus::exec;

class RowContainerTest : public ::testing::Test {
 protected:
    ColumnVectorPtr
    CreateInt64Column(const std::vector<int64_t>& data) {
        auto col = std::make_shared<ColumnVector>(DataType::INT64, data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            col->SetValueAt<int64_t>(i, data[i]);
        }
        return col;
    }

    ColumnVectorPtr
    CreateStringColumn(const std::vector<std::string>& data) {
        auto col =
            std::make_shared<ColumnVector>(DataType::VARCHAR, data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            col->SetValueAt<std::string>(i, data[i]);
        }
        return col;
    }

    ColumnVectorPtr
    ExtractColumn(RowContainer& rows, int32_t column_idx) {
        const auto& all_rows = rows.allRows();
        return rows.extractColumnVector(
            all_rows.data(), static_cast<int32_t>(all_rows.size()), column_idx);
    }
};

TEST_F(RowContainerTest, ExtractColumnVectorReturnsFilledInt64Values) {
    RowContainer rows({DataType::INT64}, {});
    auto input = CreateInt64Column({10, 20, 30});

    for (size_t i = 0; i < input->size(); ++i) {
        auto row = rows.newRow();
        rows.store(input, i, row, 0);
    }

    auto output = ExtractColumn(rows, 0);
    ASSERT_EQ(output->size(), 3);
    EXPECT_TRUE(output->ValidAt(0));
    EXPECT_TRUE(output->ValidAt(1));
    EXPECT_TRUE(output->ValidAt(2));
    EXPECT_EQ(output->ValueAt<int64_t>(0), 10);
    EXPECT_EQ(output->ValueAt<int64_t>(1), 20);
    EXPECT_EQ(output->ValueAt<int64_t>(2), 30);
}

TEST_F(RowContainerTest, ExtractColumnVectorPreservesNullValidity) {
    RowContainer rows({DataType::INT64}, {});
    auto input = CreateInt64Column({10, 20, 30, 40});
    input->nullAt(1);
    input->nullAt(3);

    for (size_t i = 0; i < input->size(); ++i) {
        auto row = rows.newRow();
        rows.store(input, i, row, 0);
    }

    auto output = ExtractColumn(rows, 0);
    ASSERT_EQ(output->size(), 4);
    EXPECT_TRUE(output->ValidAt(0));
    EXPECT_FALSE(output->ValidAt(1));
    EXPECT_TRUE(output->ValidAt(2));
    EXPECT_FALSE(output->ValidAt(3));
    EXPECT_EQ(output->ValueAt<int64_t>(0), 10);
    EXPECT_EQ(output->ValueAt<int64_t>(2), 30);
}

TEST_F(RowContainerTest, ExtractColumnVectorCopiesStringValues) {
    RowContainer rows({DataType::VARCHAR}, {});
    auto input = CreateStringColumn({"alpha", "beta", "gamma"});

    for (size_t i = 0; i < input->size(); ++i) {
        auto row = rows.newRow();
        rows.store(input, i, row, 0);
    }

    auto output = ExtractColumn(rows, 0);
    ASSERT_EQ(output->size(), 3);
    EXPECT_TRUE(output->ValidAt(0));
    EXPECT_TRUE(output->ValidAt(1));
    EXPECT_TRUE(output->ValidAt(2));
    EXPECT_EQ(output->ValueAt<std::string>(0), "alpha");
    EXPECT_EQ(output->ValueAt<std::string>(1), "beta");
    EXPECT_EQ(output->ValueAt<std::string>(2), "gamma");
}
