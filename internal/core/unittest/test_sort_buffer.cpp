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

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "common/Types.h"
#include "common/Vector.h"
#include "exec/SortBuffer.h"

using namespace milvus;
using namespace milvus::exec;

class SortBufferTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
    }

    void
    TearDown() override {
    }

    // Helper to create ColumnVector with int64 data
    ColumnVectorPtr
    CreateInt64Column(const std::vector<int64_t>& data) {
        auto col = std::make_shared<ColumnVector>(DataType::INT64, data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            col->SetValueAt<int64_t>(i, data[i]);
        }
        return col;
    }

    // Helper to create ColumnVector with double data
    ColumnVectorPtr
    CreateDoubleColumn(const std::vector<double>& data) {
        auto col =
            std::make_shared<ColumnVector>(DataType::DOUBLE, data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            col->SetValueAt<double>(i, data[i]);
        }
        return col;
    }

    // Helper to create ColumnVector with string data
    ColumnVectorPtr
    CreateStringColumn(const std::vector<std::string>& data) {
        auto col =
            std::make_shared<ColumnVector>(DataType::VARCHAR, data.size());
        for (size_t i = 0; i < data.size(); ++i) {
            col->SetValueAt<std::string>(i, data[i]);
        }
        return col;
    }

    // Helper to extract int64 values from output
    std::vector<int64_t>
    ExtractInt64Values(const std::vector<VectorPtr>& output, int col_idx) {
        std::vector<int64_t> result;
        if (output.empty() || col_idx >= output.size()) {
            return result;
        }
        auto col = std::dynamic_pointer_cast<ColumnVector>(output[col_idx]);
        if (!col) {
            return result;
        }
        for (size_t i = 0; i < col->size(); ++i) {
            result.push_back(col->ValueAt<int64_t>(i));
        }
        return result;
    }
};

TEST_F(SortBufferTest, BasicSortAscending) {
    // Test basic ascending sort with single int64 column
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0)  // column 0, ASC (default)
    };

    SortBuffer buffer(column_types, sort_keys);

    // Add unsorted data
    std::vector<int64_t> data = {5, 2, 8, 1, 9, 3};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }

    buffer.NoMoreInput();

    ASSERT_TRUE(buffer.IsSorted());
    ASSERT_EQ(buffer.NumInputRows(), 6);

    // Get all output
    auto output = buffer.GetOutput(100);
    ASSERT_FALSE(output.empty());

    auto sorted = ExtractInt64Values(output, 0);
    ASSERT_EQ(sorted.size(), 6);

    // Verify sorted order
    std::vector<int64_t> expected = {1, 2, 3, 5, 8, 9};
    EXPECT_EQ(sorted, expected);
}

TEST_F(SortBufferTest, BasicSortDescending) {
    // Test descending sort
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, false, true)  // column 0, DESC (asc=false), NULLS FIRST
    };

    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> data = {5, 2, 8, 1, 9, 3};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }

    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto sorted = ExtractInt64Values(output, 0);

    std::vector<int64_t> expected = {9, 8, 5, 3, 2, 1};
    EXPECT_EQ(sorted, expected);
}

TEST_F(SortBufferTest, SortWithLimit) {
    // Test sort with limit (TopK scenario)
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0)  // column 0, ASC (default)
    };

    int64_t limit = 3;
    SortBuffer buffer(column_types, sort_keys, limit);

    std::vector<int64_t> data = {5, 2, 8, 1, 9, 3, 7, 4, 6};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }

    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto sorted = ExtractInt64Values(output, 0);

    // Should only return top 3 smallest
    ASSERT_EQ(sorted.size(), 3);
    std::vector<int64_t> expected = {1, 2, 3};
    EXPECT_EQ(sorted, expected);
}

// Note: Offset test removed - offset is not supported at segment level.
// In distributed queries, offset must be applied at proxy reduce level
// after k-way merge. Segments use (offset + limit) as the limit parameter.

TEST_F(SortBufferTest, MultiFieldSort) {
    // Test multi-field sort: first by col0 ASC, then by col1 DESC
    std::vector<DataType> column_types = {DataType::INT64, DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, true),  // column 0 ASC
        SortKeyInfo(1, false)  // column 1 DESC
    };

    SortBuffer buffer(column_types, sort_keys);

    // Data: (col1, col2)
    // (1, 10), (1, 20), (2, 15), (2, 5), (1, 15)
    std::vector<int64_t> col1_data = {1, 1, 2, 2, 1};
    std::vector<int64_t> col2_data = {10, 20, 15, 5, 15};

    auto col1 = CreateInt64Column(col1_data);
    auto col2 = CreateInt64Column(col2_data);
    std::vector<ColumnVectorPtr> columns = {col1, col2};

    for (size_t i = 0; i < col1_data.size(); ++i) {
        buffer.AddRow(columns, i);
    }

    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);

    auto sorted_col1 = ExtractInt64Values(output, 0);
    auto sorted_col2 = ExtractInt64Values(output, 1);

    // Expected order:
    // (1, 20), (1, 15), (1, 10), (2, 15), (2, 5)
    std::vector<int64_t> expected_col1 = {1, 1, 1, 2, 2};
    std::vector<int64_t> expected_col2 = {20, 15, 10, 15, 5};

    EXPECT_EQ(sorted_col1, expected_col1);
    EXPECT_EQ(sorted_col2, expected_col2);
}

TEST_F(SortBufferTest, BatchedOutput) {
    // Test getting output in batches
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0)  // column 0, ASC (default)
    };

    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> data = {5, 2, 8, 1, 9, 3, 7, 4, 6};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }

    buffer.NoMoreInput();

    // Get output in batches of 3
    std::vector<int64_t> all_sorted;

    while (buffer.HasOutput()) {
        auto output = buffer.GetOutput(3);
        auto batch = ExtractInt64Values(output, 0);
        all_sorted.insert(all_sorted.end(), batch.begin(), batch.end());
    }

    std::vector<int64_t> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(all_sorted, expected);
    EXPECT_EQ(buffer.NumOutputRows(), 9);
}

TEST_F(SortBufferTest, EmptyInput) {
    // Test with no input
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0)  // column 0, ASC (default)
    };

    SortBuffer buffer(column_types, sort_keys);
    buffer.NoMoreInput();

    ASSERT_TRUE(buffer.IsSorted());
    ASSERT_EQ(buffer.NumInputRows(), 0);
    ASSERT_FALSE(buffer.HasOutput());

    auto output = buffer.GetOutput(100);
    ASSERT_TRUE(output.empty());
}

TEST_F(SortBufferTest, SingleRow) {
    // Test with single row
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0)  // column 0, ASC (default)
    };

    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> data = {42};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};
    buffer.AddRow(columns, 0);

    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto sorted = ExtractInt64Values(output, 0);

    ASSERT_EQ(sorted.size(), 1);
    EXPECT_EQ(sorted[0], 42);
}

TEST_F(SortBufferTest, LargeDataSet) {
    // Test with larger dataset to exercise TopK optimization
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0)  // column 0, ASC (default)
    };

    int64_t limit = 10;
    SortBuffer buffer(column_types, sort_keys, limit);

    // Generate random data
    std::vector<int64_t> data(1000);
    std::iota(data.begin(), data.end(), 1);  // 1, 2, 3, ..., 1000
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(data.begin(), data.end(), g);

    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }

    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto sorted = ExtractInt64Values(output, 0);

    // Should return top 10 smallest: 1, 2, 3, ..., 10
    ASSERT_EQ(sorted.size(), 10);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(sorted[i], i + 1);
    }
}

TEST_F(SortBufferTest, DoubleSort) {
    // Test sorting double values
    std::vector<DataType> column_types = {DataType::DOUBLE};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0)  // column 0, ASC (default)
    };

    SortBuffer buffer(column_types, sort_keys);

    std::vector<double> data = {3.14, 1.41, 2.71, 0.57, 1.73};
    auto col = CreateDoubleColumn(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }

    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto col_out = std::dynamic_pointer_cast<ColumnVector>(output[0]);

    ASSERT_NE(col_out, nullptr);
    ASSERT_EQ(col_out->size(), 5);

    // Verify ascending order
    double prev = col_out->ValueAt<double>(0);
    for (size_t i = 1; i < col_out->size(); ++i) {
        double curr = col_out->ValueAt<double>(i);
        EXPECT_LE(prev, curr);
        prev = curr;
    }
}
