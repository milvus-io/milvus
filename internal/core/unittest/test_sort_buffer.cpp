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
#include <cmath>
#include <iomanip>
#include <limits>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
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

    ColumnVectorPtr
    CreateTimestamptzColumn(const std::vector<int64_t>& data) {
        auto col =
            std::make_shared<ColumnVector>(DataType::TIMESTAMPTZ, data.size());
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

TEST_F(SortBufferTest, SortTimestamptzAscending) {
    std::vector<DataType> column_types = {DataType::TIMESTAMPTZ};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};

    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> data = {3000000000000, 1000000000000, 2000000000000};
    auto col = CreateTimestamptzColumn(data);
    std::vector<ColumnVectorPtr> columns = {col};

    buffer.AddRows(columns, data.size());
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto sorted = ExtractInt64Values(output, 0);

    std::vector<int64_t> expected = {
        1000000000000, 2000000000000, 3000000000000};
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

TEST_F(SortBufferTest, ExtractedBatchesAppendWithoutOverwritingRows) {
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};
    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> data(1500);
    for (int64_t i = 0; i < data.size(); ++i) {
        data[i] = i;
    }
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};
    buffer.AddRows(columns, data.size());
    buffer.NoMoreInput();

    auto first_output = buffer.GetOutput(1024);
    auto second_output = buffer.GetOutput(476);
    auto first_col = std::dynamic_pointer_cast<ColumnVector>(first_output[0]);
    auto second_col = std::dynamic_pointer_cast<ColumnVector>(second_output[0]);
    ASSERT_EQ(first_col->size(), 1024);
    ASSERT_EQ(second_col->size(), 476);

    ASSERT_NO_THROW(first_col->append(*second_col));
    ASSERT_EQ(first_col->size(), 1500);
    EXPECT_EQ(first_col->ValueAt<int64_t>(0), 0);
    EXPECT_EQ(first_col->ValueAt<int64_t>(1023), 1023);
    EXPECT_EQ(first_col->ValueAt<int64_t>(1024), 1024);
    EXPECT_EQ(first_col->ValueAt<int64_t>(1499), 1499);
}

TEST_F(SortBufferTest, ExtractedNullableBatchesAppendWithoutLosingValidity) {
    std::vector<DataType> column_types = {DataType::INT64, DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};
    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> sort_data(1500);
    std::vector<int64_t> value_data(1500);
    for (int64_t i = 0; i < sort_data.size(); ++i) {
        sort_data[i] = i;
        value_data[i] = i;
    }
    auto sort_col = CreateInt64Column(sort_data);
    auto value_col = CreateInt64Column(value_data);
    value_col->nullAt(5);
    value_col->nullAt(1024);
    value_col->nullAt(1499);

    std::vector<ColumnVectorPtr> columns = {sort_col, value_col};
    buffer.AddRows(columns, sort_data.size());
    buffer.NoMoreInput();

    auto first_output = buffer.GetOutput(1024);
    auto second_output = buffer.GetOutput(476);
    auto first_col = std::dynamic_pointer_cast<ColumnVector>(first_output[1]);
    auto second_col = std::dynamic_pointer_cast<ColumnVector>(second_output[1]);
    ASSERT_EQ(first_col->size(), 1024);
    ASSERT_EQ(second_col->size(), 476);
    ASSERT_EQ(first_col->nullCount(), 1);
    ASSERT_EQ(second_col->nullCount(), 2);

    ASSERT_NO_THROW(first_col->append(*second_col));
    ASSERT_EQ(first_col->size(), 1500);
    EXPECT_EQ(first_col->nullCount(), 3);
    EXPECT_TRUE(first_col->ValidAt(0));
    EXPECT_FALSE(first_col->ValidAt(5));
    EXPECT_TRUE(first_col->ValidAt(1023));
    EXPECT_FALSE(first_col->ValidAt(1024));
    EXPECT_TRUE(first_col->ValidAt(1498));
    EXPECT_FALSE(first_col->ValidAt(1499));
    EXPECT_EQ(first_col->ValueAt<int64_t>(0), 0);
    EXPECT_EQ(first_col->ValueAt<int64_t>(1023), 1023);
    EXPECT_EQ(first_col->ValueAt<int64_t>(1498), 1498);
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

// =========================================================================
// Phase 1: NULL Handling Tests
// =========================================================================

TEST_F(SortBufferTest, NullsFirst_Ascending) {
    // ASC + NULLS FIRST: NULLs should appear before non-null values
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, true, true)  // ASC, NULLS FIRST
    };

    SortBuffer buffer(column_types, sort_keys);

    // Data: [5, NULL, 2, NULL, 8, 1]
    auto col = std::make_shared<ColumnVector>(DataType::INT64, 6);
    col->SetValueAt<int64_t>(0, 5);
    col->nullAt(1);
    col->SetValueAt<int64_t>(2, 2);
    col->nullAt(3);
    col->SetValueAt<int64_t>(4, 8);
    col->SetValueAt<int64_t>(5, 1);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 6; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_NE(out_col, nullptr);
    ASSERT_EQ(out_col->size(), 6);

    // First 2 should be NULL
    EXPECT_FALSE(out_col->ValidAt(0));
    EXPECT_FALSE(out_col->ValidAt(1));
    // Remaining should be sorted ASC: 1, 2, 5, 8
    EXPECT_TRUE(out_col->ValidAt(2));
    EXPECT_EQ(out_col->ValueAt<int64_t>(2), 1);
    EXPECT_EQ(out_col->ValueAt<int64_t>(3), 2);
    EXPECT_EQ(out_col->ValueAt<int64_t>(4), 5);
    EXPECT_EQ(out_col->ValueAt<int64_t>(5), 8);
}

TEST_F(SortBufferTest, NullsLast_Ascending) {
    // ASC + NULLS LAST: NULLs should appear after non-null values
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, true, false)  // ASC, NULLS LAST
    };

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::INT64, 6);
    col->SetValueAt<int64_t>(0, 5);
    col->nullAt(1);
    col->SetValueAt<int64_t>(2, 2);
    col->nullAt(3);
    col->SetValueAt<int64_t>(4, 8);
    col->SetValueAt<int64_t>(5, 1);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 6; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 6);

    // First 4 should be sorted ASC: 1, 2, 5, 8
    EXPECT_TRUE(out_col->ValidAt(0));
    EXPECT_EQ(out_col->ValueAt<int64_t>(0), 1);
    EXPECT_EQ(out_col->ValueAt<int64_t>(1), 2);
    EXPECT_EQ(out_col->ValueAt<int64_t>(2), 5);
    EXPECT_EQ(out_col->ValueAt<int64_t>(3), 8);
    // Last 2 should be NULL
    EXPECT_FALSE(out_col->ValidAt(4));
    EXPECT_FALSE(out_col->ValidAt(5));
}

TEST_F(SortBufferTest, NullsFirst_Descending) {
    // DESC + NULLS FIRST: NULLs first, then values descending
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, false, true)  // DESC, NULLS FIRST
    };

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::INT64, 5);
    col->SetValueAt<int64_t>(0, 3);
    col->nullAt(1);
    col->SetValueAt<int64_t>(2, 7);
    col->SetValueAt<int64_t>(3, 1);
    col->nullAt(4);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 5; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 5);

    // First 2 should be NULL
    EXPECT_FALSE(out_col->ValidAt(0));
    EXPECT_FALSE(out_col->ValidAt(1));
    // Then DESC: 7, 3, 1
    EXPECT_EQ(out_col->ValueAt<int64_t>(2), 7);
    EXPECT_EQ(out_col->ValueAt<int64_t>(3), 3);
    EXPECT_EQ(out_col->ValueAt<int64_t>(4), 1);
}

TEST_F(SortBufferTest, NullsLast_Descending) {
    // DESC + NULLS LAST: values descending, then NULLs
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, false, false)  // DESC, NULLS LAST
    };

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::INT64, 5);
    col->SetValueAt<int64_t>(0, 3);
    col->nullAt(1);
    col->SetValueAt<int64_t>(2, 7);
    col->SetValueAt<int64_t>(3, 1);
    col->nullAt(4);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 5; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 5);

    // DESC: 7, 3, 1
    EXPECT_EQ(out_col->ValueAt<int64_t>(0), 7);
    EXPECT_EQ(out_col->ValueAt<int64_t>(1), 3);
    EXPECT_EQ(out_col->ValueAt<int64_t>(2), 1);
    // Last 2 should be NULL
    EXPECT_FALSE(out_col->ValidAt(3));
    EXPECT_FALSE(out_col->ValidAt(4));
}

TEST_F(SortBufferTest, AllNulls) {
    // All rows have NULL in the sort key
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, true, true)  // ASC, NULLS FIRST
    };

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::INT64, 4);
    for (size_t i = 0; i < 4; ++i) {
        col->nullAt(i);
    }
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 4; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 4);

    // All should be NULL
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_FALSE(out_col->ValidAt(i));
    }
}

TEST_F(SortBufferTest, MixedNullMultiKey) {
    // Multi-key sort with NULLs in different columns
    // col0 ASC NULLS LAST, col1 DESC NULLS FIRST
    std::vector<DataType> column_types = {DataType::INT64, DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, true, false),  // col0 ASC, NULLS LAST
        SortKeyInfo(1, false, true),  // col1 DESC, NULLS FIRST
    };

    SortBuffer buffer(column_types, sort_keys);

    // Rows: (col0, col1)
    // row0: (1,    NULL)
    // row1: (1,    20)
    // row2: (NULL, 30)
    // row3: (1,    10)
    // row4: (NULL, 40)
    auto col0 = std::make_shared<ColumnVector>(DataType::INT64, 5);
    auto col1 = std::make_shared<ColumnVector>(DataType::INT64, 5);

    col0->SetValueAt<int64_t>(0, 1);
    col1->nullAt(0);

    col0->SetValueAt<int64_t>(1, 1);
    col1->SetValueAt<int64_t>(1, 20);

    col0->nullAt(2);
    col1->SetValueAt<int64_t>(2, 30);

    col0->SetValueAt<int64_t>(3, 1);
    col1->SetValueAt<int64_t>(3, 10);

    col0->nullAt(4);
    col1->SetValueAt<int64_t>(4, 40);

    std::vector<ColumnVectorPtr> columns = {col0, col1};
    for (size_t i = 0; i < 5; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out0 = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    auto out1 = std::dynamic_pointer_cast<ColumnVector>(output[1]);
    ASSERT_EQ(out0->size(), 5);

    // Expected order by (col0 ASC NULLS LAST, col1 DESC NULLS FIRST):
    // row0: (1, NULL)  -- col0=1, col1=NULL (NULLS FIRST in col1 DESC)
    // row1: (1, 20)    -- col0=1, col1=20
    // row3: (1, 10)    -- col0=1, col1=10
    // row4: (NULL, 40) -- col0=NULL (NULLS LAST)
    // row2: (NULL, 30) -- col0=NULL (NULLS LAST), col1=40>30 DESC

    // First 3 rows: col0 = 1
    EXPECT_TRUE(out0->ValidAt(0));
    EXPECT_EQ(out0->ValueAt<int64_t>(0), 1);
    EXPECT_TRUE(out0->ValidAt(1));
    EXPECT_EQ(out0->ValueAt<int64_t>(1), 1);
    EXPECT_TRUE(out0->ValidAt(2));
    EXPECT_EQ(out0->ValueAt<int64_t>(2), 1);

    // row0 has NULL in col1 (NULLS FIRST), so it comes first among col0=1
    EXPECT_FALSE(out1->ValidAt(0));
    // Then 20, 10 DESC
    EXPECT_EQ(out1->ValueAt<int64_t>(1), 20);
    EXPECT_EQ(out1->ValueAt<int64_t>(2), 10);

    // Last 2 rows: col0 = NULL
    EXPECT_FALSE(out0->ValidAt(3));
    EXPECT_FALSE(out0->ValidAt(4));
    // col1 DESC: 40, 30
    EXPECT_EQ(out1->ValueAt<int64_t>(3), 40);
    EXPECT_EQ(out1->ValueAt<int64_t>(4), 30);
}

// =========================================================================
// Phase 1: Data Type Coverage Tests
// =========================================================================

TEST_F(SortBufferTest, DataType_Bool) {
    std::vector<DataType> column_types = {DataType::BOOL};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};  // ASC

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::BOOL, 4);
    col->SetValueAt<bool>(0, true);
    col->SetValueAt<bool>(1, false);
    col->SetValueAt<bool>(2, true);
    col->SetValueAt<bool>(3, false);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 4; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 4);

    // ASC: false(0), false(0), true(1), true(1)
    EXPECT_EQ(out_col->ValueAt<bool>(0), false);
    EXPECT_EQ(out_col->ValueAt<bool>(1), false);
    EXPECT_EQ(out_col->ValueAt<bool>(2), true);
    EXPECT_EQ(out_col->ValueAt<bool>(3), true);
}

TEST_F(SortBufferTest, DataType_Int8) {
    std::vector<DataType> column_types = {DataType::INT8};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0, false)};  // DESC

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::INT8, 5);
    col->SetValueAt<int8_t>(0, 10);
    col->SetValueAt<int8_t>(1, -5);
    col->SetValueAt<int8_t>(2, 127);
    col->SetValueAt<int8_t>(3, -128);
    col->SetValueAt<int8_t>(4, 0);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 5; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 5);

    // DESC: 127, 10, 0, -5, -128
    EXPECT_EQ(out_col->ValueAt<int8_t>(0), 127);
    EXPECT_EQ(out_col->ValueAt<int8_t>(1), 10);
    EXPECT_EQ(out_col->ValueAt<int8_t>(2), 0);
    EXPECT_EQ(out_col->ValueAt<int8_t>(3), -5);
    EXPECT_EQ(out_col->ValueAt<int8_t>(4), -128);
}

TEST_F(SortBufferTest, DataType_Int16) {
    std::vector<DataType> column_types = {DataType::INT16};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};  // ASC

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::INT16, 4);
    col->SetValueAt<int16_t>(0, 300);
    col->SetValueAt<int16_t>(1, -200);
    col->SetValueAt<int16_t>(2, 100);
    col->SetValueAt<int16_t>(3, -400);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 4; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 4);

    // ASC: -400, -200, 100, 300
    EXPECT_EQ(out_col->ValueAt<int16_t>(0), -400);
    EXPECT_EQ(out_col->ValueAt<int16_t>(1), -200);
    EXPECT_EQ(out_col->ValueAt<int16_t>(2), 100);
    EXPECT_EQ(out_col->ValueAt<int16_t>(3), 300);
}

TEST_F(SortBufferTest, DataType_Int32) {
    std::vector<DataType> column_types = {DataType::INT32};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};  // ASC

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::INT32, 4);
    col->SetValueAt<int32_t>(0, 100000);
    col->SetValueAt<int32_t>(1, -50000);
    col->SetValueAt<int32_t>(2, 0);
    col->SetValueAt<int32_t>(3, 999999);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 4; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 4);

    EXPECT_EQ(out_col->ValueAt<int32_t>(0), -50000);
    EXPECT_EQ(out_col->ValueAt<int32_t>(1), 0);
    EXPECT_EQ(out_col->ValueAt<int32_t>(2), 100000);
    EXPECT_EQ(out_col->ValueAt<int32_t>(3), 999999);
}

TEST_F(SortBufferTest, DataType_Float) {
    std::vector<DataType> column_types = {DataType::FLOAT};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};  // ASC

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::FLOAT, 5);
    col->SetValueAt<float>(0, 3.14f);
    col->SetValueAt<float>(1, -1.5f);
    col->SetValueAt<float>(2, 0.0f);
    col->SetValueAt<float>(3, 2.71f);
    col->SetValueAt<float>(4, -0.001f);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 5; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 5);

    // ASC: -1.5, -0.001, 0.0, 2.71, 3.14
    EXPECT_FLOAT_EQ(out_col->ValueAt<float>(0), -1.5f);
    EXPECT_FLOAT_EQ(out_col->ValueAt<float>(1), -0.001f);
    EXPECT_FLOAT_EQ(out_col->ValueAt<float>(2), 0.0f);
    EXPECT_FLOAT_EQ(out_col->ValueAt<float>(3), 2.71f);
    EXPECT_FLOAT_EQ(out_col->ValueAt<float>(4), 3.14f);
}

TEST_F(SortBufferTest, StringSort) {
    // Test VARCHAR sort with various strings
    std::vector<DataType> column_types = {DataType::VARCHAR};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};  // ASC

    SortBuffer buffer(column_types, sort_keys);

    std::vector<std::string> data = {"banana", "apple", "cherry", "", "date"};
    auto col = CreateStringColumn(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 5);

    // ASC: "", "apple", "banana", "cherry", "date"
    auto* raw = reinterpret_cast<std::string*>(out_col->GetRawData());
    EXPECT_EQ(raw[0], "");
    EXPECT_EQ(raw[1], "apple");
    EXPECT_EQ(raw[2], "banana");
    EXPECT_EQ(raw[3], "cherry");
    EXPECT_EQ(raw[4], "date");
}

TEST_F(SortBufferTest, StringSortDescending) {
    std::vector<DataType> column_types = {DataType::VARCHAR};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0, false)};  // DESC

    SortBuffer buffer(column_types, sort_keys);

    std::vector<std::string> data = {"banana", "apple", "cherry"};
    auto col = CreateStringColumn(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 3);

    auto* raw = reinterpret_cast<std::string*>(out_col->GetRawData());
    EXPECT_EQ(raw[0], "cherry");
    EXPECT_EQ(raw[1], "banana");
    EXPECT_EQ(raw[2], "apple");
}

TEST_F(SortBufferTest, StringWithNulls) {
    // VARCHAR with NULLs, ASC NULLS LAST
    std::vector<DataType> column_types = {DataType::VARCHAR};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0, true, false)};

    SortBuffer buffer(column_types, sort_keys);

    auto col = std::make_shared<ColumnVector>(DataType::VARCHAR, 4);
    col->SetValueAt<std::string>(0, "banana");
    col->nullAt(1);
    col->SetValueAt<std::string>(2, "apple");
    col->nullAt(3);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 4; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 4);

    // ASC NULLS LAST: "apple", "banana", NULL, NULL
    EXPECT_TRUE(out_col->ValidAt(0));
    EXPECT_TRUE(out_col->ValidAt(1));
    EXPECT_FALSE(out_col->ValidAt(2));
    EXPECT_FALSE(out_col->ValidAt(3));

    auto* raw = reinterpret_cast<std::string*>(out_col->GetRawData());
    EXPECT_EQ(raw[0], "apple");
    EXPECT_EQ(raw[1], "banana");
}

// =========================================================================
// Phase 1: Edge Case Tests
// =========================================================================

TEST_F(SortBufferTest, LimitZero) {
    // limit = 0 means unlimited (same as -1)
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};

    SortBuffer buffer(column_types, sort_keys, 0);

    std::vector<int64_t> data = {5, 2, 8, 1};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto sorted = ExtractInt64Values(output, 0);

    // limit=0 is treated as no limit, should return all rows sorted
    ASSERT_EQ(sorted.size(), 4);
    std::vector<int64_t> expected = {1, 2, 5, 8};
    EXPECT_EQ(sorted, expected);
}

TEST_F(SortBufferTest, LimitLargerThanRows) {
    // limit > num_rows: should return all rows
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};

    SortBuffer buffer(column_types, sort_keys, 100);

    std::vector<int64_t> data = {5, 2, 8};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(200);
    auto sorted = ExtractInt64Values(output, 0);

    ASSERT_EQ(sorted.size(), 3);
    std::vector<int64_t> expected = {2, 5, 8};
    EXPECT_EQ(sorted, expected);
}

TEST_F(SortBufferTest, NoMoreInputIdempotent) {
    // Calling NoMoreInput() multiple times should be safe
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};

    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> data = {3, 1, 2};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }

    buffer.NoMoreInput();
    buffer.NoMoreInput();  // Second call should be safe
    buffer.NoMoreInput();  // Third call too

    auto output = buffer.GetOutput(100);
    auto sorted = ExtractInt64Values(output, 0);
    std::vector<int64_t> expected = {1, 2, 3};
    EXPECT_EQ(sorted, expected);
}

TEST_F(SortBufferTest, ExhaustedOutput) {
    // After consuming all output, GetOutput returns empty
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};

    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> data = {2, 1};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < data.size(); ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    // First call gets all data
    auto output = buffer.GetOutput(100);
    ASSERT_FALSE(output.empty());

    // Second call should return empty
    EXPECT_FALSE(buffer.HasOutput());
    auto output2 = buffer.GetOutput(100);
    EXPECT_TRUE(output2.empty());
}

TEST_F(SortBufferTest, AddRowsHelper) {
    // Test AddRows (batch add) helper
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};

    SortBuffer buffer(column_types, sort_keys);

    std::vector<int64_t> data = {5, 2, 8, 1};
    auto col = CreateInt64Column(data);
    std::vector<ColumnVectorPtr> columns = {col};

    buffer.AddRows(columns, 4);
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto sorted = ExtractInt64Values(output, 0);

    std::vector<int64_t> expected = {1, 2, 5, 8};
    EXPECT_EQ(sorted, expected);
}

TEST_F(SortBufferTest, NullsWithLimit) {
    // NULL handling combined with limit
    // ASC NULLS FIRST with limit=3 should get NULLs first + top values
    std::vector<DataType> column_types = {DataType::INT64};
    std::vector<SortKeyInfo> sort_keys = {
        SortKeyInfo(0, true, true)  // ASC, NULLS FIRST
    };

    SortBuffer buffer(column_types, sort_keys, 3);

    auto col = std::make_shared<ColumnVector>(DataType::INT64, 6);
    col->nullAt(0);
    col->SetValueAt<int64_t>(1, 5);
    col->nullAt(2);
    col->SetValueAt<int64_t>(3, 2);
    col->SetValueAt<int64_t>(4, 8);
    col->SetValueAt<int64_t>(5, 1);
    std::vector<ColumnVectorPtr> columns = {col};

    for (size_t i = 0; i < 6; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto out_col = std::dynamic_pointer_cast<ColumnVector>(output[0]);
    ASSERT_EQ(out_col->size(), 3);

    // Top 3 with ASC NULLS FIRST: NULL, NULL, 1
    EXPECT_FALSE(out_col->ValidAt(0));
    EXPECT_FALSE(out_col->ValidAt(1));
    EXPECT_TRUE(out_col->ValidAt(2));
    EXPECT_EQ(out_col->ValueAt<int64_t>(2), 1);
}

TEST_F(SortBufferTest, NonSortColumnPreserved) {
    // Verify non-sort columns are correctly carried along
    // Sort by col0 (INT64 ASC), carry col1 (VARCHAR)
    std::vector<DataType> column_types = {DataType::INT64, DataType::VARCHAR};
    std::vector<SortKeyInfo> sort_keys = {SortKeyInfo(0)};  // Only sort by col0

    SortBuffer buffer(column_types, sort_keys);

    auto col0 = CreateInt64Column({3, 1, 2});
    auto col1 = CreateStringColumn({"three", "one", "two"});
    std::vector<ColumnVectorPtr> columns = {col0, col1};

    for (size_t i = 0; i < 3; ++i) {
        buffer.AddRow(columns, i);
    }
    buffer.NoMoreInput();

    auto output = buffer.GetOutput(100);
    auto sorted_int = ExtractInt64Values(output, 0);
    auto out_str = std::dynamic_pointer_cast<ColumnVector>(output[1]);

    // After sorting by col0 ASC: (1, "one"), (2, "two"), (3, "three")
    std::vector<int64_t> expected_int = {1, 2, 3};
    EXPECT_EQ(sorted_int, expected_int);

    auto* raw = reinterpret_cast<std::string*>(out_str->GetRawData());
    EXPECT_EQ(raw[0], "one");
    EXPECT_EQ(raw[1], "two");
    EXPECT_EQ(raw[2], "three");
}

// =========================================================================
// TopK (limit > 0): the result must equal the first `limit` rows of a full
// sort (limit = -1), and every output row must still carry its own columns.
// =========================================================================

namespace {

struct TopKSpec {
    DataType key_type = DataType::INT64;
    bool nullable = false;
    int64_t key_cardinality = 0;  // 0 = (almost) unique keys
    bool second_key = false;      // add a VARCHAR second sort key
    bool varchar_pk = false;
    int sorted_input = 0;  // +1 / -1: key increases / decreases with arrival
};

// Columns: [pk, key, (key2 varchar), row_id (int64)]
struct TopKTable {
    std::vector<DataType> types;
    std::vector<std::vector<ColumnVectorPtr>> batches;
    std::vector<vector_size_t> batch_rows;
};

void
SetKey(const ColumnVectorPtr& col, size_t i, DataType type, int64_t v) {
    switch (type) {
        case DataType::BOOL:
            col->SetValueAt<bool>(i, (v & 1) != 0);
            break;
        case DataType::INT8:
            col->SetValueAt<int8_t>(i, static_cast<int8_t>(v));
            break;
        case DataType::INT16:
            col->SetValueAt<int16_t>(i, static_cast<int16_t>(v));
            break;
        case DataType::INT32:
            col->SetValueAt<int32_t>(i, static_cast<int32_t>(v));
            break;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            col->SetValueAt<int64_t>(i, v);
            break;
        case DataType::FLOAT:
            // every 13th value is NaN, which sorts as the largest value
            col->SetValueAt<float>(i,
                                   v % 13 == 0
                                       ? std::numeric_limits<float>::quiet_NaN()
                                       : static_cast<float>(v % 100000) / 3);
            break;
        case DataType::DOUBLE:
            col->SetValueAt<double>(
                i,
                v % 13 == 0 ? std::numeric_limits<double>::quiet_NaN()
                            : static_cast<double>(v) / 7);
            break;
        default:  // VARCHAR
            col->SetValueAt<std::string>(i, "k" + std::to_string(v));
    }
}

TopKTable
GenerateTopKTable(const TopKSpec& spec,
                  int64_t num_rows,
                  uint64_t seed,
                  int64_t batch_size = 256) {
    TopKTable table;
    table.types = {spec.varchar_pk ? DataType::VARCHAR : DataType::INT64,
                   spec.key_type};
    if (spec.second_key) {
        table.types.push_back(DataType::VARCHAR);
    }
    table.types.push_back(DataType::INT64);

    std::mt19937_64 rng(seed);
    for (int64_t start = 0; start < num_rows; start += batch_size) {
        auto n = std::min(batch_size, num_rows - start);
        std::vector<ColumnVectorPtr> cols;
        for (auto type : table.types) {
            cols.push_back(std::make_shared<ColumnVector>(type, n));
        }
        for (int64_t i = 0; i < n; ++i) {
            int64_t id = start + i;
            uint64_t r = rng();
            if (spec.varchar_pk) {
                cols[0]->SetValueAt<std::string>(i, "pk-" + std::to_string(id));
            } else {
                cols[0]->SetValueAt<int64_t>(i, id);
            }
            if (spec.nullable && r % 10 == 0) {
                cols[1]->nullAt(i);
            } else {
                int64_t v =
                    spec.sorted_input > 0   ? id
                    : spec.sorted_input < 0 ? num_rows - id
                    : spec.key_cardinality > 0
                        ? static_cast<int64_t>((r >> 8) % spec.key_cardinality)
                        : static_cast<int64_t>(r >> 2);
                SetKey(cols[1], i, spec.key_type, v);
            }
            if (spec.second_key) {
                if (spec.nullable && r % 7 == 0) {
                    cols[2]->nullAt(i);
                } else {
                    cols[2]->SetValueAt<std::string>(
                        i, "s" + std::to_string((r >> 40) % 5));
                }
            }
            cols.back()->SetValueAt<int64_t>(i, id);
        }
        table.batches.push_back(std::move(cols));
        table.batch_rows.push_back(static_cast<vector_size_t>(n));
    }
    return table;
}

template <typename T>
std::string
RenderFloat(T v) {
    if (std::isnan(v)) {
        return "nan";
    }
    std::ostringstream os;
    os << std::setprecision(std::numeric_limits<T>::max_digits10) << v;
    return os.str();
}

std::string
RenderCell(const ColumnVectorPtr& col, size_t i, DataType type) {
    if (!col->ValidAt(i)) {
        return "<null>";
    }
    switch (type) {
        case DataType::BOOL:
            return std::to_string(col->ValueAt<bool>(i));
        case DataType::INT8:
            return std::to_string(col->ValueAt<int8_t>(i));
        case DataType::INT16:
            return std::to_string(col->ValueAt<int16_t>(i));
        case DataType::INT32:
            return std::to_string(col->ValueAt<int32_t>(i));
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return std::to_string(col->ValueAt<int64_t>(i));
        case DataType::FLOAT:
            return RenderFloat(col->ValueAt<float>(i));
        case DataType::DOUBLE:
            return RenderFloat(col->ValueAt<double>(i));
        default:  // VARCHAR
            return reinterpret_cast<std::string*>(col->GetRawData())[i];
    }
}

// Each output row rendered as "key1|key2|...#pk#row_id"
std::vector<std::string>
RunSortBuffer(const TopKTable& table,
              const std::vector<SortKeyInfo>& sort_keys,
              int64_t limit,
              bool row_by_row = false) {
    SortBuffer buffer(table.types, sort_keys, limit);
    for (size_t b = 0; b < table.batches.size(); ++b) {
        if (row_by_row) {
            for (vector_size_t i = 0; i < table.batch_rows[b]; ++i) {
                buffer.AddRow(table.batches[b], i);
            }
        } else {
            buffer.AddRows(table.batches[b], table.batch_rows[b]);
        }
    }
    buffer.NoMoreInput();

    std::vector<std::string> rows;
    while (buffer.HasOutput()) {
        auto out = buffer.GetOutput(97);
        if (out.empty()) {
            break;
        }
        auto col = [&](size_t c) {
            return std::dynamic_pointer_cast<ColumnVector>(out[c]);
        };
        for (size_t i = 0; i < out[0]->size(); ++i) {
            std::string row;
            for (const auto& sk : sort_keys) {
                row +=
                    RenderCell(
                        col(sk.column_index), i, table.types[sk.column_index]) +
                    "|";
            }
            row += "#" + RenderCell(col(0), i, table.types[0]) + "#" +
                   RenderCell(col(out.size() - 1), i, DataType::INT64);
            rows.push_back(row);
        }
    }
    return rows;
}

std::string
KeyPart(const std::string& row) {
    return row.substr(0, row.find('#'));
}

// 'got' must have the key sequence of the first 'limit' rows of the full sort
// 'full' (rows with equal keys may differ, as with any unstable sort), and
// each of its rows must be a distinct, intact input row.
void
ExpectFullSortPrefix(const std::vector<std::string>& got,
                     const std::vector<std::string>& full,
                     int64_t limit) {
    auto expected = std::min<size_t>(limit, full.size());
    ASSERT_EQ(got.size(), expected);
    std::set<std::string> input_rows(full.begin(), full.end());
    std::set<std::string> seen;
    for (size_t i = 0; i < got.size(); ++i) {
        ASSERT_EQ(KeyPart(got[i]), KeyPart(full[i])) << "row " << i;
        ASSERT_TRUE(input_rows.count(got[i])) << got[i];
        ASSERT_TRUE(seen.insert(got[i]).second) << "duplicate " << got[i];
    }
}

}  // namespace

TEST_F(SortBufferTest, TopKMatchesFullSortPrefix) {
    struct Case {
        TopKSpec spec;
        std::vector<SortKeyInfo> keys;
    };
    std::vector<Case> cases;
    for (bool asc : {true, false}) {
        for (bool nulls_first : {true, false}) {
            for (auto type : {DataType::BOOL,
                              DataType::INT8,
                              DataType::INT16,
                              DataType::INT32,
                              DataType::INT64,
                              DataType::TIMESTAMPTZ,
                              DataType::FLOAT,
                              DataType::DOUBLE,
                              DataType::VARCHAR}) {
                for (int64_t card : {int64_t{0}, int64_t{17}}) {
                    TopKSpec s;
                    s.key_type = type;
                    s.nullable = true;
                    s.key_cardinality = card;
                    s.varchar_pk = type == DataType::VARCHAR;
                    cases.push_back({s, {SortKeyInfo(1, asc, nulls_first)}});
                }
            }
            TopKSpec m;  // multi-key with ties on the first key
            m.key_type = DataType::INT32;
            m.nullable = true;
            m.key_cardinality = 17;
            m.second_key = true;
            cases.push_back({m,
                             {SortKeyInfo(1, asc, nulls_first),
                              SortKeyInfo(2, !asc, !nulls_first)}});
        }
    }

    const int64_t kRows = 3000;
    uint64_t seed = 42;
    for (const auto& c : cases) {
        auto table = GenerateTopKTable(c.spec, kRows, seed++);
        auto full = RunSortBuffer(table, c.keys, -1);
        ASSERT_EQ(full.size(), static_cast<size_t>(kRows));
        // below, at and above 2 * limit (when the candidates get shrunk)
        for (int64_t limit : {int64_t{1},
                              int64_t{7},
                              int64_t{100},
                              int64_t{1500},
                              int64_t{3000},
                              int64_t{5000}}) {
            SCOPED_TRACE(
                "type=" + std::to_string(static_cast<int>(c.spec.key_type)) +
                " card=" + std::to_string(c.spec.key_cardinality) +
                " keys=" + std::to_string(c.keys.size()) +
                " asc=" + std::to_string(c.keys[0].ascending) +
                " nulls_first=" + std::to_string(c.keys[0].nulls_first) +
                " limit=" + std::to_string(limit));
            ExpectFullSortPrefix(
                RunSortBuffer(table, c.keys, limit), full, limit);
        }
        // AddRow() offers rows one by one
        ExpectFullSortPrefix(
            RunSortBuffer(table, c.keys, 100, /*row_by_row=*/true), full, 100);
    }
}

// Sorted input: either every row beats the threshold (most shrinks) or none.
TEST_F(SortBufferTest, TopKSortedInput) {
    for (int sorted_input : {1, -1}) {
        for (bool asc : {true, false}) {
            TopKSpec spec;
            spec.sorted_input = sorted_input;
            spec.varchar_pk = true;
            auto table = GenerateTopKTable(spec, 5000, 3);
            std::vector<SortKeyInfo> keys = {SortKeyInfo(1, asc, false)};
            auto full = RunSortBuffer(table, keys, -1);
            for (int64_t limit : {int64_t{1}, int64_t{64}, int64_t{1000}}) {
                SCOPED_TRACE("sorted_input=" + std::to_string(sorted_input) +
                             " asc=" + std::to_string(asc) +
                             " limit=" + std::to_string(limit));
                ExpectFullSortPrefix(
                    RunSortBuffer(table, keys, limit), full, limit);
                ExpectFullSortPrefix(
                    RunSortBuffer(table, keys, limit, /*row_by_row=*/true),
                    full,
                    limit);
            }
        }
    }
}

// A limit whose 2 * limit candidate capacity would overflow.
TEST_F(SortBufferTest, TopKHugeLimit) {
    TopKSpec spec;
    spec.nullable = true;
    auto table = GenerateTopKTable(spec, 1000, 9);
    std::vector<SortKeyInfo> keys = {SortKeyInfo(1)};
    auto full = RunSortBuffer(table, keys, -1);
    for (int64_t limit : {std::numeric_limits<int64_t>::max() / 2 + 1,
                          std::numeric_limits<int64_t>::max()}) {
        SCOPED_TRACE("limit=" + std::to_string(limit));
        ExpectFullSortPrefix(RunSortBuffer(table, keys, limit), full, limit);
        ExpectFullSortPrefix(
            RunSortBuffer(table, keys, limit, /*row_by_row=*/true),
            full,
            limit);
    }
}
