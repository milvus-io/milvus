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
