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

#include "exec/operator/query-agg/AggRawInput.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/Span.h"
#include "common/Vector.h"
#include "exec/HashTable.h"
#include "exec/VectorHasher.h"
#include "exec/operator/query-agg/AggregateInfo.h"
#include "exec/operator/query-agg/CountAggregateBase.h"
#include "exec/operator/query-agg/GroupingSet.h"
#include "exec/operator/query-agg/MaxAggregateBase.h"
#include "exec/operator/query-agg/SumAggregateBase.h"

namespace milvus::exec {
namespace {

FixedVector<bool>
MakeValid(const std::vector<bool>& valid) {
    FixedVector<bool> ret;
    ret.reserve(valid.size());
    for (auto v : valid) {
        ret.emplace_back(v);
    }
    return ret;
}

AggSelectedChunk
MakeAllSelectedChunk(int64_t row_count) {
    return AggSelectedChunk(
        0, 0, row_count, row_count, true, TargetBitmapView{});
}

SpanBase
MakeSpan(const std::vector<int32_t>& values, const FixedVector<bool>& valid) {
    return SpanBase(
        values.data(), valid.data(), values.size(), sizeof(int32_t));
}

SpanBase
MakeSpan(const std::vector<int64_t>& values, const FixedVector<bool>& valid) {
    return SpanBase(
        values.data(), valid.data(), values.size(), sizeof(int64_t));
}

std::vector<std::unique_ptr<VectorHasher>>
MakeHashers(DataType type, column_index_t column_idx) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.emplace_back(VectorHasher::create(type, column_idx));
    return hashers;
}

RowVectorPtr
MakeRowVector(std::vector<DataType> types) {
    std::vector<VectorPtr> children;
    children.reserve(types.size());
    for (auto type : types) {
        children.emplace_back(std::make_shared<ColumnVector>(type, 0));
    }
    return std::make_shared<RowVector>(std::move(children));
}

}  // namespace

TEST(AggRawExecutionTest, HashTableGroupsRawNullAndDuplicateKeys) {
    std::vector<int32_t> keys{1, 99, 1, 2, 0};
    auto valid = MakeValid({true, false, true, true, false});
    auto chunk = MakeAllSelectedChunk(keys.size());
    AggRawInput input(chunk);
    input.AddFixedWidthColumn(DataType::INT32, MakeSpan(keys, valid));

    HashTable table(MakeHashers(DataType::INT32, 0), {});
    HashLookup lookup(table.hashers());
    table.prepareForGroupProbe(lookup, input);
    table.groupProbe(lookup, input);

    ASSERT_EQ(lookup.hits_.size(), keys.size());
    EXPECT_EQ(lookup.hits_[0], lookup.hits_[2]);
    EXPECT_EQ(lookup.hits_[1], lookup.hits_[4]);
    EXPECT_NE(lookup.hits_[0], lookup.hits_[1]);
    EXPECT_NE(lookup.hits_[0], lookup.hits_[3]);
    EXPECT_NE(lookup.hits_[1], lookup.hits_[3]);
    EXPECT_EQ(table.rows()->allRows().size(), 3);
}

TEST(AggRawExecutionTest, GroupingSetUpdatesCountAndSumFromSparseRawInput) {
    std::vector<int32_t> keys{1, 1, 2, 1, 2, 3};
    std::vector<int32_t> values{10, 100, 20, 30, 50, 70};
    auto key_valid = MakeValid({true, true, true, true, true, true});
    auto value_valid = MakeValid({true, true, true, true, false, true});

    TargetBitmap filter_mask(keys.size(), false);
    filter_mask.set(1, true);
    auto chunk_input = AggChunkInput::FromChunkBounds(
        {0, static_cast<int64_t>(keys.size())}, filter_mask.view());
    ASSERT_EQ(chunk_input.chunks().size(), 1);
    AggRawInput input(chunk_input.chunks()[0]);
    input.AddFixedWidthColumn(DataType::INT32, MakeSpan(keys, key_valid));
    input.AddFixedWidthColumn(DataType::INT32, MakeSpan(values, value_valid));

    std::vector<AggregateInfo> aggregates;
    AggregateInfo count;
    count.function_ = std::make_unique<CountAggregate>();
    count.output_ = 1;
    aggregates.emplace_back(std::move(count));

    AggregateInfo sum;
    sum.function_ =
        std::make_unique<SumAggregateBase<int32_t, int64_t, int64_t, false>>(
            DataType::INT64);
    sum.input_column_idxes_ = {1};
    sum.output_ = 2;
    aggregates.emplace_back(std::move(sum));

    auto row_type = std::make_shared<RowType>(
        std::vector<std::string>{"key", "value"},
        std::vector<DataType>{DataType::INT32, DataType::INT32});
    GroupingSet grouping(
        row_type, MakeHashers(DataType::INT32, 0), std::move(aggregates));
    grouping.addRawInput(input);

    std::vector<int32_t> next_keys{1, 2};
    std::vector<int32_t> next_values{5, 7};
    auto next_key_valid = MakeValid({true, true});
    auto next_value_valid = MakeValid({true, true});
    auto next_chunk = MakeAllSelectedChunk(next_keys.size());
    AggRawInput next_input(next_chunk);
    next_input.AddFixedWidthColumn(DataType::INT32,
                                   MakeSpan(next_keys, next_key_valid));
    next_input.AddFixedWidthColumn(DataType::INT32,
                                   MakeSpan(next_values, next_value_valid));
    grouping.addRawInput(next_input);
    EXPECT_EQ(grouping.outputRowCount(), 3);

    auto result =
        MakeRowVector({DataType::INT32, DataType::INT64, DataType::INT64});
    ASSERT_TRUE(grouping.getOutput(result));
    ASSERT_EQ(result->size(), 3);

    auto key_col = std::dynamic_pointer_cast<ColumnVector>(result->child(0));
    auto count_col = std::dynamic_pointer_cast<ColumnVector>(result->child(1));
    auto sum_col = std::dynamic_pointer_cast<ColumnVector>(result->child(2));
    ASSERT_NE(key_col, nullptr);
    ASSERT_NE(count_col, nullptr);
    ASSERT_NE(sum_col, nullptr);

    std::unordered_map<int32_t, std::pair<int64_t, int64_t>> actual;
    for (int64_t i = 0; i < result->size(); ++i) {
        ASSERT_TRUE(sum_col->ValidAt(i));
        actual.emplace(key_col->ValueAt<int32_t>(i),
                       std::make_pair(count_col->ValueAt<int64_t>(i),
                                      sum_col->ValueAt<int64_t>(i)));
    }

    ASSERT_EQ(actual.size(), 3);
    EXPECT_EQ(actual[1], (std::make_pair<int64_t, int64_t>(3, 45)));
    EXPECT_EQ(actual[2], (std::make_pair<int64_t, int64_t>(3, 27)));
    EXPECT_EQ(actual[3], (std::make_pair<int64_t, int64_t>(1, 70)));
}

TEST(AggRawExecutionTest, GroupingSetGroupsRawStringKeys) {
    std::vector<std::string> keys{"alpha", "beta", "alpha", "gamma", "beta"};
    std::vector<std::string_view> views;
    views.reserve(keys.size());
    for (const auto& key : keys) {
        views.emplace_back(key);
    }
    FixedVector<bool> valid;

    auto chunk = MakeAllSelectedChunk(keys.size());
    AggRawInput input(chunk);
    input.AddStringColumn(DataType::VARCHAR, views, valid, false);

    std::vector<AggregateInfo> aggregates;
    AggregateInfo count;
    count.function_ = std::make_unique<CountAggregate>();
    count.output_ = 1;
    aggregates.emplace_back(std::move(count));

    auto row_type =
        std::make_shared<RowType>(std::vector<std::string>{"key"},
                                  std::vector<DataType>{DataType::VARCHAR});
    GroupingSet grouping(
        row_type, MakeHashers(DataType::VARCHAR, 0), std::move(aggregates));
    grouping.addRawInput(input);

    auto result = MakeRowVector({DataType::VARCHAR, DataType::INT64});
    ASSERT_TRUE(grouping.getOutput(result));
    ASSERT_EQ(result->size(), 3);

    auto key_col = std::dynamic_pointer_cast<ColumnVector>(result->child(0));
    auto count_col = std::dynamic_pointer_cast<ColumnVector>(result->child(1));
    ASSERT_NE(key_col, nullptr);
    ASSERT_NE(count_col, nullptr);

    std::unordered_map<std::string, int64_t> actual;
    for (int64_t i = 0; i < result->size(); ++i) {
        actual.emplace(key_col->ValueAt<std::string>(i),
                       count_col->ValueAt<int64_t>(i));
    }

    ASSERT_EQ(actual.size(), 3);
    EXPECT_EQ(actual["alpha"], 2);
    EXPECT_EQ(actual["beta"], 2);
    EXPECT_EQ(actual["gamma"], 1);
}

TEST(AggRawExecutionTest, RawStringMaxOwnsChunkViewValues) {
    std::vector<std::string> values{"alpha", "omega", "beta"};
    std::vector<std::string_view> views;
    views.reserve(values.size());
    for (const auto& value : values) {
        views.emplace_back(value);
    }
    FixedVector<bool> valid;

    auto chunk = MakeAllSelectedChunk(values.size());
    AggRawInput input(chunk);
    input.AddStringColumn(DataType::VARCHAR, views, valid, false);

    std::vector<AggregateInfo> aggregates;
    AggregateInfo max;
    max.function_ = std::make_unique<MaxStringAggregate>(DataType::VARCHAR);
    max.input_column_idxes_ = {0};
    max.output_ = 0;
    aggregates.emplace_back(std::move(max));

    auto row_type =
        std::make_shared<RowType>(std::vector<std::string>{"value"},
                                  std::vector<DataType>{DataType::VARCHAR});
    GroupingSet grouping(row_type, {}, std::move(aggregates));
    grouping.addRawInput(input);

    for (auto& value : values) {
        value.assign("z_mutated_after_raw_update");
    }

    auto result = MakeRowVector({DataType::VARCHAR});
    ASSERT_TRUE(grouping.getOutput(result));
    ASSERT_EQ(result->size(), 1);
    auto max_col = std::dynamic_pointer_cast<ColumnVector>(result->child(0));
    ASSERT_NE(max_col, nullptr);
    ASSERT_TRUE(max_col->ValidAt(0));
    EXPECT_EQ(max_col->ValueAt<std::string>(0), "omega");
}

}  // namespace milvus::exec
