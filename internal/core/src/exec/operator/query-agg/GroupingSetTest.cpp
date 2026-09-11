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

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/Vector.h"
#include "exec/VectorHasher.h"
#include "exec/operator/query-agg/GroupingSet.h"
#include "exec/operator/query-agg/MaxAggregateBase.h"
#include "exec/operator/query-agg/MinAggregateBase.h"

namespace milvus::exec {
namespace {

struct CleanupCounts {
    size_t rows = 0;
    size_t strings = 0;
};

// Observe the real string aggregate's cleanup while exercising GroupingSet's
// ownership boundary; the delegate still allocates and destroys its own state.
class ObservedStringAggregate : public Aggregate {
 public:
    ObservedStringAggregate(bool minimum, CleanupCounts& counts)
        : Aggregate(DataType::VARCHAR), counts_(counts) {
        if (minimum) {
            delegate_ = std::make_unique<MinStringAggregate>(DataType::VARCHAR);
        } else {
            delegate_ = std::make_unique<MaxStringAggregate>(DataType::VARCHAR);
        }
    }

    int32_t
    accumulatorFixedWidthSize() const override {
        return delegate_->accumulatorFixedWidthSize();
    }

    void
    addSingleGroupRawInput(char* group,
                           int64_t numRows,
                           const std::vector<VectorPtr>& input) override {
        delegate_->addSingleGroupRawInput(group, numRows, input);
    }

    void
    addRawInput(char** groups,
                int numGroups,
                const std::vector<VectorPtr>& input) override {
        delegate_->addRawInput(groups, numGroups, input);
    }

    void
    extractValues(char** groups,
                  int32_t numGroups,
                  VectorPtr* result) override {
        delegate_->extractValues(groups, numGroups, result);
    }

    void
    destroy(char* group) noexcept override {
        ++counts_.rows;
        counts_.strings += *delegate_->value<std::string*>(group) != nullptr;
        delegate_->destroy(group);
        EXPECT_EQ(*delegate_->value<std::string*>(group), nullptr);
    }

 protected:
    void
    setOffsetsInternal(int32_t offset,
                       int32_t nullByte,
                       uint8_t nullMask,
                       int32_t rowSizeOffset) override {
        Aggregate::setOffsetsInternal(
            offset, nullByte, nullMask, rowSizeOffset);
        delegate_->setOffsets(offset, nullByte, nullMask, rowSizeOffset);
    }

    void
    initializeNewGroupsInternal(
        char** groups, folly::Range<const vector_size_t*> indices) override {
        delegate_->initializeNewGroups(groups, indices);
    }

 private:
    CleanupCounts& counts_;
    std::unique_ptr<Aggregate> delegate_;
};

// minimum / grouped
class StringAggregateCleanupTest
    : public testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
    bool
    minimum() const {
        return std::get<0>(GetParam());
    }

    bool
    grouped() const {
        return std::get<1>(GetParam());
    }

    size_t
    groupCount() const {
        return grouped() ? 2 : 1;
    }

    std::unique_ptr<GroupingSet>
    makeGroupingSet(CleanupCounts& counts, bool two_aggregates = false) {
        std::vector<std::unique_ptr<VectorHasher>> hashers;
        if (grouped()) {
            hashers.emplace_back(VectorHasher::create(DataType::INT64, 0));
        }
        std::vector<AggregateInfo> aggregates;
        for (int i = 0; i < (two_aggregates ? 2 : 1); ++i) {
            aggregates.push_back(
                {std::make_unique<ObservedStringAggregate>(minimum(), counts),
                 {1},
                 static_cast<column_index_t>((grouped() ? 1 : 0) + i)});
        }
        auto input_type = std::make_shared<RowType>(
            std::vector<std::string>{"key", "value"},
            std::vector<DataType>{DataType::INT64, DataType::VARCHAR});
        return std::make_unique<GroupingSet>(
            input_type, std::move(hashers), std::move(aggregates));
    }

    RowVectorPtr
    makeInput() {
        auto keys = std::make_shared<ColumnVector>(DataType::INT64, 3);
        auto values = std::make_shared<ColumnVector>(DataType::VARCHAR, 3);
        for (int i = 0; i < 3; ++i) {
            keys->SetValueAt<int64_t>(i, i == 1 ? 2 : 1);
            values->SetValueAt<std::string>(
                i, std::string(256, i == 0 ? 'z' : (i == 1 ? 'm' : 'a')));
        }
        return std::make_shared<RowVector>(
            std::vector<VectorPtr>{keys, values});
    }

    RowVectorPtr
    makeOutput() {
        std::vector<DataType> types;
        if (grouped()) {
            types.push_back(DataType::INT64);
        }
        types.push_back(DataType::VARCHAR);
        return std::make_shared<RowVector>(types, groupCount());
    }
};

INSTANTIATE_TEST_SUITE_P(MinMaxGroupedAndGlobal,
                         StringAggregateCleanupTest,
                         testing::Combine(testing::Bool(), testing::Bool()));

TEST_P(StringAggregateCleanupTest, DestroysStateWithoutOutput) {
    CleanupCounts counts;
    auto groups = makeGroupingSet(counts);
    groups->addInput(makeInput());
    groups.reset();
    EXPECT_EQ(counts.rows, groupCount());
    EXPECT_EQ(counts.strings, groupCount());
}

TEST_P(StringAggregateCleanupTest, SuccessfulOutputDoesNotDoubleFree) {
    CleanupCounts counts;
    auto groups = makeGroupingSet(counts);
    groups->addInput(makeInput());
    auto output = makeOutput();
    ASSERT_TRUE(groups->getOutput(output));
    groups.reset();
    EXPECT_EQ(counts.rows, groupCount());
    EXPECT_EQ(counts.strings, 0);

    auto values = std::dynamic_pointer_cast<ColumnVector>(
        output->child(grouped() ? 1 : 0));
    for (size_t i = 0; i < groupCount(); ++i) {
        char expected = minimum() ? 'a' : 'z';
        if (grouped()) {
            auto keys =
                std::dynamic_pointer_cast<ColumnVector>(output->child(0));
            if (keys->ValueAt<int64_t>(i) == 2) {
                expected = 'm';
            }
        }
        EXPECT_EQ(values->ValueAt<std::string>(i), std::string(256, expected));
    }
}

TEST_P(StringAggregateCleanupTest, FailedOutputStillDestroysState) {
    CleanupCounts counts;
    auto groups = makeGroupingSet(counts, true);
    groups->addInput(makeInput());
    // The first aggregate extracts and frees its state. The second output
    // vector triggers the existing assertion, leaving its state for teardown.
    std::vector<VectorPtr> columns;
    if (grouped()) {
        columns.push_back(
            std::make_shared<ColumnVector>(DataType::INT64, groupCount()));
    }
    columns.push_back(
        std::make_shared<ColumnVector>(DataType::VARCHAR, groupCount()));
    columns.push_back(std::make_shared<RowVector>(std::vector<VectorPtr>{}));
    auto output = std::make_shared<RowVector>(std::move(columns));
    EXPECT_THROW(groups->getOutput(output), SegcoreError);
    groups.reset();
    EXPECT_EQ(counts.rows, groupCount() * 2);
    EXPECT_EQ(counts.strings, groupCount());
}

TEST_P(StringAggregateCleanupTest, NullGroupsNeedNoStringCleanup) {
    CleanupCounts counts;
    auto groups = makeGroupingSet(counts);
    auto input = makeInput();
    auto values = std::dynamic_pointer_cast<ColumnVector>(input->child(1));
    for (size_t i = 0; i < values->size(); ++i) {
        values->nullAt(i);
    }
    groups->addInput(input);
    groups.reset();
    EXPECT_EQ(counts.rows, groupCount());
    EXPECT_EQ(counts.strings, 0);
}

TEST_P(StringAggregateCleanupTest, DestroysZeroInitializedState) {
    CleanupCounts counts;
    auto groups = makeGroupingSet(counts);
    if (grouped()) {
        groups->createHashTable();
    } else {
        groups->initializeGlobalAggregation();
    }
    groups.reset();
    EXPECT_EQ(counts.rows, grouped() ? 0 : 1);
    EXPECT_EQ(counts.strings, 0);

    ObservedStringAggregate aggregate(minimum(), counts);
    aggregate.setOffsets(8, 0, 1, 0);
    alignas(std::string*) char row[32]{};
    // A freshly allocated row can be visited by teardown before its
    // initializeNewGroups call, or after an earlier cleanup.
    aggregate.destroy(row);
    aggregate.destroy(row);
    EXPECT_EQ(counts.strings, 0);
}

}  // namespace
}  // namespace milvus::exec
