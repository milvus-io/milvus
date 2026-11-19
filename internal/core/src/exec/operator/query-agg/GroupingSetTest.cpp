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
#include "exec/SortBuffer.h"
#include "exec/operator/query-agg/CountAggregateBase.h"
#include "exec/operator/query-agg/SumAggregateBase.h"
#include "exec/operator/query-agg/GroupingSet.h"
#include "exec/operator/query-agg/MaxAggregateBase.h"
#include "exec/operator/query-agg/MinAggregateBase.h"

namespace milvus::exec {
namespace {

// A test-only logical vector: no flat buffer, duplicate/out-of-order rows,
// and NULLs inherited through the mapping. Production DictionaryVector is a
// separate change; consumers must already work through SimpleVector here.
template <typename T>
class MappedTestVector final : public SimpleVector<T> {
 public:
    MappedTestVector(SimpleVectorPtr<T> base, std::vector<size_t> indices)
        : SimpleVector<T>(base->type(), indices.size()),
          base_(std::move(base)),
          indices_(std::move(indices)) {
    }

    typename SimpleVector<T>::TValueAt
    ValueAt(size_t index) const override {
        return base_->ValueAt(indices_.at(index));
    }

    bool
    ValidAt(size_t index) const override {
        return base_->ValidAt(indices_.at(index));
    }

    VectorPtr
    cloneEmpty(vector_size_t size) const override {
        return std::make_shared<MappedTestVector<T>>(
            base_, std::vector<size_t>(size, 0));
    }

 private:
    SimpleVectorPtr<T> base_;
    std::vector<size_t> indices_;
};

class SimpleVectorAggregationTest : public testing::TestWithParam<bool> {};

INSTANTIATE_TEST_SUITE_P(GroupedAndGlobal,
                         SimpleVectorAggregationTest,
                         testing::Bool());

TEST_P(SimpleVectorAggregationTest, MappedAndConstantInputsAcrossBatches) {
    const bool grouped = GetParam();
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    if (grouped) {
        hashers.push_back(VectorHasher::create(DataType::VARCHAR, 0));
    }
    std::vector<AggregateInfo> aggregates;
    column_index_t channel = grouped ? 1 : 0;
    aggregates.push_back(
        {std::make_unique<SumAggregateBase<int64_t, int64_t, int64_t, false>>(
             DataType::INT64),
         {1},
         channel++});
    aggregates.push_back(
        {std::make_unique<MinAggregateBase<int64_t, int64_t, int64_t>>(
             DataType::INT64),
         {1},
         channel++});
    aggregates.push_back(
        {std::make_unique<MaxAggregateBase<int64_t, int64_t, int64_t>>(
             DataType::INT64),
         {1},
         channel++});
    aggregates.push_back({std::make_unique<CountAggregate>(), {1}, channel++});
    aggregates.push_back({std::make_unique<CountAggregate>(), {}, channel++});
    aggregates.push_back(
        {std::make_unique<MinStringAggregate>(DataType::VARCHAR),
         {2},
         channel++});
    aggregates.push_back(
        {std::make_unique<MaxStringAggregate>(DataType::VARCHAR),
         {2},
         channel++});
    aggregates.push_back({std::make_unique<CountAggregate>(), {2}, channel++});
    auto type = std::make_shared<RowType>(
        std::vector<std::string>{"key", "number", "text"},
        std::vector<DataType>{
            DataType::VARCHAR, DataType::INT64, DataType::VARCHAR});
    GroupingSet groups(type, std::move(hashers), std::move(aggregates));
    const std::string a(256, 'a'), b(256, 'b'), m(256, 'm'), z(256, 'z');
    std::weak_ptr<BaseVector> borrowed;
    {
        auto keys =
            std::make_shared<ColumnVector<std::string>>(DataType::VARCHAR, 4);
        auto numbers =
            std::make_shared<ColumnVector<int64_t>>(DataType::INT64, 4);
        auto strings =
            std::make_shared<ColumnVector<std::string>>(DataType::VARCHAR, 4);
        keys->SetValueAt(0, "unselected");
        keys->SetValueAt(1, b);
        keys->SetValueAt(2, a);
        keys->nullAt(3);
        numbers->SetValueAt(0, 999);
        numbers->SetValueAt(1, 20);
        numbers->SetValueAt(2, 10);
        numbers->nullAt(3);
        strings->SetValueAt(0, "unselected");
        strings->SetValueAt(1, z);
        strings->SetValueAt(2, a);
        strings->nullAt(3);
        borrowed = keys;
        const std::vector<size_t> indices{2, 1, 2, 3, 3};
        groups.addInput(std::make_shared<RowVector>(std::vector<VectorPtr>{
            std::make_shared<MappedTestVector<std::string>>(keys, indices),
            std::make_shared<MappedTestVector<int64_t>>(numbers, indices),
            std::make_shared<MappedTestVector<std::string>>(strings,
                                                            indices)}));
    }
    groups.addInput(std::make_shared<RowVector>(std::vector<VectorPtr>{
        std::make_shared<ConstantVector<std::string>>(DataType::VARCHAR, 2, a),
        std::make_shared<ConstantVector<int64_t>>(DataType::INT64, 2, 5),
        std::make_shared<ConstantVector<std::string>>(
            DataType::VARCHAR, 2, m)}));
    EXPECT_TRUE(borrowed.expired());
    groups.addInput(std::make_shared<RowVector>(std::vector<VectorPtr>{
        std::make_shared<ConstantVector<std::string>>(
            DataType::VARCHAR, 2, "", 2),
        std::make_shared<ConstantVector<int64_t>>(DataType::INT64, 2, 0, 2),
        std::make_shared<ConstantVector<std::string>>(
            DataType::VARCHAR, 2, "", 2)}));

    std::vector<DataType> output_types{DataType::INT64,
                                       DataType::INT64,
                                       DataType::INT64,
                                       DataType::INT64,
                                       DataType::INT64,
                                       DataType::VARCHAR,
                                       DataType::VARCHAR,
                                       DataType::INT64};
    if (grouped) {
        output_types.insert(output_types.begin(), DataType::VARCHAR);
    }
    auto output = std::make_shared<RowVector>(output_types, grouped ? 3 : 1);
    ASSERT_TRUE(groups.getOutput(output));
    ASSERT_EQ(output->size(), grouped ? 3 : 1);
    for (size_t i = 0; i < output->size(); ++i) {
        bool null_key = false, key_a = false;
        if (grouped) {
            const auto keys = AsSimpleVector<std::string>(output->child(0));
            null_key = !keys->ValidAt(i);
            key_a = !null_key && keys->ValueAt(i) == a;
            if (!null_key && !key_a)
                EXPECT_EQ(keys->ValueAt(i), b);
        }
        const size_t first = grouped ? 1 : 0;
        auto count = [&](size_t column) {
            return AsSimpleVector<int64_t>(output->child(first + column))
                ->ValueAt(i);
        };
        EXPECT_EQ(count(3), !grouped ? 5 : null_key ? 0 : key_a ? 4 : 1);
        EXPECT_EQ(count(4), !grouped ? 9 : null_key ? 4 : key_a ? 4 : 1);
        EXPECT_EQ(count(7), count(3));
        for (size_t col : {0, 1, 2, 5, 6}) {
            VisitSimpleVector(output->child(first + col),
                              [&](const auto* values) {
                                  EXPECT_EQ(values->ValidAt(i), !null_key);
                              });
        }
        if (!null_key) {
            EXPECT_EQ(count(0), !grouped ? 50 : key_a ? 30 : 20);
            EXPECT_EQ(count(1), !grouped || key_a ? 5 : 20);
            EXPECT_EQ(count(2), !grouped || !key_a ? 20 : 10);
            EXPECT_EQ(AsSimpleVector<std::string>(output->child(first + 5))
                          ->ValueAt(i),
                      !grouped || key_a ? a : z);
            EXPECT_EQ(AsSimpleVector<std::string>(output->child(first + 6))
                          ->ValueAt(i),
                      !grouped || !key_a ? z : m);
        }
    }
}

TEST(SimpleVectorAggregationTest, MappedKeysWithForcedHashCollisions) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.push_back(VectorHasher::create(DataType::VARCHAR, 0));
    HashTable table(std::move(hashers), std::vector<Accumulator>{}, 100);
    auto base =
        std::make_shared<ColumnVector<std::string>>(DataType::VARCHAR, 3);
    base->SetValueAt(0, std::string(128, 'a'));
    base->SetValueAt(1, std::string(128, 'b'));
    base->nullAt(2);
    auto mapped = std::make_shared<MappedTestVector<std::string>>(
        base, std::vector<size_t>{1, 0, 1, 2, 2, 0});
    auto input = std::make_shared<RowVector>(std::vector<VectorPtr>{mapped});
    HashLookup lookup(table.hashers());
    for (int batch = 0; batch < 2; ++batch) {
        table.prepareForGroupProbe(lookup, input);
        std::fill(lookup.hashes_.begin(), lookup.hashes_.end(), 42);
        table.groupProbe(lookup);
        ASSERT_EQ(table.rows()->allRows().size(), 3);
        EXPECT_EQ(lookup.hits_[0], lookup.hits_[2]);
        EXPECT_EQ(lookup.hits_[1], lookup.hits_[5]);
        EXPECT_EQ(lookup.hits_[3], lookup.hits_[4]);
        EXPECT_NE(lookup.hits_[0], lookup.hits_[1]);
        EXPECT_NE(lookup.hits_[0], lookup.hits_[3]);
        if (batch)
            EXPECT_TRUE(lookup.newGroups_.empty());
    }
}

TEST(SimpleVectorAggregationTest, SortBindsLogicalColumnsAtBatchBoundary) {
    auto base = std::make_shared<ColumnVector<int64_t>>(DataType::INT64, 5);
    for (size_t i = 0; i < 5; ++i) base->SetValueAt(i, i);
    base->nullAt(2);
    auto mapped = std::make_shared<MappedTestVector<int64_t>>(
        base, std::vector<size_t>{4, 3, 2, 1, 0, 0});
    for (int64_t limit : {-1, 2}) {
        SortBuffer sort({DataType::INT64}, {{0, true, false}}, limit);
        sort.AddRows({mapped}, mapped->size());
        sort.AddRow(
            {std::make_shared<ConstantVector<int64_t>>(DataType::INT64, 1, -1)},
            0);
        sort.NoMoreInput();
        auto output = sort.GetOutput(10);
        ASSERT_EQ(output.size(), 1);
        const auto values = AsSimpleVector<int64_t>(output[0]);
        ASSERT_EQ(values->size(), limit < 0 ? 7 : 2);
        EXPECT_EQ(values->ValueAt(0), -1);
        EXPECT_EQ(values->ValueAt(1), 0);
        if (limit < 0)
            EXPECT_FALSE(values->ValidAt(6));
    }
    SortBuffer sort({DataType::INT64}, {{0, true, false}}, 1);
    EXPECT_THROW(sort.AddRows({std::make_shared<ConstantVector<double>>(
                                  DataType::DOUBLE, 2, 0)},
                              2),
                 SegcoreError);
    EXPECT_EQ(sort.NumInputRows(), 0);
}

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
        auto keys = std::make_shared<ColumnVector<int64_t>>(DataType::INT64, 3);
        auto values =
            std::make_shared<ColumnVector<std::string>>(DataType::VARCHAR, 3);
        for (int i = 0; i < 3; ++i) {
            keys->SetValueAt(i, i == 1 ? 2 : 1);
            values->SetValueAt(
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

    auto values = AsColumnVector<std::string>(output->child(grouped() ? 1 : 0));
    for (size_t i = 0; i < groupCount(); ++i) {
        char expected = minimum() ? 'a' : 'z';
        if (grouped()) {
            auto keys = AsColumnVector<int64_t>(output->child(0));
            if (keys->ValueAt(i) == 2) {
                expected = 'm';
            }
        }
        EXPECT_EQ(values->ValueAt(i), std::string(256, expected));
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
        columns.push_back(std::make_shared<ColumnVector<int64_t>>(
            DataType::INT64, groupCount()));
    }
    columns.push_back(std::make_shared<ColumnVector<std::string>>(
        DataType::VARCHAR, groupCount()));
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
    auto values = AsColumnVector<std::string>(input->child(1));
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
