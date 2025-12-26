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

#pragma once

#include <limits>
#include <string>
#include <type_traits>
#include <vector>

#include "SimpleNumericAggregate.h"
#include "common/Utils.h"

namespace milvus {
namespace exec {

template <typename TInput, typename TAccumulator, typename ResultType>
class MaxAggregateBase
    : public SimpleNumericAggregate<TInput, TAccumulator, ResultType> {
    using BaseAggregate =
        SimpleNumericAggregate<TInput, TAccumulator, ResultType>;

 public:
    explicit MaxAggregateBase(DataType resultType) : BaseAggregate(resultType) {
    }

    constexpr int32_t
    accumulatorFixedWidthSize() const override {
        return sizeof(TAccumulator);
    }

    void
    extractValues(char** groups,
                  int32_t numGroups,
                  VectorPtr* result) override {
        BaseAggregate::template doExtractValues<TAccumulator>(
            groups, numGroups, result, [&](char* group) {
                return *BaseAggregate::Aggregate::template value<TAccumulator>(
                    group);
            });
    }

    void
    addRawInput(char** groups,
                int numGroups,
                const std::vector<VectorPtr>& input) override {
        updateInternal<TAccumulator>(groups, input);
    }

    void
    addSingleGroupRawInput(char* group,
                           const std::vector<VectorPtr>& input) override {
        BaseAggregate::template updateOneGroup<TAccumulator>(
            group, input[0], &updateSingleValue<TAccumulator>);
    }

    void
    initializeNewGroupsInternal(
        char** groups, folly::Range<const vector_size_t*> indices) override {
        BaseAggregate::Aggregate::setAllNulls(groups, indices);
        for (auto i : indices) {
            (*BaseAggregate::Aggregate::template value<TAccumulator>(
                groups[i])) = std::numeric_limits<TAccumulator>::lowest();
        }
    }

 protected:
    template <typename TData, typename TValue = TInput>
    void
    updateInternal(char** groups, const std::vector<VectorPtr>& input) {
        const auto& input_column = input[0];
        if (BaseAggregate::Aggregate::numNulls_) {
            BaseAggregate::template updateGroups<true, TData, TValue>(
                groups, input_column, &updateSingleValue<TData>);
        } else {
            BaseAggregate::template updateGroups<false, TData, TValue>(
                groups, input_column, &updateSingleValue<TData>);
        }
    }

 private:
    template <typename TData>
    static void
    updateSingleValue(TData& result, TData value) {
        if (value > result) {
            result = value;
        }
    }
};

// String max aggregate: store pointer to std::string in group row.
// IMPORTANT: This class stores pointers to strings in the input vectors rather
// than copying the string content. The input vectors passed to addRawInput()
// and addSingleGroupRawInput() MUST outlive the aggregation lifecycle until
// extractValues() is called, otherwise the stored pointers will become
// dangling and cause undefined behavior.
class MaxStringAggregate final : public Aggregate {
 public:
    explicit MaxStringAggregate(DataType resultType) : Aggregate(resultType) {
    }

    int32_t
    accumulatorFixedWidthSize() const override {
        return sizeof(const std::string*);
    }

    void
    extractValues(char** groups,
                  int32_t numGroups,
                  VectorPtr* result) override {
        auto result_column = std::dynamic_pointer_cast<ColumnVector>(*result);
        AssertInfo(result_column != nullptr,
                   "input vector for extracting aggregation must be of Type "
                   "ColumnVector");
        result_column->resize(numGroups);
        for (auto i = 0; i < numGroups; i++) {
            char* group = groups[i];
            if (isNull(group)) {
                result_column->nullAt(i);
            } else {
                result_column->clearNullAt(i);
                auto ptr = *value<const std::string*>(group);
                AssertInfo(ptr != nullptr,
                           "max string aggregate should not have null pointer "
                           "when group is not null");
                result_column->SetValueAt<std::string>(i, *ptr);
            }
        }
    }

    // NOTE: The input vector must remain valid until extractValues() is called,
    // as this method stores pointers to strings in the input vector.
    void
    addRawInput(char** groups,
                int numGroups,
                const std::vector<VectorPtr>& input) override {
        AssertInfo(input.size() == 1,
                   "max aggregate expects exactly one input column");
        auto column = std::dynamic_pointer_cast<ColumnVector>(input[0]);
        AssertInfo(column != nullptr,
                   "max aggregate input must be of type ColumnVector");
        auto raw = column->RawAsValues<std::string>();
        for (auto i = 0; i < column->size(); i++) {
            if (!column->ValidAt(i)) {
                continue;
            }
            updateOne(groups[i], &raw[i]);
        }
    }

    // NOTE: The input vector must remain valid until extractValues() is called,
    // as this method stores pointers to strings in the input vector.
    void
    addSingleGroupRawInput(char* group,
                           const std::vector<VectorPtr>& input) override {
        AssertInfo(input.size() == 1,
                   "max aggregate expects exactly one input column");
        auto column = std::dynamic_pointer_cast<ColumnVector>(input[0]);
        AssertInfo(column != nullptr,
                   "max aggregate input must be of type ColumnVector");
        auto raw = column->RawAsValues<std::string>();
        for (auto i = 0; i < column->size(); i++) {
            if (!column->ValidAt(i)) {
                continue;
            }
            updateOne(group, &raw[i]);
        }
    }

    void
    initializeNewGroupsInternal(
        char** groups, folly::Range<const vector_size_t*> indices) override {
        setAllNulls(groups, indices);
        for (auto i : indices) {
            *value<const std::string*>(groups[i]) = nullptr;
        }
    }

 private:
    // Stores a pointer to the candidate string (not a copy).
    // The candidate pointer must remain valid until extractValues() is called.
    inline void
    updateOne(char* group, const std::string* candidate) {
        if (isNull(group)) {
            clearNull(group);
            *value<const std::string*>(group) = candidate;
            return;
        }
        auto& current = *value<const std::string*>(group);
        if (current == nullptr || (*candidate > *current)) {
            current = candidate;
        }
    }
};

void
registerMaxAggregate();

}  // namespace exec
}  // namespace milvus
