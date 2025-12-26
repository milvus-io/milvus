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
#pragma once
#include "SimpleNumericAggregate.h"
#include "common/Utils.h"

namespace milvus {
namespace exec {
template <typename TInput,
          typename TAccumulator,
          typename ResultType,
          bool Overflow>
class SumAggregateBase
    : public SimpleNumericAggregate<TInput, TAccumulator, ResultType> {
    using BaseAggregate =
        SimpleNumericAggregate<TInput, TAccumulator, ResultType>;

 public:
    explicit SumAggregateBase(DataType resultType)
        : BaseAggregate(resultType){};

    constexpr int32_t
    accumulatorFixedWidthSize() const override {
        return sizeof(TAccumulator);
    }

    constexpr int32_t
    accumulatorAlignmentSize() const override {
        return static_cast<int32_t>(alignof(TAccumulator));
    }

    void
    extractValues(char** groups,
                  int32_t numGroups,
                  VectorPtr* result) override {
        BaseAggregate::template doExtractValues<TAccumulator>(
            groups, numGroups, result, [&](char* group) {
                return (ResultType)(*BaseAggregate::Aggregate::template value<
                                    TAccumulator>(group));
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
        Aggregate::setAllNulls(groups, indices);
        for (auto i : indices) {
            (*Aggregate::value<TAccumulator>(groups[i])) = 0;
        }
    }

 protected:
    template <typename TData, typename TValue = TInput>
    void
    updateInternal(char** groups, const std::vector<VectorPtr>& input) {
        const auto& input_column = input[0];
        if (Aggregate::numNulls_) {
            BaseAggregate::template updateGroups<true, TData, TValue>(
                groups, input_column, &updateSingleValue<TData>);
        } else {
            BaseAggregate::template updateGroups<false, TData, TValue>(
                groups, input_column, &updateSingleValue<TData>);
        }
    }

 private:
    template <typename TData>
#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
    FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("signed-integer-overflow")
#endif
    static void updateSingleValue(TData& result, TData value) {
        if constexpr (std::is_same_v<TData, double> ||
                      std::is_same_v<TData, float> ||
                      std::is_same_v<TData, int64_t> && Overflow) {
            result += value;
        } else {
            result = checkPlus(result, value);
        }
    }
};

void
registerSumAggregate();
}  // namespace exec
}  // namespace milvus
