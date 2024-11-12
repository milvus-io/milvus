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

namespace milvus{
namespace exec {
    template <typename TInput,
            typename TAccumulator,
            typename ResultType,
            bool Overflow>
class SumAggregateBase: public SimpleNumericAggregate<TInput, TAccumulator, ResultType> {
    using BaseAggregate = SimpleNumericAggregate<TInput, TAccumulator, ResultType>;

public:
    explicit SumAggregateBase(DataType resultType): BaseAggregate(resultType){};

    constexpr int32_t accumulatorFixedWidthSize() const override {
        return sizeof(TAccumulator);
    }

    constexpr int32_t accumulatorAlignmentSize() const override {
        return 1;
    }

    void extractValues(char** groups, int32_t numGroups, VectorPtr* result) override{
        BaseAggregate::template doExtractValues<TAccumulator>(
            groups, numGroups, result, [&](char* group) {
                return (ResultType)(*BaseAggregate::Aggregate::template value<TAccumulator>(group));
            });
    }

    void initializeNewGroupsInternal(char** groups, folly::Range<const vector_size_t*> indices) override {

    }
};
}
}
