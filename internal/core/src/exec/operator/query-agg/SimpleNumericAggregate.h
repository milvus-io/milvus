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

#include "Aggregate.h"
namespace milvus {
namespace exec {
template <typename TInput, typename TAccumulator, typename TResult>
class SimpleNumericAggregate : public exec::Aggregate {
 protected:
    explicit SimpleNumericAggregate(DataType resultType)
        : Aggregate(resultType) {
    }

    // TData is either TAccumulator or TResult, which in most cases are the same,
    // but for sum(real) can differ.
    template <typename TData = TResult, typename ExtractOneValue>
    void
    doExtractValues(char** groups,
                    int32_t numGroups,
                    VectorPtr* result,
                    ExtractOneValue extractOneValue) {
        AssertInfo((*result)->elementSize() == sizeof(TData),
                   "Incorrect type size of input result vector");
        ColumnVectorPtr result_column =
            std::dynamic_pointer_cast<ColumnVector>(*result);
        AssertInfo(result_column != nullptr,
                   "input vector for extracting aggregation must be of Type "
                   "ColumnVector");
        result_column->resize(numGroups);
        TData* rawValues = static_cast<TData*>(result_column->GetRawData());
        for (auto i = 0; i < numGroups; i++) {
            char* group = groups[i];
            if (isNull(group)) {
                result_column->nullAt(i);
            } else {
                result_column->clearNullAt(i);
                rawValues[i] = extractOneValue(group);
            }
        }
    }

    template <bool tableHasNulls,
              typename TData = TResult,
              typename TValue = TInput,
              typename UpdateSingle>
    void
    updateOneGroupAtIndex(char* group,
                          const ColumnVectorPtr& column_data,
                          int32_t index,
                          UpdateSingle updateSingleValue) {
        if (column_data->ValidAt(index)) {
            updateNonNullValue<tableHasNulls, TData>(
                group,
                TData(column_data->ValueAt<TValue>(index)),
                updateSingleValue);
        }
    }

    template <typename TData = TResult,
              typename TValue = TInput,
              typename UpdateSingle>
    void
    updateOneGroup(char* group,
                   const VectorPtr& vector,
                   UpdateSingle updateSingleValue) {
        auto column_data = std::dynamic_pointer_cast<ColumnVector>(vector);
        AssertInfo(
            column_data != nullptr,
            "input column data for upgrading groups should not be nullptr");
        for (auto i = 0; i < column_data->size(); i++) {
            updateOneGroupAtIndex<true, TData, TValue>(
                group, column_data, i, updateSingleValue);
        }
    }

    template <bool tableHasNulls,
              typename TData = TResult,
              typename TValue = TInput,
              typename UpdateSingleValue>
    void
    updateGroups(char** groups,
                 const VectorPtr& vector,
                 UpdateSingleValue updateSingleValue) {
        auto column_data = std::dynamic_pointer_cast<ColumnVector>(vector);
        AssertInfo(
            column_data != nullptr,
            "input column data for upgrading groups should not be nullptr");
        for (auto i = 0; i < column_data->size(); i++) {
            updateOneGroupAtIndex<tableHasNulls, TData, TValue>(
                groups[i], column_data, i, updateSingleValue);
        }
    }

    template <bool tableHasNulls,
              typename TDataType = TAccumulator,
              typename Update>
    inline void
    updateNonNullValue(char* group, TDataType value, Update updateValue) {
        if constexpr (tableHasNulls) {
            Aggregate::clearNull(group);
        }
        updateValue(*Aggregate::value<TDataType>(group), value);
    }
};

}  // namespace exec
}  // namespace milvus