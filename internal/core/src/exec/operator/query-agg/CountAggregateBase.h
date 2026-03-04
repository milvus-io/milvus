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

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "SimpleNumericAggregate.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "fmt/core.h"
#include "folly/Range.h"

namespace milvus {
namespace exec {
class CountAggregate : public SimpleNumericAggregate<bool, int64_t, int64_t> {
    using BaseAggregate = SimpleNumericAggregate<bool, int64_t, int64_t>;

 public:
    explicit CountAggregate() : BaseAggregate(DataType::INT64) {
    }

    int32_t
    accumulatorFixedWidthSize() const override {
        return sizeof(int64_t);
    }

    void
    extractValues(char** groups,
                  int32_t numGroups,
                  VectorPtr* result) override {
        BaseAggregate::doExtractValues(
            groups, numGroups, result, [&](char* group) {
                return *value<int64_t>(group);
            });
    }

    void
    addRawInput(char** groups,
                int numGroups,
                const std::vector<VectorPtr>& input) override {
        if (!input.empty()) {
            ColumnVectorPtr input_column = nullptr;
            AssertInfo(input.size() == 1,
                       fmt::format("input column count for count aggregation "
                                   "must be one , but got:{}",
                                   input.size()));
            input_column = std::dynamic_pointer_cast<ColumnVector>(input[0]);
            AssertInfo(input_column != nullptr,
                       "input[0] must be ColumnVector for count aggregation");
            for (auto i = 0; i < input_column->size(); i++) {
                if (input_column->ValidAt(i)) {
                    addToGroup(groups[i], 1);
                }
            }
            return;
        }
        for (auto i = 0; i < numGroups; i++) {
            addToGroup(groups[i], 1);
        }
    }

    void
    addSingleGroupRawInput(char* group,
                           const std::vector<VectorPtr>& input) override {
        AssertInfo(input.size() == 1,
                   fmt::format("input column count for count aggregation "
                               "must be exactly one for now, but got:{}",
                               input.size()));
        const auto& column = std::dynamic_pointer_cast<ColumnVector>(input[0]);
        AssertInfo(column != nullptr,
                   "input[0] must be ColumnVector for count aggregation");
        if (column->IsBitmap()) {
            // Use validity bitmap to count non-null values
            BitsetTypeView view(column->GetRawData(), column->size());
            addToGroup(group, view.size() - view.count());
        } else {
            for (auto i = 0; i < column->size(); i++) {
                if (column->ValidAt(i)) {
                    addToGroup(group, 1);
                }
            }
        }
    }

    void
    initializeNewGroupsInternal(
        char** groups, folly::Range<const vector_size_t*> indices) override {
        // no need to set nulls for count aggregate as count result is always a number
        for (auto i : indices) {
            // initialized result of count is always zero
            *value<int64_t>(groups[i]) = static_cast<int64_t>(0);
        }
    }

 private:
    inline void
    addToGroup(char* group, int64_t count) {
        *value<int64_t>(group) += count;
    }
};

void
registerCount(const std::string& name);

void
registerCountAggregate();

}  // namespace exec
}  // namespace milvus
