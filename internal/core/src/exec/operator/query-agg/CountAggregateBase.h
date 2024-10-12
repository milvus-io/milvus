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
                const TargetBitmapView& activeRows,
                const std::vector<VectorPtr>& input) override {
        ColumnVectorPtr input_column = nullptr;
        AssertInfo(input.empty() || input.size() == 1,
                   fmt::format("input column count for count aggregation "
                               "must be one or zero for now, but got:{}",
                               input.size()));
        if (input.size() == 1) {
            input_column = std::dynamic_pointer_cast<ColumnVector>(input[0]);
        }
        auto start = -1;
        do {
            auto next_active_idx = activeRows.find_next(start);
            if (!next_active_idx.has_value()) {
                break;
            }
            auto active_idx = next_active_idx.value();
            if ((input_column && input_column->ValidAt(active_idx)) ||
                !input_column) {
                addToGroup(groups[active_idx], 1);
            }
            start = active_idx;
        } while (true);
    }

    void
    addSingleGroupRawInput(char* group,
                           const TargetBitmapView& activeRows,
                           const std::vector<VectorPtr>& input) override {
        AssertInfo(input.size() == 1,
                   fmt::format("input column count for count aggregation "
                               "must be exactly one for now, but got:{}",
                               input.size()));
        const auto& column = std::dynamic_pointer_cast<ColumnVector>(input[0]);
        if (column->IsBitmap()) {
            BitsetTypeView view(column->GetRawData(), column->size());
            auto cnt = view.size() - view.count();
            addToGroup(group, cnt);
        } else {
            auto start = -1;
            do {
                auto next_active_idx = activeRows.find_next(start);
                if (!next_active_idx.has_value()) {
                    break;
                }
                auto active_idx = next_active_idx.value();
                if (column->ValidAt(active_idx)) {
                    addToGroup(group, 1);
                }
                start = active_idx;
            } while (true);
        }
    }

    void
    initializeNewGroupsInternal(
        char** groups, folly::Range<const vector_size_t*> indices) override {
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

}  // namespace exec
}  // namespace milvus
