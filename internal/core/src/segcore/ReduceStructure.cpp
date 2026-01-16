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

#include "segcore/ReduceStructure.h"
#include "segcore/ReduceUtils.h"
#include <variant>
#include <cmath>

int
SearchResultPairComparator::CompareGroupByValue(
    const milvus::GroupByValueType& lhs, const milvus::GroupByValueType& rhs) {
    return ::CompareGroupByValue(lhs, rhs);
}

// Use the shared CompareOrderByValue from milvus::segcore namespace
using milvus::segcore::CompareOrderByValue;

int
CompareGroupByValue(const milvus::GroupByValueType& lhs,
                    const milvus::GroupByValueType& rhs) {
    if (!lhs.has_value() && !rhs.has_value()) {
        return 0;
    }
    if (!lhs.has_value()) {
        return -1;  // null < non-null
    }
    if (!rhs.has_value()) {
        return 1;  // non-null > null
    }

    const auto& lv = lhs.value();
    const auto& rv = rhs.value();

    // Compare based on variant type
    if (std::holds_alternative<bool>(lv) && std::holds_alternative<bool>(rv)) {
        auto l = std::get<bool>(lv);
        auto r = std::get<bool>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<int8_t>(lv) &&
        std::holds_alternative<int8_t>(rv)) {
        auto l = std::get<int8_t>(lv);
        auto r = std::get<int8_t>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<int16_t>(lv) &&
        std::holds_alternative<int16_t>(rv)) {
        auto l = std::get<int16_t>(lv);
        auto r = std::get<int16_t>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<int32_t>(lv) &&
        std::holds_alternative<int32_t>(rv)) {
        auto l = std::get<int32_t>(lv);
        auto r = std::get<int32_t>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<int64_t>(lv) &&
        std::holds_alternative<int64_t>(rv)) {
        auto l = std::get<int64_t>(lv);
        auto r = std::get<int64_t>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    if (std::holds_alternative<std::string>(lv) &&
        std::holds_alternative<std::string>(rv)) {
        auto l = std::get<std::string>(lv);
        auto r = std::get<std::string>(rv);
        if (l < r)
            return -1;
        if (l > r)
            return 1;
        return 0;
    }
    // Type mismatch or unsupported
    return 0;
}
