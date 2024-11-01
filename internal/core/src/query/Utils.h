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

#include <limits>
#include <string>

#include "common/Utils.h"

namespace milvus::query {

template <typename T, typename U>
inline bool
Match(const T& x, const U& y, OpType op) {
    PanicInfo(NotImplemented, "not supported");
}

template <>
inline bool
Match<std::string>(const std::string& str, const std::string& val, OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::PrefixNotMatch:
            return !PrefixMatch(str, val);
        case OpType::PostfixNotMatch:
            return !PostfixMatch(str, val);
        default:
            PanicInfo(OpTypeInvalid, "not supported");
    }
}

template <>
inline bool
Match<std::string_view>(const std::string_view& str,
                        const std::string& val,
                        OpType op) {
    switch (op) {
        case OpType::PrefixMatch:
            return PrefixMatch(str, val);
        case OpType::PostfixMatch:
            return PostfixMatch(str, val);
        case OpType::PrefixNotMatch:
            return !PrefixMatch(str, val);
        case OpType::PostfixNotMatch:
            return !PostfixMatch(str, val);
        default:
            PanicInfo(OpTypeInvalid, "not supported");
    }
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
gt_ub(int64_t t) {
    return t > std::numeric_limits<T>::max();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
lt_lb(int64_t t) {
    return t < std::numeric_limits<T>::min();
}

template <typename T, typename = std::enable_if_t<std::is_integral_v<T>>>
inline bool
out_of_range(int64_t t) {
    return gt_ub<T>(t) || lt_lb<T>(t);
}

inline bool
dis_closer(float dis1, float dis2, const MetricType& metric_type) {
    if (PositivelyRelated(metric_type))
        return dis1 > dis2;
    return dis1 < dis2;
}

}  // namespace milvus::query
