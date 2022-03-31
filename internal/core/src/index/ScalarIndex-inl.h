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

#include <string>
#include <iostream>
#include "index/Meta.h"

namespace milvus::scalar {
template <typename T>
const TargetBitmapPtr
ScalarIndex<T>::Query(const DatasetPtr& dataset) {
    auto op = dataset->Get<OperatorType>(OPERATOR_TYPE);
    switch (op) {
        case LT:
        case LE:
        case GT:
        case GE: {
            auto value = dataset->Get<T>(RANGE_VALUE);
            return Range(value, op);
        }

        case RangeOp: {
            auto lower_bound_value = dataset->Get<T>(LOWER_BOUND_VALUE);
            auto upper_bound_value = dataset->Get<T>(UPPER_BOUND_VALUE);
            auto lower_bound_inclusive = dataset->Get<bool>(LOWER_BOUND_INCLUSIVE);
            auto upper_bound_inclusive = dataset->Get<bool>(UPPER_BOUND_INCLUSIVE);
            return Range(lower_bound_value, lower_bound_inclusive, upper_bound_value, upper_bound_inclusive);
        }

        case InOp: {
            auto n = dataset->Get<int64_t>(knowhere::meta::ROWS);
            auto values = dataset->Get<const void*>(knowhere::meta::TENSOR);
            return In(n, reinterpret_cast<const T*>(values));
        }

        case NotInOp: {
            auto n = dataset->Get<int64_t>(knowhere::meta::ROWS);
            auto values = dataset->Get<const void*>(knowhere::meta::TENSOR);
            return NotIn(n, reinterpret_cast<const T*>(values));
        }

        case PrefixMatchOp:
        case PostfixMatchOp:
        default:
            throw std::invalid_argument(std::string("unsupported operator type: " + std::to_string(op)));
    }
}
}  // namespace milvus::scalar
