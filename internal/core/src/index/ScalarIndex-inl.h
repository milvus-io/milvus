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

#include <iostream>
#include <string>

#include "index/Meta.h"
#include "knowhere/index/vector_index/adapter/VectorAdapter.h"

namespace milvus::scalar {
template <typename T>
const TargetBitmapPtr
ScalarIndex<T>::Query(const DatasetPtr& dataset) {
    auto op = dataset->Get<OpType>(OPERATOR_TYPE);
    switch (op) {
        case OpType::LessThan:
        case OpType::LessEqual:
        case OpType::GreaterThan:
        case OpType::GreaterEqual: {
            auto value = dataset->Get<T>(RANGE_VALUE);
            return Range(value, op);
        }

        case OpType::Range: {
            auto lower_bound_value = dataset->Get<T>(LOWER_BOUND_VALUE);
            auto upper_bound_value = dataset->Get<T>(UPPER_BOUND_VALUE);
            auto lower_bound_inclusive = dataset->Get<bool>(LOWER_BOUND_INCLUSIVE);
            auto upper_bound_inclusive = dataset->Get<bool>(UPPER_BOUND_INCLUSIVE);
            return Range(lower_bound_value, lower_bound_inclusive, upper_bound_value, upper_bound_inclusive);
        }

        case OpType::In: {
            auto n = knowhere::GetDatasetRows(dataset);
            auto values = knowhere::GetDatasetTensor(dataset);
            return In(n, reinterpret_cast<const T*>(values));
        }

        case OpType::NotIn: {
            auto n = knowhere::GetDatasetRows(dataset);
            auto values = knowhere::GetDatasetTensor(dataset);
            return NotIn(n, reinterpret_cast<const T*>(values));
        }

        case OpType::PrefixMatch:
        case OpType::PostfixMatch:
        default:
            throw std::invalid_argument(std::string("unsupported operator type: " + std::to_string(op)));
    }
}
}  // namespace milvus::scalar
