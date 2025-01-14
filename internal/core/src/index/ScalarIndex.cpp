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

#include <iostream>
#include <string>
#include <vector>

#include "index/Meta.h"
#include "knowhere/dataset.h"
#include "common/Types.h"
#include "index/ScalarIndex.h"

namespace milvus::index {
template <typename T>
const TargetBitmap
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
            auto lower_bound_inclusive =
                dataset->Get<bool>(LOWER_BOUND_INCLUSIVE);
            auto upper_bound_inclusive =
                dataset->Get<bool>(UPPER_BOUND_INCLUSIVE);
            return Range(lower_bound_value,
                         lower_bound_inclusive,
                         upper_bound_value,
                         upper_bound_inclusive);
        }

        case OpType::In: {
            auto n = dataset->GetRows();
            auto values = dataset->GetTensor();
            return In(n, reinterpret_cast<const T*>(values));
        }

        case OpType::NotIn: {
            auto n = dataset->GetRows();
            auto values = dataset->GetTensor();
            return NotIn(n, reinterpret_cast<const T*>(values));
        }

        case OpType::PrefixMatch:
        case OpType::PostfixMatch:
        default:
            PanicInfo(OpTypeInvalid,
                      fmt::format("unsupported operator type: {}", op));
    }
}

template <>
void
ScalarIndex<std::string>::BuildWithRawDataForUT(size_t n,
                                           const void* values,
                                           const Config& config) {
    proto::schema::StringArray arr;
    auto ok = arr.ParseFromArray(values, n);
    Assert(ok);

    // TODO :: optimize here. avoid memory copy.
    std::vector<std::string> vecs{arr.data().begin(), arr.data().end()};
    Build(arr.data_size(), vecs.data());
}

template <>
void
ScalarIndex<bool>::BuildWithRawDataForUT(size_t n,
                                    const void* values,
                                    const Config& config) {
    proto::schema::BoolArray arr;
    auto ok = arr.ParseFromArray(values, n);
    Assert(ok);
    Build(arr.data_size(), arr.data().data());
}

template <>
void
ScalarIndex<int8_t>::BuildWithRawDataForUT(size_t n,
                                      const void* values,
                                      const Config& config) {
    auto data = reinterpret_cast<int8_t*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<int16_t>::BuildWithRawDataForUT(size_t n,
                                       const void* values,
                                       const Config& config) {
    auto data = reinterpret_cast<int16_t*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<int32_t>::BuildWithRawDataForUT(size_t n,
                                       const void* values,
                                       const Config& config) {
    auto data = reinterpret_cast<int32_t*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<int64_t>::BuildWithRawDataForUT(size_t n,
                                       const void* values,
                                       const Config& config) {
    auto data = reinterpret_cast<int64_t*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<float>::BuildWithRawDataForUT(size_t n,
                                     const void* values,
                                     const Config& config) {
    auto data = reinterpret_cast<float*>(const_cast<void*>(values));
    Build(n, data);
}

template <>
void
ScalarIndex<double>::BuildWithRawDataForUT(size_t n,
                                      const void* values,
                                      const Config& config) {
    auto data = reinterpret_cast<double*>(const_cast<void*>(values));
    Build(n, data);
}

template class ScalarIndex<bool>;
template class ScalarIndex<int8_t>;
template class ScalarIndex<int16_t>;
template class ScalarIndex<int32_t>;
template class ScalarIndex<int64_t>;
template class ScalarIndex<float>;
template class ScalarIndex<double>;
template class ScalarIndex<std::string>;
}  // namespace milvus::index
