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

#include "VectorHasher.h"

#include <cstddef>

#include "common/BitUtil.h"
#include "common/EasyAssert.h"
#include "common/float_util_c.h"
#include "folly/hash/Hash.h"

namespace milvus {
namespace exec {
std::vector<std::unique_ptr<VectorHasher>>
createVectorHashers(const RowTypePtr& rowType,
                    const std::vector<expr::FieldAccessTypeExprPtr>& exprs) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.reserve(exprs.size());
    for (const auto& expr : exprs) {
        auto column_idx = rowType->GetChildIndex(expr->name());
        hashers.emplace_back(VectorHasher::create(expr->type(), column_idx));
    }
    return hashers;
}

template <DataType Type>
void
VectorHasher::hashValues(const ColumnVectorPtr& column_data,
                         bool mix,
                         uint64_t* result) {
    if constexpr (Type == DataType::ROW || Type == DataType::ARRAY ||
                  Type == DataType::JSON) {
        ThrowInfo(milvus::DataTypeInvalid,
                  "hash not supported for complex types ROW/ARRAY/JSON: {}",
                  Type);
    } else {
        using T = typename TypeTraits<Type>::NativeType;
        for (size_t row_idx = 0; row_idx < column_data->size(); ++row_idx) {
            if (!column_data->ValidAt(row_idx)) {
                result[row_idx] =
                    mix ? milvus::bits::hashMix(result[row_idx], kNullHash)
                        : kNullHash;
            } else {
                T raw_value = column_data->ValueAt<T>(row_idx);
                uint64_t hash_value = kNullHash;
                if constexpr (std::is_floating_point_v<T>) {
                    hash_value = milvus::NaNAwareHash<T>()(raw_value);
                } else {
                    hash_value = folly::hasher<T>()(raw_value);
                }
                result[row_idx] =
                    mix ? milvus::bits::hashMix(result[row_idx], hash_value)
                        : hash_value;
            }
        }
    }
}

void
VectorHasher::hash(bool mix, std::vector<uint64_t>& result) {
    auto element_data_type = ChannelDataType();
    MILVUS_DYNAMIC_TYPE_DISPATCH(
        hashValues, element_data_type, columnData(), mix, result.data());
}

}  // namespace exec
}  // namespace milvus
