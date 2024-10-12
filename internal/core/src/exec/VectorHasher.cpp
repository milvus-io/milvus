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
#include "common/float_util_c.h"
#include <folly/Hash.h>
#include "common/BitUtil.h"

namespace milvus{
namespace exec {
std::vector<std::unique_ptr<VectorHasher>> createVectorHashers(
        const RowTypePtr& rowType,
        const std::vector<expr::FieldAccessTypeExprPtr>& exprs) {
    std::vector<std::unique_ptr<VectorHasher>> hashers;
    hashers.reserve(exprs.size());
    for (const auto& expr: exprs) {
        auto column_idx = rowType->GetChildIndex(expr->name());
        hashers.emplace_back(VectorHasher::create(expr->type(), column_idx));
    }
    return hashers;
}

template<DataType Type>
void VectorHasher::hashValues(const ColumnVectorPtr& column_data, bool mix, uint64_t* result) {
    if constexpr (Type==DataType::ROW || Type==DataType::ARRAY || Type==DataType::JSON) {
        PanicInfo(milvus::DataTypeInvalid, "NotSupport hash for complext type row/array/json:{}", Type);
    } else {
        using T = typename TypeTraits<Type>::NativeType;
        auto element_data_type = ChannelDataType();
        auto element_size = GetDataTypeSize(element_data_type);
        auto element_count = column_data->size();
        for(auto i = 0; i < element_count; i++) {
            void *raw_value = column_data->RawValueAt(i, element_size);
            AssertInfo(raw_value != nullptr, "Failed to get raw value pointer from column data");
            if (!column_data->ValidAt(i)) {
                result[i] = kNullHash;
                continue;
            }
            auto value = static_cast<T *>(raw_value);
            uint64_t hash_value = kNullHash;
            if constexpr (std::is_floating_point_v<T>) {
                hash_value = milvus::NaNAwareHash<T>()(*value);
            } else {
                hash_value = folly::hasher<T>()(*value);
            }
            result[i] = mix? milvus::bits::hashMix(result[i], hash_value) : hash_value;
        }
    }
}

void
VectorHasher::hash(bool mix, std::vector<uint64_t>& result) {
    
    // auto element_size = GetDataTypeSize(element_data_type);
    // auto element_count = column_data->size();

    // for(auto i = 0; i < element_count; i++) {
    //     void* raw_value = column_data->RawValueAt(i, element_size);  
    // }
    auto element_data_type = ChannelDataType();
    MILVUS_DYNAMIC_TYPE_DISPATCH(hashValues, element_data_type, columnData(), mix, result.data());
    //PanicInfo(DataTypeInvalid, "Unsupported data type for dispatch");
}



}
}
