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

#include "storage/Types.h"
#include "tantivy-binding.h"

namespace milvus::index {
inline TantivyDataType
get_tantivy_data_type(DataType data_type) {
    switch (data_type) {
        case DataType::BOOL: {
            return TantivyDataType::Bool;
        }

        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64: {
            return TantivyDataType::I64;
        }

        case DataType::FLOAT:
        case DataType::DOUBLE: {
            return TantivyDataType::F64;
        }

        case DataType::VARCHAR: {
            return TantivyDataType::Keyword;
        }

        default:
            PanicInfo(ErrorCode::NotImplemented,
                      fmt::format("not implemented data type: {}", data_type));
    }
}

struct TantivyConfig {
    DataType data_type_;
    DataType element_type_;

    TantivyDataType
    to_tantivy_data_type() {
        switch (data_type_) {
            case DataType::ARRAY: {
                return get_tantivy_data_type(element_type_);
            }
            default:
                return get_tantivy_data_type(data_type_);
        }
    }
};
}  // namespace milvus::index