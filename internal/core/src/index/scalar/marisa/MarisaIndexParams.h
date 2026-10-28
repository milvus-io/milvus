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

#pragma once

#include "index/ParamUtils.h"

namespace milvus::index::marisa_params {

inline DataType
ParseValueType(const Config& params) {
    const char* key = nullptr;
    if (params.is_object() && params.contains("value_type")) {
        key = "value_type";
    } else if (params.is_object() && params.contains("field_type")) {
        key = "field_type";
    } else {
        return DataType::VARCHAR;
    }

    const auto type = milvus::index::ParseDataTypeValue(params.at(key), key);

    if (!IsStringDataType(type)) {
        ThrowInfo(DataTypeInvalid,
                  "marisa requires STRING, VARCHAR, or TEXT, got {}",
                  static_cast<int32_t>(type));
    }
    return type;
}

inline bool
ParseNested(const Config& params) {
    return ReadNestedConfigParam(params, "marisa").value_or(false);
}

}  // namespace milvus::index::marisa_params
