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

#include "common/EasyAssert.h"
#include "common/JsonCastType.h"
#include "index/Meta.h"
#include "segcore/Types.h"

namespace milvus::segcore {

inline bool
NeedsNaNTotalOrder(const LoadIndexInfo& info) {
    auto is_float = [](DataType type) {
        return type == DataType::FLOAT || type == DataType::DOUBLE;
    };
    if (is_float(info.field_type) ||
        (info.field_type == DataType::ARRAY && is_float(info.element_type))) {
        return true;
    }
    if (info.field_type == DataType::JSON) {
        auto cast = info.index_params.find(JSON_CAST_TYPE);
        if (cast == info.index_params.end()) {
            return true;
        }
        auto type = JsonCastType::FromString(cast->second).element_type();
        return type == JsonCastType::DataType::DOUBLE ||
               type == JsonCastType::DataType::JSON;
    }
    return false;
}

// Keep load admission and SegmentLoadInfo's cache on the same compatibility
// decision. Old indexes cannot prove that their floating keys are lossless.
inline bool
CanLoadIndex(const LoadIndexInfo& info, int64_t scalar_index_version) {
    if (scalar_index_version >= index::kMinScalarIndexVersionForNaNTotalOrder ||
        !NeedsNaNTotalOrder(info)) {
        return true;
    }
    auto cast = info.index_params.find(JSON_CAST_FUNCTION);
    if (info.field_type == DataType::JSON && cast != info.index_params.end() &&
        !cast->second.empty()) {
        ThrowInfo(Unsupported,
                  "JSON cast indexes below scalar engine version 6 "
                  "must be rebuilt before loading; raw JSON does "
                  "not apply the index-time cast");
    }
    // Raw data is the only lossless common fallback for old SORT, INVERTED,
    // BITMAP and HYBRID indexes, including indexes whose data happen to be finite.
    return false;
}

}  // namespace milvus::segcore
