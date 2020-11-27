// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <string>

#include "db/meta/Utils.h"

namespace milvus::engine::meta {

template <typename V>
inline std::string
FieldValue2Str(const V& v) {
    return std::to_string(v);
}

template <>
inline std::string
FieldValue2Str<std::string>(const std::string& v) {
    return v;
}

template <>
inline std::string
FieldValue2Str<snapshot::FTYPE_TYPE>(const snapshot::FTYPE_TYPE& v) {
    return std::to_string(static_cast<int>(v));
}

template <>
inline std::string
FieldValue2Str<snapshot::FETYPE_TYPE>(const snapshot::FETYPE_TYPE& v) {
    return std::to_string(static_cast<int>(v));
}

template <>
inline std::string
FieldValue2Str<snapshot::MappingT>(const snapshot::MappingT& v) {
    std::string value;
    mappings2str(v, value);
    return value;
}

}  // namespace milvus::engine::meta
