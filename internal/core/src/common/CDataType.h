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

#include "common/type_c.h"
#include <string>

namespace milvus {
template <typename T, typename = std::enable_if_t<std::is_fundamental_v<T> || std::is_same_v<T, std::string>>>
inline CDataType
GetDType() {
    return None;
}

template <>
inline CDataType
GetDType<bool>() {
    return Bool;
}

template <>
inline CDataType
GetDType<int8_t>() {
    return Int8;
}

template <>
inline CDataType
GetDType<int16_t>() {
    return Int16;
}

template <>
inline CDataType
GetDType<int32_t>() {
    return Int32;
}

template <>
inline CDataType
GetDType<int64_t>() {
    return Int64;
}

template <>
inline CDataType
GetDType<float>() {
    return Float;
}

template <>
inline CDataType
GetDType<double>() {
    return Double;
}

template <>
inline CDataType
GetDType<std::string>() {
    return VarChar;
}
}  // namespace milvus
