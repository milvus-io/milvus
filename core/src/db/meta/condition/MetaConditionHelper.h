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

#include <limits>
#include <string>

#include "db/meta/Utils.h"
#include "utils/Json.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

///////////////// Convert field value to str //////////////////////
template <typename V>
inline std::string
FieldValue2Str(const V& v) {
    return std::to_string(v);
}

template <>
inline std::string
FieldValue2Str<std::string>(const std::string& v) {
    return "\'" + v + "\'";
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
FieldValue2Str<snapshot::State>(const snapshot::State& v) {
    return std::to_string(static_cast<int>(v));
}

template <>
inline std::string
FieldValue2Str<snapshot::MappingT>(const snapshot::MappingT& v) {
    std::string value;
    mappings2str(v, value);
    return value;
}

//////////////////////// Convert str to field value ////////////////////
template <typename V>
inline V
Str2FieldValue(const std::string& s) {
    return std::stol(s);
}

template <>
inline uint64_t
Str2FieldValue<uint64_t>(const std::string& s) {
    return std::stoul(s);
}

template <>
inline std::string
Str2FieldValue<std::string>(const std::string& s) {
    if (*s.begin() == '\'' && *s.rbegin() == '\'') {
        std::string so = s;
        StringHelpFunctions::TrimStringQuote(so, "\'");
        return so;
    }

    return s;
}

template <>
inline snapshot::FTYPE_TYPE
Str2FieldValue<snapshot::FTYPE_TYPE>(const std::string& s) {
    int64_t i = std::stol(s);
    return static_cast<snapshot::FTYPE_TYPE>(i);
}

template <>
inline snapshot::FETYPE_TYPE
Str2FieldValue<snapshot::FETYPE_TYPE>(const std::string& s) {
    int64_t i = std::stol(s);
    return static_cast<snapshot::FETYPE_TYPE>(i);
}

template <>
inline snapshot::State
Str2FieldValue<snapshot::State>(const std::string& s) {
    int64_t i = std::stol(s);
    return static_cast<snapshot::State>(i);
}

template <>
inline snapshot::MappingT
Str2FieldValue<snapshot::MappingT>(const std::string& s) {
    std::string so = s;
    if (*s.begin() == '\'' && *s.rbegin() == '\'') {
        StringHelpFunctions::TrimStringQuote(so, "\'");
    }

    return json::parse(so);
}

//////////////////////// Field equal ////////////////////////////
template <typename V>
inline int64_t
/**
 * Return:
 *     -1   --  lv < rv
 *     0    --  lv == rv
 *     1    --  lv > rv
 *     min  --  V cannot be compared
 */
FieldCompare(const V& lv, const V& rv) {
    if (lv < rv) {
        return -1;
    } else if (lv > rv) {
        return 1;
    } else {
        return 0;
    }
}

template <>
inline int64_t
FieldCompare<std::string>(const std::string& lv, const std::string& rv) {
    return lv.compare(rv);
}

template <>
inline int64_t
FieldCompare<snapshot::FTYPE_TYPE>(const snapshot::FTYPE_TYPE& lv, const snapshot::FTYPE_TYPE& rv) {
    return FieldCompare<int>(static_cast<int>(lv), static_cast<int>(rv));
}

template <>
inline int64_t
FieldCompare<snapshot::FETYPE_TYPE>(const snapshot::FETYPE_TYPE& lv, const snapshot::FETYPE_TYPE& rv) {
    return FieldCompare<int>(static_cast<int>(lv), static_cast<int>(rv));
}

template <>
inline int64_t
FieldCompare<snapshot::State>(const snapshot::State& lv, const snapshot::State& rv) {
    return FieldCompare<int>(static_cast<int>(lv), static_cast<int>(rv));
}

template <>
inline int64_t
FieldCompare<snapshot::MappingT>(const snapshot::MappingT& lv, const snapshot::MappingT& rv) {
    return std::numeric_limits<int64_t>::min();
}

}  // namespace milvus::engine::meta
