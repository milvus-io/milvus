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

// TODO(yhz): Rename this file name

#pragma once

#include <limits>
#include <set>
#include <string>
#include <type_traits>

#include "db/meta/Utils.h"
#include "db/meta/MetaTraits.h"
#include "utils/Json.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

///////////////// Convert field value to str //////////////////////

template <typename V>
inline std::string
FieldValue2Str(const V& v) {
    if constexpr(decay_equal<V, std::string>::value) {
        return "\'" + v + "\'";
    } else if constexpr(decay_equal_v<V, const snapshot::FTYPE_TYPE>) {
        return std::to_string(static_cast<int>(v));
    } else if constexpr(decay_equal_v<V, const snapshot::FETYPE_TYPE>) {
        return std::to_string(static_cast<int>(v));
    } else if constexpr(decay_equal_v<V, const snapshot::State>) {
        return std::to_string(static_cast<int>(v));
    } else if constexpr(decay_equal_v<V, const snapshot::MappingT>) {
        std::string value;
        mappings2str(v, value);
        return value;
    } else {
        return std::to_string(v);
    }
}

//////////////////////// Convert str to field value ////////////////////
template <typename V>
inline V
Str2FieldValue(const std::string& s) {
    if constexpr(decay_equal_v<V, uint64_t>) {
        return std::stoul(s);
    } else if constexpr(decay_equal_v<V, std::string>) {
        if (s.length() > 1 && *s.begin() == '\'' && *s.rbegin() == '\'') {
            std::string so = s;
            StringHelpFunctions::TrimStringQuote(so, "\'");
            return so;
        }

        return s;
    } else if constexpr(decay_equal_v<V, snapshot::FTYPE_TYPE>) {
        return static_cast<snapshot::FTYPE_TYPE>(std::stol(s));
    } else if constexpr(decay_equal_v<V, snapshot::FETYPE_TYPE>) {
        return static_cast<snapshot::FETYPE_TYPE>(std::stol(s));
    } else if constexpr(decay_equal_v<V, snapshot::State>){
        return static_cast<snapshot::State>(std::stol(s));
    } else if constexpr(decay_equal_v<V, snapshot::MappingT>) {
        std::string so = s;
        if (*s.begin() == '\'' && *s.rbegin() == '\'') {
            StringHelpFunctions::TrimStringQuote(so, "\'");
        }

        auto mapping_json = json::parse(so);
        std::set<int64_t> mappings;
        for (auto& ele : mapping_json) {
            mappings.insert(ele.get<int64_t>());
        }

        return mappings;
    } else if constexpr(decay_equal_v<V, json>) {
        std::string so = s;
        if (*s.begin() == '\'' && *s.rbegin() == '\'') {
            StringHelpFunctions::TrimStringQuote(so, "\'");
        }

        return json::parse(so);
    } else {
        return std::stol(s);
    }
}

//////////////////////// Field equal ////////////////////////////
/**
 * Return:
 *     -1   --  lv < rv
 *     0    --  lv == rv
 *     1    --  lv > rv
 *     min  --  V cannot be compared
 */
template <typename V>
inline int64_t
FieldCompare(const V& lv, const V& rv) {
    if constexpr(std::is_arithmetic_v<V>) {
        if (lv < rv) {
            return -1;
        } else if (lv > rv) {
            return 1;
        } else {
            return 0;
        }
    } else if constexpr(decay_equal_v<V, std::string>) {
        return lv.compare(rv);
    } else if constexpr(decay_equal_v<V, snapshot::FTYPE_TYPE>) {
        return FieldCompare<int>(static_cast<int>(lv), static_cast<int>(rv));
    } else if constexpr(decay_equal_v<V, snapshot::FETYPE_TYPE>) {
        return FieldCompare<int>(static_cast<int>(lv), static_cast<int>(rv));
    } else if constexpr(decay_equal_v<V, snapshot::State>) {
        return FieldCompare<int>(static_cast<int>(lv), static_cast<int>(rv));
    } else if constexpr(decay_equal_v<V, snapshot::MappingT>) {
        return std::numeric_limits<int64_t>::min();
    }
}

}  // namespace milvus::engine::meta
