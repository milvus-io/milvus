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

#include "db/metax/backend/convertor/MetaSqlConvertor.h"

#include <string>

#include "utils/StringHelpFunctions.h"

namespace milvus::engine::metax {

std::string
MetaSqlConvertor::int2str(const int64_t& v) {
    return std::to_string(v);
}

std::string
MetaSqlConvertor::uint2str(const uint64_t& v) {
    return std::to_string(v);
}

std::string
MetaSqlConvertor::str2str(const std::string& v) {
    return "\'" + v + "\'";
}

std::string
MetaSqlConvertor::json2str(const json& v) {
    return "\'" + v.dump() + "\'";
}

int64_t
MetaSqlConvertor::intfstr(const std::string& s) {
    return std::stol(s);
}

uint64_t
MetaSqlConvertor::uintfstr(const std::string& s) {
    return std::stoul(s);
}

std::string
MetaSqlConvertor::strfstr(const std::string& s) {
    std::string so = s;
    if (*s.begin() == '\'' && *s.rbegin() == '\'') {
        StringHelpFunctions::TrimStringQuote(so, "\'");
    }

    return so;
}

json
MetaSqlConvertor::jsonfstr(const std::string& s) {
    std::string so = s;
    if (*s.begin() == '\'' && *s.rbegin() == '\'') {
        StringHelpFunctions::TrimStringQuote(so, "\'");
    }

    return json::parse(so);
}

}  // namespace milvus::engine::metax
