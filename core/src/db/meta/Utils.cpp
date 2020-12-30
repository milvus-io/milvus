
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

#include <string>

#include "db/meta/Utils.h"
#include "db/snapshot/ResourceTypes.h"
#include "utils/Json.h"

namespace milvus::engine::meta {

void
int2str(const int64_t& ival, std::string& val) {
    val = std::to_string(ival);
}

void
uint2str(const uint64_t& uival, std::string& val) {
    val = std::to_string(uival);
}

void
state2str(const snapshot::State& sval, std::string& val) {
    val = std::to_string(sval);
}

void
mappings2str(const snapshot::MappingT& mval, std::string& val) {
    auto value_json = json::array();
    for (auto& m : mval) {
        value_json.emplace_back(m);
    }

    val = "\'" + value_json.dump() + "\'";
}

void
str2str(const std::string& sval, std::string& val) {
    val = "\'" + sval + "\'";
}

void
json2str(const json& jval, std::string& val) {
    val = "\'" + jval.dump() + "\'";
}

}  // namespace milvus::engine::meta
