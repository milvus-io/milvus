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

#include "index/ParamUtils.h"

#include "common/Consts.h"
#include "index/Families.h"
#include "index/Meta.h"

namespace milvus::index {

std::string
GetMetricTypeFromConfig(const Config& config) {
    auto metric_type = GetValueFromConfig<std::string>(config, "metric_type");
    AssertInfo(metric_type.has_value(), "metric_type not exist in config");
    return metric_type.value();
}

std::string
GetLowCardinalityFamilyFromConfig(const Config& config) {
    auto type = GetValueFromConfig<std::string>(
        config, HYBRID_LOW_CARDINALITY_INDEX_TYPE);
    if (!type.has_value()) {
        return families::kBitmap;
    }
    if (*type == "BITMAP") {
        return families::kBitmap;
    }
    if (*type == "STLSORT" || *type == ASCENDING_SORT) {
        return families::kSort;
    }
    if (*type == "MARISA" || *type == MARISA_TRIE ||
        *type == MARISA_TRIE_UPPER) {
        return families::kMarisa;
    }
    if (*type == "INVERTED" || *type == INVERTED_INDEX_TYPE) {
        return families::kInverted;
    }
    AssertInfo(false, "unsupported hybrid scalar index type: {}", *type);
    return {};
}

std::string
GetHighCardinalityFamilyFromConfig(const Config& config) {
    auto type = GetValueFromConfig<std::string>(
        config, HYBRID_HIGH_CARDINALITY_INDEX_TYPE);
    if (!type.has_value()) {
        return families::kSort;
    }
    Config copy = config;
    copy[HYBRID_LOW_CARDINALITY_INDEX_TYPE] = *type;
    return GetLowCardinalityFamilyFromConfig(copy);
}

Config
ParseConfigFromIndexParams(
    const std::map<std::string, std::string>& index_params) {
    Config config;
    for (auto& p : index_params) {
        config[p.first] = p.second;
    }

    return config;
}

}  // namespace milvus::index
