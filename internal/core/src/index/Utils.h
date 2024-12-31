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

#include <unordered_map>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fcntl.h>
#include <sys/stat.h>
#include <tuple>
#include <map>
#include <string>
#include <boost/algorithm/string.hpp>

#include "common/Types.h"
#include "common/FieldData.h"
#include "common/QueryInfo.h"
#include "common/RangeSearchHelper.h"
#include "index/IndexInfo.h"
#include "storage/Types.h"

namespace milvus::index {

size_t
get_file_size(int fd);

std::vector<IndexType>
NM_List();

std::vector<IndexType>
BIN_List();

std::vector<std::tuple<IndexType, MetricType>>
unsupported_index_combinations();

bool
is_in_bin_list(const IndexType& index_type);

bool
is_in_nm_list(const IndexType& index_type);

bool
is_unsupported(const IndexType& index_type, const MetricType& metric_type);

bool
CheckKeyInConfig(const Config& cfg, const std::string& key);

void
ParseFromString(google::protobuf::Message& params, const std::string& str);

template <typename T>
void inline CheckParameter(Config& conf,
                           const std::string& key,
                           std::function<T(std::string)> fn,
                           std::optional<T> default_v) {
    if (!conf.contains(key)) {
        if (default_v.has_value()) {
            conf[key] = default_v.value();
        }
    } else {
        auto value = conf[key];
        conf[key] = fn(value);
    }
}

template <typename T>
inline std::optional<T>
GetValueFromConfig(const Config& cfg, const std::string& key) {
    // cfg value are all string type
    if (cfg.contains(key)) {
        if constexpr (std::is_same_v<T, bool>) {
            return boost::algorithm::to_lower_copy(
                       cfg.at(key).get<std::string>()) == "true";
        }
        return cfg.at(key).get<T>();
    }
    return std::nullopt;
}

template <typename T>
inline void
SetValueToConfig(Config& cfg, const std::string& key, const T value) {
    cfg[key] = value;
}

template <typename T>
inline void
CheckMetricTypeSupport(const MetricType& metric_type) {
    if constexpr (std::is_same_v<T, bin1>) {
        AssertInfo(
            IsBinaryVectorMetricType(metric_type),
            "binary vector does not float vector metric type: " + metric_type);
    } else {
        AssertInfo(
            IsFloatVectorMetricType(metric_type),
            "float vector does not binary vector metric type: " + metric_type);
    }
}

int64_t
GetDimFromConfig(const Config& config);

std::string
GetMetricTypeFromConfig(const Config& config);

std::string
GetIndexTypeFromConfig(const Config& config);

IndexVersion
GetIndexEngineVersionFromConfig(const Config& config);

int32_t
GetBitmapCardinalityLimitFromConfig(const Config& config);

storage::FieldDataMeta
GetFieldDataMetaFromConfig(const Config& config);

storage::IndexMeta
GetIndexMetaFromConfig(const Config& config);

Config
ParseConfigFromIndexParams(
    const std::map<std::string, std::string>& index_params);

void
AssembleIndexDatas(std::map<std::string, FieldDataPtr>& index_datas);

void
AssembleIndexDatas(std::map<std::string, FieldDataChannelPtr>& index_datas,
                   std::unordered_map<std::string, FieldDataPtr>& result);

// On Linux, read() (and similar system calls) will transfer at most 0x7ffff000 (2,147,479,552) bytes once
void
ReadDataFromFD(int fd, void* buf, size_t size, size_t chunk_size = 0x7ffff000);

bool
CheckAndUpdateKnowhereRangeSearchParam(const SearchInfo& search_info,
                                       const int64_t topk,
                                       const MetricType& metric_type,
                                       knowhere::Json& search_config);

}  // namespace milvus::index
