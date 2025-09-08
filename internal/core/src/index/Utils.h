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

#include "common/Common.h"
#include "common/Types.h"
#include "common/FieldData.h"
#include "common/QueryInfo.h"
#include "common/RangeSearchHelper.h"
#include "index/IndexInfo.h"
#include "storage/Types.h"
#include "storage/DataCodec.h"
#include "log/Log.h"

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
    if (!cfg.contains(key)) {
        return std::nullopt;
    }

    const auto& value = cfg.at(key);
    if (value.is_null()) {
        return std::nullopt;
    }

    try {
        if constexpr (std::is_same_v<T, bool>) {
            if (value.is_boolean()) {
                return value.get<bool>();
            }
            // compatibility for boolean string
            return boost::algorithm::to_lower_copy(value.get<std::string>()) ==
                   "true";
        }
        return value.get<T>();
    } catch (const nlohmann::json::type_error& e) {
        if (!CONFIG_PARAM_TYPE_CHECK_ENABLED) {
            LOG_WARN("config type mismatch for key {}: {}", key, e.what());
            return std::nullopt;
        }
        ThrowInfo(ErrorCode::UnexpectedError,
                  "config type error for key {}: {}",
                  key,
                  e.what());
    } catch (const std::exception& e) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Unexpected error for key {}: {}",
                  key,
                  e.what());
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
            "binary vector does not support metric type: " + metric_type);
    } else if constexpr (std::is_same_v<T, int8>) {
        AssertInfo(IsIntVectorMetricType(metric_type),
                   "int vector does not support metric type: " + metric_type);
    } else {
        AssertInfo(IsFloatVectorMetricType(metric_type),
                   "float vector does not support metric type: " + metric_type);
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

struct IndexDataCodec {
    std::list<std::unique_ptr<storage::DataCodec>> codecs_{};
    int64_t size_{0};
};

std::map<std::string, IndexDataCodec>
CompactIndexDatas(
    std::map<std::string, std::unique_ptr<storage::DataCodec>>& index_datas);

void
AssembleIndexDatas(
    std::map<std::string, std::unique_ptr<storage::DataCodec>>& index_datas,
    BinarySet& index_binary_set);

void
AssembleIndexDatas(std::map<std::string, IndexDataCodec>& index_datas,
                   BinarySet& index_binary_set);

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

// for unused
void inline SetBitsetUnused(void* bitset, const uint32_t* doc_id, uintptr_t n) {
    ThrowInfo(ErrorCode::UnexpectedError, "SetBitsetUnused is not supported");
}

// For sealed segment, the doc_id is guaranteed to be less than bitset size which equals to the doc count of tantivy before querying.
void inline SetBitsetSealed(void* bitset, const uint32_t* doc_id, uintptr_t n) {
    TargetBitmap* bitmap = static_cast<TargetBitmap*>(bitset);
    const auto bitmap_size = bitmap->size();

    for (uintptr_t i = 0; i < n; ++i) {
        assert(doc_id[i] < bitmap_size);
        (*bitmap)[doc_id[i]] = true;
    }
}

// For growing segment, concurrent insert exists, so the doc_id may exceed bitset size.
void inline SetBitsetGrowing(void* bitset,
                             const uint32_t* doc_id,
                             uintptr_t n) {
    TargetBitmap* bitmap = static_cast<TargetBitmap*>(bitset);
    const auto bitmap_size = bitmap->size();

    for (uintptr_t i = 0; i < n; ++i) {
        const auto id = doc_id[i];
        if (id >= bitmap_size) {
            // Ideally, the doc_id is sorted and we can return directly. But I don't want to have this strong guarantee.
            continue;
        }
        (*bitmap)[id] = true;
    }
}

inline size_t
vector_element_size(const DataType data_type) {
    switch (data_type) {
        case DataType::VECTOR_FLOAT:
            return sizeof(float);
        case DataType::VECTOR_FLOAT16:
            return sizeof(float16);
        case DataType::VECTOR_BFLOAT16:
            return sizeof(bfloat16);
        case DataType::VECTOR_INT8:
            return sizeof(int8);
        default:
            ThrowInfo(UnexpectedError,
                      fmt::format("invalid data type: {}", data_type));
    }
}

}  // namespace milvus::index
