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
#include "xxhash.h"
#include <boost/algorithm/string.hpp>
#include "ankerl/unordered_dense.h"

#include "common/Common.h"
#include "common/FieldDataInterface.h"
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

inline bool
SupportsSkipIndex(arrow::Type::type type) {
    switch (type) {
        case arrow::Type::BOOL:
        case arrow::Type::INT8:
        case arrow::Type::INT16:
        case arrow::Type::INT32:
        case arrow::Type::INT64:
        case arrow::Type::FLOAT:
        case arrow::Type::DOUBLE:
        case arrow::Type::STRING:
            return true;
        default:
            return false;
    }
}

inline bool
SupportsSkipIndex(DataType type) {
    switch (type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TIMESTAMPTZ:
            return true;
        default:
            return false;
    }
}

class BloomFilter {
 public:
    explicit BloomFilter(uint64_t hash_count,
                         uint64_t bit_size,
                         std::vector<uint64_t> bit_array)
        : hash_count_(hash_count),
          bit_size_(bit_size),
          bit_array_(std::move(bit_array)) {
    }

    template <typename T>
    static std::unique_ptr<BloomFilter>
    Build(const ankerl::unordered_dense::set<T>& items, double p = 0.01) {
        auto n = items.size();
        if (n == 0) {
            return nullptr;
        }
        auto bit_size = EstimateBitSize(n, p);
        auto hash_count = EstimateHashCount(n, bit_size);
        auto bit_array = std::vector<uint64_t>{};
        bit_array.assign(bit_size / 64, 0);
        for (const auto& item : items) {
            uint64_t hash1 = Hash(item, 0x9e3779b9);
            uint64_t hash2 = Hash(item, hash1);
            for (size_t i = 0; i < hash_count; ++i) {
                size_t bit_pos = (hash1 + i * hash2) % bit_size;
                bit_array[bit_pos / 64] |= (1ULL << (bit_pos % 64));
            }
        }
        return std::make_unique<BloomFilter>(hash_count, bit_size, bit_array);
    }

    static uint64_t
    EstimateCost(size_t n, double p = 0.01) {
        return EstimateBitSize(n, p) / 8 + sizeof(uint64_t) * 2;
    }

    template <typename T>
    bool
    MightContain(const T& item) const {
        if (bit_size_ == 0) {
            return true;
        }
        uint64_t hash1 = Hash(item, 0x9e3779b9);
        uint64_t hash2 = Hash(item, hash1);
        for (size_t i = 0; i < hash_count_; ++i) {
            size_t bit_pos = (hash1 + i * hash2) % bit_size_;
            if (!(bit_array_[bit_pos / 64] & (1ULL << (bit_pos % 64)))) {
                return false;
            }
        }
        return true;
    }

    // 暴露内部状态用于序列化
    const std::vector<uint64_t>&
    GetBitArray() const {
        return bit_array_;
    }
    size_t
    GetHashCount() const {
        return hash_count_;
    }
    size_t
    GetBitSize() const {
        return bit_size_;
    }
    bool
    IsValid() const {
        return bit_size_ > 0 && !bit_array_.empty();
    }

    std::string
    Serialize() const {
        std::stringstream ss(std::ios::binary | std::ios::out);
        uint64_t array_size = bit_array_.size();
        ss.write(reinterpret_cast<const char*>(&hash_count_),
                 sizeof(hash_count_));
        ss.write(reinterpret_cast<const char*>(&bit_size_), sizeof(bit_size_));
        ss.write(reinterpret_cast<const char*>(&array_size),
                 sizeof(array_size));
        for (const auto& block : bit_array_) {
            ss.write(reinterpret_cast<const char*>(&block), sizeof(block));
        }
        return ss.str();
    }

    static std::unique_ptr<BloomFilter>
    Deserialize(std::string_view data) {
        if (data.empty()) {
            return nullptr;
        }

        const char* ptr = data.data();
        const char* end = ptr + data.size();
        if (data.size() < sizeof(uint64_t) * 3) {
            return nullptr;
        }

        uint64_t hash_count, bit_size, array_size;
        std::memcpy(&hash_count, ptr, sizeof(hash_count));
        ptr += sizeof(uint64_t);

        std::memcpy(&bit_size, ptr, sizeof(bit_size));
        ptr += sizeof(uint64_t);

        std::memcpy(&array_size, ptr, sizeof(array_size));
        ptr += sizeof(uint64_t);
        if (ptr + array_size * sizeof(uint64_t) > end) {
            return nullptr;
        }

        std::vector<uint64_t> bit_array(array_size);
        std::memcpy(bit_array.data(), ptr, array_size * sizeof(uint64_t));

        return std::make_unique<BloomFilter>(
            hash_count, bit_size, std::move(bit_array));
    }

 private:
    static uint64_t
    EstimateBitSize(size_t n, double p) {
        if (n == 0) {
            return 0;
        }
        double bit_size_double =
            -1.0 * n * std::log(p) / (std::log(2) * std::log(2));
        auto bit_size = static_cast<uint64_t>(bit_size_double);
        // Align to 64 bits (8 bytes)
        return ((bit_size + 63) / 64) * 64;
    }

    static uint64_t
    EstimateHashCount(size_t n, uint64_t bit_size) {
        if (n == 0 || bit_size == 0) {
            return 0;
        }
        auto hash_count = static_cast<uint64_t>(
            (static_cast<double>(bit_size) / n) * std::log(2));
        if (hash_count < 1) {
            hash_count = 1;
        }
        if (hash_count > 10) {
            hash_count = 10;
        }
        return hash_count;
    }

    template <typename T>
    static uint64_t
    Hash(const T& item, uint64_t seed) {
        if constexpr (std::is_same_v<T, std::string> ||
                      std::is_same_v<T, std::string_view>) {
            return XXH64(item.data(), item.size(), seed);
        } else if constexpr (std::is_arithmetic_v<T>) {
            return XXH64(&item, sizeof(T), seed);
        }
    }

    std::vector<uint64_t> bit_array_;
    uint64_t hash_count_;
    uint64_t bit_size_;
};

}  // namespace milvus::index
