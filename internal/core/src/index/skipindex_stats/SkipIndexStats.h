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

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "nlohmann/json.hpp"
#include "ankerl/unordered_dense.h"
#include "arrow/type_fwd.h"
#include "arrow/array/array_primitive.h"
#include "parquet/statistics.h"
#include "common/BloomFilter.h"
#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/Types.h"
#include "common/FieldDataInterface.h"
#include "index/Utils.h"
#include "index/skipindex_stats/utils.h"

namespace milvus::index {

using Metrics = std::variant<bool,
                             int8_t,
                             int16_t,
                             int32_t,
                             int64_t,
                             float,
                             double,
                             std::string,
                             std::string_view>;

template <typename T>
using MetricsDataType =
    std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

const std::string NONE_FIELD_CHUNK_METRICS = "NONE";
const std::string BOOLEAN_FIELD_CHUNK_METRICS = "BOOLEAN";
const std::string FLOAT_FIELD_CHUNK_METRICS = "FLOAT";
const std::string INT_FIELD_CHUNK_METRICS = "INT";
const std::string STRING_FIELD_CHUNK_METRICS = "STRING";

enum class FieldChunkMetricsType {
    NONE = 0,
    BOOLEAN,
    FLOAT,
    INT,
    STRING,
};

inline std::string
FieldChunkMetricsTypeToString(FieldChunkMetricsType type) {
    switch (type) {
        case FieldChunkMetricsType::BOOLEAN:
            return BOOLEAN_FIELD_CHUNK_METRICS;
        case FieldChunkMetricsType::FLOAT:
            return FLOAT_FIELD_CHUNK_METRICS;
        case FieldChunkMetricsType::INT:
            return INT_FIELD_CHUNK_METRICS;
        case FieldChunkMetricsType::STRING:
            return STRING_FIELD_CHUNK_METRICS;
        default:
            return NONE_FIELD_CHUNK_METRICS;
    }
}

inline FieldChunkMetricsType
StringToFieldChunkMetricsType(std::string_view name) {
    if (name == BOOLEAN_FIELD_CHUNK_METRICS) {
        return FieldChunkMetricsType::BOOLEAN;
    }
    if (name == FLOAT_FIELD_CHUNK_METRICS) {
        return FieldChunkMetricsType::FLOAT;
    }
    if (name == INT_FIELD_CHUNK_METRICS) {
        return FieldChunkMetricsType::INT;
    }
    if (name == STRING_FIELD_CHUNK_METRICS) {
        return FieldChunkMetricsType::STRING;
    }
    return FieldChunkMetricsType::NONE;
}

template <typename T>
inline bool
RangeShouldSkip(const T& value,
                const T& lower_bound,
                const T& upper_bound,
                OpType op_type) {
    bool should_skip = false;
    switch (op_type) {
        case OpType::Equal: {
            should_skip = value > upper_bound || value < lower_bound;
            break;
        }
        case OpType::LessThan: {
            should_skip = value <= lower_bound;
            break;
        }
        case OpType::LessEqual: {
            should_skip = value < lower_bound;
            break;
        }
        case OpType::GreaterThan: {
            should_skip = value >= upper_bound;
            break;
        }
        case OpType::GreaterEqual: {
            should_skip = value > upper_bound;
            break;
        }
        default: {
            should_skip = false;
        }
    }
    return should_skip;
}

template <typename T>
inline bool
RangeShouldSkip(const T& lower_val,
                const T& upper_val,
                const T& lower_bound,
                const T& upper_bound,
                bool lower_inclusive,
                bool upper_inclusive) {
    bool should_skip = false;
    if (lower_inclusive && upper_inclusive) {
        should_skip = (lower_val > upper_bound) || (upper_val < lower_bound);
    } else if (lower_inclusive && !upper_inclusive) {
        should_skip = (lower_val > upper_bound) || (upper_val <= lower_bound);
    } else if (!lower_inclusive && upper_inclusive) {
        should_skip = (lower_val >= upper_bound) || (upper_val < lower_bound);
    } else {
        should_skip = (lower_val >= upper_bound) || (upper_val <= lower_bound);
    }
    return should_skip;
}

class FieldChunkMetrics {
 public:
    FieldChunkMetrics() = default;
    virtual ~FieldChunkMetrics() = default;

    virtual std::unique_ptr<FieldChunkMetrics>
    Clone() const = 0;

    virtual FieldChunkMetricsType
    GetMetricsType() const = 0;

    virtual bool
    CanSkipUnaryRange(OpType op_type, const Metrics& val) const = 0;

    virtual bool
    CanSkipBinaryRange(const Metrics& lower_val,
                       const Metrics& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        return false;
    }

    virtual bool
    CanSkipIn(const std::vector<Metrics>& values) const {
        return false;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const {
        return cell_size_;
    }

    void
    SetCellSize(cachinglayer::ResourceUsage cell_size) {
        cell_size_ = cell_size;
    }

    virtual nlohmann::json
    ToJson() const = 0;

 protected:
    bool has_value_{false};
    cachinglayer::ResourceUsage cell_size_ = {0, 0};
};

class NoneFieldChunkMetrics : public FieldChunkMetrics {
 public:
    NoneFieldChunkMetrics() = default;
    ~NoneFieldChunkMetrics() = default;

    std::unique_ptr<FieldChunkMetrics>
    Clone() const override {
        return std::make_unique<NoneFieldChunkMetrics>();
    }

    FieldChunkMetricsType
    GetMetricsType() const override {
        return FieldChunkMetricsType::NONE;
    }

    bool
    CanSkipUnaryRange(OpType op_type, const Metrics& val) const override {
        return false;
    }

    bool
    CanSkipBinaryRange(const Metrics& lower_val,
                       const Metrics& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const override {
        return false;
    }

    bool
    CanSkipIn(const std::vector<Metrics>& values) const override {
        return false;
    }

    nlohmann::json
    ToJson() const override {
        nlohmann::json j;
        j["type"] = FieldChunkMetricsTypeToString(GetMetricsType());
        return j;
    }
};

class BooleanFieldChunkMetrics : public FieldChunkMetrics {
 public:
    BooleanFieldChunkMetrics() = default;
    BooleanFieldChunkMetrics(bool has_true, bool has_false)
        : has_true_(has_true), has_false_(has_false) {
        this->has_value_ = true;
    }

    std::unique_ptr<FieldChunkMetrics>
    Clone() const override {
        if (!this->has_value_) {
            return std::make_unique<NoneFieldChunkMetrics>();
        }
        return std::make_unique<BooleanFieldChunkMetrics>(has_true_,
                                                          has_false_);
    }

    bool
    CanSkipUnaryRange(OpType op_type, const Metrics& val) const override {
        return false;
    }

    bool
    CanSkipBinaryRange(const Metrics& lower_val,
                       const Metrics& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const override {
        return false;
    }

    bool
    CanSkipIn(const std::vector<Metrics>& values) const override {
        if (!this->has_value_ || values.size() != 2) {
            return false;
        }
        if (!std::holds_alternative<bool>(values[0]) ||
            !std::holds_alternative<bool>(values[1])) {
            return false;
        }
        bool contains_true = std::get<bool>(values[0]);
        bool contains_false = std::get<bool>(values[1]);
        if (contains_true && has_true_ || contains_false && has_false_) {
            return false;
        }
        return true;
    }

    FieldChunkMetricsType
    GetMetricsType() const override {
        return FieldChunkMetricsType::BOOLEAN;
    }

    nlohmann::json
    ToJson() const override {
        nlohmann::json j;
        j["type"] = FieldChunkMetricsTypeToString(GetMetricsType());
        j["has_value"] = this->has_value_;
        j["has_true"] = has_true_;
        j["has_false"] = has_false_;
        return j;
    }

 private:
    bool has_true_ = false;
    bool has_false_ = false;
};

template <typename T>
class FloatFieldChunkMetrics : public FieldChunkMetrics {
 public:
    FloatFieldChunkMetrics() = default;
    FloatFieldChunkMetrics(T min, T max) : min_(min), max_(max) {
        this->has_value_ = true;
    }

    std::unique_ptr<FieldChunkMetrics>
    Clone() const override {
        if (!this->has_value_) {
            return std::make_unique<NoneFieldChunkMetrics>();
        }
        return std::make_unique<FloatFieldChunkMetrics<T>>(min_, max_);
    }

    bool
    CanSkipUnaryRange(OpType op_type, const Metrics& val) const override {
        if (!this->has_value_) {
            return false;
        }
        if (!std::holds_alternative<T>(val)) {
            return false;
        }
        const T& typed_val = std::get<T>(val);
        return RangeShouldSkip(typed_val, min_, max_, op_type);
    }

    bool
    CanSkipIn(const std::vector<Metrics>& values) const override {
        if (!this->has_value_ || values.empty()) {
            return false;
        }
        for (const auto& v : values) {
            if (!std::holds_alternative<T>(v)) {
                return false;  // Mixed types in IN list, cannot evaluate
            }
        }
        T typed_min = std::get<T>(values[0]);
        T typed_max = std::get<T>(values[0]);
        for (const auto& v : values) {
            const T& current_val = std::get<T>(v);
            if (current_val < typed_min) {
                typed_min = current_val;
            }
            if (current_val > typed_max) {
                typed_max = current_val;
            }
        }
        return RangeShouldSkip(typed_min, typed_max, min_, max_, true, true);
    }

    bool
    CanSkipBinaryRange(const Metrics& lower_val,
                       const Metrics& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const override {
        if (!std::holds_alternative<T>(lower_val) ||
            !std::holds_alternative<T>(upper_val)) {
            return false;
        }
        if (!this->has_value_) {
            return false;
        }
        const T& typed_lower = std::get<T>(lower_val);
        const T& typed_upper = std::get<T>(upper_val);
        return RangeShouldSkip(typed_lower,
                               typed_upper,
                               min_,
                               max_,
                               lower_inclusive,
                               upper_inclusive);
    }

    FieldChunkMetricsType
    GetMetricsType() const override {
        return FieldChunkMetricsType::FLOAT;
    }

    nlohmann::json
    ToJson() const override {
        nlohmann::json j;
        j["type"] = FieldChunkMetricsTypeToString(GetMetricsType());
        if (this->has_value_) {
            j["min"] = min_;
            j["max"] = max_;
        }
        return j;
    }

 private:
    T min_;
    T max_;
};

template <typename T>
class IntFieldChunkMetrics : public FieldChunkMetrics {
 public:
    IntFieldChunkMetrics() = default;
    IntFieldChunkMetrics(T min, T max, BloomFilterPtr bloom_filter)
        : min_(min), max_(max), bloom_filter_(bloom_filter) {
        this->has_value_ = true;
    }

    std::unique_ptr<FieldChunkMetrics>
    Clone() const override {
        if (!this->has_value_) {
            return std::make_unique<NoneFieldChunkMetrics>();
        }
        return std::make_unique<IntFieldChunkMetrics>(
            min_, max_, bloom_filter_);
    }

    FieldChunkMetricsType
    GetMetricsType() const override {
        return FieldChunkMetricsType::INT;
    }

    bool
    CanSkipUnaryRange(OpType op_type, const Metrics& val) const override {
        if (!this->has_value_) {
            return false;
        }
        if (!std::holds_alternative<T>(val)) {
            return false;
        }
        const T& typed_val = std::get<T>(val);
        if (op_type == OpType::Equal && bloom_filter_) {
            return !bloom_filter_->Test(
                reinterpret_cast<const uint8_t*>(&typed_val),
                sizeof(typed_val));
        }
        return RangeShouldSkip(typed_val, min_, max_, op_type);
    }

    bool
    CanSkipIn(const std::vector<Metrics>& values) const override {
        if (!this->has_value_ || values.empty()) {
            return false;
        }
        for (const auto& v : values) {
            if (!std::holds_alternative<T>(v)) {
                return false;
            }
        }
        if (!bloom_filter_) {
            T typed_min = std::get<T>(values[0]);
            T typed_max = std::get<T>(values[0]);
            for (const auto& v : values) {
                const T& current_val = std::get<T>(v);
                if (current_val < typed_min) {
                    typed_min = current_val;
                }
                if (current_val > typed_max) {
                    typed_max = current_val;
                }
            }
            return RangeShouldSkip(
                typed_min, typed_max, min_, max_, true, true);
        }
        for (const auto& v : values) {
            const T& current_val = std::get<T>(v);
            if (bloom_filter_->Test(
                    reinterpret_cast<const uint8_t*>(&current_val),
                    sizeof(current_val))) {
                return false;
            }
        }
        return true;
    }

    bool
    CanSkipBinaryRange(const Metrics& lower_val,
                       const Metrics& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const override {
        if (!std::holds_alternative<T>(lower_val) ||
            !std::holds_alternative<T>(upper_val)) {
            return false;
        }
        if (!this->has_value_) {
            return false;
        }
        const T& typed_lower = std::get<T>(lower_val);
        const T& typed_upper = std::get<T>(upper_val);
        return RangeShouldSkip(typed_lower,
                               typed_upper,
                               min_,
                               max_,
                               lower_inclusive,
                               upper_inclusive);
    }

    nlohmann::json
    ToJson() const override {
        nlohmann::json j;
        j["type"] = FieldChunkMetricsTypeToString(GetMetricsType());

        if (this->has_value_) {
            j["min"] = min_;
            j["max"] = max_;
            if (bloom_filter_) {
                auto bf_data = bloom_filter_->ToJson();
                j["bloom_filter"] = nlohmann::json::binary(bf_data);
            }
        }

        return j;
    }

 private:
    T min_;
    T max_;
    BloomFilterPtr bloom_filter_{nullptr};
};

class StringFieldChunkMetrics : public FieldChunkMetrics {
 public:
    StringFieldChunkMetrics(std::string min,
                            std::string max,
                            BloomFilterPtr bloom_filter,
                            BloomFilterPtr ngram_bloom_filter)
        : min_(min),
          max_(max),
          bloom_filter_(bloom_filter),
          ngram_bloom_filter_(ngram_bloom_filter) {
        this->has_value_ = true;
    }

    std::unique_ptr<FieldChunkMetrics>
    Clone() const override {
        if (!this->has_value_) {
            return std::make_unique<NoneFieldChunkMetrics>();
        }
        return std::make_unique<StringFieldChunkMetrics>(
            min_, max_, bloom_filter_, ngram_bloom_filter_);
    }

    FieldChunkMetricsType
    GetMetricsType() const override {
        return FieldChunkMetricsType::STRING;
    }

    bool
    CanSkipUnaryRange(OpType op_type, const Metrics& val) const override {
        if (!this->has_value_) {
            return false;
        }
        auto typed_val = ExtractStringView(val);
        if (!typed_val.has_value()) {
            return false;
        }
        auto value = *typed_val;
        switch (op_type) {
            case OpType::Equal: {
                if (!bloom_filter_) {
                    return RangeShouldSkip(*typed_val,
                                           std::string_view(min_),
                                           std::string_view(max_),
                                           op_type);
                }
                return !bloom_filter_->Test(value);
            }
            case OpType::LessThan:
            case OpType::LessEqual:
            case OpType::GreaterThan:
            case OpType::GreaterEqual: {
                return RangeShouldSkip(*typed_val,
                                       std::string_view(min_),
                                       std::string_view(max_),
                                       op_type);
            }
            case OpType::InnerMatch:
            case OpType::PrefixMatch:
            case OpType::PostfixMatch: {
                if (!ngram_bloom_filter_ ||
                    typed_val->size() < DEFAULT_SKIPINDEX_MIN_NGRAM_LENGTH) {
                    return false;
                }

                ankerl::unordered_dense::set<std::string> ngrams;
                ExtractNgrams(
                    ngrams, *typed_val, DEFAULT_SKIPINDEX_MIN_NGRAM_LENGTH);
                for (const auto& ngram : ngrams) {
                    if (!ngram_bloom_filter_->Test(ngram)) {
                        return true;
                    }
                }
                return false;
            }
            default:
                return false;
        }
        return false;
    }

    bool
    CanSkipIn(const std::vector<Metrics>& values) const override {
        if (!this->has_value_ || values.empty()) {
            return false;
        }
        std::vector<std::string_view> string_values;
        string_values.reserve(values.size());
        for (const auto& v : values) {
            auto sv = ExtractStringView(v);
            if (!sv.has_value()) {
                return false;
            }
            string_values.push_back(*sv);
        }
        if (!bloom_filter_) {
            std::string_view min, max;
            for (auto v : string_values) {
                if (min.empty() || v < min) {
                    min = v;
                }
                if (max.empty() || v > max) {
                    max = v;
                }
            }
            return RangeShouldSkip(min,
                                   max,
                                   std::string_view(min_),
                                   std::string_view(max_),
                                   true,
                                   true);
        }
        for (auto v : string_values) {
            if (bloom_filter_->Test(v)) {
                return false;
            }
        }
        return true;
    }

    bool
    CanSkipBinaryRange(const Metrics& lower_val,
                       const Metrics& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const override {
        if (!this->has_value_) {
            return false;
        }
        auto typed_min = ExtractStringView(lower_val);
        auto typed_max = ExtractStringView(upper_val);
        if (!typed_min.has_value() || !typed_max.has_value()) {
            return false;
        }

        return RangeShouldSkip(*typed_min,
                               *typed_max,
                               std::string_view(min_),
                               std::string_view(max_),
                               lower_inclusive,
                               upper_inclusive);
    }

    nlohmann::json
    ToJson() const override {
        nlohmann::json j;
        j["type"] = FieldChunkMetricsTypeToString(GetMetricsType());

        if (this->has_value_) {
            j["min"] = min_;
            j["max"] = max_;
            if (bloom_filter_) {
                auto bf_data = bloom_filter_->ToJson();
                j["bloom_filter"] = nlohmann::json::binary(bf_data);
            }
            if (ngram_bloom_filter_) {
                auto ngram_bf_data = ngram_bloom_filter_->ToJson();
                j["ngram_bloom_filter"] = nlohmann::json::binary(ngram_bf_data);
            }
        }

        return j;
    }

    static std::optional<std::string_view>
    ExtractStringView(const Metrics& val) {
        if (std::holds_alternative<std::string_view>(val)) {
            return std::get<std::string_view>(val);
        } else if (std::holds_alternative<std::string>(val)) {
            return std::string_view(std::get<std::string>(val));
        }
        return std::nullopt;
    }

 private:
    std::string min_;
    std::string max_;
    BloomFilterPtr bloom_filter_{nullptr};
    BloomFilterPtr ngram_bloom_filter_{nullptr};
};

template <typename T>
inline std::unique_ptr<FieldChunkMetrics>
NewFieldMetrics(const nlohmann::json& data) {
    std::unique_ptr<FieldChunkMetrics> none_metrics =
        std::make_unique<NoneFieldChunkMetrics>();
    if (!data.contains("type")) {
        return none_metrics;
    }
    auto type = StringToFieldChunkMetricsType(data["type"].get<std::string>());
    switch (type) {
        case FieldChunkMetricsType::BOOLEAN: {
            if (!data.contains("has_true") || !data.contains("has_false")) {
                return none_metrics;
            }
            bool has_true = data["has_true"].get<bool>();
            bool has_false = data["has_false"].get<bool>();
            return std::make_unique<BooleanFieldChunkMetrics>(has_true,
                                                              has_false);
        }

        case FieldChunkMetricsType::FLOAT: {
            if (!data.contains("min") || !data.contains("max")) {
                return none_metrics;
            }
            T min = data["min"].get<T>();
            T max = data["max"].get<T>();
            return std::make_unique<FloatFieldChunkMetrics<T>>(min, max);
        }
        case FieldChunkMetricsType::INT: {
            if (!data.contains("min") || !data.contains("max")) {
                return none_metrics;
            }
            T min = data["min"].get<T>();
            T max = data["max"].get<T>();
            BloomFilterPtr bloom_filter = nullptr;
            if (data.contains("bloom_filter")) {
                bloom_filter = BloomFilterFromJson(data["bloom_filter"]);
            }
            return std::make_unique<IntFieldChunkMetrics<T>>(
                min, max, bloom_filter);
        }

        case FieldChunkMetricsType::STRING: {
            if (!data.contains("min") || !data.contains("max")) {
                return none_metrics;
            }
            std::string min = data["min"].get<std::string>();
            std::string max = data["max"].get<std::string>();
            BloomFilterPtr bloom_filter = nullptr;
            if (data.contains("bloom_filter")) {
                bloom_filter = BloomFilterFromJson(data["bloom_filter"]);
            }
            BloomFilterPtr ngram_filter = nullptr;
            if (data.contains("ngram_bloom_filter")) {
                ngram_filter = BloomFilterFromJson(data["ngram_bloom_filter"]);
            }
            return std::make_unique<StringFieldChunkMetrics>(
                min, max, bloom_filter, ngram_filter);
        }
        default:
            return none_metrics;
    }
    return none_metrics;
}

class SkipIndexStatsBuilder {
 public:
    SkipIndexStatsBuilder() = default;
    SkipIndexStatsBuilder(const Config& config) {
        auto enable_bloom_filter =
            GetValueFromConfig<bool>(config, "enable_bloom_filter");
        if (enable_bloom_filter.has_value()) {
            enable_bloom_filter_ = *enable_bloom_filter;
        }
    }

    std::unique_ptr<FieldChunkMetrics>
    Build(DataType data_type,
          const std::shared_ptr<parquet::Statistics>& statistic) const;

    std::unique_ptr<FieldChunkMetrics>
    Build(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
          int col_idx,
          arrow::Type::type data_type) const;

    std::unique_ptr<FieldChunkMetrics>
    Build(DataType data_type, const Chunk* chunk) const;

 private:
    template <typename T>
    struct metricsInfo {
        int64_t total_rows_ = 0;
        int64_t null_count_ = 0;

        MetricsDataType<T> min_;
        MetricsDataType<T> max_;

        bool contains_true_ = false;
        bool contains_false_ = false;

        ankerl::unordered_dense::set<MetricsDataType<T>> unique_values_;
        ankerl::unordered_dense::set<std::string> ngram_values_;
    };

    template <typename ParquetType, typename T>
    metricsInfo<T>
    ProcessFieldMetrics(
        const std::shared_ptr<parquet::Statistics>& statistics) const {
        auto typed_statistics =
            std::dynamic_pointer_cast<parquet::TypedStatistics<ParquetType>>(
                statistics);
        MetricsDataType<T> min;
        MetricsDataType<T> max;
        if constexpr (std::is_same_v<T, std::string>) {
            min = std::string_view(typed_statistics->min());
            max = std::string_view(typed_statistics->max());
        } else {
            min = static_cast<T>(typed_statistics->min());
            max = static_cast<T>(typed_statistics->max());
        }
        return {typed_statistics->num_values(),
                typed_statistics->null_count(),
                min,
                max,
                false,
                false,
                {}};
    }

    template <typename T, typename ArrayType>
    metricsInfo<T>
    ProcessFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) const {
        T min, max;
        int64_t total_rows = 0;
        int64_t null_count = 0;
        bool contains_true = false;
        bool contains_false = false;
        ankerl::unordered_dense::set<T> unique_values;

        bool has_first_valid = false;
        for (const auto& batch : batches) {
            auto arr = batch->column(col_idx);
            auto array = std::static_pointer_cast<ArrayType>(arr);
            for (int64_t i = 0; i < array->length(); ++i) {
                if (array->IsNull(i)) {
                    null_count++;
                    continue;
                }
                T value = array->Value(i);
                if constexpr (std::is_same_v<T, bool>) {
                    if (value) {
                        contains_true = true;
                    } else {
                        contains_false = true;
                    }
                    continue;
                }
                if (!has_first_valid) {
                    min = value;
                    max = value;
                    has_first_valid = true;
                } else {
                    if (value < min) {
                        min = value;
                    }
                    if (value > max) {
                        max = value;
                    }
                }
                if (!enable_bloom_filter_) {
                    continue;
                }
                if constexpr (std::is_integral_v<T>) {
                    unique_values.insert(value);
                }
            }
            total_rows += array->length();
        }
        return {total_rows,
                null_count,
                min,
                max,
                contains_true,
                contains_false,
                std::move(unique_values)};
    }

    metricsInfo<std::string>
    ProcessStringFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) const {
        int64_t total_rows = 0;
        int64_t null_count = 0;
        std::string_view min;
        std::string_view max;
        ankerl::unordered_dense::set<std::string_view> unique_values;
        ankerl::unordered_dense::set<std::string> ngram_values;

        bool has_first_valid = false;
        for (const auto& batch : batches) {
            auto arr = batch->column(col_idx);
            auto array = std::static_pointer_cast<arrow::StringArray>(arr);
            for (int64_t i = 0; i < array->length(); ++i) {
                if (array->IsNull(i)) {
                    null_count++;
                    continue;
                }
                auto value = array->GetView(i);
                if (!has_first_valid) {
                    min = value;
                    max = value;
                    has_first_valid = true;
                } else {
                    if (value < min) {
                        min = value;
                    }
                    if (value > max) {
                        max = value;
                    }
                }
                if (!enable_bloom_filter_ ||
                    unique_values.find(value) != unique_values.end()) {
                    continue;
                }
                unique_values.insert(value);
                size_t length = value.length();
                ExtractNgrams(
                    ngram_values, value, DEFAULT_SKIPINDEX_MIN_NGRAM_LENGTH);
            }
            total_rows += array->length();
        }
        return {total_rows,
                null_count,
                min,
                max,
                false,
                false,
                std::move(unique_values),
                std::move(ngram_values)};
    }

    template <typename T>
    metricsInfo<T>
    ProcessFieldMetrics(const T* data,
                        const bool* valid_data,
                        int64_t count) const {
        bool has_first_valid = false;
        T min, max;
        int64_t total_rows = count;
        int64_t null_count = 0;
        bool contains_true = false;
        bool contains_false = false;
        ankerl::unordered_dense::set<T> unique_values;

        for (int64_t i = 0; i < count; i++) {
            T value = data[i];
            if (valid_data != nullptr && !valid_data[i]) {
                null_count++;
                continue;
            }
            if constexpr (std::is_same_v<T, bool>) {
                if (value) {
                    contains_true = true;
                } else {
                    contains_false = true;
                }
                continue;
            }
            if (!has_first_valid) {
                min = value;
                max = value;
                has_first_valid = true;
            } else {
                if (value < min) {
                    min = value;
                }
                if (value > max) {
                    max = value;
                }
            }
            if (!enable_bloom_filter_) {
                continue;
            }
            if constexpr (std::is_integral_v<T>) {
                unique_values.insert(value);
            }
        }
        return {total_rows,
                null_count,
                min,
                max,
                contains_true,
                contains_false,
                std::move(unique_values)};
    }

    metricsInfo<std::string>
    ProcessStringFieldMetrics(const StringChunk* chunk) const {
        // all captured by reference
        bool has_first_valid = false;
        int64_t total_rows = chunk->RowNums();
        int64_t null_count = 0;
        std::string_view min;
        std::string_view max;
        ankerl::unordered_dense::set<std::string_view> unique_values;
        ankerl::unordered_dense::set<std::string> ngram_values;

        for (int64_t i = 0; i < total_rows; ++i) {
            bool is_valid = chunk->isValid(i);
            if (!is_valid) {
                null_count++;
                continue;
            }
            auto value = chunk->operator[](i);
            if (!has_first_valid) {
                min = value;
                max = value;
                has_first_valid = true;
            } else {
                if (value < min) {
                    min = value;
                }
                if (value > max) {
                    max = value;
                }
            }
            if (!enable_bloom_filter_ ||
                unique_values.find(value) != unique_values.end()) {
                continue;
            }
            unique_values.insert(value);
            size_t length = value.length();
            ExtractNgrams(
                ngram_values, value, DEFAULT_SKIPINDEX_MIN_NGRAM_LENGTH);
        }
        return {total_rows,
                null_count,
                min,
                max,
                false,
                false,
                std::move(unique_values),
                std::move(ngram_values)};
    }

    template <typename T>
    std::unique_ptr<FieldChunkMetrics>
    LoadMetrics(const metricsInfo<T>& info) const {
        if (info.total_rows_ - info.null_count_ == 0) {
            return std::make_unique<NoneFieldChunkMetrics>();
        }
        if constexpr (std::is_same_v<T, bool>) {
            if (info.contains_true_ && info.contains_false_) {
                return std::make_unique<NoneFieldChunkMetrics>();
            }
            return std::make_unique<BooleanFieldChunkMetrics>(
                info.contains_true_, info.contains_false_);
        } else {
            T min, max;
            if constexpr (std::is_same_v<T, std::string>) {
                min = std::string(info.min_);
                max = std::string(info.max_);
            } else {
                min = info.min_;
                max = info.max_;
            }
            if constexpr (std::is_floating_point_v<T>) {
                return std::make_unique<FloatFieldChunkMetrics<T>>(min, max);
            }
            if (!enable_bloom_filter_) {
                if constexpr (std::is_same_v<T, std::string>) {
                    return std::make_unique<StringFieldChunkMetrics>(
                        min, max, nullptr, nullptr);
                }
                return std::make_unique<IntFieldChunkMetrics<T>>(
                    min, max, nullptr);
            }
            BloomFilterPtr bloom_filter =
                NewBloomFilterWithType(info.unique_values_.size(),
                                       DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE,
                                       BFType::Blocked);
            if constexpr (std::is_same_v<T, std::string>) {
                for (const auto& val : info.unique_values_) {
                    bloom_filter->Add(val);
                }
                if (info.ngram_values_.empty()) {
                    return std::make_unique<StringFieldChunkMetrics>(
                        min, max, std::move(bloom_filter), nullptr);
                }
                BloomFilterPtr ngram_bloom_filter = NewBloomFilterWithType(
                    info.ngram_values_.size(),
                    DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE,
                    BFType::Blocked);
                for (const auto& ngram : info.ngram_values_) {
                    ngram_bloom_filter->Add(std::string_view(ngram));
                }
                return std::make_unique<StringFieldChunkMetrics>(
                    min,
                    max,
                    std::move(bloom_filter),
                    std::move(ngram_bloom_filter));
            }

            for (const auto& val : info.unique_values_) {
                bloom_filter->Add(reinterpret_cast<const uint8_t*>(&val),
                                  sizeof(val));
            }
            return std::make_unique<IntFieldChunkMetrics<T>>(
                min, max, std::move(bloom_filter));
        }
    }

 private:
    bool enable_bloom_filter_ = false;
};

}  // namespace milvus::index
