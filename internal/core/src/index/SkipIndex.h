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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "ankerl/unordered_dense.h"
#include "arrow/record_batch.h"
#include "arrow/type_traits.h"
#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/Types.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/common/constants.h"

namespace milvus {

using Metrics = std::
    variant<int8_t, int16_t, int32_t, int64_t, float, double, std::string_view>;

// MetricsDataType is used to avoid copy when get min/max value from FieldChunkMetrics
template <typename T>
using MetricsDataType =
    std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

// ReverseMetricsDataType is used to avoid copy when get min/max value from FieldChunkMetrics
template <typename T>
using ReverseMetricsDataType =
    std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;

enum class FieldChunkMetricType {
    MINMAX,
    SET,
    BLOOM_FILTER,
    NGRAM_FILTER,
    TOKEN_FILTER,
};

class FieldChunkMetric {
 public:
    virtual ~FieldChunkMetric() = default;

    virtual FieldChunkMetricType
    GetType() const = 0;

    virtual std::string
    Serialize() const = 0;

    bool hasValue_ = false;
};

template <typename T>
class MinMaxFieldChunkMetric : public FieldChunkMetric {
 public:
    explicit MinMaxFieldChunkMetric(T min, T max) : min_(std::move(min)), max_(std::move(max)) {
        hasValue_ = true;
    }

    explicit MinMaxFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return ;
        }
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        if constexpr (std::is_same_v<T, std::string>) {
            size_t min_len, max_len;
            ss.read(reinterpret_cast<char*>(&min_len), sizeof(min_len));
            min_.resize(min_len);
            ss.read(min_.data(), min_len);

            ss.read(reinterpret_cast<char*>(&max_len), sizeof(max_len));
            max_.resize(max_len);
            ss.read(max_.data(), max_len);
        } else {
            ss.read(reinterpret_cast<char*>(&min_), sizeof(min_));
            ss.read(reinterpret_cast<char*>(&max_), sizeof(max_));
        }
        hasValue_ = true;
    }

    bool
    CanSkipUnaryRange(OpType op_type, const T& val) const {
        auto [lower_bound, upper_bound] = GetMinMax();
        if (lower_bound == MetricsDataType<T>() ||
            upper_bound == MetricsDataType<T>()) {
            return false;
        }
        return RangeShouldSkip(val, lower_bound, upper_bound, op_type);
    }

    bool
    CanSkipIn(const std::vector<T>& values) const {
        if (!hasValue_) {
            return false;
        }
        const auto [min_val, max_val] =
            std::minmax_element(values.begin(), values.end());
        return CanSkipBinaryRange(*min_val, *max_val, true, true);
    }

    bool
    CanSkipBinaryRange(const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        auto [lower_bound, upper_bound] = GetMinMax();
        if (lower_bound == MetricsDataType<T>() ||
            upper_bound == MetricsDataType<T>()) {
            return false;
        }
        bool should_skip = false;
        if (lower_inclusive && upper_inclusive) {
            should_skip =
                (lower_val > upper_bound) || (upper_val < lower_bound);
        } else if (lower_inclusive && !upper_inclusive) {
            should_skip =
                (lower_val > upper_bound) || (upper_val <= lower_bound);
        } else if (!lower_inclusive && upper_inclusive) {
            should_skip =
                (lower_val >= upper_bound) || (upper_val < lower_bound);
        } else {
            should_skip =
                (lower_val >= upper_bound) || (upper_val <= lower_bound);
        }
        return should_skip;
    }

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::MINMAX;
    }

    std::string
    Serialize() const override {
        std::stringstream ss(std::ios::binary | std::ios::out);
        if constexpr (std::is_same_v<T, std::string>) {
            size_t min_len = min_.length();
            ss.write(reinterpret_cast<const char*>(&min_len), sizeof(min_len));
            ss.write(min_.data(), min_len);

            size_t max_len = max_.length();
            ss.write(reinterpret_cast<const char*>(&max_len), sizeof(max_len));
            ss.write(max_.data(), max_len);
        } else {
            ss.write(reinterpret_cast<const char*>(&min_), sizeof(min_));
            ss.write(reinterpret_cast<const char*>(&max_), sizeof(max_));
        }
        return ss.str();
    }

 private:
    bool
    RangeShouldSkip(const T& value,
                    const MetricsDataType<T> lower_bound,
                    const MetricsDataType<T> upper_bound,
                    OpType op_type) const {
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

    std::pair<MetricsDataType<T>, MetricsDataType<T>>
    GetMinMax() const {
        if constexpr (std::is_same_v<T, std::string>) {
            return {std::string_view(min_), std::string_view(max_)};
        } else {
            return {min_, max_};
        }
    }

    T min_;
    T max_;
};

template <typename T>
class SetFieldChunkMetric : public FieldChunkMetric {
 public:
    explicit SetFieldChunkMetric(std::unordered_set<T> unique_values)
        : unique_values_(std::move(unique_values)) {
        hasValue_ = true;
    }

    explicit SetFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        // 1. 读取集合大小
        uint32_t count;
        ss.read(reinterpret_cast<char*>(&count), sizeof(count));
        unique_values_.reserve(count);

        // 2. 依次读取每个元素
        for (uint32_t i = 0; i < count; ++i) {
            if constexpr (std::is_same_v<T, std::string>) {
                uint64_t len;
                ss.read(reinterpret_cast<char*>(&len), sizeof(len));
                std::string value(len, '\0');
                ss.read(value.data(), len);
                unique_values_.insert(std::move(value));
            } else {
                T value;
                ss.read(reinterpret_cast<char*>(&value), sizeof(value));
                unique_values_.insert(std::move(value));
            }
        }
        hasValue_ = true;
    }

    bool
    CanSkipEqual(const T& value) const {
        return unique_values_.find(value) == unique_values_.end();
    }

    bool
    CanSkipIn(const std::vector<T>& values) const {
        for (const auto& value : values) {
            if (unique_values_.count(value)) {
                return false;  // 找到一个匹配值，不能跳过
            }
        }
        return true;  // 没有找到任何匹配值，可以跳过
    }

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::SET;
    }

    std::string
    Serialize() const override {
        std::stringstream ss(std::ios::binary | std::ios::out);
        // 1. 写入集合大小
        uint32_t count = unique_values_.size();
        ss.write(reinterpret_cast<const char*>(&count), sizeof(count));

        // 2. 依次写入每个元素
        for (const auto& value : unique_values_) {
            if constexpr (std::is_same_v<T, std::string>) {
                uint64_t len = value.length();
                ss.write(reinterpret_cast<const char*>(&len), sizeof(len));
                ss.write(value.data(), len);
            } else {
                ss.write(reinterpret_cast<const char*>(&value), sizeof(value));
            }
        }
        return ss.str();
    }

 private:
    std::unordered_set<T> unique_values_;
};

template <>
class SetFieldChunkMetric<bool> : public FieldChunkMetric {
 public:
    explicit SetFieldChunkMetric(const bool contains_true, const bool contains_false)
        : contains_true_(contains_true), contains_false_(contains_false) {
        hasValue_ = true;
    }

    explicit SetFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        ss.read(reinterpret_cast<char*>(&contains_true_),
                sizeof(contains_true_));
        ss.read(reinterpret_cast<char*>(&contains_false_),
                sizeof(contains_false_));
        hasValue_ = true;
    }

    bool
    CanSkipEqual(const bool value) const {
        return !((value && contains_true_) || (!value && contains_false_));
    }

    bool
    CanSkipIn(const std::vector<bool>& values) const {
        if (values.size() != 2) {
            return false;
        }
        bool query_contains_true = values[0];
        bool query_contains_false = values[1];
        if ((query_contains_true && contains_true_) ||
            (query_contains_false && contains_false_)) {
            return false;
        }
        return true;
    }

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::SET;
    }

    std::string
    Serialize() const override {
        std::stringstream ss(std::ios::binary | std::ios::out);
        ss.write(reinterpret_cast<const char*>(&contains_true_),
                 sizeof(contains_true_));
        ss.write(reinterpret_cast<const char*>(&contains_false_),
                 sizeof(contains_false_));
        return ss.str();
    }

 private:
    bool contains_true_ = false;
    bool contains_false_ = false;
};

template <typename T>
class BloomFilter {
 public:
    explicit BloomFilter(const std::unordered_set<T>& items, double p = 0.01) {
        Initialize(items.size(), p);
        if (bit_size_ == 0) {
            return;
        }
        for (const auto& item : items) {
            Add(item);
        }
    }

    // 用于反序列化
    BloomFilter(size_t hash_count,
                size_t bit_size,
                std::vector<uint64_t> bit_array)
        : hash_count_(hash_count),
          bit_size_(bit_size),
          bit_array_(std::move(bit_array)) {
    }

    void
    Add(const T& item) {
        if (bit_size_ == 0) {
            return;
        }
        uint64_t hash1 = Hash(item, 0x9e3779b9);
        uint64_t hash2 = Hash(item, hash1);
        for (size_t i = 0; i < hash_count_; ++i) {
            size_t bit_pos = (hash1 + i * hash2) % bit_size_;
            bit_array_[bit_pos / 64] |= (1ULL << (bit_pos % 64));
        }
    }

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

    static std::unique_ptr<BloomFilter<T>>
    Deserialize(const std::string& data) {
        if (data.empty()) {
            return std::unique_ptr<BloomFilter<T>>(
                new BloomFilter<T>(0, 0, {}));
        }

        std::stringstream ss(data, std::ios::binary | std::ios::in);
        uint64_t hash_count, bit_size, array_size;
        ss.read(reinterpret_cast<char*>(&hash_count), sizeof(hash_count));
        ss.read(reinterpret_cast<char*>(&bit_size), sizeof(bit_size));
        ss.read(reinterpret_cast<char*>(&array_size), sizeof(array_size));

        std::vector<uint64_t> bit_array(array_size);
        ss.read(reinterpret_cast<char*>(bit_array.data()),
                array_size * sizeof(uint64_t));

        return std::make_unique<BloomFilter<T>>(
            hash_count, bit_size, std::move(bit_array));
    }

 private:
    void
    Initialize(size_t n, double p) {
        if (n == 0) {
            bit_size_ = 0;
            hash_count_ = 0;
            return;
        }
        bit_size_ = static_cast<size_t>(-1.0 * n * std::log(p) /
                                        (std::log(2) * std::log(2)));
        hash_count_ = static_cast<size_t>((static_cast<double>(bit_size_) / n) *
                                          std::log(2));
        if (hash_count_ < 1) {
            hash_count_ = 1;
        }
        if (hash_count_ > 10) {
            hash_count_ = 10;
        }
        bit_size_ = ((bit_size_ + 63) / 64) * 64;
        if (bit_size_ > 0) {
            bit_array_.assign(bit_size_ / 64, 0);
        }
    }

    uint64_t
    Hash(const T& item, uint64_t seed) const {
        if constexpr (std::is_same_v<T, std::string>) {
            return std::hash<std::string>{}(item) ^ seed;
        } else {
            return std::hash<T>{}(item) ^ seed;
        }
    }

    std::vector<uint64_t> bit_array_;
    uint64_t hash_count_;
    uint64_t bit_size_;
};

template <typename T>
class BloomFilterFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<BloomFilter<MetricsDataType<T>>> filter_;

 public:
    explicit BloomFilterFieldChunkMetric(
        const std::unordered_set<MetricsDataType<T>>& unique_values) {
        filter_ =
            std::make_unique<BloomFilter<MetricsDataType<T>>>(unique_values);
        hasValue_ = filter_->IsValid();
    }

    explicit BloomFilterFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        filter_ = BloomFilter<MetricsDataType<T>>::Deserialize(data);
    }

    bool
    CanSkipEqual(const T& value) const {
        if (!hasValue_) {
            return false;
        }
        return !filter_->MightContain(value);
    }

    bool
    CanSkipIn(const std::vector<T>& values) const {
        if (!hasValue_) {
            return false;
        }
        for (const auto& value : values) {
            if (filter_->MightContain(value)) {
                return false;
            }
        }
        return true;
    }

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::BLOOM_FILTER;
    }

    std::string
    Serialize() const override {
        return filter_ ? filter_->Serialize() : "";
    }
};

class NgramFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<BloomFilter<std::string_view>> filter_;
    size_t ngram_size_;

 public:
    explicit NgramFieldChunkMetric(
        const std::unordered_set<std::string_view>& unique_values,
        size_t ngram_size = 3)
        : ngram_size_(ngram_size) {

        std::unordered_set<std::string_view> unique_ngrams;
        for (const auto& text : unique_values) {
            if (text.length() >= ngram_size_) {
                for (size_t j = 0; j <= text.length() - ngram_size_; ++j) {
                    unique_ngrams.insert(text.substr(j, ngram_size_));
                }
            }
        }

        filter_ =
            std::make_unique<BloomFilter<std::string_view>>(unique_ngrams);
        hasValue_ = filter_->IsValid();
    }

    explicit NgramFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        filter_ = BloomFilter<std::string_view>::Deserialize(data);
        hasValue_ = filter_->IsValid();
    }

    bool
    CanSkipSubstringMatch(const std::string& pattern) const {
        if (!hasValue_ || pattern.length() < ngram_size_) {
            return false;
        }

        for (size_t i = 0; i <= pattern.length() - ngram_size_; ++i) {
            std::string ngram = pattern.substr(i, ngram_size_);
            // 步骤3: 查询逻辑委托给内部的 filter_
            if (!filter_->MightContain(ngram)) {
                return true;
            }
        }
        return false;
    }

    bool
    CanSkipPrefixMatch(const std::string& prefix) const {
        return CanSkipSubstringMatch(prefix);
    }

    bool
    CanSkipPostfixMatch(const std::string& suffix) const {
        return CanSkipSubstringMatch(suffix);
    }

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::NGRAM_FILTER;
    }

    std::string
    Serialize() const override {
        return filter_ ? filter_->Serialize() : "";
    }
};

class TokenFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<BloomFilter<std::string>> filter_;

 public:
    explicit TokenFieldChunkMetric(const std::unordered_set<std::string_view>& unique_values) {
        std::unordered_set<std::string> unique_tokens;
        for (const auto& text : unique_values) {
            auto tokens = TokenizeText(text);
            for (const auto& token : tokens) {
                unique_tokens.insert(token);
            }
        }

        filter_ = std::make_unique<BloomFilter<std::string>>(unique_tokens);
        hasValue_ = filter_->IsValid();
    }

    explicit TokenFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        filter_ = BloomFilter<std::string>::Deserialize(data);
        hasValue_ = filter_->IsValid();
    }

    bool
    CanSkipFullTextSearch(const std::vector<std::string>& search_tokens) const {
        if (!hasValue_) {
            return false;
        }

        for (const auto& token : search_tokens) {
            if (!filter_->MightContain(token)) {
                return true;  
            }
        }
        return false;  
    }

    bool
    CanSkipAnyTokenMatch(const std::vector<std::string>& search_tokens) const {
        if (!hasValue_) {
            return false;
        }

        // 检查是否包含任意搜索词
        for (const auto& token : search_tokens) {
            // 委托给内部的 filter_
            if (filter_->MightContain(token)) {
                return false;  // 找到可能匹配的词，不能跳过
            }
        }
        return true;  // 确定不包含任何词，可以跳过
    }

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::TOKEN_FILTER;
    }

    std::string
    Serialize() const override {
        return filter_ ? filter_->Serialize() : "";
    }

 private:
    // 分词逻辑保持不变，作为一个私有辅助函数
    std::vector<std::string>
    TokenizeText(const std::string_view& text) {
        std::vector<std::string> tokens;
        std::string current_token;

        for (char c : text) {
            if (std::isalnum(c)) {
                current_token += std::tolower(c);
            } else {
                if (!current_token.empty()) {
                    tokens.push_back(current_token);
                    current_token.clear();
                }
            }
        }

        if (!current_token.empty()) {
            tokens.push_back(current_token);
        }

        return tokens;
    }
};

class FieldChunkMetrics {
 public:
    explicit FieldChunkMetrics(
        arrow::Type::type data_type,
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx);

    explicit FieldChunkMetrics(arrow::Type::type data_type)
        : data_type_(data_type){};

    void
    Load(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
         int col_idx);

    std::unique_ptr<FieldChunkMetric>
    LoadMetric(arrow::Type::type data_type,
               FieldChunkMetricType metric_type,
               const std::string& data);

    // 获取特定类型的统计
    template <typename MetricType>
    MetricType*
    GetMetric(FieldChunkMetricType type) const {
        for (const auto& [metric_type, metric] : metrics_) {
            if (metric_type == type) {
                return dynamic_cast<MetricType*>(metric.get());
            }
        }
        return nullptr;
    }

    arrow::Type::type
    GetDataType() const {
        return data_type_;
    }

    size_t
    Size() const {
        return metrics_.size();
    }

    std::string
    Serialize() const;

    void
    Deserialize(const std::string& data);

    static bool
    CanSkipField(arrow::Type::type type) {
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

 private:
    template <typename T>
    struct metricsInfo {
        int64_t total_rows_ = 0;
        int64_t null_count_ = 0;

        MetricsDataType<T> min_;
        MetricsDataType<T> max_;

        bool contains_true_ = false;
        bool contains_false_ = false;

        std::unordered_set<MetricsDataType<T>> unique_values_;

        size_t string_length_ = 0;
        size_t max_string_length_ = 0;
        bool has_spaces_ = false;
    };

    template <typename T, typename ArrayType>
    void
    LoadMetrics(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                int col_idx) {
        metricsInfo<T> metrics_info{};
        ProcessFieldMetrics<T, ArrayType>(batches, col_idx, metrics_info);
        if (metrics_info.total_rows_ - metrics_info.null_count_ < 10) {
            return;
        }
        metrics_.emplace_back(FieldChunkMetricType::MINMAX,
                              std::make_unique<MinMaxFieldChunkMetric<T>>(
                                  metrics_info.min_, metrics_info.max_));
        if (metrics_info.unique_values_.size() * 10 <
            metrics_info.total_rows_ - metrics_info.null_count_) {
            metrics_.emplace_back(FieldChunkMetricType::SET,
                                  std::make_unique<SetFieldChunkMetric<T>>(
                                      std::move(metrics_info.unique_values_)));
        } else {
            metrics_.emplace_back(
                FieldChunkMetricType::BLOOM_FILTER,
                std::make_unique<BloomFilterFieldChunkMetric<T>>(
                    metrics_info.unique_values_));
        }
    }

    void
    LoadBooleanMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) {
        metricsInfo<bool> info{};
        ProcessBooleanFieldMetrics(batches, col_idx, info);
        if (info.total_rows_ - info.null_count_ == 0) {
            return;
        }
        metrics_.emplace_back(FieldChunkMetricType::SET,
                              std::make_unique<SetFieldChunkMetric<bool>>(
                                  info.contains_true_, info.contains_false_));
    }

    void
    LoadStringMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) {
        metricsInfo<std::string> info{};
        ProcessStringFieldMetrics(batches, col_idx, info);
        if (info.total_rows_ - info.null_count_ < 10) {
            return;
        }
        metrics_.emplace_back(
            FieldChunkMetricType::MINMAX,
            std::make_unique<MinMaxFieldChunkMetric<std::string>>(
                std::string(info.min_), std::string(info.max_)));
        if (info.unique_values_.size() * 10 <
            info.total_rows_ - info.null_count_) {
            std::unordered_set<std::string> unique_values;
            for (const auto& val : info.unique_values_) {
                unique_values.insert(std::string(val));
            }
            metrics_.emplace_back(
                FieldChunkMetricType::SET,
                std::make_unique<SetFieldChunkMetric<std::string>>(
                    std::move(unique_values)));
        } else {
            metrics_.emplace_back(
                FieldChunkMetricType::BLOOM_FILTER,
                std::make_unique<BloomFilterFieldChunkMetric<std::string>>(
                    info.unique_values_));
        }

        if (info.has_spaces_) {
            metrics_.emplace_back(FieldChunkMetricType::NGRAM_FILTER,
                                  std::make_unique<NgramFieldChunkMetric>(
                                      info.unique_values_, 3));
            metrics_.emplace_back(
                FieldChunkMetricType::TOKEN_FILTER,
                std::make_unique<TokenFieldChunkMetric>(info.unique_values_));
        }
    }

    template <typename T, typename ArrayType>
    void
    ProcessFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx,
        metricsInfo<T>& info) {
        bool has_first_valid = false;

        for (const auto& batch : batches) {
            auto arr = batch->column(col_idx);
            auto array = std::static_pointer_cast<ArrayType>(arr);
            for (int64_t i = 0; i < array->length(); ++i) {
                if (array->IsNull(i)) {
                    info.null_count_++;
                    continue;
                }
                T value = array->Value(i);

                if (!has_first_valid) {
                    info.min_ = value;
                    info.max_ = value;
                    has_first_valid = true;
                } else {
                    if (value < info.min_) {
                        info.min_ = value;
                    }
                    if (value > info.max_) {
                        info.max_ = value;
                    }
                }
                info.unique_values_.insert(value);
            }
            info.total_rows_ += array->length();
        }
    }

    void
    ProcessStringFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx,
        metricsInfo<std::string>& info) {
        bool has_first_valid = false;
        for (const auto& batch : batches) {
            auto array = batch->column(col_idx);
            auto string_array =
                std::static_pointer_cast<arrow::StringArray>(array);
            for (int64_t i = 0; i < string_array->length(); ++i) {
                if (string_array->IsNull(i)) {
                    info.null_count_++;
                    continue;
                }
                std::string_view value = string_array->GetView(i);
                size_t length = value.length();
                if (!has_first_valid) {
                    info.min_ = value;
                    info.max_ = value;
                    has_first_valid = true;
                } else {
                    if (value < info.min_) {
                        info.min_ = value;
                    }
                    if (value > info.max_) {
                        info.max_ = value;
                    }
                }
                info.unique_values_.insert(value);
                info.string_length_ += length;
                info.max_string_length_ =
                    std::max(info.max_string_length_, length);
                if (!info.has_spaces_ && value.find(' ') != std::string::npos) {
                    info.has_spaces_ = true;
                }
            }
            info.total_rows_ += string_array->length();
        }
    }

    void
    ProcessBooleanFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx,
        metricsInfo<bool>& info) {
        for (const auto& batch : batches) {
            auto array = batch->column(col_idx);
            auto bool_array =
                std::static_pointer_cast<arrow::BooleanArray>(array);
            for (int64_t i = 0; i < bool_array->length(); ++i) {
                if (bool_array->IsNull(i)) {
                    info.null_count_++;
                    continue;
                }
                bool value = bool_array->Value(i);
                if (value) {
                    info.contains_true_ = true;
                } else {
                    info.contains_false_ = true;
                }
            }
            info.total_rows_ += bool_array->length();
        }
    }

    std::vector<
        std::pair<FieldChunkMetricType, std::unique_ptr<FieldChunkMetric>>>
        metrics_;
    arrow::Type::type data_type_;
};

class ChunkSkipIndex : public milvus_storage::Metadata {
 public:
    static constexpr const char* KEY = "SKIP_INDEX";

    ChunkSkipIndex() = default;

    explicit ChunkSkipIndex(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);

    bool
    IsEmpty() const {
        return field_chunk_metrics_.empty();
    }

    const std::vector<std::pair<FieldId, std::unique_ptr<FieldChunkMetrics>>>&
    GetMetrics() const {
        return field_chunk_metrics_;
    }

    std::vector<std::pair<FieldId, std::unique_ptr<FieldChunkMetrics>>>
    Take() {
        return std::move(field_chunk_metrics_);
    }

    std::string
    Serialize() const override {
        std::stringstream ss(std::ios::binary | std::ios::out);

        uint32_t count = field_chunk_metrics_.size();
        ss.write(reinterpret_cast<const char*>(&count), sizeof(count));

        for (const auto& [field_id, metrics] : field_chunk_metrics_) {
            int64_t id = field_id.get();
            ss.write(reinterpret_cast<const char*>(&id), sizeof(id));

            arrow::Type::type data_type = metrics->GetDataType();
            ss.write(reinterpret_cast<const char*>(&data_type),
                     sizeof(data_type));

            std::string data = metrics->Serialize();
            uint64_t len = data.length();
            ss.write(reinterpret_cast<const char*>(&len), sizeof(len));
            ss.write(data.data(), len);
        }
        return ss.str();
    };

    void
    Deserialize(const std::string& data) override {
        if (data.empty()) {
            return;
        }
        std::stringstream ss(data, std::ios::binary | std::ios::in);

        uint32_t count;
        ss.read(reinterpret_cast<char*>(&count), sizeof(count));
        field_chunk_metrics_.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            int64_t field_id_val;
            ss.read(reinterpret_cast<char*>(&field_id_val),
                    sizeof(field_id_val));

            arrow::Type::type data_type;
            ss.read(reinterpret_cast<char*>(&data_type), sizeof(data_type));

            uint64_t len;
            ss.read(reinterpret_cast<char*>(&len), sizeof(len));
            std::string field_data(len, '\0');
            ss.read(&field_data[0], len);

            auto field_chunk_skipindex =
                std::make_unique<FieldChunkMetrics>(data_type);
            field_chunk_skipindex->Deserialize(field_data);

            field_chunk_metrics_.emplace_back(FieldId(field_id_val),
                                              std::move(field_chunk_skipindex));
        }
    };

 private:
    std::vector<std::pair<FieldId, std::unique_ptr<FieldChunkMetrics>>>
        field_chunk_metrics_;
};

class ChunkSkipIndexAppender : public milvus_storage::MetadataAppender {
 public:
    ChunkSkipIndexAppender() = default;

 protected:
    std::unique_ptr<milvus_storage::Metadata>
    Create(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
        override {
        return std::make_unique<ChunkSkipIndex>(batches);
    }
};

class SkipIndex {
 public:
    template <typename T>
    struct IsAllowedType {
        static constexpr bool isAllowedType =
            std::is_integral<T>::value || std::is_floating_point<T>::value ||
            std::is_same<T, std::string>::value ||
            std::is_same<T, std::string_view>::value;
        static constexpr bool isDisabledType =
            std::is_same<T, milvus::Json>::value ||
            std::is_same<T, bool>::value;
        static constexpr bool value = isAllowedType && !isDisabledType;

        static constexpr bool value_for_arith =
            std::is_integral<T>::value || std::is_floating_point<T>::value;

        static constexpr bool value_for_in = isAllowedType || std::is_same<T, bool>::value;
    };

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }
        if (op_type == OpType::Equal) {
            if (auto set_metrics =
                    field_chunk_skipindex->GetMetric<SetFieldChunkMetric<T>>(
                        FieldChunkMetricType::SET);
                set_metrics) {
                return set_metrics->CanSkipEqual(val);
            }
            if (auto bloom_metrics =
                    field_chunk_skipindex
                        ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                            FieldChunkMetricType::BLOOM_FILTER);
                bloom_metrics) {
                return bloom_metrics->CanSkipEqual(val);
            }
        }
        if (auto minmax_metrics =
                field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(
                    FieldChunkMetricType::MINMAX);
            minmax_metrics) {
            return minmax_metrics->CanSkipUnaryRange(op_type, val);
        }
        //further more filters for skip, like ngram filter, bf and so on
        return false;
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }
        if (auto minmax_metric =
                field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(
                    FieldChunkMetricType::MINMAX);
            minmax_metric) {
            return minmax_metric->CanSkipBinaryRange(
                lower_val, upper_val, lower_inclusive, upper_inclusive);
        }
        return false;
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<IsAllowedType<T>::value_for_arith, bool>
    CanSkipBinaryArithRange(FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const T& value,
                            const T& right_operand) const {
        switch (arith_type) {
            case ArithOpType::Add: {
                // field + C > V  =>  field > V - C
                T new_value = value - right_operand;
                return CanSkipUnaryRange(
                    field_id, chunk_id, op_type, new_value);
            }
            case ArithOpType::Sub: {
                // field - C > V  =>  field > V + C
                T new_value = value + right_operand;
                return CanSkipUnaryRange(
                    field_id, chunk_id, op_type, new_value);
            }
            case ArithOpType::Mul: {
                // field * C > V
                if (right_operand == 0) {
                    // field * 0 > V => 0 > V. This doesn't depend on the field's range.
                    // We can't safely skip based on MinMax.
                    return false;
                }

                T new_value = value / right_operand;
                OpType new_op_type = op_type;

                if (right_operand < 0) {
                    // If we divide by a negative number, the inequality flips.
                    new_op_type = FlipComparisonOperator(op_type);
                }
                return CanSkipUnaryRange(
                    field_id, chunk_id, new_op_type, new_value);
            }
            case ArithOpType::Div: {
                // field / C > V
                if (right_operand == 0) {
                    // Division by zero. Cannot evaluate, so cannot skip.
                    return false;
                }

                T new_value = value * right_operand;
                OpType new_op_type = op_type;

                if (right_operand < 0) {
                    // If we multiply by a negative number, the inequality flips.
                    new_op_type = FlipComparisonOperator(op_type);
                }
                return CanSkipUnaryRange(
                    field_id, chunk_id, new_op_type, new_value);
            }
            case ArithOpType::Mod: {
                // For `field % C == V`, the result of `field % C` is in [0, C-1] or [-(C-1), 0].
                // We can check if V is outside this possible range.
                if (op_type == OpType::Equal) {
                    if (right_operand > 0 &&
                        (value < 0 || value >= right_operand)) {
                        return true;  // V is outside the possible result range [0, C-1], so we can skip.
                    }
                    if (right_operand < 0 &&
                        (value > 0 || value <= right_operand)) {
                        return true;  // V is outside the possible result range [C+1, 0], so we can skip.
                    }
                }
                // For other operators or when V is in range, it's too complex to use MinMax.
                return false;
            }
            default:
                return false;
        }
    }

    template <typename T>
    std::enable_if_t<!IsAllowedType<T>::value_for_arith, bool>
    CanSkipBinaryArithRange(FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const T& value,
                            const T& right_operand) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<IsAllowedType<T>::value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }

        if (auto set_metric =
                field_chunk_skipindex->GetMetric<SetFieldChunkMetric<T>>(
                    FieldChunkMetricType::SET)) {
            return set_metric->CanSkipIn(values);
        }

        if constexpr (!std::is_same_v<T, bool>) {
            if (auto bloom_metric = field_chunk_skipindex
                                    ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                                        FieldChunkMetricType::BLOOM_FILTER)) {
                return bloom_metric->CanSkipIn(values);
            }

            if (auto minmax_metric =
                    field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(
                        FieldChunkMetricType::MINMAX)) {
                return minmax_metric->CanSkipIn(values);
            }
        }
        return false;
    }

    template <typename T>
    std::enable_if_t<!IsAllowedType<T>::value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        return false;
    }

    void
    LoadSkip(std::vector<std::unique_ptr<ChunkSkipIndex>> chunk_skipindex) {
        if (chunk_skipindex.empty()) {
            return;
        }
        size_t num_chunks = chunk_skipindex.size();
        if (chunk_skipindex[0]->IsEmpty()) {
            return;
        }

        for (const auto& [field_id, _] : chunk_skipindex[0]->GetMetrics()) {
            fieldChunkMetrics_[field_id].reserve(num_chunks);
        }

        for (size_t i = 0; i < num_chunks; ++i) {
            auto field_chunk_metrics = chunk_skipindex[i]->Take();
            for (auto& [field_id, metrics] : field_chunk_metrics) {
                fieldChunkMetrics_[field_id].emplace_back(std::move(metrics));
            }
        }
    }

 private:
    OpType
    FlipComparisonOperator(OpType op) const {
        switch (op) {
            case OpType::GreaterThan:
                return OpType::LessThan;
            case OpType::GreaterEqual:
                return OpType::LessEqual;
            case OpType::LessThan:
                return OpType::GreaterThan;
            case OpType::LessEqual:
                return OpType::GreaterEqual;
            // OpType::Equal and OpType::NotEqual do not flip
            default:
                return op;
        }
    }

    FieldChunkMetrics*
    GetFieldChunkMetrics(milvus::FieldId field_id, int64_t chunk_id) const {
        auto it = fieldChunkMetrics_.find(field_id);
        if (it == fieldChunkMetrics_.end()) {
            return nullptr;
        }
        if (chunk_id < 0 || chunk_id >= it->second.size()) {
            return nullptr;
        }
        return it->second[chunk_id].get();
    }

    std::unordered_map<FieldId, std::vector<std::unique_ptr<FieldChunkMetrics>>>
        fieldChunkMetrics_;
};
}  // namespace milvus
