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

#include "arrow/record_batch.h"
#include "arrow/type_traits.h"
#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/Types.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/common/constants.h"
#include "common/ChunkWriter.h"
#include "exec/expression/Element.h"

namespace milvus {

using Metrics =
    std::variant<int8_t, int16_t, int32_t, int64_t, float, double, std::string>;

// MetricsDataType is used to avoid copy when get min/max value from FieldChunkMetrics
template <typename T>
using MetricsDataType =
    std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

// ReverseMetricsDataType is used to avoid copy when get min/max value from FieldChunkMetrics
template <typename T>
using ReverseMetricsDataType =
    std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;

enum class FieldChunkMetricType {
    MINMAX,        // 最小最大值统计
    SET,           // 唯一值集合统计（低基数）
    BLOOM_FILTER,  // 布隆过滤器（高基数存在性检查）
    NGRAM_FILTER,  // N-gram 布隆过滤器（文本搜索）
    TOKEN_FILTER,  // 分词布隆过滤器（全文检索）
};

class FieldChunkMetric {
 public:
    virtual ~FieldChunkMetric() = default;
    virtual FieldChunkMetricType
    GetType() const = 0;
    virtual std::string
    Serialize() const = 0;
    virtual void
    Deserialize(const std::string& data) = 0;

    bool hasValue_ = false;
};

template <typename T>
class MinMaxFieldChunkMetric : public FieldChunkMetric {
 public:
    MinMaxFieldChunkMetric<T>(T min, T max) : min_(min), max_(max) {
        hasValue_ = true;
    };
    MinMaxFieldChunkMetric<T>() = default;

    std::pair<MetricsDataType<T>, MetricsDataType<T>>
    GetMinMax() const {
        if constexpr (std::is_same_v<T, std::string>) {
            return {std::string_view(min_), std::string_view(max_)};
        } else {
            return {min_, max_};
        }
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
        if (!hasValue_)
            return false;
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
        ss.write(reinterpret_cast<const char*>(&min_), sizeof(min_));
        ss.write(reinterpret_cast<const char*>(&max_), sizeof(max_));
        return ss.str();
    }
    void
    Deserialize(const std::string& data) override {
        if (data.empty())
            return;
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        ss.read(reinterpret_cast<char*>(&min_), sizeof(min_));
        ss.read(reinterpret_cast<char*>(&max_), sizeof(max_));
        hasValue_ = true;
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

    T min_;
    T max_;
};

template <typename T>
class SetFieldChunkMetric : public FieldChunkMetric {
 public:
    SetFieldChunkMetric(ankerl::unordered_dense::set<T> unique_values)
        : unique_values_(std::move(unique_values)) {
        hasValue_ = true;
    }

    SetFieldChunkMetric() = default;

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

    void
    Deserialize(const std::string& data) override {
        if (data.empty())
            return;
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

 private:
    ankerl::unordered_dense::set<T> unique_values_;
};

template <>
class SetFieldChunkMetric<bool> : public FieldChunkMetric {
 public:
    SetFieldChunkMetric(const bool contains_true, const bool contains_false)
        : contains_true(contains_true), contains_false(contains_false) {
        hasValue_ = true;
    }

    SetFieldChunkMetric() = default;

    bool
    CanSkipEqual(const bool value) const {
        return (value && contains_true) || (!value && contains_false);
    }

    bool
    CanSkipIn(std::shared_ptr<exec::SetElement<bool>> set) const {
        if ((!contains_true || set->In(true)) &&
            (!contains_false || set->In(false))) {
            return true;
        }
        return false;
    }

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::SET;
    }

    std::string
    Serialize() const override {
        std::stringstream ss(std::ios::binary | std::ios::out);
        ss.write(reinterpret_cast<const char*>(&contains_true),
                 sizeof(contains_true));
        ss.write(reinterpret_cast<const char*>(&contains_false),
                 sizeof(contains_false));
        return ss.str();
    }

    void
    Deserialize(const std::string& data) override {
        if (data.empty())
            return;
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        ss.read(reinterpret_cast<char*>(&contains_true), sizeof(contains_true));
        ss.read(reinterpret_cast<char*>(&contains_false),
                sizeof(contains_false));
        hasValue_ = true;
    }

 private:
    bool contains_true = false;
    bool contains_false = false;
};

template <typename T>
class BloomFilter {
 public:
    // 用于从头构建
    BloomFilter(const ankerl::unordered_dense::set<T>& items, double p = 0.01) {
        Initialize(items.size(), p);
        if (bit_size_ == 0)
            return;
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
        if (bit_size_ == 0)
            return;
        uint64_t hash1 = Hash(item, 0x9e3779b9);
        uint64_t hash2 = Hash(item, hash1);
        for (size_t i = 0; i < hash_count_; ++i) {
            size_t bit_pos = (hash1 + i * hash2) % bit_size_;
            bit_array_[bit_pos / 64] |= (1ULL << (bit_pos % 64));
        }
    }

    bool
    MightContain(const T& item) const {
        if (bit_size_ == 0)
            return true;
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
        if (hash_count_ < 1)
            hash_count_ = 1;
        if (hash_count_ > 10)
            hash_count_ = 10;
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
    std::unique_ptr<BloomFilter<T>> filter_;

 public:
    BloomFilterFieldChunkMetric(ankerl::unordered_dense::set<T> unique_values) {
        filter_ = std::make_unique<BloomFilter<T>>(unique_values);
        hasValue_ = filter_->IsValid();
    }

    BloomFilterFieldChunkMetric() = default;

    bool
    CanSkipEqual(const T& value) const {
        if (!hasValue_)
            return false;
        return !filter_->MightContain(value);
    }

    bool
    CanSkipIn(const std::vector<T>& values) const {
        if (!hasValue_)
            return false;
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

    void
    Deserialize(const std::string& data) override {
        if (data.empty())
            return;
        filter_ = BloomFilter<T>::Deserialize(data);
    }
};

class NgramFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<BloomFilter<std::string>> filter_;
    size_t ngram_size_;

 public:
    NgramFieldChunkMetric(
        ankerl::unordered_dense::set<std::string> unique_values,
        size_t ngram_size = 3)
        : ngram_size_(ngram_size) {
        // 步骤1: 预处理，提取所有唯一的 N-grams
        ankerl::unordered_dense::set<std::string> unique_ngrams;
        for (const auto& text : unique_values) {
            if (text.length() >= ngram_size_) {
                for (size_t j = 0; j <= text.length() - ngram_size_; ++j) {
                    unique_ngrams.insert(text.substr(j, ngram_size_));
                }
            }
        }

        // 步骤2: 使用提取出的 N-grams 构建通用的布隆过滤器
        filter_ = std::make_unique<BloomFilter<std::string>>(unique_ngrams);
        hasValue_ = filter_->IsValid();
    }

    NgramFieldChunkMetric() = default;

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

    void
    Deserialize(const std::string& data) override {
        if (data.empty())
            return;
        filter_ = BloomFilter<std::string>::Deserialize(data);
    }
};

class TokenFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<BloomFilter<std::string>> filter_;

 public:
    TokenFieldChunkMetric(
        ankerl::unordered_dense::set<std::string> unique_values) {
        ankerl::unordered_dense::set<std::string> unique_tokens;
        for (const auto& text : unique_values) {
            auto tokens = TokenizeText(text);
            for (const auto& token : tokens) {
                unique_tokens.insert(token);
            }
        }

        filter_ = std::make_unique<BloomFilter<std::string>>(unique_tokens);
        hasValue_ = filter_->IsValid();
    }

    // 用于反序列化
    TokenFieldChunkMetric() = default;

    bool
    CanSkipFullTextSearch(const std::vector<std::string>& search_tokens) const {
        if (!hasValue_)
            return false;

        // 检查是否包含所有搜索词
        for (const auto& token : search_tokens) {
            // 委托给内部的 filter_
            if (!filter_->MightContain(token)) {
                return true;  // 确定不包含某个词，可以跳过
            }
        }
        return false;  // 可能包含所有词，不能跳过
    }

    bool
    CanSkipAnyTokenMatch(const std::vector<std::string>& search_tokens) const {
        if (!hasValue_)
            return false;

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

    void
    Deserialize(const std::string& data) override {
        if (data.empty())
            return;
        filter_ = BloomFilter<std::string>::Deserialize(data);
    }

 private:
    // 分词逻辑保持不变，作为一个私有辅助函数
    std::vector<std::string>
    TokenizeText(const std::string& text) {
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

class FieldChunkSkipIndex {
 public:
    explicit FieldChunkSkipIndex(DataType data_type) : data_type_(data_type) {
    }

    void
    Load(std::unique_ptr<Chunk> chunk) {
        switch (data_type_) {
            case DataType::BOOL:
                LoadBooleanMetrics(std::move(chunk));
                break;
            case DataType::INT8:
                LoadMetrics<int8_t>(std::move(chunk));
                break;
            case DataType::INT16:
                LoadMetrics<int16_t>(std::move(chunk));
                break;
            case DataType::INT32:
                LoadMetrics<int32_t>(std::move(chunk));
                break;
            case DataType::INT64:
                LoadMetrics<int64_t>(std::move(chunk));
                break;
            case DataType::FLOAT:
                LoadMetrics<float>(std::move(chunk));
                break;
            case DataType::DOUBLE:
                LoadMetrics<double>(std::move(chunk));
                break;
            case DataType::VARCHAR:
            case DataType::STRING:
                LoadStringMetrics(std::move(chunk));
                break;
            default:
                return;
        }
    }

    std::unique_ptr<FieldChunkMetric>
    CreateMetric(DataType data_type, FieldChunkMetricType metric_type) {
        switch (metric_type) {
            case FieldChunkMetricType::MINMAX: {
                switch (data_type) {
                    case DataType::INT8:
                        return std::make_unique<
                            MinMaxFieldChunkMetric<int8_t>>();
                    case DataType::INT16:
                        return std::make_unique<
                            MinMaxFieldChunkMetric<int16_t>>();
                    case DataType::INT32:
                        return std::make_unique<
                            MinMaxFieldChunkMetric<int32_t>>();
                    case DataType::INT64:
                        return std::make_unique<
                            MinMaxFieldChunkMetric<int64_t>>();
                    case DataType::FLOAT:
                        return std::make_unique<
                            MinMaxFieldChunkMetric<float>>();
                    case DataType::DOUBLE:
                        return std::make_unique<
                            MinMaxFieldChunkMetric<double>>();
                    case DataType::VARCHAR:
                    case DataType::STRING:
                        return std::make_unique<
                            MinMaxFieldChunkMetric<std::string>>();
                    default:
                        return nullptr;
                }
            }
            case FieldChunkMetricType::SET: {
                switch (data_type) {
                    case DataType::BOOL:
                        return std::make_unique<SetFieldChunkMetric<bool>>();
                    case DataType::INT8:
                        return std::make_unique<SetFieldChunkMetric<int8_t>>();
                    case DataType::INT16:
                        return std::make_unique<SetFieldChunkMetric<int16_t>>();
                    case DataType::INT32:
                        return std::make_unique<SetFieldChunkMetric<int32_t>>();
                    case DataType::INT64:
                        return std::make_unique<SetFieldChunkMetric<int64_t>>();
                    case DataType::FLOAT:
                        return std::make_unique<SetFieldChunkMetric<float>>();
                    case DataType::DOUBLE:
                        return std::make_unique<SetFieldChunkMetric<double>>();
                    case DataType::VARCHAR:
                    case DataType::STRING:
                        return std::make_unique<
                            SetFieldChunkMetric<std::string>>();
                    default:
                        return nullptr;
                }
            }
            case FieldChunkMetricType::BLOOM_FILTER: {
                switch (data_type) {
                    case DataType::INT8:
                        return std::make_unique<
                            BloomFilterFieldChunkMetric<int8_t>>();
                    case DataType::INT16:
                        return std::make_unique<
                            BloomFilterFieldChunkMetric<int16_t>>();
                    case DataType::INT32:
                        return std::make_unique<
                            BloomFilterFieldChunkMetric<int32_t>>();
                    case DataType::INT64:
                        return std::make_unique<
                            BloomFilterFieldChunkMetric<int64_t>>();
                    case DataType::FLOAT:
                        return std::make_unique<
                            BloomFilterFieldChunkMetric<float>>();
                    case DataType::DOUBLE:
                        return std::make_unique<
                            BloomFilterFieldChunkMetric<double>>();
                    case DataType::VARCHAR:
                    case DataType::STRING:
                        return std::make_unique<
                            BloomFilterFieldChunkMetric<std::string>>();
                    default:
                        return nullptr;
                }
            }
            case FieldChunkMetricType::NGRAM_FILTER: {
                if (data_type == DataType::VARCHAR ||
                    data_type == DataType::STRING) {
                    return std::make_unique<NgramFieldChunkMetric>();
                }
            }
            case FieldChunkMetricType::TOKEN_FILTER: {
                if (data_type == DataType::VARCHAR ||
                    data_type == DataType::STRING) {
                    return std::make_unique<TokenFieldChunkMetric>();
                }
            }
            default:
                return nullptr;
        }
    }

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

    DataType
    GetDataType() const {
        return data_type_;
    }

    size_t
    Size() const {
        return metrics_.size();
    }

    std::string
    Serialize() const {
        std::stringstream ss(std::ios::binary | std::ios::out);

        uint32_t count = metrics_.size();
        ss.write(reinterpret_cast<const char*>(&count), sizeof(count));

        for (const auto& [type, metric] : metrics_) {
            ss.write(reinterpret_cast<const char*>(&type), sizeof(type));

            std::string data = metric->Serialize();
            uint64_t len = data.length();
            ss.write(reinterpret_cast<const char*>(&len), sizeof(len));
            ss.write(data.data(), len);
        }
        return ss.str();
    }

    void
    Deserialize(const std::string& data) {
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        uint32_t metric_count;
        ss.read(reinterpret_cast<char*>(&metric_count), sizeof(metric_count));
        metrics_.reserve(metric_count);

        for (uint32_t i = 0; i < metric_count; ++i) {
            FieldChunkMetricType metric_type;
            ss.read(reinterpret_cast<char*>(&metric_type), sizeof(metric_type));

            uint64_t metric_len;
            ss.read(reinterpret_cast<char*>(&metric_len), sizeof(metric_len));
            std::string metric_data(metric_len, '\0');
            ss.read(&metric_data[0], metric_len);

            auto metric = CreateMetric(data_type_, metric_type);
            if (metric) {
                metric->Deserialize(metric_data);
                metrics_.emplace_back(metric_type, std::move(metric));
            }
        }
    }

 private:
    template <typename T>
    struct metricsInfo {
        int64_t total_rows = 0;
        int64_t null_count = 0;

        T min_value;
        T max_value;

        bool contains_true = false;
        bool contains_false = false;

        ankerl::unordered_dense::set<T> unique_values;

        size_t avg_string_length = 0;
        size_t max_string_length = 0;
        bool has_spaces = false;
    };

    template <typename T>
    void
    LoadMetrics(std::unique_ptr<Chunk> chunk) {
        auto info = ProcessFieldChunk<T>(std::move(chunk));
        if (info.total_rows - info.null_count < 10) {
            return;
        }
        metrics_.emplace_back(FieldChunkMetricType::MINMAX,
                              std::make_unique<MinMaxFieldChunkMetric<T>>(
                                  info.min_value, info.max_value));
        if (info.unique_values.size() * 10 <
            info.total_rows - info.null_count) {
            metrics_.emplace_back(FieldChunkMetricType::SET,
                                  std::make_unique<SetFieldChunkMetric<T>>(
                                      std::move(info.unique_values)));
        } else {
            metrics_.emplace_back(
                FieldChunkMetricType::BLOOM_FILTER,
                std::make_unique<BloomFilterFieldChunkMetric<T>>(
                    std::move(info.unique_values)));
        }
    }

    void
    LoadBooleanMetrics(std::unique_ptr<Chunk> chunk) {
        auto info = ProcessBooleanFieldChunk(std::move(chunk));
        int64_t valid_count = info.total_rows - info.null_count;
        if (valid_count < 10) {
            return;
        }
        metrics_.emplace_back(FieldChunkMetricType::SET,
                              std::make_unique<SetFieldChunkMetric<bool>>(
                                  info.contains_true, info.contains_false));
    }

    void
    LoadStringMetrics(std::unique_ptr<Chunk> chunk) {
        auto info = ProcessStringFieldChunk(std::move(chunk));
        int64_t valid_count = info.total_rows - info.null_count;
        if (valid_count < 20) {
            return;
        }
        metrics_.emplace_back(
            FieldChunkMetricType::MINMAX,
            std::make_unique<MinMaxFieldChunkMetric<std::string>>(
                info.min_value, info.max_value));
        auto unique_values_copy_for_ngram = info.unique_values;
        metrics_.emplace_back(FieldChunkMetricType::NGRAM_FILTER,
                              std::make_unique<NgramFieldChunkMetric>(
                                  std::move(unique_values_copy_for_ngram)));

        if (info.has_spaces) {
            auto unique_values_copy_for_token = info.unique_values;
            metrics_.emplace_back(FieldChunkMetricType::TOKEN_FILTER,
                                  std::make_unique<TokenFieldChunkMetric>(
                                      std::move(unique_values_copy_for_token)));
        }
        if (info.unique_values.size() * 10 < valid_count &&
            info.avg_string_length < 20) {
            metrics_.emplace_back(
                FieldChunkMetricType::SET,
                std::make_unique<SetFieldChunkMetric<std::string>>(
                    std::move(info.unique_values)));
        } else {
            metrics_.emplace_back(
                FieldChunkMetricType::BLOOM_FILTER,
                std::make_unique<BloomFilterFieldChunkMetric<std::string>>(
                    std::move(info.unique_values)));
        }
    }

    template <typename T>
    metricsInfo<T>
    ProcessFieldChunk(std::unique_ptr<Chunk> chunk) {
        auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
        if (!fixed_chunk)
            return {};

        metricsInfo<T> metrics;
        auto span = fixed_chunk->Span();
        const T* data = static_cast<const T*>(span.data());
        const bool* valid_data = span.valid_data();
        metrics.total_rows = span.row_count();

        if (metrics.total_rows == 0)
            return metrics;

        bool has_first_valid = false;

        for (int64_t i = 0; i < metrics.total_rows; ++i) {
            if (valid_data && !valid_data[i]) {
                metrics.null_count++;
                continue;
            }

            T value = data[i];

            // 更新min/max
            if (!has_first_valid) {
                metrics.min_value = metrics.max_value = value;
                has_first_valid = true;
            } else {
                if (value < metrics.min_value)
                    metrics.min_value = value;
                if (value > metrics.max_value)
                    metrics.max_value = value;
            }
            metrics.unique_values.insert(value);
        }

        return metrics;
    }

    metricsInfo<bool>
    ProcessBooleanFieldChunk(std::unique_ptr<Chunk> chunk) {
        auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
        if (!fixed_chunk) {
            return {};
        }
        metricsInfo<bool> metrics;
        auto span = fixed_chunk->Span();
        const bool* data = static_cast<const bool*>(span.data());
        const bool* valid_data = span.valid_data();
        metrics.total_rows = span.row_count();

        if (metrics.total_rows == 0)
            return metrics;
        bool has_first_valid = false;

        for (int64_t i = 0; i < metrics.total_rows; ++i) {
            if (valid_data && !valid_data[i]) {
                metrics.null_count++;
                continue;
            }

            bool value = data[i];
            if (value) {
                metrics.contains_true = true;
            } else {
                metrics.contains_false = true;
            }
        }
        return metrics;
    }

    metricsInfo<std::string>
    ProcessStringFieldChunk(std::unique_ptr<Chunk> chunk) {
        auto string_chunk = static_cast<StringChunk*>(chunk.get());
        if (!string_chunk)
            return {};

        metricsInfo<std::string> metrics;
        metrics.total_rows = string_chunk->RowNums();

        if (metrics.total_rows == 0)
            return metrics;

        bool has_first_valid = false;
        double total_length = 0.0;
        int64_t valid_count = 0;

        for (int64_t i = 0; i < metrics.total_rows; ++i) {
            if (!string_chunk->isValid(i)) {
                metrics.null_count++;
                continue;
            }

            auto text_view = string_chunk->operator[](i);
            std::string text(text_view);
            valid_count++;
            total_length += text.length();
            metrics.max_string_length =
                std::max(metrics.max_string_length, text.length());

            if (!metrics.has_spaces && text.find(' ') != std::string::npos) {
                metrics.has_spaces = true;
            }

            if (!has_first_valid) {
                metrics.min_value = metrics.max_value = text;
                has_first_valid = true;
            } else {
                if (text < metrics.min_value)
                    metrics.min_value = text;
                if (text > metrics.max_value)
                    metrics.max_value = text;
            }

            metrics.unique_values.insert(text);
        }

        if (valid_count > 0) {
            metrics.avg_string_length = total_length / valid_count;
        }

        return metrics;
    }

    std::vector<
        std::pair<FieldChunkMetricType, std::unique_ptr<FieldChunkMetric>>>
        metrics_;
    DataType data_type_;
};

class MetricInfo {
 public:
    MetricInfo() = default;
    ~MetricInfo() = default;
    
    template<typename T>
    void ProcessArray(const std::shared_ptr<arrow::Array>& array) {
        auto typed_array = std::static_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(array);

        bool has_valid_data = false;
        T min_value;
        T max_value;
        for (int64_t i = 0; i < typed_array->length(); ++i) {
            if (typed_array->IsNull(i)) {
                null_count++;
                continue;
            }
            T value = typed_array->Value(i);

            if (!has_valid_data) {
                min_value = max_value = value;
                has_valid_data = true;
            } else {
                if (value < min_value) {
                    min_value = value;
                }
                if (value > max_value) {
                    max_value = value;
                }
            }
            unique_values.insert(value);
        }
        total_rows += typed_array->length();
    }

    void ProcessBooleanArray(const std::shared_ptr<arrow::Array>& array) {
        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(array);

        for (int64_t i = 0; i < bool_array->length(); ++i) {
            if (bool_array->IsNull(i)) {
                null_count++;
                continue;
            }
            bool value = bool_array->Value(i);
            if (value) {
                contains_true = true;
            } else {
                contains_false = true;
            }
        }
        total_rows += bool_array->length();
    }

    void ProcessStringArray(const std::shared_ptr<arrow::Array>& array) {
        auto string_array = std::static_pointer_cast<arrow::StringArray>(array);

        bool has_first_valid = false;
        std::string_view min_value;
        std::string_view max_value;
        for (int64_t i = 0; i < string_array->length(); ++i) {
            if (string_array->IsNull(i)) {
                null_count++;
                continue;
            }
            std::string_view value = string_array->GetView(i);
            size_t length = value.length();
            if (!has_first_valid) {
                min_value = value;
                max_value = value;
                has_first_valid = true;
            } else {
                if (value < min_value) {
                    min_value = value;
                }
                if (value > max_value) {
                    max_value = value;
                }
            }
            unique_values.insert(std::string(value));
            string_length += length;
            max_string_length = std::max(max_string_length, length);
            if (!has_spaces && value.find(' ') != std::string::npos) {
                has_spaces = true;
            }
        }
        total_rows += string_array->length();
    }

 public:
    int64_t total_rows = 0;
    int64_t null_count = 0;

    MetricType min_value;
    MetricType max_value;

    ankerl::unordered_dense::set<MetricType> unique_values;

    bool contains_true = false;
    bool contains_false = false;

    size_t string_length = 0;
    size_t max_string_length = 0;
    bool has_spaces = false;
}

class ChunkSkipIndex : public milvus_storage::Metadata {
 public:
    static constexpr const char* KEY = "SKIP_INDEX";

    ChunkSkipIndex() = default;

    ChunkSkipIndex(const std::shared_ptr<arrow::Table>& table,
                   const std::unordered_map<FieldId, FieldMeta>& field_metas) {
        field_chunk_metrics_.reserve(field_metas.size());

        std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
        std::vector<FieldId> field_list;
        std::vector<std::unique_ptr<FieldChunkMetric>> field_chunk_metrics;
        for (int i = 0; i < table->schema()->num_fields(); ++i) {
            auto field_id =
                std::stoll(table->schema()
                               ->field(i)
                               ->metadata()
                               ->Get(milvus_storage::ARROW_FIELD_ID_KEY)
                               ->data());
            auto fid = milvus::FieldId(field_id);
            if (fid == RowFieldID) {
                continue;
            }
            auto it = field_metas.find(fid);
            const auto& field_meta = it->second;
            auto data_type = field_meta.get_data_type();

            const arrow::ArrayVector& array_vec = table->column(i)->chunks();
            std::unique_ptr<Chunk> chunk = create_chunk(field_meta, array_vec);

            auto field_metrics =
                std::make_unique<FieldChunkSkipIndex>(data_type);
            field_metrics->Load(std::move(chunk));

            if (field_metrics->Size() == 0) {
                continue;
            }

            field_chunk_metrics_.emplace_back(
                std::make_pair(fid, std::move(field_metrics)));
        }
    }

    ChunkSkipIndex(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
        if (batches.empty()) {
            return;
        }
        auto schema = batches[0]->schema();
        std::unordered_map<FieldId, std::unique_ptr<MetricInfo>> metrics;
        for (int col_idx = 0; col_idx < schema->num_fields(); ++col_idx) {
            auto field_id = std::stoll(schema->field(col_idx)
                                       ->metadata()
                                       ->Get(milvus_storage::ARROW_FIELD_ID_KEY)
                                       ->data());
            auto fid = milvus::FieldId(field_id);
            if (fid == RowFieldID) {
                continue;
            }
            metrics[fid] = std::make_unique<MetricInfo>();
        }
        for (const auto& batch : batches) {
            for (int col_idx = 0; col_idx < batch->num_columns(); ++col_idx) {
                auto field_id = std::stoll(schema->field(col_idx)
                                           ->metadata()
                                           ->Get(milvus_storage::ARROW_FIELD_ID_KEY)
                                           ->data());
                auto fid = milvus::FieldId(field_id);
                if (fid == RowFieldID) {
                    continue;
                }
                auto array = batch->column(col_idx);
                auto data_type = schema->field(col_idx)->type()->id();
                switch (data_type) {
                    case arrow::Type::INT8:
                        metrics[fid]->ProcessArray<int8_t>(array);
                        break;
                    case arrow::Type::INT16:
                        metrics[fid]->ProcessArray<int16_t>(array);
                        break;
                    case arrow::Type::INT32:
                        metrics[fid]->ProcessArray<int32_t>(array);
                        break;
                    case arrow::Type::INT64:
                        metrics[fid]->ProcessArray<int64_t>(array);
                        break;
                    case arrow::Type::FLOAT:
                        metrics[fid]->ProcessArray<float>(array);
                        break;
                    case arrow::Type::DOUBLE:
                        metrics[fid]->ProcessArray<double>(array);
                        break;
                    case arrow::Type::BOOL:
                        metrics[fid]->ProcessBooleanArray(array);
                        break;
                    case arrow::Type::STRING:
                    case arrow::Type::LARGE_STRING:
                        metrics[fid]->ProcessStringArray(array);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    std::vector<std::pair<FieldId, std::unique_ptr<FieldChunkSkipIndex>>>
    Take() const {
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

            DataType data_type = metrics->GetDataType();
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
        if (data.empty())
            return;
        std::stringstream ss(data, std::ios::binary | std::ios::in);

        uint32_t count;
        ss.read(reinterpret_cast<char*>(&count), sizeof(count));
        field_chunk_metrics_.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            int64_t field_id_val;
            ss.read(reinterpret_cast<char*>(&field_id_val),
                    sizeof(field_id_val));

            DataType data_type;
            ss.read(reinterpret_cast<char*>(&data_type), sizeof(data_type));

            uint64_t len;
            ss.read(reinterpret_cast<char*>(&len), sizeof(len));
            std::string field_data(len, '\0');
            ss.read(&field_data[0], len);

            auto field_chunk_skipindex =
                std::make_unique<FieldChunkSkipIndex>(data_type);
            field_chunk_skipindex->Deserialize(field_data);

            field_chunk_metrics_.emplace_back(FieldId(field_id_val),
                                              std::move(field_chunk_skipindex));
        }
    };

 private:
    std::vector<std::pair<FieldId, std::unique_ptr<FieldChunkSkipIndex>>>
        field_chunk_metrics_;
};

class ChunkSkipIndexAppender : public milvus_storage::MetadataAppender {
 public:
    ChunkSkipIndexAppender() = default;
    ChunkSkipIndexAppender(
        const std::unordered_map<FieldId, FieldMeta>& field_metas)
        : field_metas_(field_metas) {
    }

 protected:
    std::unique_ptr<milvus_storage::Metadata>
    Create(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batch) override {
        return std::make_unique<ChunkSkipIndex>(table, field_metas_);
    }

 private:
    std::unordered_map<FieldId, FieldMeta> field_metas_;
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
    std::enable_if_t<IsAllowedType<T>::value_for_in, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   std::shared_ptr<milvus::exec::MultiElement> values) const {
        auto set = std::dynamic_pointer_cast<exec::SetElement<T>>(values);
        if (!set || set->Empty()) {
            return false;
        }
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }

        const auto& query_values = set->values_;

        if (auto set_metric =
                field_chunk_skipindex->GetMetric<SetFieldChunkMetric<T>>(
                    FieldChunkMetricType::SET)) {
            return set_metric->CanSkipIn(query_values);
        }

        if (auto bloom_metric = field_chunk_skipindex
                                    ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                                        FieldChunkMetricType::BLOOM_FILTER)) {
            return bloom_metric->CanSkipIn(query_values);
        }

        if (auto minmax_metric =
                field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(
                    FieldChunkMetricType::MINMAX)) {
            return minmax_metric->CanSkipIn(query_values);
        }

        return false;
    }

    template <typename T>
    std::enable_if_t<std::is_same_v<T, bool>, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   std::shared_ptr<milvus::exec::MultiElement> values) const {
        auto set = std::dynamic_pointer_cast<exec::SetElement<bool>>(values);
        if (!set || set->Empty()) {
            return false;
        }
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }

        if (auto set_metric =
                field_chunk_skipindex->GetMetric<SetFieldChunkMetric<bool>>(
                    FieldChunkMetricType::SET)) {
            return set_metric->CanSkipIn(set);
        }

        return false;
    }

    template <typename T>
    std::enable_if_t<!IsAllowedType<T>::value_for_in, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   std::shared_ptr<milvus::exec::MultiElement> values) const {
        return false;
    }

    void
    LoadSkip(std::vector<std::unique_ptr<ChunkSkipIndex>> chunk_skipindex) {
        if (chunk_skipindex.empty()) {
            return;
        }
        size_t num_chunks = chunk_skipindex.size();

        if (!chunk_skipindex[0]->IsEmpty()) {
            const auto& first_chunk_metrics = chunk_skipindex[0]->GetMetrics();
            for (const auto& [field_id, _] : first_chunk_metrics) {
                fieldChunkMetrics_[field_id].reserve(num_chunks);
            }
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

    FieldChunkSkipIndex*
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

    std::unordered_map<FieldId,
                       std::vector<std::unique_ptr<FieldChunkSkipIndex>>>
        fieldChunkMetrics_;
};
}  // namespace milvus
