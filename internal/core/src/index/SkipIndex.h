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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/Types.h"
#include "milvus-storage/common/metadata.h"
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
    MinMaxFieldChunkMetric<T>(T min, T max) : min_(min), max_(max){};

    std::pair<MetricsDataType<T>, MetricsDataType<T>>
    GetMinMax() const {
        MetricsDataType<T> lower_bound;
        MetricsDataType<T> upper_bound;
        try {
            lower_bound = std::get<ReverseMetricsDataType<T>>(min_);
            upper_bound = std::get<ReverseMetricsDataType<T>>(max_);
        } catch (const std::bad_variant_access& e) {
            return {};
        }
        return {lower_bound, upper_bound};
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
    CanSkipIn(std::shared_ptr<milvus::exec::SetElement<T>> set) const {
        if (set->Empty() || !hasValue_)
            return false;
        T min_val;
        T max_val;
        bool has_value = false;
        for (const auto& v : set->values_) {
            if (!has_value) {
                min_val = v;
                max_val = v;
                has_value = true;
                continue;
            }
            if (v < min_val) {
                min_val = v;
            }
            if (v > max_val) {
                max_val = v;
            }
        }
        return CanSkipBinaryRange(min_val, max_val, true, true);
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
        return std::string("");
    }
    void
    Deserialize(const std::string& data) override {
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
    }

    bool
    CanSkipEqual(const T& value) const {
        return unique_values_.find(value) == unique_values_.end();
    }

    bool
    CanSkipIn(std::shared_ptr<milvus::exec::SetElement<T>> set) const {
        for (const auto& value : set->values_) {
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
        // TODO: 实现序列化
        return "";
    }

    void
    Deserialize(const std::string& data) override {
        // TODO: 实现反序列化
    }

 private:
    ankerl::unordered_dense::set<T> unique_values_;
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
    size_t hash_count_;
    size_t bit_size_;
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
        return std::string("");
    }

    void
    Deserialize(const std::string& data) override {
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
        return "";
    }

    void
    Deserialize(const std::string& data) override {
    }
};

class TokenFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<BloomFilter<std::string>> filter_;

 public:
    TokenFieldChunkMetric(
        ankerl::unordered_dense::set<std::string> unique_values) {
        // 步骤1: 预处理，提取所有唯一的 Tokens
        ankerl::unordered_dense::set<std::string> unique_tokens;
        for (const auto& text : unique_values) {
            auto tokens = TokenizeText(text);
            for (const auto& token : tokens) {
                unique_tokens.insert(token);
            }
        }

        // 步骤2: 使用提取出的 Tokens 构建通用的布隆过滤器
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
        return std::string("");
    }

    void
    Deserialize(const std::string& data) override {
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
    Load(std::shared_ptr<Chunk> chunk) {
        switch (data_type_) {
            case DataType::INT8:
                LoadMetrics<int8_t>(chunk);
                break;
            case DataType::INT16:
                LoadMetrics<int16_t>(chunk);
                break;
            case DataType::INT32:
                LoadMetrics<int32_t>(chunk);
                break;
            case DataType::INT64:
                LoadMetrics<int64_t>(chunk);
                break;
            case DataType::FLOAT:
                LoadMetrics<float>(chunk);
                break;
            case DataType::DOUBLE:
                LoadMetrics<double>(chunk);
                break;
            case DataType::VARCHAR:
            case DataType::STRING:
                LoadStringMetrics(chunk);
                break;
            default:
                return;
        }
    }

    // 获取特定类型的统计
    template <typename MetricType>
    std::shared_ptr<MetricType>
    GetMetric(FieldChunkMetricType type) const {
        for (const auto& [metric_type, metric] : metrics_) {
            if (metric_type == type) {
                return std::dynamic_pointer_cast<MetricType>(metric);
            }
        }
        return nullptr;
    }

    // 检查是否包含某种类型
    bool
    HasMetricType(FieldChunkMetricType type) const {
        for (const auto& [metric_type, metric] : metrics_) {
            if (metric_type == type)
                return true;
        }
        return false;
    }

    std::vector<
        std::pair<FieldChunkMetricType, std::shared_ptr<FieldChunkMetric>>>
    GetMetrics() const {
        return metrics_;
    }

    size_t
    Size() const {
        return metrics_.size();
    }

    std::string
    Serialize() const {
        // TODO: 实现序列化
        return "";
    }

    void
    Deserialize(const std::string& data) {
        // TODO: 实现反序列化
    }

 private:
    template <typename T>
    struct metricsInfo {
        int64_t total_rows = 0;
        int64_t null_count = 0;

        T min_value;
        T max_value;

        ankerl::unordered_dense::set<T> unique_values;

        // 字符串特有的指标
        size_t avg_string_length = 0;
        size_t max_string_length = 0;
        bool has_spaces = false;
    };

    template <typename T>
    void
    LoadMetrics(std::shared_ptr<Chunk> chunk) {
        auto info = ProcessFieldChunk<T>(chunk);
        if (info.total_rows - info.null_count < 10) {
            return;
        }
        metrics_.emplace_back(FieldChunkMetricType::MINMAX,
                              std::make_shared<MinMaxFieldChunkMetric<T>>(
                                  info.min_value, info.max_value));
        if (info.unique_values.size() * 10 <
            info.total_rows - info.null_count) {
            metrics_.emplace_back(
                FieldChunkMetricType::SET,
                std::make_shared<SetFieldChunkMetric<T>>(info.unique_values));
        } else {
            metrics_.emplace_back(
                FieldChunkMetricType::BLOOM_FILTER,
                std::make_shared<BloomFilterFieldChunkMetric<T>>(
                    info.unique_values));
        }
    }

    void
    LoadStringMetrics(std::shared_ptr<Chunk> chunk) {
        auto info = ProcessStringFieldChunk(chunk);
        int64_t valid_count = info.total_rows - info.null_count;
        if (valid_count < 20) {
            return;
        }
        metrics_.emplace_back(
            FieldChunkMetricType::MINMAX,
            std::make_shared<MinMaxFieldChunkMetric<std::string>>(
                info.min_value, info.max_value));
        if (info.unique_values.size() * 10 < valid_count &&
            info.avg_string_length < 20) {
            metrics_.emplace_back(
                FieldChunkMetricType::SET,
                std::make_shared<SetFieldChunkMetric<std::string>>(
                    info.unique_values));
        } else {
            metrics_.emplace_back(
                FieldChunkMetricType::BLOOM_FILTER,
                std::make_shared<BloomFilterFieldChunkMetric<std::string>>(
                    info.unique_values));
        }
    }

    template <typename T>
    metricsInfo<T>
    ProcessFieldChunk(std::shared_ptr<Chunk> chunk) {
        auto fixed_chunk = std::dynamic_pointer_cast<FixedWidthChunk>(chunk);
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

    metricsInfo<std::string>
    ProcessStringFieldChunk(std::shared_ptr<Chunk> chunk) {
        auto string_chunk = std::dynamic_pointer_cast<StringChunk>(chunk);
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

            // 检查是否包含空格
            if (!metrics.has_spaces && text.find(' ') != std::string::npos) {
                metrics.has_spaces = true;
            }

            // 更新min/max
            if (!has_first_valid) {
                metrics.min_value = metrics.max_value = text;
                has_first_valid = true;
            } else {
                if (text < metrics.min_value)
                    metrics.min_value = text;
                if (text > metrics.max_value)
                    metrics.max_value = text;
            }

            // 采样唯一值
            metrics.unique_values.insert(text);
        }

        if (valid_count > 0) {
            metrics.avg_string_length = total_length / valid_count;
        }

        return metrics;
    }

    std::vector<
        std::pair<FieldChunkMetricType, std::shared_ptr<FieldChunkMetric>>>
        metrics_;
    DataType data_type_;
};

class ChunkSkipIndex : public milvus_storage::Metadata {
 public:
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
                // ignore row id field
                continue;
            }
            auto it = field_metas.find(fid);
            const auto& field_meta = it->second;
            auto data_type = field_meta.get_data_type();

            const arrow::ArrayVector& array_vec = table->column(i)->chunks();
            std::shared_ptr<Chunk> chunk = create_chunk(field_meta, array_vec);

            auto field_metrics =
                std::make_shared<FieldChunkSkipIndex>(data_type);
            field_metrics->Load(chunk);

            field_chunk_metrics_.emplace_back(
                std::make_pair(fid, field_metrics));
        }
    }

    explicit ChunkSkipIndex(
        std::vector<std::pair<FieldId, std::shared_ptr<FieldChunkSkipIndex>>>
            field_chunk_metrics)
        : field_chunk_metrics_(std::move(field_chunk_metrics)) {
    }

    std::vector<std::pair<FieldId, std::shared_ptr<FieldChunkSkipIndex>>>
    GetFieldChunkMetrics() const {
        return field_chunk_metrics_;
    }

    std::string
    Serialize() const override {
        return "";
    };

    void
    Deserialize(const std::string& data) override{
        // Implement deserialization logic here
    };

 private:
    std::vector<std::pair<FieldId, std::shared_ptr<FieldChunkSkipIndex>>>
        field_chunk_metrics_;
};

class ChunkSkipIndexBuilder
    : public milvus_storage::MetadataBuilder<ChunkSkipIndexBuilder,
                                             ChunkSkipIndex> {
 public:
    ChunkSkipIndexBuilder() = default;
    ChunkSkipIndexBuilder(
        const std::unordered_map<FieldId, FieldMeta>& field_metas)
        : field_metas_(field_metas) {
    }

 protected:
    std::shared_ptr<ChunkSkipIndex>
    BuildImpl(const std::shared_ptr<arrow::Table>& table) override {
        return std::make_shared<ChunkSkipIndex>(table, field_metas_);
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
        // auto pw = GetFieldChunkMetrics(field_id, chunk_id);
        // auto field_chunk_metrics = pw.get();
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
    std::enable_if_t<IsAllowedType<T>::value, bool>
    CanSkipBinaryArithRange(FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const T& value,
                            const T& right_operand) const {
        bool can_skip = false;
        // switch (arith_type) {
        //     case ArithOpType::Add: {
        //         can_skip = CanSkipUnaryRange(field_id, chunk_id, op_type, right_operand - value);
        //         break;
        //     }
        //     case ArithOpType::Sub: {
        //         can_skip = CanSkipUnaryRange(field_id, chunk_id, op_type, right_operand + value);
        //         break;
        //     }
        //     case ArithOpType::Mul: {
        //         if (right_operand == 0) {
        //             can_skip = false;
        //         } else if (right_operand > 0) {
        //             can_skip = CanSkipUnaryRange(field_id, chunk_id, op_type, value / right_operand);
        //         } else {
        //             auto new_op_type = op_type;
        //             switch (op_type) {
        //                 case OpType::GreaterThan:
        //                     new_op_type = OpType::LessThan;
        //                     break;
        //                 case OpType::GreaterEqual:
        //                     new_op_type = OpType::LessEqual;
        //                     break;
        //                 case OpType::LessThan:
        //                     new_op_type = OpType::GreaterThan;
        //                     break;
        //                 case OpType::LessEqual:
        //                     new_op_type = OpType::GreaterEqual;
        //                     break;
        //                 default:
        //                     break;
        //             }
        //             can_skip = CanSkipUnaryRange(field_id, chunk_id, new_op_type, value / right_operand);
        //         }
        //     }
        //     case ArithOpType::Div: {
        //         if (right_operand == 0) {
        //             can_skip = false;
        //         } else if (right_operand > 0) {
        //             can_skip = CanSkipUnaryRange(field_id, chunk_id, op_type, value * right_operand);
        //         } else {
        //             auto new_op_type = op_type;
        //             switch (op_type) {
        //                 case OpType::GreaterThan:
        //                     new_op_type = OpType::LessThan;
        //                     break;
        //                 case OpType::GreaterEqual:
        //                     new_op_type = OpType::LessEqual;
        //                     break;
        //                 case OpType::LessThan:
        //                     new_op_type = OpType::GreaterThan;
        //                     break;
        //                 case OpType::LessEqual:
        //                     new_op_type = OpType::GreaterEqual;
        //                     break;
        //                 default:
        //                     break;
        //             }

        //             can_skip = CanSkipUnaryRange(field_id, chunk_id, new_op_type, value * right_operand);
        //         }
        //     }
        //     default:
        //         can_skip = false;
        // }
        return can_skip;
    }

    template <typename T>
    std::enable_if_t<!IsAllowedType<T>::value, bool>
    CanSkipBinaryArithRange(FieldId field_id,
                            int64_t chunk_id,
                            OpType op_type,
                            ArithOpType arith_type,
                            const T& value,
                            const T& right_operand) const {
        return false;
    }

    // IN查询优化
    template <typename T>
    std::enable_if_t<IsAllowedType<T>::value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   std::shared_ptr<milvus::exec::MultiElement> values) const {
        auto set = std::dynamic_pointer_cast<exec::SetElement<T>>(values);
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex)
            return false;

        // IN查询优先使用SET统计
        if (auto set_metric =
                field_chunk_skipindex->GetMetric<SetFieldChunkMetric<T>>(
                    FieldChunkMetricType::SET)) {
            return set_metric->CanSkipIn(set);
        }

        if (auto bloom_metric = field_chunk_skipindex
                                    ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                                        FieldChunkMetricType::BLOOM_FILTER)) {
            return bloom_metric->CanSkipIn(set);
        }

        // 降级到MinMax：计算值范围
        if (auto minmax_metric =
                field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(
                    FieldChunkMetricType::MINMAX)) {
            return minmax_metric->CanSkipIn(set);
        }

        return false;
    }

    template <typename T>
    std::enable_if_t<!IsAllowedType<T>::value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   std::shared_ptr<exec::MultiElement> values) const {
        return false;
    }

    void
    LoadSkip(std::vector<std::shared_ptr<ChunkSkipIndex>> chunk_skipindex) {
        if (chunk_skipindex.empty()) {
            return;
        }
        size_t size = chunk_skipindex.size();
        for (size_t i = 0; i < size; ++i) {
            const auto& field_chunk_metrics =
                chunk_skipindex[i]->GetFieldChunkMetrics();
            for (const auto& [field_id, metrics] : field_chunk_metrics) {
                if (i == 0) {
                    fieldChunkMetrics_[field_id] =
                        std::vector<std::shared_ptr<FieldChunkSkipIndex>>{};
                    fieldChunkMetrics_[field_id].reserve(size);
                }
                fieldChunkMetrics_[field_id].emplace_back(metrics);
            }
        }
    }

 private:
    std::shared_ptr<FieldChunkSkipIndex>
    GetFieldChunkMetrics(milvus::FieldId field_id, int chunk_id) const {
        return fieldChunkMetrics_.at(field_id).at(chunk_id);
    }

    std::unordered_map<FieldId,
                       std::vector<std::shared_ptr<FieldChunkSkipIndex>>>
        fieldChunkMetrics_;
    // mutable std::shared_mutex mutex_;
};
}  // namespace milvus
