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
#include "common/type_c.h"
#include "mmap/ChunkedColumnInterface.h"
#include "milvus-storage/common/metadata.h"
#include "common/ChunkWriter.h"

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
    MINMAX,           // 最小最大值统计
    SET,              // 唯一值集合统计（低基数）
    BLOOM_FILTER,     // 布隆过滤器（高基数存在性检查）
    NGRAM_FILTER,     // N-gram 布隆过滤器（文本搜索）
    TOKEN_FILTER,     // 分词布隆过滤器（全文检索）
    COUNT_DISTINCT,   // 唯一值计数
    NULL_COUNT,       // 空值统计
};

class FieldChunkMetric {
public: 
    virtual ~FieldChunkMetric() = default;
    virtual FieldChunkMetricType GetType() const = 0;
    virtual std::string Serialize() const = 0;
    virtual void Deserialize(const std::string& data) = 0;

    bool hasValue_ = false;
};

template<typename T>
class MinMaxFieldChunkMetric : public FieldChunkMetric {
public:
    Metrics min_;
    Metrics max_;
    int64_t null_count_;

    MinMaxFieldChunkMetric(std::shared_ptr<Chunk> chunk) {
        if constexpr (std::is_same_v<T, std::string>) {
            ProcessStringFieldMetrics(chunk);
        } else {
            ProcessFieldMetrics(chunk);
        }
    };

    std::pair<MetricsDataType<T>, MetricsDataType<T>>
    GetMinMax() const {
        AssertInfo(hasValue_,
                   "GetMinMax should never be called when hasValue_ is false");
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

    bool CanSkipUnaryRange(OpType op_type,
                      const T& val) const {
        auto [lower_bound, upper_bound] = GetMinMax();
        if (lower_bound == MetricsDataType<T>() ||
            upper_bound == MetricsDataType<T>()) {
            return false;
        }
        return RangeShouldSkip(val, lower_bound, upper_bound, op_type);
    }

    bool CanSkipBinaryRange(const T& lower_val,
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

    FieldChunkMetricType GetType() const override {
        return FieldChunkMetricType::MINMAX;
    }
    std::string Serialize() const override {
        return "";
    }
    void Deserialize(const std::string& data) override {
    }

 private:
    void ProcessFieldMetrics(std::shared_ptr<Chunk> chunk) {
        auto fixed_chunk = static_cast<FixedWidthChunk*>(chunk.get());
        auto span = fixed_chunk->Span();
        const int8_t* chunk_data = static_cast<const int8_t*>(span.data());
        const bool* valid_data = span.valid_data();
        int num_rows = span.row_count();

        //double check to avoid crash
        if (chunk_data == nullptr || num_rows == 0) {
            return;
        }
        // find first not null value
        int64_t start = 0;
        for (int64_t i = start; i < num_rows; i++) {
            if (valid_data != nullptr && !valid_data[i]) {
                start++;
                continue;
            }
            break;
        }
        if (start > num_rows - 1) {
            return;
        }
        T minValue = chunk_data[start];
        T maxValue = chunk_data[start];
        int64_t null_count = start;
        for (int64_t i = start; i < num_rows; i++) {
            T value = chunk_data[i];
            if (valid_data != nullptr && !valid_data[i]) {
                null_count++;
                continue;
            }
            if (value < minValue) {
                minValue = value;
            }
            if (value > maxValue) {
                maxValue = value;
            }
        }
        min_ = Metrics(minValue);
        max_ = Metrics(maxValue);
        null_count_ = null_count;
        hasValue_ = true;
    }

    void ProcessStringFieldMetrics(std::shared_ptr<Chunk> chunk) {
        auto string_chunk = static_cast<StringChunk*>(chunk.get());
        int num_rows = string_chunk->RowNums();
        if (num_rows == 0) {
            return;
        }
        int64_t start = 0;
        for (int64_t i = start; i < num_rows; i++) {
            if (!string_chunk->isValid(i)) {
                start++;
                continue;
            }
            break;
        }
        // all captured by reference
        std::string_view min_string;
        std::string_view max_string;
        int64_t null_count = start;

        for (int64_t i = 0; i < num_rows; ++i) {
            bool is_valid = string_chunk->isValid(i);
            if (!is_valid) {
                null_count++;
                continue;
            }
            auto value = string_chunk->operator[](i);
            if (value < min_string) {
                min_string = value;
            }
            if (value > max_string) {
                max_string = value;
            }
        }
        // The field data may later be released, so we need to copy the string to avoid invalid memory access.
        min_ = Metrics(std::string(min_string));
        max_ = Metrics(std::string(max_string));
        null_count_ = null_count;
        hasValue_ = true;
    }

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
};

template<typename T>
class SetFieldChunkMetric : public FieldChunkMetric {
private:
    std::set<T> unique_values_;
    int64_t null_count_;
    int64_t total_count_;
    bool is_complete_;

public:
    SetFieldChunkMetric(std::shared_ptr<Chunk> chunk) 
        : null_count_(0), total_count_(0), is_complete_(true) {
        if constexpr (std::is_same_v<T, std::string>) {
            ProcessStringChunk(chunk);
        } else {
            ProcessNumericChunk(chunk);
        }
    }

    bool CanSkipEqual(const T& value) const {
        if (!is_complete_) return false;
        return unique_values_.find(value) == unique_values_.end();
    }

    bool CanSkipIn(const std::vector<T>& values) const {
        if (!is_complete_) return false;
        
        for (const auto& value : values) {
            if (unique_values_.find(value) != unique_values_.end()) {
                return false;  // 找到匹配值，不能跳过
            }
        }
        return true;  // 没有找到任何匹配值，可以跳过
    }

    FieldChunkMetricType GetType() const override {
        return FieldChunkMetricType::SET;
    }

    std::string Serialize() const override {
        // TODO: 实现序列化
        return "";
    }

    void Deserialize(const std::string& data) override {
        // TODO: 实现反序列化
    }

private:
    void ProcessNumericChunk(std::shared_ptr<Chunk> chunk) {
        auto fixed_chunk = std::dynamic_pointer_cast<FixedWidthChunk>(chunk);
        if (!fixed_chunk) return;

        auto span = fixed_chunk->Span();
        const T* data = static_cast<const T*>(span.data());
        const bool* valid_data = span.valid_data();
        total_count_ = span.row_count();

        static constexpr size_t MAX_SET_SIZE = 256;

        for (int64_t i = 0; i < total_count_; ++i) {
            if (valid_data && !valid_data[i]) {
                null_count_++;
                continue;
            }

            T value = data[i];
            
            if (unique_values_.size() >= MAX_SET_SIZE) {
                is_complete_ = false;
                break;
            }
            
            unique_values_.insert(value);
        }

        hasValue_ = is_complete_ && !unique_values_.empty();
    }

    void ProcessStringChunk(std::shared_ptr<Chunk> chunk) {
        auto string_chunk = std::dynamic_pointer_cast<StringChunk>(chunk);
        if (!string_chunk) return;

        total_count_ = string_chunk->RowNums();
        static constexpr size_t MAX_SET_SIZE = 256;

        for (int64_t i = 0; i < total_count_; ++i) {
            if (!string_chunk->isValid(i)) {
                null_count_++;
                continue;
            }

            if (unique_values_.size() >= MAX_SET_SIZE) {
                is_complete_ = false;
                break;
            }

            auto value = std::string(string_chunk->operator[](i));
            unique_values_.insert(value);
        }

        hasValue_ = is_complete_ && !unique_values_.empty();
    }
};

class NgramFieldChunkMetric : public FieldChunkMetric {
private:
    std::vector<uint64_t> bit_array_;
    size_t ngram_size_;
    size_t hash_count_;
    size_t bit_size_;
    
public:
    NgramFieldChunkMetric(std::shared_ptr<Chunk> chunk, size_t ngram_size = 3) 
        : ngram_size_(ngram_size), hash_count_(3) {
        ProcessStringChunk(chunk);
    }
    
    bool CanSkipSubstringMatch(const std::string& pattern) const {
        if (!hasValue_ || pattern.length() < ngram_size_) {
            return false;
        }
        
        // 检查模式的所有N-gram是否都可能存在
        for (size_t i = 0; i <= pattern.length() - ngram_size_; ++i) {
            std::string ngram = pattern.substr(i, ngram_size_);
            if (!MightContainNgram(ngram)) {
                return true;  // 确定不包含此子串，可以跳过
            }
        }
        return false;  // 可能包含，不能跳过
    }
    
    bool CanSkipPrefixMatch(const std::string& prefix) const {
        return CanSkipSubstringMatch(prefix);
    }
    
    bool CanSkipPostfixMatch(const std::string& suffix) const {
        return CanSkipSubstringMatch(suffix);
    }
    
    FieldChunkMetricType GetType() const override {
        return FieldChunkMetricType::NGRAM_FILTER;
    }
    
    std::string Serialize() const override {
        return "";
    }
    
    void Deserialize(const std::string& data) override {
        
    }

private:
    void ProcessStringChunk(std::shared_ptr<Chunk> chunk) {
        auto string_chunk = std::dynamic_pointer_cast<StringChunk>(chunk);
        if (!string_chunk) return;
        
        // 估算N-gram数量来确定布隆过滤器大小
        size_t estimated_ngrams = EstimateNgramCount(string_chunk.get());
        InitializeBloomFilter(estimated_ngrams);
        
        // 处理所有字符串
        for (int64_t i = 0; i < string_chunk->RowNums(); ++i) {
            if (string_chunk->isValid(i)) {
                auto text = std::string(string_chunk->operator[](i));
                AddStringToFilter(text);
            }
        }
        
        hasValue_ = true;
    }
    
    void InitializeBloomFilter(size_t estimated_ngrams) {
        // 根据估算的N-gram数量和期望误报率计算最优大小
        double false_positive_rate = 0.01;  // 1%误报率
        bit_size_ = static_cast<size_t>(
            -1.0 * estimated_ngrams * std::log(false_positive_rate) / 
            (std::log(2) * std::log(2))
        );
        
        // 确保是64的倍数以便于存储
        bit_size_ = ((bit_size_ + 63) / 64) * 64;
        bit_array_.resize(bit_size_ / 64, 0);
    }
    
    void AddStringToFilter(const std::string& text) {
        for (size_t i = 0; i <= text.length() - ngram_size_; ++i) {
            std::string ngram = text.substr(i, ngram_size_);
            AddNgram(ngram);
        }
    }
    
    void AddNgram(const std::string& ngram) {
        uint64_t hash1 = std::hash<std::string>{}(ngram);
        uint64_t hash2 = hash1 >> 32;
        
        for (size_t i = 0; i < hash_count_; ++i) {
            size_t bit_pos = (hash1 + i * hash2) % bit_size_;
            size_t word_index = bit_pos / 64;
            size_t bit_index = bit_pos % 64;
            bit_array_[word_index] |= (1ULL << bit_index);
        }
    }
    
    bool MightContainNgram(const std::string& ngram) const {
        uint64_t hash1 = std::hash<std::string>{}(ngram);
        uint64_t hash2 = hash1 >> 32;
        
        for (size_t i = 0; i < hash_count_; ++i) {
            size_t bit_pos = (hash1 + i * hash2) % bit_size_;
            size_t word_index = bit_pos / 64;
            size_t bit_index = bit_pos % 64;
            if (!(bit_array_[word_index] & (1ULL << bit_index))) {
                return false;
            }
        }
        return true;
    }
    
    size_t EstimateNgramCount(const StringChunk* chunk) {
        size_t total_ngrams = 0;
        size_t sample_size = std::min(100L, chunk->RowNums());  // 采样100个字符串
        
        for (int64_t i = 0; i < sample_size; ++i) {
            if (chunk->isValid(i)) {
                auto text = chunk->operator[](i);
                if (text.length() >= ngram_size_) {
                    total_ngrams += text.length() - ngram_size_ + 1;
                }
            }
        }
        
        // 按比例估算总数
        return (total_ngrams * chunk->RowNums()) / sample_size;
    }
};

class TokenFieldChunkMetric : public FieldChunkMetric {
private:
    std::vector<uint64_t> bit_array_;
    size_t hash_count_;
    size_t bit_size_;
    
public:
    TokenFieldChunkMetric(std::shared_ptr<Chunk> chunk) : hash_count_(3) {
        ProcessStringChunk(chunk);
    }
    
    bool CanSkipFullTextSearch(const std::vector<std::string>& search_tokens) const {
        if (!hasValue_) return false;
        
        // 检查是否包含所有搜索词
        for (const auto& token : search_tokens) {
            if (!MightContainToken(token)) {
                return true;  // 确定不包含某个词，可以跳过
            }
        }
        return false;  // 可能包含所有词，不能跳过
    }
    
    bool CanSkipAnyTokenMatch(const std::vector<std::string>& search_tokens) const {
        if (!hasValue_) return false;
        
        // 检查是否包含任意搜索词
        for (const auto& token : search_tokens) {
            if (MightContainToken(token)) {
                return false;  // 找到可能匹配的词，不能跳过
            }
        }
        return true;  // 确定不包含任何词，可以跳过
    }
    
    FieldChunkMetricType GetType() const override {
        return FieldChunkMetricType::TOKEN_FILTER;
    }
    
    std::string Serialize() const override {
        // BinarySerializer serializer;
        // serializer.Write<size_t>(hash_count_);
        // serializer.Write<size_t>(bit_size_);
        // serializer.Write<size_t>(bit_array_.size());
        
        // for (uint64_t bits : bit_array_) {
        //     serializer.Write<uint64_t>(bits);
        // }
        
        // return serializer.GetResult();
        return "";
    }
    
    void Deserialize(const std::string& data) override {
        // BinaryDeserializer deserializer(data);
        // hash_count_ = deserializer.Read<size_t>();
        // bit_size_ = deserializer.Read<size_t>();
        
        // size_t array_size = deserializer.Read<size_t>();
        // bit_array_.clear();
        // bit_array_.reserve(array_size);
        
        // for (size_t i = 0; i < array_size; ++i) {
        //     bit_array_.push_back(deserializer.Read<uint64_t>());
        // }
        
        // hasValue_ = !bit_array_.empty();
    }

private:
    void ProcessStringChunk(std::shared_ptr<Chunk> chunk) {
        auto string_chunk = std::dynamic_pointer_cast<StringChunk>(chunk);
        if (!string_chunk) return;
        
        // 估算Token数量
        size_t estimated_tokens = EstimateTokenCount(string_chunk.get());
        InitializeBloomFilter(estimated_tokens);
        
        // 处理所有字符串
        for (int64_t i = 0; i < string_chunk->RowNums(); ++i) {
            if (string_chunk->isValid(i)) {
                auto text = std::string(string_chunk->operator[](i));
                AddStringToFilter(text);
            }
        }
        
        hasValue_ = true;
    }
    
    void InitializeBloomFilter(size_t estimated_tokens) {
        double false_positive_rate = 0.01;
        bit_size_ = static_cast<size_t>(
            -1.0 * estimated_tokens * std::log(false_positive_rate) / 
            (std::log(2) * std::log(2))
        );
        bit_size_ = ((bit_size_ + 63) / 64) * 64;
        bit_array_.resize(bit_size_ / 64, 0);
    }
    
    void AddStringToFilter(const std::string& text) {
        auto tokens = TokenizeText(text);
        for (const auto& token : tokens) {
            AddToken(token);
        }
    }
    
    std::vector<std::string> TokenizeText(const std::string& text) {
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
    
    void AddToken(const std::string& token) {
        uint64_t hash1 = std::hash<std::string>{}(token);
        uint64_t hash2 = hash1 >> 32;
        
        for (size_t i = 0; i < hash_count_; ++i) {
            size_t bit_pos = (hash1 + i * hash2) % bit_size_;
            size_t word_index = bit_pos / 64;
            size_t bit_index = bit_pos % 64;
            bit_array_[word_index] |= (1ULL << bit_index);
        }
    }
    
    bool MightContainToken(const std::string& token) const {
        uint64_t hash1 = std::hash<std::string>{}(token);
        uint64_t hash2 = hash1 >> 32;
        
        for (size_t i = 0; i < hash_count_; ++i) {
            size_t bit_pos = (hash1 + i * hash2) % bit_size_;
            size_t word_index = bit_pos / 64;
            size_t bit_index = bit_pos % 64;
            if (!(bit_array_[word_index] & (1ULL << bit_index))) {
                return false;
            }
        }
        return true;
    }
    
    size_t EstimateTokenCount(const StringChunk* chunk) {
        size_t total_tokens = 0;
        size_t sample_size = std::min(100L, chunk->RowNums());
        
        for (int64_t i = 0; i < sample_size; ++i) {
            if (chunk->isValid(i)) {
                auto text = std::string(chunk->operator[](i));
                auto tokens = TokenizeText(text);
                total_tokens += tokens.size();
            }
        }
        
        return (total_tokens * chunk->RowNums()) / sample_size;
    }
};

template<typename T>
struct DataMetrics {
    int64_t total_rows = 0;
    int64_t null_count = 0;
    T min_value;
    T max_value;
    std::set<T> unique_values;
    size_t unique_count = 0;
    double cardinality_ratio = 0.0;
    
    // 字符串特有的指标
    double avg_string_length = 0.0;
    size_t max_string_length = 0;
    std::set<std::string> sample_ngrams;
    std::set<std::string> sample_tokens;
    bool has_spaces = false;
    bool is_complete_sample = true;  // 是否完整采样了所有唯一值
    
    // 基数分类
    bool IsLowCardinality() const {
        return cardinality_ratio < 0.1 && unique_count <= 256 && is_complete_sample;
    }
    
    bool IsMediumCardinality() const {
        return cardinality_ratio >= 0.1 && cardinality_ratio <= 0.7 && unique_count <= 10000;
    }
    
    bool IsHighCardinality() const {
        return cardinality_ratio > 0.7 || unique_count > 10000;
    }
    
    // 文本特征判断
    bool ShouldUseNgramFilter() const {
        if constexpr (std::is_same_v<T, std::string>) {
            return avg_string_length >= 4.0 && 
                   avg_string_length <= 1000.0 && 
                   sample_ngrams.size() >= 10;
        }
        return false;
    }
    
    bool ShouldUseTokenFilter() const {
        if constexpr (std::is_same_v<T, std::string>) {
            return avg_string_length >= 8.0 && 
                   sample_tokens.size() >= 3 &&
                   has_spaces;
        }
        return false;
    }
};

template<typename T>
class DataAnalyzer {
public:
    static DataMetrics<T> AnalyzeChunk(std::shared_ptr<Chunk> chunk) {
        if constexpr (std::is_same_v<T, std::string>) {
            return AnalyzeStringChunk(chunk);
        } else {
            return AnalyzeNumericChunk(chunk);
        }
    }

private:
    static DataMetrics<T> AnalyzeNumericChunk(std::shared_ptr<Chunk> chunk) {
        auto fixed_chunk = std::dynamic_pointer_cast<FixedWidthChunk>(chunk);
        if (!fixed_chunk) return {};
        
        DataMetrics<T> metrics;
        auto span = fixed_chunk->Span();
        const T* data = static_cast<const T*>(span.data());
        const bool* valid_data = span.valid_data();
        metrics.total_rows = span.row_count();
        
        if (metrics.total_rows == 0) return metrics;
        
        bool has_first_valid = false;
        static constexpr size_t MAX_UNIQUE_SAMPLE = 1000;  // 最大采样唯一值数量
        
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
                if (value < metrics.min_value) metrics.min_value = value;
                if (value > metrics.max_value) metrics.max_value = value;
            }
            
            // 采样唯一值
            if (metrics.unique_values.size() < MAX_UNIQUE_SAMPLE) {
                metrics.unique_values.insert(value);
            } else {
                metrics.is_complete_sample = false;
            }
        }
        
        metrics.unique_count = metrics.unique_values.size();
        if (metrics.total_rows > metrics.null_count) {
            metrics.cardinality_ratio = static_cast<double>(metrics.unique_count) / 
                                       (metrics.total_rows - metrics.null_count);
        }
        
        return metrics;
    }
    
    static DataMetrics<std::string> AnalyzeStringChunk(std::shared_ptr<Chunk> chunk) {
        auto string_chunk = std::dynamic_pointer_cast<StringChunk>(chunk);
        if (!string_chunk) return {};
        
        DataMetrics<std::string> metrics;
        metrics.total_rows = string_chunk->RowNums();
        
        if (metrics.total_rows == 0) return metrics;
        
        bool has_first_valid = false;
        double total_length = 0.0;
        int64_t valid_count = 0;
        static constexpr size_t MAX_UNIQUE_SAMPLE = 500;
        static constexpr size_t MAX_NGRAM_SAMPLE = 1000;
        static constexpr size_t MAX_TOKEN_SAMPLE = 500;
        
        for (int64_t i = 0; i < metrics.total_rows; ++i) {
            if (!string_chunk->isValid(i)) {
                metrics.null_count++;
                continue;
            }
            
            auto text_view = string_chunk->operator[](i);
            std::string text(text_view);
            valid_count++;
            total_length += text.length();
            metrics.max_string_length = std::max(metrics.max_string_length, text.length());
            
            // 检查是否包含空格
            if (!metrics.has_spaces && text.find(' ') != std::string::npos) {
                metrics.has_spaces = true;
            }
            
            // 更新min/max
            if (!has_first_valid) {
                metrics.min_value = metrics.max_value = text;
                has_first_valid = true;
            } else {
                if (text < metrics.min_value) metrics.min_value = text;
                if (text > metrics.max_value) metrics.max_value = text;
            }
            
            // 采样唯一值
            if (metrics.unique_values.size() < MAX_UNIQUE_SAMPLE) {
                metrics.unique_values.insert(text);
            } else {
                metrics.is_complete_sample = false;
            }
            
            // 采样N-gram
            if (metrics.sample_ngrams.size() < MAX_NGRAM_SAMPLE) {
                ExtractNgrams(text, metrics.sample_ngrams, 3);
            }
            
            // 采样Token
            if (metrics.sample_tokens.size() < MAX_TOKEN_SAMPLE) {
                ExtractTokens(text, metrics.sample_tokens);
            }
        }
        
        if (valid_count > 0) {
            metrics.avg_string_length = total_length / valid_count;
        }
        
        metrics.unique_count = metrics.unique_values.size();
        if (valid_count > 0) {
            metrics.cardinality_ratio = static_cast<double>(metrics.unique_count) / valid_count;
        }
        
        return metrics;
    }
    
    static void ExtractNgrams(const std::string& text, std::set<std::string>& ngrams, size_t n) {
        if (text.length() < n) return;
        for (size_t i = 0; i <= text.length() - n; ++i) {
            ngrams.insert(text.substr(i, n));
        }
    }
    
    static void ExtractTokens(const std::string& text, std::set<std::string>& tokens) {
        std::string current_token;
        for (char c : text) {
            if (std::isalnum(c)) {
                current_token += std::tolower(c);
            } else {
                if (!current_token.empty()) {
                    tokens.insert(current_token);
                    current_token.clear();
                }
            }
        }
        if (!current_token.empty()) {
            tokens.insert(current_token);
        }
    }
};

class MetricsDecisionEngine {
public:
    template<typename T>
    static std::vector<FieldChunkMetricType> DecideMetrics(
        const DataMetrics<T>& metrics, DataType data_type) {
        
        std::vector<FieldChunkMetricType> result;
        
        // MinMax：总是创建（基础统计）
        result.push_back(FieldChunkMetricType::MINMAX);
        
        if constexpr (std::is_same_v<T, std::string>) {
            // 字符串类型的决策
            DecideStringMetrics(metrics, result);
        } else {
            // 数值类型的决策
            DecideNumericMetrics(metrics, result);
        }
        
        return result;
    }

private:
    template<typename T>
    static void DecideNumericMetrics(const DataMetrics<T>& metrics, 
                                   std::vector<FieldChunkMetricType>& result) {
        if (metrics.IsLowCardinality()) {
            // 低基数：使用SET统计
            result.push_back(FieldChunkMetricType::SET);
        } else if (metrics.IsMediumCardinality() || metrics.IsHighCardinality()) {
            // 中高基数：使用BloomFilter
            result.push_back(FieldChunkMetricType::BLOOM_FILTER);
        }
    }
    
    static void DecideStringMetrics(const DataMetrics<std::string>& metrics, 
                                  std::vector<FieldChunkMetricType>& result) {
        if (metrics.IsLowCardinality()) {
            // 低基数字符串：使用SET
            result.push_back(FieldChunkMetricType::SET);
        } else {
            // 高基数字符串：使用BloomFilter作为基础
            result.push_back(FieldChunkMetricType::BLOOM_FILTER);
            
            // 根据文本特征决定是否添加文本搜索索引
            if (metrics.ShouldUseNgramFilter()) {
                result.push_back(FieldChunkMetricType::NGRAM_FILTER);
            }
            
            if (metrics.ShouldUseTokenFilter()) {
                result.push_back(FieldChunkMetricType::TOKEN_FILTER);
            }
        }
    }
};

class FieldChunkSkipIndex {
public:
    explicit FieldChunkSkipIndex(DataType data_type) : data_type_(data_type) {}

    void Load(std::shared_ptr<Chunk> chunk) {
        switch (data_type_) {
            case DataType::INT8:
                LoadTypedMetrics<int8_t>(chunk);
                break;
            case DataType::INT16:
                LoadTypedMetrics<int16_t>(chunk);
                break;
            case DataType::INT32:
                LoadTypedMetrics<int32_t>(chunk);
                break;
            case DataType::INT64:
                LoadTypedMetrics<int64_t>(chunk);
                break;
            case DataType::FLOAT:
                LoadTypedMetrics<float>(chunk);
                break;
            case DataType::DOUBLE:
                LoadTypedMetrics<double>(chunk);
                break;
            case DataType::VARCHAR:
            case DataType::STRING:
                LoadTypedMetrics<std::string>(chunk);
                break;
            default:
                return;
        }
    }

    template<typename T>
    void LoadTypedMetrics(std::shared_ptr<Chunk> chunk) {
        auto data_metrics = DataAnalyzer<T>::AnalyzeChunk(chunk);
        auto metric_types = MetricsDecisionEngine::DecideMetrics(data_metrics, data_type_);
        
        for (auto type : metric_types) {
            auto metric = CreateMetricFromAnalysis<T>(type, data_metrics, chunk);
            if (metric && metric->hasValue_) {
                Add(type, metric);
            }
        }
        
        // 5. 记录决策日志
        LogDecision(metric_types, data_metrics);
    }

    // 获取特定类型的统计
    template<typename MetricType>
    std::shared_ptr<MetricType> GetMetric(FieldChunkMetricType type) const {
        for (const auto& [metric_type, metric] : metrics_) {
            if (metric_type == type) {
                return std::dynamic_pointer_cast<MetricType>(metric);
            }
        }
        return nullptr;
    }

    // 检查是否包含某种类型
    bool HasMetricType(FieldChunkMetricType type) const {
        for (const auto& [metric_type, metric] : metrics_) {
            if (metric_type == type) return true;
        }
        return false;
    }

    std::vector<std::pair<FieldChunkMetricType, std::shared_ptr<FieldChunkMetric>>> GetMetrics() const {
        return metrics_;
    }

    size_t Size() const {
        return metrics_.size();
    }

    std::string Serialize() const {
        // TODO: 实现序列化
        return "";
    }

    void Deserialize(const std::string& data) {
        // TODO: 实现反序列化
    }

private:
    template<typename T>
    std::shared_ptr<FieldChunkMetric> CreateMetricFromAnalysis(
        FieldChunkMetricType type, const DataMetrics<T>& data_metrics, std::shared_ptr<Chunk> chunk) {
        
        switch (type) {
            case FieldChunkMetricType::MINMAX:
                return CreateMinMaxFromAnalysis<T>(data_metrics);
            case FieldChunkMetricType::SET:
                return CreateSetFromAnalysis<T>(data_metrics, chunk);
            case FieldChunkMetricType::BLOOM_FILTER:
                return CreateBloomFilterFromAnalysis<T>(data_metrics, chunk);
            case FieldChunkMetricType::NGRAM_FILTER:
                if constexpr (std::is_same_v<T, std::string>) {
                    return std::make_shared<NgramFieldChunkMetric>(chunk, 3);
                }
                break;
            case FieldChunkMetricType::TOKEN_FILTER:
                if constexpr (std::is_same_v<T, std::string>) {
                    return std::make_shared<TokenFieldChunkMetric>(chunk);
                }
                break;
        }
        return nullptr;
    }

    template<typename T>
    std::shared_ptr<FieldChunkMetric> CreateMinMaxFromAnalysis(const DataMetrics<T>& data_metrics) {
        auto metric = std::make_shared<MinMaxFieldChunkMetric<T>>();
        metric->min_ = Metrics(data_metrics.min_value);
        metric->max_ = Metrics(data_metrics.max_value);
        metric->null_count_ = data_metrics.null_count;
        metric->hasValue_ = true;
        return metric;
    }

    template<typename T>
    std::shared_ptr<FieldChunkMetric> CreateSetFromAnalysis(
        const DataMetrics<T>& data_metrics, std::shared_ptr<Chunk> chunk) {
        // 只有在完整采样且低基数时才创建SET
        if (!data_metrics.is_complete_sample || !data_metrics.IsLowCardinality()) {
            return nullptr;
        }
        return std::make_shared<SetFieldChunkMetric<T>>(chunk);
    }

    template<typename T>
    std::shared_ptr<FieldChunkMetric> CreateBloomFilterFromAnalysis(
        const DataMetrics<T>& data_metrics, std::shared_ptr<Chunk> chunk) {
        // 只有在中高基数时才创建BloomFilter
        if (data_metrics.IsLowCardinality()) {
            return nullptr;
        }
        // TODO: 实现BloomFilterFieldChunkMetrics
        return nullptr; // 暂时返回nullptr
    }

    void Add(FieldChunkMetricType type, std::shared_ptr<FieldChunkMetric> metric) {
        metrics_.emplace_back(type, std::move(metric));
    }

    std::vector<std::pair<FieldChunkMetricType, std::shared_ptr<FieldChunkMetric>>> metrics_;
    DataType data_type_;
    std::string analysis_summary_;  // 存储分析摘要
};

class ChunkSkipIndex : public milvus_storage::Metadata {
   public:

    ChunkSkipIndex() = default;

    ChunkSkipIndex(const std::shared_ptr<arrow::Table>& table, const std::unordered_map<FieldId, FieldMeta>& field_metas) {
        field_chunk_metrics_.reserve(field_metas.size());

        std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
        std::vector<FieldId> field_list;
        std::vector<std::unique_ptr<FieldChunkMetric>> field_chunk_metrics;
        for (int i = 0; i < table->schema()->num_fields(); ++i) {
            auto field_id = std::stoll(table->schema()
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

            auto field_metrics = std::make_shared<FieldChunkSkipIndex>(data_type);
            field_metrics->Load(chunk);

            field_chunk_metrics_.emplace_back(std::make_pair(fid, field_metrics));
        }    
    }

    explicit ChunkSkipIndex(std::vector<std::pair<FieldId, std::shared_ptr<FieldChunkSkipIndex>>> field_chunk_metrics)
        : field_chunk_metrics_(std::move(field_chunk_metrics)) {}

    std::vector<std::pair<FieldId, std::shared_ptr<FieldChunkSkipIndex>>> GetFieldChunkMetrics() const {
        return field_chunk_metrics_;
    }

    std::string Serialize() const override {
        return "";
    };

    void Deserialize(const std::string &data) override {
        // Implement deserialization logic here
    };

 private:
    std::vector<std::pair<FieldId, std::shared_ptr<FieldChunkSkipIndex>>> field_chunk_metrics_;
};

class ChunkSkipIndexBuilder : public milvus_storage::MetadataBuilder<ChunkSkipIndexBuilder, ChunkSkipIndex> {
 public:
    ChunkSkipIndexBuilder() = default;
    ChunkSkipIndexBuilder(const std::unordered_map<FieldId, FieldMeta>& field_metas)
        : field_metas_(field_metas) {}
 protected:
    std::shared_ptr<ChunkSkipIndex> BuildImpl(const std::shared_ptr<arrow::Table>& table) override {
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
            if (auto set_metrics = field_chunk_skipindex->GetMetric<SetFieldChunkMetric<T>>(FieldChunkMetricType::SET);
                set_metrics) {
                return set_metrics->CanSkipEqual(val);
            }
        }
        if (auto minmax_metrics = field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(FieldChunkMetricType::MINMAX);
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
        if (auto minmax_metric = field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(FieldChunkMetricType::MINMAX);
            minmax_metric) {
            return minmax_metric->CanSkipBinaryRange(lower_val, upper_val, lower_inclusive, upper_inclusive);
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

    // IN查询优化
    template <typename T>
    std::enable_if_t<IsAllowedType<T>::value, bool>
    CanSkipInQuery(FieldId field_id, int64_t chunk_id, const std::vector<T>& values) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) return false;

        // IN查询优先使用SET统计
        if (auto set_metric = field_chunk_skipindex->GetMetric<SetFieldChunkMetric<T>>(FieldChunkMetricType::SET)) {
            return set_metric->CanSkipIn(values);
        }

        // 降级到MinMax：计算值范围
        if (auto minmax_metric = field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(FieldChunkMetricType::MINMAX)) {
            if (values.empty()) return false;
            
            auto min_val = *std::min_element(values.begin(), values.end());
            auto max_val = *std::max_element(values.begin(), values.end());
            return minmax_metric->CanSkipBinaryRange(min_val, max_val, true, true);
        }

        return false;
    }

    // 子串匹配查询
    bool CanSkipSubstringMatch(FieldId field_id, int64_t chunk_id, 
                              const std::string& pattern) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }
        if (auto ngram_metric = field_chunk_skipindex->GetMetric<NgramFieldChunkMetric>(FieldChunkMetricType::NGRAM_FILTER);
            ngram_metric) {
            return ngram_metric->CanSkipSubstringMatch(pattern);
        }
        return false;
    }
    
    // 全文搜索查询
    bool CanSkipFullTextSearch(FieldId field_id, int64_t chunk_id,
                              const std::vector<std::string>& keywords) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }
        if (auto token_metric = field_chunk_skipindex->GetMetric<TokenFieldChunkMetric>(FieldChunkMetricType::TOKEN_FILTER);
            token_metric) {
            return token_metric->CanSkipFullTextSearch(keywords);
        }
        return false;
    }

    void
    LoadSkip(std::vector<std::shared_ptr<ChunkSkipIndex>> chunk_skipindex) {
        if (chunk_skipindex.empty()) {
            return;
        }
        size_t size = chunk_skipindex.size();
        for (size_t i = 0; i < size; ++i) {
            const auto& field_chunk_metrics = chunk_skipindex[i]->GetFieldChunkMetrics();
            for (const auto& [field_id, metrics] : field_chunk_metrics) {
                if (i == 0) {
                    fieldChunkMetrics_[field_id] = std::vector<std::shared_ptr<FieldChunkSkipIndex>>{};
                    fieldChunkMetrics_[field_id].reserve(size);
                }
                fieldChunkMetrics_[field_id].emplace_back(metrics);
            }
        }
    }

 private:
    std::shared_ptr<FieldChunkSkipIndex> GetFieldChunkMetrics(milvus::FieldId field_id, int chunk_id) const {
        return fieldChunkMetrics_.at(field_id).at(chunk_id);
    }

    std::unordered_map<FieldId, std::vector<std::shared_ptr<FieldChunkSkipIndex>>> fieldChunkMetrics_;
    mutable std::shared_mutex mutex_;
};
}  // namespace milvus
