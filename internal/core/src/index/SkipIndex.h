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
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/EasyAssert.h"
#include "tokenizer.h"
#include "xxhash.h"
#include "ankerl/unordered_dense.h"
#include "arrow/record_batch.h"
#include "arrow/type_traits.h"
#include "common/Schema.h"
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

// False positive rate for Bloom filter
static constexpr double FPR = 0.01;
static constexpr int64_t NGRAM_SIZE = 3;
static constexpr int64_t MIN_ROWS_TO_BUILD_METRICS = 10;
static constexpr double CARDINALITY_RATIO_FOR_SET = 0.1;
static constexpr double METADATA_BUDGET_RATIO = 0.01;

template <typename T>
struct MetricsInfo {
    int64_t total_rows_ = 0;
    int64_t null_count_ = 0;

    MetricsDataType<T> min_;
    MetricsDataType<T> max_;

    bool contains_true_ = false;
    bool contains_false_ = false;

    ankerl::unordered_dense::set<MetricsDataType<T>> unique_values_;

    size_t string_length_ = 0;
    size_t max_string_length_ = 0;
    size_t total_string_length_ = 0;
};

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
    explicit MinMaxFieldChunkMetric(const MetricsInfo<T>& info) {
        if constexpr (std::is_same_v<T, std::string>) {
            min_ = std::string(info.min_);
            max_ = std::string(info.max_);
        } else {
            min_ = info.min_;
            max_ = info.max_;
        }
        hasValue_ = true;
    }

    explicit MinMaxFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
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
    explicit SetFieldChunkMetric(const MetricsInfo<T>& info) {
        for (const auto& val : info.unique_values_) {
            unique_values_.insert(val);
        }
        hasValue_ = true;
    }

    explicit SetFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        uint32_t count;
        ss.read(reinterpret_cast<char*>(&count), sizeof(count));
        unique_values_.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            T value;
            ss.read(reinterpret_cast<char*>(&value), sizeof(value));
            unique_values_.insert(std::move(value));
        }
        hasValue_ = true;
    }

    static uint64_t
    EstimateCost(const MetricsInfo<T>& info) {
        return info.unique_values_.size() * sizeof(T);
    }

    bool
    CanSkipEqual(const T& value) const {
        return unique_values_.find(value) == unique_values_.end();
    }

    bool
    CanSkipNotEqual(const T& value) const {
        return unique_values_.find(value) != unique_values_.end();
    }

    bool
    CanSkipIn(const std::vector<T>& values) const {
        for (const auto& value : values) {
            if (unique_values_.count(value)) {
                return false;
            }
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
        uint64_t count = unique_values_.size();
        ss.write(reinterpret_cast<const char*>(&count), sizeof(count));

        for (const auto& value : unique_values_) {
            ss.write(reinterpret_cast<const char*>(&value), sizeof(value));
        }
        return ss.str();
    }

 private:
    ankerl::unordered_dense::set<T> unique_values_;
};

template <>
class SetFieldChunkMetric<std::string> : public FieldChunkMetric {
 public:
    explicit SetFieldChunkMetric(const MetricsInfo<std::string>& info) {
        for (const auto& val : info.unique_values_) {
            unique_values_.insert(std::string(val));
        }
        hasValue_ = true;
    }

    explicit SetFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        uint32_t count;
        ss.read(reinterpret_cast<char*>(&count), sizeof(count));
        unique_values_.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            uint64_t len;
            ss.read(reinterpret_cast<char*>(&len), sizeof(len));
            std::string value(len, '\0');
            ss.read(value.data(), len);
            unique_values_.insert(std::move(value));
        }
        hasValue_ = true;
    }

    static uint64_t
    EstimateCost(const MetricsInfo<std::string>& info) {
        return info.total_string_length_;
    }

    bool
    CanSkipEqual(const std::string& value) const {
        return unique_values_.find(value) == unique_values_.end();
    }

    bool
    CanSkipNotEqual(const std::string& value) const {
        return unique_values_.find(value) != unique_values_.end();
    }

    bool
    CanSkipIn(const std::vector<std::string>& values) const {
        for (const auto& value : values) {
            if (unique_values_.count(value)) {
                return false;
            }
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
        uint64_t count = unique_values_.size();
        ss.write(reinterpret_cast<const char*>(&count), sizeof(count));

        for (const auto& value : unique_values_) {
            uint64_t len = value.length();
            ss.write(reinterpret_cast<const char*>(&len), sizeof(len));
            ss.write(value.data(), len);
        }
        return ss.str();
    }

 private:
    ankerl::unordered_dense::set<std::string> unique_values_;
};

template <>
class SetFieldChunkMetric<bool> : public FieldChunkMetric {
 public:
    explicit SetFieldChunkMetric(const MetricsInfo<bool>& info)
        : contains_true_(info.contains_true_),
          contains_false_(info.contains_false_) {
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
    CanSkipNotEqual(const bool value) const {
        return (value && contains_true_) || (!value && contains_false_);
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
    Build(const std::vector<T>& items) {
        auto n = items.size();
        if (n == 0) {
            return nullptr;
        }
        auto bit_size = EstimateBitSize(n);
        auto hash_count = EstimateHashCount(n, bit_size);
        auto bit_array = std::vector<uint64_t>{};
        bit_array.assign(bit_size / 64, 0);
        auto bloom_filter =
            std::make_unique<BloomFilter>(hash_count, bit_size, bit_array);
        for (const auto& item : items) {
            bloom_filter->template Add<T>(item);
        }
        return bloom_filter;
    }

    template <typename T>
    static std::unique_ptr<BloomFilter>
    Build(const ankerl::unordered_dense::set<T>& items) {
        auto n = items.size();
        if (n == 0) {
            return nullptr;
        }
        auto bit_size = EstimateBitSize(n);
        auto hash_count = EstimateHashCount(n, bit_size);
        auto bit_array = std::vector<uint64_t>{};
        bit_array.assign(bit_size / 64, 0);
        auto bloom_filter =
            std::make_unique<BloomFilter>(hash_count, bit_size, bit_array);
        for (const auto& item : items) {
            bloom_filter->template Add<T>(item);
        }
        return bloom_filter;
    }

    static uint64_t
    EstimateCost(size_t n) {
        return EstimateBitSize(n) + sizeof(uint64_t) * 2;
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
    Deserialize(const std::string& data) {
        if (data.empty()) {
            return std::make_unique<BloomFilter>(0, 0, std::vector<uint64_t>{});
        }

        std::stringstream ss(data, std::ios::binary | std::ios::in);
        uint64_t hash_count, bit_size, array_size;
        ss.read(reinterpret_cast<char*>(&hash_count), sizeof(hash_count));
        ss.read(reinterpret_cast<char*>(&bit_size), sizeof(bit_size));
        ss.read(reinterpret_cast<char*>(&array_size), sizeof(array_size));

        std::vector<uint64_t> bit_array(array_size);
        ss.read(reinterpret_cast<char*>(bit_array.data()),
                array_size * sizeof(uint64_t));

        return std::make_unique<BloomFilter>(
            hash_count, bit_size, std::move(bit_array));
    }

 private:
    static uint64_t
    EstimateBitSize(size_t n) {
        if (n == 0) {
            return 0;
        }
        double bit_size_double =
            -1.0 * n * std::log(FPR) / (std::log(2) * std::log(2));
        auto bit_size = static_cast<uint64_t>(bit_size_double);
        // Align to 64 bits (8 bytes)
        return ((bit_size + 63) / 64) * 8;
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
    void
    Add(const T& item) {
        if (bit_size_ == 0) {
            return;
        }
        uint64_t hash1 = Hash<T>(item, 0x9e3779b9);
        uint64_t hash2 = Hash<T>(item, hash1);
        for (size_t i = 0; i < hash_count_; ++i) {
            size_t bit_pos = (hash1 + i * hash2) % bit_size_;
            bit_array_[bit_pos / 64] |= (1ULL << (bit_pos % 64));
        }
    }

    template <typename T>
    uint64_t
    Hash(const T& item, uint64_t seed) const {
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

template <typename T>
class BloomFilterFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<BloomFilter> filter_;

 public:
    explicit BloomFilterFieldChunkMetric(const MetricsInfo<T>& info) {
        filter_ = BloomFilter::Build<MetricsDataType<T>>(info.unique_values_);
        hasValue_ = filter_ && filter_->IsValid();
    }

    explicit BloomFilterFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        filter_ = BloomFilter::Deserialize(data);
        hasValue_ = filter_ && filter_->IsValid();
    }

    static uint64_t
    EstimateCost(const MetricsInfo<T>& info) {
        return BloomFilter::EstimateCost(info.unique_values_.size());
    }

    bool
    CanSkipEqual(const T& value) const {
        if (!hasValue_) {
            return false;
        }
        return !filter_->MightContain<T>(value);
    }

    bool
    CanSkipNotEqual(const T& value) const {
        if (!hasValue_) {
            return false;
        }
        return filter_->MightContain<T>(value);
    }

    bool
    CanSkipIn(const std::vector<T>& values) const {
        if (!hasValue_) {
            return false;
        }
        for (const auto& value : values) {
            if (filter_->MightContain<T>(value)) {
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
    std::unique_ptr<BloomFilter> filter_;

 public:
    explicit NgramFieldChunkMetric(const MetricsInfo<std::string>& info) {
        ankerl::unordered_dense::set<std::string_view> ngrams;
        for (const auto& text : info.unique_values_) {
            if (text.length() >= NGRAM_SIZE) {
                for (size_t j = 0; j <= text.length() - NGRAM_SIZE; ++j) {
                    ngrams.insert(text.substr(j, NGRAM_SIZE));
                }
            }
        }

        filter_ = BloomFilter::Build<std::string_view>(ngrams);
        hasValue_ = filter_->IsValid();
    }

    explicit NgramFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        filter_ = BloomFilter::Deserialize(data);
        hasValue_ = filter_ && filter_->IsValid();
    }

    static uint64_t
    EstimateCost(
        const ankerl::unordered_dense::set<std::string_view>& unique_values) {
        size_t total_ngrams = 0;
        for (const auto& text : unique_values) {
            if (text.length() >= 3) {
                total_ngrams += (text.length() - 3 + 1);
            }
        }
        return BloomFilter::EstimateCost(total_ngrams);
    }

    bool
    CanSkipSubstringMatch(const std::string& pattern) const {
        if (!hasValue_ || pattern.length() < NGRAM_SIZE) {
            return false;
        }

        for (size_t i = 0; i <= pattern.length() - NGRAM_SIZE; ++i) {
            std::string ngram = pattern.substr(i, NGRAM_SIZE);
            if (!filter_->MightContain<std::string>(ngram)) {
                return true;
            }
        }
        return false;
    }

    bool
    CanSkipSubstringMatch(const std::string_view& pattern) const {
        if (!hasValue_ || pattern.length() < NGRAM_SIZE) {
            return false;
        }

        for (size_t i = 0; i <= pattern.length() - NGRAM_SIZE; ++i) {
            std::string_view ngram = pattern.substr(i, NGRAM_SIZE);
            if (!filter_->MightContain<std::string_view>(ngram)) {
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
    std::unique_ptr<BloomFilter> filter_;
    std::string analyzer_params_;
    std::unique_ptr<milvus::tantivy::Tokenizer> tokenizer_;

 public:
    explicit TokenFieldChunkMetric(const MetricsInfo<std::string>& info,
                                   const char* analyzer_params)
        : analyzer_params_(analyzer_params),
          tokenizer_(std::make_unique<milvus::tantivy::Tokenizer>(
              std::string(analyzer_params))) {
        ankerl::unordered_dense::set<std::string> unique_tokens;
        for (auto& text : info.unique_values_) {
            auto token_stream =
                tokenizer_->CreateTokenStream(std::string(text));
            while (token_stream->advance()) {
                auto token = token_stream->get_token();
                unique_tokens.insert(token);
            }
        }

        filter_ = BloomFilter::Build<std::string>(unique_tokens);
        hasValue_ = filter_->IsValid();
    }

    explicit TokenFieldChunkMetric(const std::string& data) {
        if (data.empty()) {
            return;
        }
        std::stringstream ss(data, std::ios::binary | std::ios::in);
        size_t len;
        ss.read(reinterpret_cast<char*>(&len), sizeof(len));
        std::string analyzer_params_(len, '\0');
        ss.read(analyzer_params_.data(), len);
        auto filter_data = data.substr(sizeof(len) + len);
        filter_ = BloomFilter::Deserialize(filter_data);
        tokenizer_ = std::make_unique<milvus::tantivy::Tokenizer>(
            std::string(analyzer_params_));

        hasValue_ = filter_ && filter_->IsValid();
    }

    bool
    CanSkipFullTextSearch(const std::string& query_string) {
        if (!hasValue_) {
            return false;
        }

        auto token_stream = tokenizer_->CreateTokenStreamCopyText(query_string);
        while (token_stream->advance()) {
            auto token = token_stream->get_token();
            if (!filter_->MightContain<std::string>(token)) {
                return true;
            }
        }
        return false;
    }

    bool
    CanSkipAnyTokenMatch(const std::string& query_string) {
        if (!hasValue_) {
            return false;
        }

        auto token_stream = tokenizer_->CreateTokenStreamCopyText(query_string);
        while (token_stream->advance()) {
            auto token = token_stream->get_token();
            if (filter_->MightContain<std::string>(token)) {
                return false;
            }
        }
        return true;
    }

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::TOKEN_FILTER;
    }

    std::string
    Serialize() const override {
        std::stringstream ss(std::ios::binary | std::ios::out);
        size_t len = analyzer_params_.size();
        ss.write(reinterpret_cast<const char*>(&len), sizeof(len));
        ss.write(analyzer_params_.data(), len);
        auto filter_data = filter_ ? filter_->Serialize() : "";
        ss.write(filter_data.data(), filter_data.size());
        return ss.str();
    }
};

class FieldChunkMetrics {
 public:
    explicit FieldChunkMetrics(DataType data_type) : data_type_(data_type){};

    void
    Load(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
         int col_idx,
         const FieldMeta& field_meta);

    void
    Add(std::unique_ptr<FieldChunkMetric> metric) {
        metrics_.emplace_back(std::move(metric));
    }

    std::unique_ptr<FieldChunkMetric>
    LoadMetric(DataType data_type,
               FieldChunkMetricType metric_type,
               const std::string& data);

    template <typename MetricType>
    MetricType*
    GetMetric(FieldChunkMetricType type) const {
        for (const auto& metric : metrics_) {
            if (metric->GetType() == type) {
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
    Serialize() const;

    void
    Deserialize(const std::string& data);

 private:
    std::vector<std::unique_ptr<FieldChunkMetric>> metrics_;
    DataType data_type_;
};

class ChunkSkipIndex : public milvus_storage::Metadata {
 public:
    static constexpr const char* KEY = "SKIP_INDEX";

    ChunkSkipIndex() = default;

    explicit ChunkSkipIndex(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        SchemaPtr schema);

    void
    Add(FieldId field_id, std::unique_ptr<FieldChunkMetrics> metrics) {
        field_chunk_metrics_.emplace_back(field_id, std::move(metrics));
    }

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

            DataType data_type;
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
    explicit ChunkSkipIndexAppender(SchemaPtr schema)
        : schema_(std::move(schema)){};

 protected:
    std::unique_ptr<milvus_storage::Metadata>
    Create(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
        override {
        auto chunk_skip_index = std::make_unique<ChunkSkipIndex>();
        if (batches.empty()) {
            return chunk_skip_index;
        }
        auto field_schema = batches[0]->schema();
        for (int col_idx = 0; col_idx < field_schema->num_fields(); ++col_idx) {
            auto field_schema = batches[0]->schema();
            auto field_id =
                std::stoll(field_schema->field(col_idx)
                               ->metadata()
                               ->Get(milvus_storage::ARROW_FIELD_ID_KEY)
                               ->data());
            auto fid = FieldId(field_id);
            auto field_metrics = LoadFieldChunkMetrics(batches, fid, col_idx);
            if (field_metrics) {
                chunk_skip_index->Add(fid, std::move(field_metrics));
            }
        }
        return chunk_skip_index;
    }

 private:
    bool
    CanLoadField(DataType type) {
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
            case DataType::TEXT:
            case DataType::TIMESTAMPTZ:
                return true;
            default:
                return false;
        }
    }

    std::unique_ptr<FieldChunkMetrics>
    LoadFieldChunkMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        milvus::FieldId fid,
        int col_idx) {
        auto field_meta = schema_->operator[](fid);
        auto data_type = field_meta.get_data_type();
        if (fid == RowFieldID || !CanLoadField(data_type)) {
            return nullptr;
        }
        auto metrics = LoadMetrics(batches, col_idx, fid, data_type);
        if (metrics.empty()) {
            return nullptr;
        }
        auto field_chunk_metrics =
            std::make_unique<FieldChunkMetrics>(data_type);
        for (auto& metric : metrics) {
            field_chunk_metrics->Add(std::move(metric));
        }
        return field_chunk_metrics;
    }

    std::vector<std::unique_ptr<FieldChunkMetric>>
    LoadMetrics(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                int col_idx,
                FieldId fid,
                DataType data_type) {
        switch (data_type) {
            case DataType::BOOL:
                return LoadBooleanMetrics(batches, col_idx);
            case DataType::INT8:
                return LoadNumMetrics<int8_t, arrow::Int8Array>(batches,
                                                                col_idx);
            case DataType::INT16:
                return LoadNumMetrics<int16_t, arrow::Int16Array>(batches,
                                                                  col_idx);
            case DataType::INT32:
                return LoadNumMetrics<int32_t, arrow::Int32Array>(batches,
                                                                  col_idx);
            case DataType::INT64:
                return LoadNumMetrics<int64_t, arrow::Int64Array>(batches,
                                                                  col_idx);
            case DataType::FLOAT:
                return LoadNumMetrics<float, arrow::FloatArray>(batches,
                                                                col_idx);
            case DataType::DOUBLE:
                return LoadNumMetrics<double, arrow::DoubleArray>(batches,
                                                                  col_idx);
            case DataType::VARCHAR:
            case DataType::STRING:
                return LoadStringMetrics(batches, col_idx);
            case DataType::TEXT:
                return LoadTextMetrics(batches, col_idx, fid);
            default:
                return {};
        }
    }

    template <typename T, typename ArrayType>
    std::vector<std::unique_ptr<FieldChunkMetric>>
    LoadNumMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) {
        MetricsInfo<T> info{};
        ProcessFieldMetrics<T, ArrayType>(batches, col_idx, info);

        if (info.total_rows_ - info.null_count_ < MIN_ROWS_TO_BUILD_METRICS) {
            return {};
        }

        auto metrics = std::vector<std::unique_ptr<FieldChunkMetric>>{};
        metrics.emplace_back(std::make_unique<MinMaxFieldChunkMetric<T>>(info));
        auto bf_cost = BloomFilterFieldChunkMetric<T>::EstimateCost(info);
        auto set_cost = SetFieldChunkMetric<T>::EstimateCost(info);
        if (bf_cost > set_cost) {
            metrics.emplace_back(
                std::make_unique<SetFieldChunkMetric<T>>(info));
        } else {
            metrics.emplace_back(
                std::make_unique<BloomFilterFieldChunkMetric<T>>(info));
        }
        return metrics;
    }

    std::vector<std::unique_ptr<FieldChunkMetric>>
    LoadBooleanMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) {
        MetricsInfo<bool> info{};
        ProcessBooleanFieldMetrics(batches, col_idx, info);
        if (info.total_rows_ - info.null_count_ < MIN_ROWS_TO_BUILD_METRICS) {
            return {};
        }
        auto metrics = std::vector<std::unique_ptr<FieldChunkMetric>>{};
        metrics.emplace_back(std::make_unique<SetFieldChunkMetric<bool>>(info));
        return metrics;
    }

    std::vector<std::unique_ptr<FieldChunkMetric>>
    LoadStringMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) {
        MetricsInfo<std::string> info{};
        ProcessStringFieldMetrics(batches, col_idx, info);
        if (info.total_rows_ - info.null_count_ < MIN_ROWS_TO_BUILD_METRICS) {
            return {};
        }

        std::vector<std::unique_ptr<FieldChunkMetric>> metrics;
        auto remaining_budget = static_cast<size_t>(info.total_string_length_ *
                                                    METADATA_BUDGET_RATIO);
        metrics.emplace_back(
            std::make_unique<MinMaxFieldChunkMetric<std::string>>(info));
        remaining_budget -=
            sizeof(size_t) * 2 + info.min_.size() + info.max_.size();

        auto bf_cost =
            BloomFilterFieldChunkMetric<std::string>::EstimateCost(info);
        auto set_cost = SetFieldChunkMetric<std::string>::EstimateCost(info);
        if (set_cost < bf_cost && set_cost < remaining_budget) {
            metrics.emplace_back(
                std::make_unique<SetFieldChunkMetric<std::string>>(info));
            remaining_budget -= set_cost;
        } else if (bf_cost < remaining_budget) {
            metrics.emplace_back(
                std::make_unique<BloomFilterFieldChunkMetric<std::string>>(
                    info));
            remaining_budget -= bf_cost;
        }
    }

    std::vector<std::unique_ptr<FieldChunkMetric>>
    LoadTextMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx,
        FieldId fid) {
        MetricsInfo<std::string> info{};
        ProcessStringFieldMetrics(batches, col_idx, info);
        if (info.total_rows_ - info.null_count_ < MIN_ROWS_TO_BUILD_METRICS) {
            return {};
        }

        std::vector<std::unique_ptr<FieldChunkMetric>> metrics;
        auto field_meta = schema_->operator[](fid);
        metrics.emplace_back(std::make_unique<TokenFieldChunkMetric>(
            info, field_meta.get_analyzer_params().c_str()));
    }

    template <typename T, typename ArrayType>
    void
    ProcessFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx,
        MetricsInfo<T>& info) {
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
        MetricsInfo<std::string>& info) {
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
                info.total_string_length_ += length;
                if (info.unique_values_.find(value) !=
                    info.unique_values_.end()) {
                    continue;
                }
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
            }
            info.total_rows_ += string_array->length();
        }
    }

    void
    ProcessBooleanFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx,
        MetricsInfo<bool>& info) {
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

    SchemaPtr schema_;
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

        static constexpr bool value_for_in =
            isAllowedType || std::is_same<T, bool>::value;
    };

    template <typename T>
    std::enable_if_t<std::is_same_v<T, std::string> ||
                         std::is_same_v<T, std::string_view>,
                     bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }
        switch (op_type) {
            case OpType::PrefixMatch:
            case OpType::InnerMatch:
            case OpType::PostfixMatch: {
                if (auto ngram_metrics =
                        field_chunk_skipindex->GetMetric<NgramFieldChunkMetric>(
                            FieldChunkMetricType::NGRAM_FILTER)) {
                    return ngram_metrics->CanSkipSubstringMatch(val);
                }
                break;
            }
            case OpType::Equal: {
                if (auto set_metrics = field_chunk_skipindex
                                           ->GetMetric<SetFieldChunkMetric<T>>(
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
                break;
            }
            case OpType::NotEqual: {
                if (auto set_metrics = field_chunk_skipindex
                                           ->GetMetric<SetFieldChunkMetric<T>>(
                                               FieldChunkMetricType::SET);
                    set_metrics) {
                    return set_metrics->CanSkipNotEqual(val);
                }
                if (auto bloom_metrics =
                        field_chunk_skipindex
                            ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                                FieldChunkMetricType::BLOOM_FILTER);
                    bloom_metrics) {
                    return bloom_metrics->CanSkipNotEqual(val);
                }
                break;
            }
            case OpType::LessEqual:
            case OpType::LessThan:
            case OpType::GreaterEqual:
            case OpType::GreaterThan: {
                if (auto minmax_metrics =
                        field_chunk_skipindex
                            ->GetMetric<MinMaxFieldChunkMetric<T>>(
                                FieldChunkMetricType::MINMAX);
                    minmax_metrics) {
                    return minmax_metrics->CanSkipUnaryRange(op_type, val);
                }
                break;
            }
            default:
                break;
        }
        return false;
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value &&
                         !std::is_same_v<T, std::string> &&
                         !std::is_same_v<T, std::string_view>,
                     bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }
        switch (op_type) {
            case OpType::Equal: {
                if (auto set_metrics = field_chunk_skipindex
                                           ->GetMetric<SetFieldChunkMetric<T>>(
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
                break;
            }
            case OpType::NotEqual: {
                if (auto set_metrics = field_chunk_skipindex
                                           ->GetMetric<SetFieldChunkMetric<T>>(
                                               FieldChunkMetricType::SET);
                    set_metrics) {
                    return set_metrics->CanSkipNotEqual(val);
                }
                if (auto bloom_metrics =
                        field_chunk_skipindex
                            ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                                FieldChunkMetricType::BLOOM_FILTER);
                    bloom_metrics) {
                    return bloom_metrics->CanSkipNotEqual(val);
                }
                break;
            }
            case OpType::LessEqual:
            case OpType::LessThan:
            case OpType::GreaterEqual:
            case OpType::GreaterThan: {
                if (auto minmax_metrics =
                        field_chunk_skipindex
                            ->GetMetric<MinMaxFieldChunkMetric<T>>(
                                FieldChunkMetricType::MINMAX);
                    minmax_metrics) {
                    return minmax_metrics->CanSkipUnaryRange(op_type, val);
                }
                break;
            }
            default:
                break;
        }
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
            if (auto bloom_metric =
                    field_chunk_skipindex
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
    LoadSkipIndex(
        std::vector<std::unique_ptr<ChunkSkipIndex>> chunk_skipindex) {
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

    FieldChunkMetrics const*
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
