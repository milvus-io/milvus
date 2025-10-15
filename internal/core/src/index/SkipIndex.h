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
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/Manager.h"
#include "cachinglayer/Translator.h"
#include "cachinglayer/Utils.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "common/FieldDataInterface.h"
#include "ankerl/unordered_dense.h"
#include "arrow/record_batch.h"
#include "common/Chunk.h"
#include "common/Consts.h"
#include "common/Types.h"
#include "mmap/ChunkedColumnInterface.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/common/constants.h"
#include "Utils.h"

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

template <typename T>
using HighPrecisionType =
    std::conditional_t<std::is_integral_v<T> && !std::is_same_v<bool, T>,
                       int64_t,
                       T>;

// False positive rate for Bloom filter
static constexpr double FPR = 0.01;
static constexpr size_t NGRAM_SIZE = 3;
// static constexpr int64_t MIN_ROWS_TO_BUILD_METRICS = 10;
// static constexpr double CARDINALITY_RATIO_FOR_SET = 0.1;
// static constexpr double METADATA_BUDGET_RATIO = 0.01;

template <typename T>
struct MetricsInfo {
    int64_t total_rows_ = 0;
    int64_t null_count_ = 0;

    MetricsDataType<T> min_;
    MetricsDataType<T> max_;

    bool contains_true_ = false;
    bool contains_false_ = false;

    ankerl::unordered_dense::set<MetricsDataType<T>> unique_values_;
    ankerl::unordered_dense::set<std::string_view> ngram_values_;

    int64_t string_length_ = 0;
    int64_t total_string_length_ = 0;
};

enum class FieldChunkMetricType {
    MINMAX,
    SET,
    BLOOM_FILTER,
    NGRAM_FILTER,
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

    explicit MinMaxFieldChunkMetric(const std::string_view data) {
        if (data.empty()) {
            return;
        }

        const char* ptr = data.data();

        if constexpr (std::is_same_v<T, std::string>) {
            const char* end = ptr + data.size();

            if (ptr + sizeof(uint32_t) > end)
                return;
            uint32_t min_len;
            std::memcpy(&min_len, ptr, sizeof(min_len));
            ptr += sizeof(uint32_t);

            if (ptr + min_len > end)
                return;
            min_.assign(ptr, min_len);
            ptr += min_len;

            if (ptr + sizeof(uint32_t) > end)
                return;
            uint32_t max_len;
            std::memcpy(&max_len, ptr, sizeof(max_len));
            ptr += sizeof(uint32_t);

            if (ptr + max_len > end)
                return;
            max_.assign(ptr, max_len);
        } else {
            if (data.size() < sizeof(T) * 2)
                return;

            std::memcpy(&min_, ptr, sizeof(min_));
            ptr += sizeof(T);

            std::memcpy(&max_, ptr, sizeof(max_));
        }
        hasValue_ = true;
    }

    bool
    CanSkipUnaryRange(OpType op_type, const T& val) const {
        if (!hasValue_) {
            return false;
        }
        auto [lower_bound, upper_bound] = GetMinMax();
        if (lower_bound == MetricsDataType<T>() ||
            upper_bound == MetricsDataType<T>()) {
            return false;
        }
        return RangeShouldSkip(val, lower_bound, upper_bound, op_type);
    }

    bool
    CanSkipIn(const std::vector<T>& values) const {
        if (!hasValue_ || values.empty()) {
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
        if (!hasValue_) {
            return false;
        }
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
            uint32_t min_len = min_.length();
            ss.write(reinterpret_cast<const char*>(&min_len), sizeof(min_len));
            ss.write(min_.data(), min_len);

            uint32_t max_len = max_.length();
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

    explicit SetFieldChunkMetric(std::string_view data) {
        if (data.empty()) {
            return;
        }

        const char* ptr = data.data();
        const char* end = ptr + data.size();

        if (data.size() < sizeof(uint32_t))
            return;
        uint32_t count;
        std::memcpy(&count, ptr, sizeof(count));
        ptr += sizeof(uint32_t);

        unique_values_.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            if (ptr + sizeof(T) > end)
                return;
            T value;
            std::memcpy(&value, ptr, sizeof(T));
            ptr += sizeof(T);
            unique_values_.insert(value);
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
        if (!hasValue_ || values.empty()) {
            return false;
        }
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
        uint32_t count = unique_values_.size();
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

    explicit SetFieldChunkMetric(std::string_view data) {
        if (data.empty()) {
            return;
        }

        const char* ptr = data.data();
        const char* end = ptr + data.size();

        if (data.size() < sizeof(uint32_t))
            return;
        uint32_t count;
        std::memcpy(&count, ptr, sizeof(count));
        ptr += sizeof(uint32_t);

        unique_values_.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            if (ptr + sizeof(uint32_t) > end)
                return;

            uint32_t len;
            std::memcpy(&len, ptr, sizeof(len));
            ptr += sizeof(uint32_t);

            if (ptr + len > end)
                return;

            std::string value(ptr, len);
            ptr += len;
            unique_values_.insert(std::move(value));
        }
        hasValue_ = true;
    }

    static uint64_t
    EstimateCost(const MetricsInfo<std::string>& info) {
        return info.string_length_;
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
        if (!hasValue_ || values.empty()) {
            return false;
        }
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
        uint32_t count = unique_values_.size();
        ss.write(reinterpret_cast<const char*>(&count), sizeof(count));

        for (const auto& value : unique_values_) {
            uint32_t len = value.length();
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

    explicit SetFieldChunkMetric(std::string_view data) {
        if (data.empty()) {
            return;
        }
        if (data.size() < sizeof(bool) * 2)
            return;

        const char* ptr = data.data();
        std::memcpy(&contains_true_, ptr, sizeof(contains_true_));
        ptr += sizeof(bool);
        std::memcpy(&contains_false_, ptr, sizeof(contains_false_));

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
        if (!hasValue_ || values.size() != 2) {
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
class BloomFilterFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<index::BloomFilter> filter_;

 public:
    explicit BloomFilterFieldChunkMetric(const MetricsInfo<T>& info) {
        filter_ = index::BloomFilter::Build<MetricsDataType<T>>(
            info.unique_values_, FPR);
        hasValue_ = filter_ && filter_->IsValid();
    }

    explicit BloomFilterFieldChunkMetric(const std::string_view data) {
        if (data.empty()) {
            return;
        }
        filter_ = index::BloomFilter::Deserialize(data);
        hasValue_ = filter_ && filter_->IsValid();
    }

    static uint64_t
    EstimateCost(const MetricsInfo<T>& info) {
        return index::BloomFilter::EstimateCost(info.unique_values_.size());
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
        if (!hasValue_ || values.empty()) {
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

class NgramFilterFieldChunkMetric : public FieldChunkMetric {
 private:
    std::unique_ptr<index::BloomFilter> filter_;

 public:
    explicit NgramFilterFieldChunkMetric(const MetricsInfo<std::string>& info) {
        if (info.ngram_values_.empty()) {
            return;
        }
        filter_ = index::BloomFilter::Build<std::string_view>(
            info.ngram_values_, FPR);
        hasValue_ = filter_->IsValid();
    }

    explicit NgramFilterFieldChunkMetric(const std::string_view data) {
        if (data.empty()) {
            return;
        }
        filter_ = index::BloomFilter::Deserialize(data);
        hasValue_ = filter_ && filter_->IsValid();
    }

    static uint64_t
    EstimateCost(const MetricsInfo<std::string>& info) {
        return index::BloomFilter::EstimateCost(info.ngram_values_.size());
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

    FieldChunkMetricType
    GetType() const override {
        return FieldChunkMetricType::NGRAM_FILTER;
    }

    std::string
    Serialize() const override {
        return filter_ ? filter_->Serialize() : "";
    }
};

class FieldChunkMetrics {
 public:
    FieldChunkMetrics();

    explicit FieldChunkMetrics(arrow::Type::type data_type)
        : data_type_(data_type){};

    explicit FieldChunkMetrics(DataType type, const Chunk* chunk);

    explicit FieldChunkMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx,
        arrow::Type::type data_type);

    void
    Load(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
         int col_idx,
         const FieldMeta& field_meta);

    void
    Add(std::unique_ptr<FieldChunkMetric> metric) {
        metrics_.emplace_back(std::move(metric));
    }

    std::unique_ptr<FieldChunkMetric>
    LoadMetric(arrow::Type::type data_type,
               FieldChunkMetricType metric_type,
               const std::string_view data);

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

    bool
    HasMetric(FieldChunkMetricType type) const {
        for (const auto& metric : metrics_) {
            if (metric->GetType() == type) {
                return true;
            }
        }
        return false;
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
    Deserialize(const std::string_view data);

    cachinglayer::ResourceUsage
    CellByteSize() const {
        return {0, 0};
    }

 private:
    arrow::Type::type
    ToArrowType(DataType type);

    template <typename T>
    void
    LoadIntMetrics(const MetricsInfo<T>& info) {
        if (info.total_rows_ - info.null_count_ == 0) {
            return;
        }

        metrics_.emplace_back(
            std::make_unique<MinMaxFieldChunkMetric<T>>(info));
        auto bf_cost = BloomFilterFieldChunkMetric<T>::EstimateCost(info);
        auto set_cost = SetFieldChunkMetric<T>::EstimateCost(info);
        if (bf_cost > set_cost) {
            metrics_.emplace_back(
                std::make_unique<SetFieldChunkMetric<T>>(info));
        } else {
            metrics_.emplace_back(
                std::make_unique<BloomFilterFieldChunkMetric<T>>(info));
        }
    }

    template <typename T>
    void
    LoadFloatMetrics(const MetricsInfo<T>& info) {
        if (info.total_rows_ - info.null_count_ == 0) {
            return;
        }
        metrics_.emplace_back(
            std::make_unique<MinMaxFieldChunkMetric<T>>(info));
    }

    void
    LoadBooleanMetrics(const MetricsInfo<bool>& info) {
        if (info.total_rows_ - info.null_count_ == 0) {
            return;
        }
        metrics_.emplace_back(
            std::make_unique<SetFieldChunkMetric<bool>>(info));
    }

    void
    LoadStringMetrics(const MetricsInfo<std::string>& info) {
        if (info.total_rows_ - info.null_count_ == 0) {
            return;
        }

        metrics_.emplace_back(
            std::make_unique<MinMaxFieldChunkMetric<std::string>>(info));

        auto bf_cost =
            BloomFilterFieldChunkMetric<std::string>::EstimateCost(info);
        auto set_cost = SetFieldChunkMetric<std::string>::EstimateCost(info);
        if (set_cost < bf_cost) {
            metrics_.emplace_back(
                std::make_unique<SetFieldChunkMetric<std::string>>(info));
        } else {
            metrics_.emplace_back(
                std::make_unique<BloomFilterFieldChunkMetric<std::string>>(
                    info));
        }
        if (!info.ngram_values_.empty()) {
            metrics_.emplace_back(
                std::make_unique<NgramFilterFieldChunkMetric>(info));
        }
    }

    template <typename T>
    MetricsInfo<T>
    ProcessFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) {
        using ArrayType = typename arrow::TypeTraits<
            typename arrow::CTypeTraits<T>::ArrowType>::ArrayType;

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

    template <typename T>
    MetricsInfo<T>
    ProcessFieldMetrics(const T* data, const bool* valid_data, int64_t count) {
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

    MetricsInfo<std::string>
    ProcessStringFieldMetrics(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
        int col_idx) {
        bool has_first_valid = false;
        int64_t total_rows = 0;
        int64_t null_count = 0;
        int64_t total_string_length = 0;
        int64_t string_length = 0;
        std::string_view min;
        std::string_view max;
        ankerl::unordered_dense::set<std::string_view> unique_values;
        ankerl::unordered_dense::set<std::string_view> ngram_values;

        for (const auto& batch : batches) {
            auto array = batch->column(col_idx);
            auto string_array =
                std::static_pointer_cast<arrow::StringArray>(array);
            for (int64_t i = 0; i < string_array->length(); ++i) {
                if (string_array->IsNull(i)) {
                    null_count++;
                    continue;
                }
                std::string_view value = string_array->GetView(i);
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
                size_t length = value.length();
                total_string_length += length;
                if (unique_values.find(value) != unique_values.end()) {
                    continue;
                }
                if (length >= NGRAM_SIZE) {
                    for (size_t j = 0; j + NGRAM_SIZE <= length; ++j) {
                        std::string_view ngram = value.substr(j, NGRAM_SIZE);
                        ngram_values.insert(ngram);
                    }
                }
                unique_values.insert(value);
                string_length += length;
            }
            total_rows += string_array->length();
        }
        return {total_rows,
                null_count,
                min,
                max,
                false,
                false,
                std::move(unique_values),
                std::move(ngram_values),
                string_length,
                total_string_length};
    }

    MetricsInfo<std::string>
    ProcessStringFieldMetrics(const StringChunk* chunk) {
        // all captured by reference
        bool has_first_valid = false;
        int64_t total_rows = chunk->RowNums();
        int64_t null_count = 0;
        std::string_view min;
        std::string_view max;
        ankerl::unordered_dense::set<std::string_view> unique_values;
        ankerl::unordered_dense::set<std::string_view> ngram_values;
        int64_t total_string_length = 0;
        int64_t string_length = 0;

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
            size_t length = value.length();
            total_string_length += length;
            if (unique_values.find(value) != unique_values.end()) {
                continue;
            }
            if (length >= NGRAM_SIZE) {
                for (size_t j = 0; j + NGRAM_SIZE <= length; ++j) {
                    std::string_view ngram = value.substr(j, NGRAM_SIZE);
                    ngram_values.insert(ngram);
                }
            }
            unique_values.insert(value);
            string_length += length;
        }
        // The field data may later be released, so we need to copy the string to avoid invalid memory access.
        return {total_rows,
                null_count,
                min,
                max,
                false,
                false,
                std::move(unique_values),
                std::move(ngram_values),
                string_length,
                total_string_length};
    }

 private:
    std::vector<std::unique_ptr<FieldChunkMetric>> metrics_;
    arrow::Type::type data_type_;
};

class ChunkSkipIndex : public milvus_storage::Metadata {
 public:
    static constexpr const char* KEY = "SKIP_INDEX";

    ChunkSkipIndex() = default;

    ChunkSkipIndex(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
        if (batches.empty()) {
            return;
        }
        auto field_schema = batches[0]->schema();
        for (int col_idx = 0; col_idx < field_schema->num_fields(); ++col_idx) {
            auto field_id =
                std::stoll(field_schema->field(col_idx)
                               ->metadata()
                               ->Get(milvus_storage::ARROW_FIELD_ID_KEY)
                               ->data());
            auto fid = FieldId(field_id);
            auto data_type = field_schema->field(col_idx)->type()->id();
            if (fid == RowFieldID || !index::SupportsSkipIndex(data_type)) {
                continue;
            }
            auto field_metrics = std::make_unique<FieldChunkMetrics>(
                batches, col_idx, data_type);
            if (field_metrics->Size() > 0) {
                field_chunk_metrics_.emplace_back(fid,
                                                  std::move(field_metrics));
            }
        }
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

            std::string data = metrics->Serialize();
            uint64_t len = data.length();
            ss.write(reinterpret_cast<const char*>(&len), sizeof(len));
            ss.write(data.data(), len);
        }
        return ss.str();
    };

    void
    Deserialize(std::string_view data) override {
        if (data.empty()) {
            return;
        }

        const char* ptr = data.data();
        const char* end = ptr + data.size();
        if (data.size() < sizeof(uint32_t)) {
            return;
        }
        uint32_t count;
        std::memcpy(&count, ptr, sizeof(count));
        ptr += sizeof(uint32_t);

        field_chunk_metrics_.reserve(count);

        for (uint32_t i = 0; i < count; ++i) {
            if (ptr + sizeof(int64_t) > end) {
                return;
            }
            int64_t field_id_val;
            std::memcpy(&field_id_val, ptr, sizeof(field_id_val));
            ptr += sizeof(int64_t);

            if (ptr + sizeof(uint64_t) > end) {
                return;
            }
            uint64_t len;
            std::memcpy(&len, ptr, sizeof(len));
            ptr += sizeof(uint64_t);

            if (ptr + len > end) {
                return;
            }

            std::string_view field_data(ptr, len);
            ptr += len;
            auto field_chunk_skipindex = std::make_unique<FieldChunkMetrics>();
            field_chunk_skipindex->Deserialize(field_data);

            field_chunk_metrics_.emplace_back(FieldId(field_id_val),
                                              std::move(field_chunk_skipindex));
        }
    }

 private:
    std::vector<std::pair<FieldId, std::unique_ptr<FieldChunkMetrics>>>
        field_chunk_metrics_;
};

class ChunkSkipIndexBuilder : public milvus_storage::MetadataBuilder {
 public:
    explicit ChunkSkipIndexBuilder() = default;

    std::vector<std::unique_ptr<milvus_storage::Metadata>>
    Take() {
        return std::move(metadata_collection_);
    }

 private:
    std::unique_ptr<milvus_storage::Metadata>
    Create(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
        override {
        return std::make_unique<ChunkSkipIndex>(batches);
    }
};

class FieldChunkMetricsTranslator
    : public cachinglayer::Translator<FieldChunkMetrics> {
 public:
    FieldChunkMetricsTranslator(int64_t segment_id,
                                FieldId field_id,
                                milvus::DataType data_type,
                                std::shared_ptr<ChunkedColumnInterface> column)
        : key_(fmt::format("skip_seg_{}_f_{}", segment_id, field_id.get())),
          data_type_(data_type),
          column_(column),
          meta_(cachinglayer::StorageType::MEMORY,
                milvus::cachinglayer::CellIdMappingMode::IDENTICAL,
                milvus::cachinglayer::CellDataType::OTHER,
                CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                false) {
    }

    size_t
    num_cells() const override {
        return column_->num_chunks();
    }
    milvus::cachinglayer::cid_t
    cell_id_of(milvus::cachinglayer::uid_t uid) const override {
        return uid;
    }
    std::pair<milvus::cachinglayer::ResourceUsage,
              milvus::cachinglayer::ResourceUsage>
    estimated_byte_size_of_cell(
        milvus::cachinglayer::cid_t cid) const override {
        // TODO(tiered storage 1): provide a better estimation.
        return {{0, 0}, {0, 0}};
    }
    const std::string&
    key() const override {
        return key_;
    }
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<FieldChunkMetrics>>>
    get_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override;

    milvus::cachinglayer::Meta*
    meta() override {
        return &meta_;
    }

    int64_t
    cells_storage_bytes(
        const std::vector<milvus::cachinglayer::cid_t>& cids) const override {
        return 0;
    }

 private:
    std::string key_;
    milvus::DataType data_type_;
    cachinglayer::Meta meta_;
    std::shared_ptr<ChunkedColumnInterface> column_;
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
        static constexpr bool value = isAllowedType;

        static constexpr bool value_for_arith =
            (std::is_integral<T>::value && !std::is_same_v<T, bool>) ||
            std::is_floating_point<T>::value;

        static constexpr bool value_for_in = isAllowedType;
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
                        field_chunk_skipindex
                            ->GetMetric<NgramFilterFieldChunkMetric>(
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
    std::enable_if_t<std::is_integral_v<T>, bool>
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
                if (auto minmax_metrics =
                        field_chunk_skipindex
                            ->GetMetric<MinMaxFieldChunkMetric<T>>(
                                FieldChunkMetricType::MINMAX);
                    minmax_metrics &&
                    minmax_metrics->CanSkipUnaryRange(op_type, val)) {
                    return true;
                }
                if (auto set_metrics = field_chunk_skipindex
                                           ->GetMetric<SetFieldChunkMetric<T>>(
                                               FieldChunkMetricType::SET);
                    set_metrics && set_metrics->CanSkipEqual(val)) {
                    return true;
                }
                if (auto bloom_metrics =
                        field_chunk_skipindex
                            ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                                FieldChunkMetricType::BLOOM_FILTER);
                    bloom_metrics && bloom_metrics->CanSkipEqual(val)) {
                    return true;
                }
                break;
            }
            case OpType::NotEqual: {
                if (auto minmax_metrics =
                        field_chunk_skipindex
                            ->GetMetric<MinMaxFieldChunkMetric<T>>(
                                FieldChunkMetricType::MINMAX);
                    minmax_metrics &&
                    minmax_metrics->CanSkipUnaryRange(op_type, val)) {
                    return true;
                }
                if (auto set_metrics = field_chunk_skipindex
                                           ->GetMetric<SetFieldChunkMetric<T>>(
                                               FieldChunkMetricType::SET);
                    set_metrics && set_metrics->CanSkipNotEqual(val)) {
                    return true;
                }
                if (auto bloom_metrics =
                        field_chunk_skipindex
                            ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                                FieldChunkMetricType::BLOOM_FILTER);
                    bloom_metrics && bloom_metrics->CanSkipNotEqual(val)) {
                    return true;
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
                    minmax_metrics &&
                    minmax_metrics->CanSkipUnaryRange(op_type, val)) {
                    return true;
                }
                break;
            }
            default:
                break;
        }
        return false;
    }

    template <typename T>
    std::enable_if_t<std::is_floating_point_v<T>, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }
        if (auto minmax_metrics =
                field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(
                    FieldChunkMetricType::MINMAX);
            minmax_metrics && minmax_metrics->CanSkipUnaryRange(op_type, val)) {
            return true;
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
            minmax_metric &&
            minmax_metric->CanSkipBinaryRange(
                lower_val, upper_val, lower_inclusive, upper_inclusive)) {
            return true;
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
                            const HighPrecisionType<T> value,
                            const HighPrecisionType<T> right_operand) const {
        auto check_and_skip = [&](HighPrecisionType<T> new_value_hp,
                                  OpType new_op_type) {
            if constexpr (std::is_integral_v<T>) {
                if (new_value_hp > std::numeric_limits<T>::max() ||
                    new_value_hp < std::numeric_limits<T>::min()) {
                    // Overflow detected. The transformed value cannot be represented by T.
                    // We cannot make a safe comparison with the chunk's min/max.
                    return false;
                }
            }
            return CanSkipUnaryRange<T>(
                field_id, chunk_id, new_op_type, static_cast<T>(new_value_hp));
        };
        switch (arith_type) {
            case ArithOpType::Add: {
                // field + C > V  =>  field > V - C
                return check_and_skip(value - right_operand, op_type);
            }
            case ArithOpType::Sub: {
                // field - C > V  =>  field > V + C
                return check_and_skip(value + right_operand, op_type);
            }
            case ArithOpType::Mul: {
                // field * C > V
                if (right_operand == 0) {
                    // field * 0 > V => 0 > V. This doesn't depend on the field's range.
                    return false;
                }

                OpType new_op_type = op_type;
                if (right_operand < 0) {
                    new_op_type = FlipComparisonOperator(op_type);
                }
                return check_and_skip(value / right_operand, new_op_type);
            }
            case ArithOpType::Div: {
                // field / C > V
                if (right_operand == 0) {
                    // Division by zero. Cannot evaluate, so cannot skip.
                    return false;
                }

                OpType new_op_type = op_type;
                if (right_operand < 0) {
                    new_op_type = FlipComparisonOperator(op_type);
                }
                return check_and_skip(value * right_operand, new_op_type);
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
                   const std::vector<T>& values) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }

        if (auto set_metric =
                field_chunk_skipindex->GetMetric<SetFieldChunkMetric<T>>(
                    FieldChunkMetricType::SET);
            set_metric && set_metric->CanSkipIn(values)) {
            return true;
        }

        if constexpr (!std::is_same_v<T, bool>) {
            if (auto minmax_metric =
                    field_chunk_skipindex->GetMetric<MinMaxFieldChunkMetric<T>>(
                        FieldChunkMetricType::MINMAX);
                minmax_metric && minmax_metric->CanSkipIn(values)) {
                return true;
            }
            if (auto bloom_metric =
                    field_chunk_skipindex
                        ->GetMetric<BloomFilterFieldChunkMetric<T>>(
                            FieldChunkMetricType::BLOOM_FILTER);
                bloom_metric && bloom_metric->CanSkipIn(values)) {
                return true;
            }
        }
        return false;
    }

    template <typename T>
    std::enable_if_t<!IsAllowedType<T>::value_for_in, bool>
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
            v2_fieldChunkMetrics_[field_id].reserve(num_chunks);
        }

        for (size_t i = 0; i < num_chunks; ++i) {
            auto field_chunk_metrics = chunk_skipindex[i]->Take();
            for (auto& [field_id, metrics] : field_chunk_metrics) {
                v2_fieldChunkMetrics_[field_id].emplace_back(
                    std::move(metrics));
            }
        }
    }

    void
    LoadSkipIndex(int64_t segment_id,
                  milvus::FieldId field_id,
                  milvus::DataType data_type,
                  std::shared_ptr<ChunkedColumnInterface> column) {
        auto translator = std::make_unique<FieldChunkMetricsTranslator>(
            segment_id, field_id, data_type, column);
        auto cache_slot =
            cachinglayer::Manager::GetInstance()
                .CreateCacheSlot<FieldChunkMetrics>(std::move(translator));

        std::unique_lock lck(mutex_);
        v1_fieldChunkMetrics_[field_id] = std::move(cache_slot);
    }

    bool
    HasMetric(FieldId field_id,
              int64_t chunk_id,
              FieldChunkMetricType type) const {
        auto field_chunk_skipindex = GetFieldChunkMetrics(field_id, chunk_id);
        if (!field_chunk_skipindex) {
            return false;
        }
        return field_chunk_skipindex->HasMetric(type);
    }

    FieldChunkMetrics const*
    GetFieldChunkMetrics(milvus::FieldId field_id, int64_t chunk_id) const {
        if (auto metrics = GetFieldChunkMetricsV2(field_id, chunk_id)) {
            return metrics;
        }
        return GetFieldChunkMetricsV1(field_id, chunk_id).get();
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
    GetFieldChunkMetricsV2(milvus::FieldId field_id, int64_t chunk_id) const {
        auto it = v2_fieldChunkMetrics_.find(field_id);
        if (it == v2_fieldChunkMetrics_.end()) {
            return nullptr;
        }
        if (chunk_id < 0 || chunk_id >= it->second.size()) {
            return nullptr;
        }
        return it->second[chunk_id].get();
    }

    const cachinglayer::PinWrapper<const FieldChunkMetrics*>
    GetFieldChunkMetricsV1(FieldId field_id, int chunk_id) const;

    std::unordered_map<FieldId, std::vector<std::unique_ptr<FieldChunkMetrics>>>
        v2_fieldChunkMetrics_;
    std::unordered_map<
        FieldId,
        std::shared_ptr<cachinglayer::CacheSlot<FieldChunkMetrics>>>
        v1_fieldChunkMetrics_;
    mutable std::shared_mutex mutex_;
};
}  // namespace milvus
