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
#include <cmath>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include "common/EasyAssert.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/Index.h"
#include "index/InvertedIndexTantivy.h"
#include "index/InvertedIndexUtil.h"
#include "index/ScalarIndex.h"
#include "log/Log.h"
namespace milvus::index {

class JsonFlatIndex;
// JsonFlatIndexQueryExecutor is used to execute queries on a specified json path, and can be constructed by JsonFlatIndex
template <typename T>
class JsonFlatIndexQueryExecutor : public InvertedIndexTantivy<T> {
 public:
    JsonFlatIndexQueryExecutor(std::string& json_path,
                               const JsonFlatIndex& json_flat_index,
                               bool use_comparable_value_mask);

    ~JsonFlatIndexQueryExecutor() {
        this->wrapper_ = nullptr;
    }

    const TargetBitmap
    In(size_t n, const T* values) override {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::In",
                              tracer::GetRootSpan());
        auto bitset = TermBitset(n, values);
        return bitset;
    }

    TargetBitmap
    Exists() {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::Exists",
                              tracer::GetRootSpan());
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_exist_query(json_path_, &bitset);
        return bitset;
    }

    const TargetBitmap
    InApplyFilter(
        size_t n,
        const T* values,
        const std::function<bool(size_t /* offset */)>& filter) override {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::InApplyFilter",
                              tracer::GetRootSpan());
        auto bitset = TermBitset(n, values);
        apply_hits_with_filter(bitset, filter);
        return bitset;
    }

    virtual void
    InApplyCallback(
        size_t n,
        const T* values,
        const std::function<void(size_t /* offset */)>& callback) override {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::InApplyCallback",
                              tracer::GetRootSpan());
        auto bitset = TermBitset(n, values);
        apply_hits_with_callback(bitset, callback);
    }

    const TargetBitmap
    NotIn(size_t n, const T* values) override {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::NotIn",
                              tracer::GetRootSpan());
        auto bitset = TermBitset(n, values);

        bitset.flip();

        // TODO: optimize this
        auto null_bitset = this->IsNotNull();
        bitset &= null_bitset;

        return bitset;
    }

    const TargetBitmap
    IsNull() override {
        auto bitset = IsNotNull();
        bitset.flip();
        return bitset;
    }

    TargetBitmap
    IsNotNull() override {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::IsNotNull",
                              tracer::GetRootSpan());
        if (!use_comparable_value_mask_) {
            return InvertedIndexTantivy<T>::IsNotNull();
        }
        return ComparableValueBitset();
    }

    const TargetBitmap
    Range(const T& value, OpType op) override {
        LOG_INFO("[executor] JsonFlatIndexQueryExecutor Range");
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::Range",
                              tracer::GetRootSpan());
        TargetBitmap bitset(this->Count());
        switch (op) {
            case OpType::LessThan: {
                this->wrapper_->json_range_query(
                    json_path_, T(), value, true, false, false, false, &bitset);
            } break;
            case OpType::LessEqual: {
                this->wrapper_->json_range_query(
                    json_path_, T(), value, true, false, true, false, &bitset);
            } break;
            case OpType::GreaterThan: {
                this->wrapper_->json_range_query(
                    json_path_, value, T(), false, true, false, false, &bitset);
            } break;
            case OpType::GreaterEqual: {
                this->wrapper_->json_range_query(
                    json_path_, value, T(), false, true, true, false, &bitset);
            } break;
            default:
                ThrowInfo(OpTypeInvalid,
                          fmt::format("Invalid OperatorType: {}", op));
        }
        OrU64Range(bitset, U64RangeForValue(value, op));
        return bitset;
    }

    const TargetBitmap
    Query(const DatasetPtr& dataset) override {
        return InvertedIndexTantivy<T>::Query(dataset);
    }

    const TargetBitmap
    Range(const T& lower_bound_value,
          bool lb_inclusive,
          const T& upper_bound_value,
          bool ub_inclusive) override {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::RangeWithBounds",
                              tracer::GetRootSpan());
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_range_query(json_path_,
                                         lower_bound_value,
                                         upper_bound_value,
                                         false,
                                         false,
                                         lb_inclusive,
                                         ub_inclusive,
                                         &bitset);
        OrU64Range(bitset,
                   U64RangeForBounds(lower_bound_value,
                                     lb_inclusive,
                                     upper_bound_value,
                                     ub_inclusive));
        return bitset;
    }

    const TargetBitmap
    PrefixMatch(const std::string_view prefix) override {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::PrefixMatch",
                              tracer::GetRootSpan());
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_prefix_query(
            json_path_, std::string(prefix), &bitset);
        return bitset;
    }

    const TargetBitmap
    RegexQuery(const std::string& pattern) override {
        tracer::AutoSpan span("JsonFlatIndexQueryExecutor::RegexQuery",
                              tracer::GetRootSpan());
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_regex_query(json_path_, pattern, &bitset);
        return bitset;
    }

 private:
    TargetBitmap
    NumericComparableBitset() {
        TargetBitmap bitset(this->Count());

        TargetBitmap int_bitset(this->Count());
        this->wrapper_->json_range_query(json_path_,
                                         std::numeric_limits<int64_t>::lowest(),
                                         std::numeric_limits<int64_t>::max(),
                                         false,
                                         false,
                                         true,
                                         true,
                                         &int_bitset);
        bitset |= int_bitset;

        TargetBitmap double_bitset(this->Count());
        this->wrapper_->json_range_query(json_path_,
                                         std::numeric_limits<double>::lowest(),
                                         std::numeric_limits<double>::max(),
                                         false,
                                         false,
                                         true,
                                         true,
                                         &double_bitset);
        bitset |= double_bitset;

        TargetBitmap u64_bitset(this->Count());
        this->wrapper_->json_range_query(json_path_,
                                         std::numeric_limits<uint64_t>::min(),
                                         std::numeric_limits<uint64_t>::max(),
                                         false,
                                         false,
                                         true,
                                         true,
                                         &u64_bitset);
        bitset |= u64_bitset;

        return bitset;
    }

    using U64Range = std::optional<std::pair<uint64_t, uint64_t>>;

    TargetBitmap
    TermBitset(size_t n, const T* values) {
        TargetBitmap bitset(this->Count());
        this->wrapper_->json_terms_query(json_path_, values, n, &bitset);
        OrU64TermRanges(bitset, n, values);
        return bitset;
    }

    void
    OrU64TermRanges(TargetBitmap& bitset, size_t n, const T* values) {
        if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
            for (size_t i = 0; i < n; ++i) {
                auto value = static_cast<double>(values[i]);
                auto range = DoubleRangeForBounds(value, true, value, true);
                if (RangeCoveredByExactU64Term(values[i], range)) {
                    continue;
                }
                OrU64Range(bitset, range);
            }
        }
    }

    void
    OrU64Range(TargetBitmap& bitset, U64Range range) {
        if (!range.has_value()) {
            return;
        }
        this->wrapper_->json_range_query(json_path_,
                                         range->first,
                                         range->second,
                                         false,
                                         false,
                                         true,
                                         true,
                                         &bitset);
    }

    template <typename Predicate>
    static std::optional<uint64_t>
    FirstU64Where(Predicate pred) {
        constexpr auto min = std::numeric_limits<uint64_t>::min();
        constexpr auto max = std::numeric_limits<uint64_t>::max();
        if (pred(min)) {
            return min;
        }
        if (!pred(max)) {
            return std::nullopt;
        }

        uint64_t low = min;
        uint64_t high = max;
        while (low < high) {
            auto mid = low + (high - low) / 2;
            if (pred(mid)) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    template <typename Predicate>
    static std::optional<uint64_t>
    LastU64Where(Predicate pred) {
        constexpr auto min = std::numeric_limits<uint64_t>::min();
        constexpr auto max = std::numeric_limits<uint64_t>::max();
        if (!pred(min)) {
            return std::nullopt;
        }
        if (pred(max)) {
            return max;
        }

        uint64_t low = min;
        uint64_t high = max;
        while (low < high) {
            auto mid = high - (high - low) / 2;
            if (pred(mid)) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        return low;
    }

    static U64Range
    DoubleRangeForValue(double value, OpType op) {
        if (std::isnan(value)) {
            return std::nullopt;
        }

        switch (op) {
            case OpType::LessThan: {
                auto upper = LastU64Where([value](uint64_t u) {
                    return static_cast<double>(u) < value;
                });
                if (!upper.has_value()) {
                    return std::nullopt;
                }
                return std::make_pair(std::numeric_limits<uint64_t>::min(),
                                      *upper);
            }
            case OpType::LessEqual: {
                auto upper = LastU64Where([value](uint64_t u) {
                    return static_cast<double>(u) <= value;
                });
                if (!upper.has_value()) {
                    return std::nullopt;
                }
                return std::make_pair(std::numeric_limits<uint64_t>::min(),
                                      *upper);
            }
            case OpType::GreaterThan: {
                auto lower = FirstU64Where([value](uint64_t u) {
                    return static_cast<double>(u) > value;
                });
                if (!lower.has_value()) {
                    return std::nullopt;
                }
                return std::make_pair(*lower,
                                      std::numeric_limits<uint64_t>::max());
            }
            case OpType::GreaterEqual: {
                auto lower = FirstU64Where([value](uint64_t u) {
                    return static_cast<double>(u) >= value;
                });
                if (!lower.has_value()) {
                    return std::nullopt;
                }
                return std::make_pair(*lower,
                                      std::numeric_limits<uint64_t>::max());
            }
            default:
                ThrowInfo(OpTypeInvalid,
                          fmt::format("Invalid OperatorType: {}", op));
        }
    }

    static U64Range
    DoubleRangeForBounds(double lower_bound_value,
                         bool lb_inclusive,
                         double upper_bound_value,
                         bool ub_inclusive) {
        if (std::isnan(lower_bound_value) || std::isnan(upper_bound_value)) {
            return std::nullopt;
        }

        auto lower =
            FirstU64Where([lower_bound_value, lb_inclusive](uint64_t u) {
                auto value = static_cast<double>(u);
                return lb_inclusive ? value >= lower_bound_value
                                    : value > lower_bound_value;
            });
        if (!lower.has_value()) {
            return std::nullopt;
        }

        auto upper =
            LastU64Where([upper_bound_value, ub_inclusive](uint64_t u) {
                auto value = static_cast<double>(u);
                return ub_inclusive ? value <= upper_bound_value
                                    : value < upper_bound_value;
            });
        if (!upper.has_value() || *lower > *upper) {
            return std::nullopt;
        }

        return std::make_pair(*lower, *upper);
    }

    static bool
    CanCastDoubleToU64(double value) {
        return std::isfinite(value) && std::floor(value) == value &&
               value >= 0 &&
               static_cast<long double>(value) <=
                   static_cast<long double>(
                       std::numeric_limits<uint64_t>::max());
    }

    static bool
    RangeCoveredByExactU64Term(T value, const U64Range& range) {
        if (!range.has_value() || range->first != range->second) {
            return false;
        }

        if constexpr (std::is_integral_v<T>) {
            if constexpr (std::is_signed_v<T>) {
                return value >= 0 &&
                       static_cast<uint64_t>(value) == range->first;
            } else {
                return static_cast<uint64_t>(value) == range->first;
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            return CanCastDoubleToU64(value) &&
                   static_cast<uint64_t>(value) == range->first;
        } else {
            return false;
        }
    }

    static U64Range
    U64RangeForValue(T value, OpType op) {
        if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
            return DoubleRangeForValue(static_cast<double>(value), op);
        } else {
            return std::nullopt;
        }
    }

    static U64Range
    U64RangeForBounds(T lower_bound_value,
                      bool lb_inclusive,
                      T upper_bound_value,
                      bool ub_inclusive) {
        if constexpr (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) {
            return DoubleRangeForBounds(static_cast<double>(lower_bound_value),
                                        lb_inclusive,
                                        static_cast<double>(upper_bound_value),
                                        ub_inclusive);
        } else {
            return std::nullopt;
        }
    }

    TargetBitmap
    ComparableValueBitset() {
        TargetBitmap bitset(this->Count());
        if constexpr (std::is_same_v<T, bool>) {
            bool values[] = {false, true};
            this->wrapper_->json_terms_query(json_path_, values, 2, &bitset);
        } else if constexpr (std::is_integral_v<T>) {
            bitset = NumericComparableBitset();
        } else if constexpr (std::is_floating_point_v<T>) {
            bitset = NumericComparableBitset();
        } else if constexpr (std::is_same_v<T, std::string>) {
            this->wrapper_->json_prefix_query(json_path_, "", &bitset);
        } else {
            this->wrapper_->json_exist_query(json_path_, &bitset);
        }
        return bitset;
    }

    std::string json_path_;
    bool use_comparable_value_mask_{true};
};

// JsonFlatIndex is not bound to any specific type,
// we need to reuse InvertedIndexTantivy's Build and Load implementation, so we specify the template parameter as std::string
// JsonFlatIndex should not be used to execute queries, use JsonFlatIndexQueryExecutor instead
class JsonFlatIndex : public InvertedIndexTantivy<std::string> {
    template <typename T>
    friend class JsonFlatIndexQueryExecutor;

 public:
    JsonFlatIndex() : InvertedIndexTantivy<std::string>() {
    }

    explicit JsonFlatIndex(
        const storage::FileManagerContext& ctx,
        const std::string& nested_path,
        const int64_t tantivy_index_version = TANTIVY_INDEX_LATEST_VERSION)
        : InvertedIndexTantivy<std::string>(
              tantivy_index_version, ctx, false, false),
          nested_path_(nested_path) {
    }

    void
    build_index_for_json(const std::vector<std::shared_ptr<FieldDataBase>>&
                             field_datas) override;

    template <typename T>
    std::shared_ptr<JsonFlatIndexQueryExecutor<T>>
    create_executor(std::string json_path,
                    bool use_comparable_value_mask = true) const {
        // json path should be in the format of /a/b/c, we need to convert it to tantivy path like a.b.c
        std::replace(json_path.begin(), json_path.end(), '/', '.');
        if (!json_path.empty()) {
            json_path = json_path.substr(1);
        }

        return std::make_shared<JsonFlatIndexQueryExecutor<T>>(
            json_path, *this, use_comparable_value_mask);
    }

    JsonCastType
    GetCastType() const override {
        return JsonCastType::FromString("JSON");
    }

    std::string
    GetNestedPath() const {
        return nested_path_;
    }

    void
    finish() {
        this->wrapper_->finish();
    }

    void
    create_reader(SetBitsetFn set_bitset) {
        this->wrapper_->create_reader(set_bitset);
    }

 private:
    std::string nested_path_;
};

template <typename T>
JsonFlatIndexQueryExecutor<T>::JsonFlatIndexQueryExecutor(
    std::string& json_path,
    const JsonFlatIndex& json_flat_index,
    bool use_comparable_value_mask) {
    json_path_ = json_path;
    use_comparable_value_mask_ = use_comparable_value_mask;
    this->wrapper_ = json_flat_index.wrapper_;
    this->null_offset_ = json_flat_index.null_offset_;
}
}  // namespace milvus::index
