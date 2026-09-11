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

#include <algorithm>
#include <cmath>
#include <set>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <fmt/core.h>
#include <simdjson.h>

#include "ankerl/unordered_dense.h"

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "exec/expression/JsonNumberComparison.h"
#include "segcore/SegmentInterface.h"
#include "common/bson_view.h"
#include "exec/expression/Utils.h"
#include "index/json_stats/bson_inverted.h"
#include "cachinglayer/CacheSlot.h"

namespace milvus {
namespace exec {

class ShreddingArrayBsonContainsArrayExecutor {
 public:
    explicit ShreddingArrayBsonContainsArrayExecutor(
        const std::vector<proto::plan::Array>& elems)
        : elements_(elems) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& sub_value : array_view.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    milvus::bson::array_view>(sub_value.get_value());
                if (!sub_array.has_value())
                    continue;
                for (const auto& element : elements_) {
                    if (CompareTwoJsonArray(sub_array.value(), element)) {
                        matched = true;
                        break;
                    }
                }
                if (matched)
                    break;
            }
            res[i] = matched;
        }
    }

 private:
    const std::vector<proto::plan::Array> elements_;
};

class ShreddingArrayBsonContainsAllArrayExecutor {
 public:
    explicit ShreddingArrayBsonContainsAllArrayExecutor(
        const std::vector<proto::plan::Array>& elems)
        : elements_(elems) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            std::set<int> exist_elements_index;
            for (const auto& sub_value : array_view.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    milvus::bson::array_view>(sub_value.get_value());
                if (!sub_array.has_value())
                    continue;

                for (int idx = 0; idx < static_cast<int>(elements_.size());
                     ++idx) {
                    if (CompareTwoJsonArray(sub_array.value(),
                                            elements_[idx])) {
                        exist_elements_index.insert(idx);
                    }
                }
                if (exist_elements_index.size() == elements_.size()) {
                    break;
                }
            }
            res[i] = exist_elements_index.size() == elements_.size();
        }
    }

 private:
    const std::vector<proto::plan::Array> elements_;
};

template <typename GetType>
class ShreddingArrayBsonContainsAnyExecutor {
 public:
    explicit ShreddingArrayBsonContainsAnyExecutor(
        std::shared_ptr<MultiElement> arg_set)
        : arg_set_(std::move(arg_set)) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& element : array_view.value()) {
                if constexpr (std::is_same_v<GetType, int64_t> ||
                              std::is_same_v<GetType, double>) {
                    auto value =
                        GetBsonNumberExact<GetType>(element.get_value());
                    if (value.has_value() && arg_set_->In(*value)) {
                        matched = true;
                        break;
                    }
                } else {
                    auto value =
                        milvus::BsonView::GetValueFromBsonView<GetType>(
                            element.get_value());
                    if (value.has_value() && arg_set_->In(value.value())) {
                        matched = true;
                        break;
                    }
                }
            }
            res[i] = matched;
        }
    }

 private:
    std::shared_ptr<MultiElement> arg_set_;
};

template <typename GetType>
class ShreddingArrayBsonContainsAllExecutor {
 public:
    explicit ShreddingArrayBsonContainsAllExecutor(
        const std::set<GetType>& elements)
        : elements_(elements) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            std::set<GetType> tmp_elements(elements_);
            for (const auto& element : array_view.value()) {
                auto value = [&]() -> std::optional<GetType> {
                    if constexpr (std::is_same_v<GetType, int64_t> ||
                                  std::is_same_v<GetType, double>) {
                        return GetBsonNumberExact<GetType>(element.get_value());
                    } else {
                        return milvus::BsonView::GetValueFromBsonView<GetType>(
                            element.get_value());
                    }
                }();
                if (!value.has_value()) {
                    continue;
                }
                tmp_elements.erase(value.value());
                if (tmp_elements.empty()) {
                    break;
                }
            }
            res[i] = tmp_elements.empty();
        }
    }

 private:
    std::set<GetType> elements_;
};

class ShreddingArrayBsonContainsAllWithDiffTypeExecutor {
 public:
    ShreddingArrayBsonContainsAllWithDiffTypeExecutor(
        std::vector<proto::plan::GenericValue> elements,
        std::set<int> elements_index)
        : elements_(std::move(elements)),
          elements_index_(std::move(elements_index)) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array = bson.ParseAsArrayAtOffset(0);
            if (!array.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            std::set<int> tmp_elements_index(elements_index_);
            for (const auto& sub_value : array.value()) {
                int idx = -1;
                for (auto& element : elements_) {
                    idx++;
                    switch (element.val_case()) {
                        case proto::plan::GenericValue::kBoolVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<bool>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.bool_val()) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            auto comparison = CompareBsonNumberToBound(
                                sub_value.get_value(), element);
                            if (comparison.has_value() && *comparison == 0) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto comparison = CompareBsonNumberToBound(
                                sub_value.get_value(), element);
                            if (comparison.has_value() && *comparison == 0) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                std::string>(sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.string_val()) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                milvus::bson::array_view>(
                                sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (CompareTwoJsonArray(val.value(),
                                                    element.array_val())) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        default:
                            ThrowInfo(UnexpectedError,
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
                    }
                    if (tmp_elements_index.size() == 0) {
                        break;
                    }
                }
                if (tmp_elements_index.size() == 0) {
                    break;
                }
            }
            res[i] = tmp_elements_index.size() == 0;
        }
    }

 private:
    std::vector<proto::plan::GenericValue> elements_;
    std::set<int> elements_index_;
};

class ShreddingArrayBsonContainsAnyWithDiffTypeExecutor {
 public:
    explicit ShreddingArrayBsonContainsAnyWithDiffTypeExecutor(
        std::vector<proto::plan::GenericValue> elements)
        : elements_(std::move(elements)) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array = bson.ParseAsArrayAtOffset(0);
            if (!array.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& sub_value : array.value()) {
                for (auto const& element : elements_) {
                    switch (element.val_case()) {
                        case proto::plan::GenericValue::kBoolVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<bool>(
                                    sub_value.get_value());
                            if (val.has_value() &&
                                val.value() == element.bool_val()) {
                                matched = true;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            auto comparison = CompareBsonNumberToBound(
                                sub_value.get_value(), element);
                            if (comparison.has_value() && *comparison == 0) {
                                matched = true;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto comparison = CompareBsonNumberToBound(
                                sub_value.get_value(), element);
                            if (comparison.has_value() && *comparison == 0) {
                                matched = true;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                std::string>(sub_value.get_value());
                            if (val.has_value() &&
                                val.value() == element.string_val()) {
                                matched = true;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                milvus::bson::array_view>(
                                sub_value.get_value());
                            if (val.has_value() &&
                                CompareTwoJsonArray(val.value(),
                                                    element.array_val())) {
                                matched = true;
                            }
                            break;
                        }
                        default:
                            ThrowInfo(UnexpectedError,
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
                    }
                    if (matched)
                        break;
                }
                if (matched)
                    break;
            }
            res[i] = matched;
        }
    }

 private:
    std::vector<proto::plan::GenericValue> elements_;
};

// Row i must not be evaluated: the column value is NULL (folded to UNKNOWN by
// KernelAdapter) or bitmap_input pre-filtered it (reset to (0,1)).
template <typename T>
inline bool
IsSkippedCandidate(const CandidateBatch<T>& b, size_t i) {
    return (b.validity && !b.validity[i]) || !b.IsCandidate(i);
}

// Replaces per-row std::set copy with a value->bit-index map built once.
// For <= 64 targets uses uint64_t bitmask (zero heap alloc per row).
// For > 64 targets uses vector<uint64_t> dynamic bitset.
template <typename T>
class ContainsAllMatcher {
 public:
    explicit ContainsAllMatcher(const std::set<T>& targets) {
        target_count_ = targets.size();
        use_small_ = (target_count_ <= 64);
        uint32_t idx = 0;
        for (const auto& t : targets) {
            value_to_bit_[t] = idx++;
        }
        if (use_small_) {
            full_mask_ = (target_count_ == 64)
                             ? ~uint64_t(0)
                             : (uint64_t(1) << target_count_) - 1;
        } else {
            num_words_ = (target_count_ + 63) / 64;
        }
    }

    // Small path: look up a value and set its bit. Returns true when all
    // targets have been found.
    bool
    set_if_found(const T& val, uint64_t& found) const {
        auto it = value_to_bit_.find(val);
        if (it != value_to_bit_.end()) {
            found |= (uint64_t(1) << it->second);
            return found == full_mask_;
        }
        return false;
    }

    // Large path: returns true when all targets found.
    bool
    set_if_found(const T& val,
                 std::vector<uint64_t>& found,
                 size_t& remaining) const {
        auto it = value_to_bit_.find(val);
        if (it != value_to_bit_.end()) {
            uint32_t idx = it->second;
            uint64_t bit = uint64_t(1) << (idx % 64);
            uint64_t& word = found[idx / 64];
            if (!(word & bit)) {
                word |= bit;
                return --remaining == 0;
            }
        }
        return false;
    }

    bool
    use_small() const {
        return use_small_;
    }
    size_t
    target_count() const {
        return target_count_;
    }
    uint64_t
    full_mask() const {
        return full_mask_;
    }
    size_t
    num_words() const {
        return num_words_;
    }

 private:
    ankerl::unordered_dense::map<T, uint32_t> value_to_bit_;
    size_t target_count_{0};
    bool use_small_{true};
    uint64_t full_mask_{0};
    size_t num_words_{0};
};

// ARRAY column: contains / contains_any. Row (or element, element-level)
// is TRUE when any array item is in `elements`; never UNKNOWN (NULL rows are
// folded by KernelAdapter).
template <typename ArrayType, typename GetType, typename TypedSet>
struct ArrayContainsAnyKernel {
    const TypedSet* elements;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<ArrayType>& b, TriStateOut out) const {
        for (size_t i = 0; i < b.size; ++i) {
            if (IsSkippedCandidate(b, i)) {
                continue;
            }
            const auto& array = b.data[i];
            const auto array_size = SegmentExpr::GetArrayRowSize(array);
            for (size_t j = 0; j < array_size; ++j) {
                if (elements->find(array.template get_data<GetType>(j)) !=
                    elements->end()) {
                    out.SetTrue(i);
                    break;
                }
            }
        }
    }
};

// ARRAY column: contains_all.
template <typename ArrayType, typename GetType>
struct ArrayContainsAllKernel {
    explicit ArrayContainsAllKernel(const std::set<GetType>& elements)
        : matcher(elements),
          found_large(matcher.use_small() ? 0 : matcher.num_words()) {
    }

    ContainsAllMatcher<GetType> matcher;
    std::vector<uint64_t> found_large;  // per-row scratch for > 64 targets

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<ArrayType>& b, TriStateOut out) {
        for (size_t i = 0; i < b.size; ++i) {
            if (IsSkippedCandidate(b, i)) {
                continue;
            }
            if (Matches(b.data[i])) {
                out.SetTrue(i);
            }
        }
    }

    bool
    Matches(const ArrayType& array) {
        const auto array_size = SegmentExpr::GetArrayRowSize(array);
        if (array_size < matcher.target_count()) {
            return false;
        }
        if (matcher.use_small()) {
            uint64_t found = 0;
            for (size_t j = 0; j < array_size; ++j) {
                if (matcher.set_if_found(array.template get_data<GetType>(j),
                                         found)) {
                    return true;
                }
            }
            return found == matcher.full_mask();
        }
        std::fill(found_large.begin(), found_large.end(), 0);
        size_t remaining = matcher.target_count();
        for (size_t j = 0; j < array_size; ++j) {
            if (matcher.set_if_found(array.template get_data<GetType>(j),
                                     found_large,
                                     remaining)) {
                return true;
            }
        }
        return remaining == 0;
    }
};

// JSON column, same-type scalar literals: contains / contains_any.
// Path missing / JSON null / not an array → UNKNOWN; items of another type
// are skipped.
template <typename GetType>
struct JsonContainsAnyKernel {
    std::string pointer;
    const MultiElement* elements;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        for (size_t i = 0; i < b.size; ++i) {
            if (IsSkippedCandidate(b, i)) {
                continue;
            }
            auto doc = b.data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                out.SetUnknown(i);
                continue;
            }
            if (AnyIn(array)) {
                out.SetTrue(i);
            }
        }
    }

    template <typename JsonArray>
    bool
    AnyIn(JsonArray& array) const {
        for (auto&& it : array) {
            auto val = it.template get<GetType>();
            if (val.error()) {
                if constexpr (std::is_same_v<GetType, int64_t>) {
                    auto double_val = it.template get<double>();
                    if (!double_val.error() &&
                        double_val.value() == std::floor(double_val.value())) {
                        if (elements->In(
                                static_cast<int64_t>(double_val.value()))) {
                            return true;
                        }
                    }
                }
                continue;
            }
            if (elements->In(val.value())) {
                return true;
            }
        }
        return false;
    }
};

// JSON column, same-type scalar literals: contains_all.
template <typename GetType>
struct JsonContainsAllKernel {
    JsonContainsAllKernel(std::string pointer_in,
                          const std::set<GetType>& elements)
        : pointer(std::move(pointer_in)),
          matcher(elements),
          found_large(matcher.use_small() ? 0 : matcher.num_words()) {
    }

    std::string pointer;
    ContainsAllMatcher<GetType> matcher;
    std::vector<uint64_t> found_large;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) {
        for (size_t i = 0; i < b.size; ++i) {
            if (IsSkippedCandidate(b, i)) {
                continue;
            }
            auto doc = b.data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                out.SetUnknown(i);
                continue;
            }
            if (AllIn(array)) {
                out.SetTrue(i);
            }
        }
    }

    template <typename JsonArray>
    bool
    AllIn(JsonArray& array) {
        if (matcher.use_small()) {
            uint64_t found = 0;
            for (auto&& it : array) {
                auto val = it.template get<GetType>();
                if (val.error()) {
                    if constexpr (std::is_same_v<GetType, int64_t>) {
                        auto double_val = it.template get<double>();
                        if (!double_val.error() &&
                            double_val.value() ==
                                std::floor(double_val.value())) {
                            if (matcher.set_if_found(
                                    static_cast<int64_t>(double_val.value()),
                                    found)) {
                                return true;
                            }
                        }
                    }
                    continue;
                }
                if (matcher.set_if_found(val.value(), found)) {
                    return true;
                }
            }
            return found == matcher.full_mask();
        }
        std::fill(found_large.begin(), found_large.end(), 0);
        size_t remaining = matcher.target_count();
        for (auto&& it : array) {
            auto val = it.template get<GetType>();
            if (val.error()) {
                if constexpr (std::is_same_v<GetType, int64_t>) {
                    auto double_val = it.template get<double>();
                    if (!double_val.error() &&
                        double_val.value() == std::floor(double_val.value())) {
                        if (matcher.set_if_found(
                                static_cast<int64_t>(double_val.value()),
                                found_large,
                                remaining)) {
                            return true;
                        }
                    }
                }
                continue;
            }
            if (matcher.set_if_found(val.value(), found_large, remaining)) {
                return true;
            }
        }
        return remaining == 0;
    }
};

// JSON column, array literals: contains / contains_any.
struct JsonContainsArrayKernel {
    std::string pointer;
    const std::vector<proto::plan::Array>* elements;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        for (size_t i = 0; i < b.size; ++i) {
            if (IsSkippedCandidate(b, i)) {
                continue;
            }
            auto doc = b.data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                out.SetUnknown(i);
                continue;
            }
            if (AnyArrayIn(array)) {
                out.SetTrue(i);
            }
        }
    }

    template <typename JsonArray>
    bool
    AnyArrayIn(JsonArray& array) const {
        for (auto&& it : array) {
            auto val = it.get_array();
            if (val.error()) {
                continue;
            }
            std::vector<simdjson::simdjson_result<simdjson::ondemand::value>>
                json_array;
            json_array.reserve(val.count_elements());
            for (auto&& e : val) {
                json_array.emplace_back(e);
            }
            for (const auto& element : *elements) {
                if (CompareTwoJsonArray(json_array, element)) {
                    return true;
                }
            }
        }
        return false;
    }
};

// JSON column, array literals: contains_all.
struct JsonContainsAllArrayKernel {
    std::string pointer;
    std::vector<proto::plan::Array> elements;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        for (size_t i = 0; i < b.size; ++i) {
            if (IsSkippedCandidate(b, i)) {
                continue;
            }
            auto doc = b.data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                out.SetUnknown(i);
                continue;
            }
            if (AllArraysIn(array)) {
                out.SetTrue(i);
            }
        }
    }

    template <typename JsonArray>
    bool
    AllArraysIn(JsonArray& array) const {
        std::unordered_set<int> exist_elements_index;
        for (auto&& it : array) {
            auto val = it.get_array();
            if (val.error()) {
                continue;
            }
            std::vector<simdjson::simdjson_result<simdjson::ondemand::value>>
                json_array;
            json_array.reserve(val.count_elements());
            for (auto&& e : val) {
                json_array.emplace_back(e);
            }
            for (int index = 0; index < elements.size(); ++index) {
                if (CompareTwoJsonArray(json_array, elements[index])) {
                    exist_elements_index.insert(index);
                }
            }
            if (exist_elements_index.size() == elements.size()) {
                return true;
            }
        }
        return exist_elements_index.size() == elements.size();
    }
};

// JSON column, mixed-type literals: contains / contains_any (DOM parser).
struct JsonContainsAnyWithDiffTypeKernel {
    std::string pointer;
    const std::vector<proto::plan::GenericValue>* elements;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        for (size_t i = 0; i < b.size; ++i) {
            if (IsSkippedCandidate(b, i)) {
                continue;
            }
            auto doc = b.data[i].dom_doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                out.SetUnknown(i);
                continue;
            }
            if (AnyIn(array)) {
                out.SetTrue(i);
            }
        }
    }

    template <typename JsonArray>
    bool
    AnyIn(JsonArray& array) const {
        // Note: array can only be iterated once
        for (auto&& it : array) {
            for (auto const& element : *elements) {
                switch (element.val_case()) {
                    case proto::plan::GenericValue::kBoolVal: {
                        auto val = it.template get<bool>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.bool_val()) {
                            return true;
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kInt64Val: {
                        auto val = it.template get<int64_t>();
                        if (val.error()) {
                            auto double_val = it.template get<double>();
                            if (!double_val.error() &&
                                double_val.value() == element.int64_val()) {
                                return true;
                            }
                            continue;
                        }
                        if (val.value() == element.int64_val()) {
                            return true;
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kFloatVal: {
                        auto val = it.template get<double>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.float_val()) {
                            return true;
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kStringVal: {
                        auto val = it.template get<std::string_view>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.string_val()) {
                            return true;
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kArrayVal: {
                        auto val = it.get_array();
                        if (val.error()) {
                            continue;
                        }
                        if (CompareTwoJsonArray(val, element.array_val())) {
                            return true;
                        }
                        break;
                    }
                    default:
                        ThrowInfo(UnexpectedError,
                                  "unsupported data type {}",
                                  element.val_case());
                }
            }
        }
        return false;
    }
};

// JSON column, mixed-type literals: contains_all (DOM parser).
struct JsonContainsAllWithDiffTypeKernel {
    std::string pointer;
    const std::vector<proto::plan::GenericValue>* elements;
    std::unordered_set<int> elements_index;  // {0 .. elements->size()-1}

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        for (size_t i = 0; i < b.size; ++i) {
            if (IsSkippedCandidate(b, i)) {
                continue;
            }
            auto doc = b.data[i].dom_doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                out.SetUnknown(i);
                continue;
            }
            if (AllIn(array)) {
                out.SetTrue(i);
            }
        }
    }

    template <typename JsonArray>
    bool
    AllIn(JsonArray& array) const {
        std::unordered_set<int> tmp_elements_index(elements_index);
        for (auto&& it : array) {
            int idx = -1;
            for (auto& element : *elements) {
                idx++;
                switch (element.val_case()) {
                    case proto::plan::GenericValue::kBoolVal: {
                        auto val = it.template get<bool>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.bool_val()) {
                            tmp_elements_index.erase(idx);
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kInt64Val: {
                        auto val = it.template get<int64_t>();
                        if (val.error()) {
                            auto double_val = it.template get<double>();
                            if (!double_val.error() &&
                                double_val.value() == element.int64_val()) {
                                tmp_elements_index.erase(idx);
                            }
                            continue;
                        }
                        if (val.value() == element.int64_val()) {
                            tmp_elements_index.erase(idx);
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kFloatVal: {
                        auto val = it.template get<double>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.float_val()) {
                            tmp_elements_index.erase(idx);
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kStringVal: {
                        auto val = it.template get<std::string_view>();
                        if (val.error()) {
                            continue;
                        }
                        if (val.value() == element.string_val()) {
                            tmp_elements_index.erase(idx);
                        }
                        break;
                    }
                    case proto::plan::GenericValue::kArrayVal: {
                        auto val = it.get_array();
                        if (val.error()) {
                            continue;
                        }
                        if (CompareTwoJsonArray(val, element.array_val())) {
                            tmp_elements_index.erase(idx);
                        }
                        break;
                    }
                    default:
                        ThrowInfo(UnexpectedError,
                                  "unsupported data type {}",
                                  element.val_case());
                }
                if (tmp_elements_index.size() == 0) {
                    return true;
                }
            }
            if (tmp_elements_index.size() == 0) {
                return true;
            }
        }
        return tmp_elements_index.size() == 0;
    }
};

class PhyJsonContainsFilterExpr : public SegmentExpr {
 public:
    PhyJsonContainsFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::JsonContainsExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level,
        const query::PlanOptions& plan_options = {})
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      expr->vals_.empty()
                          ? DataType::NONE
                          : FromValCase(expr->vals_[0].val_case()),
                      active_count,
                      batch_size,
                      consistency_level,
                      false,
                      true,
                      plan_options),
          expr_(expr) {
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    bool
    IsSource() const override {
        return true;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    bool
    IsElementLevelExpression() const override {
        return expr_->column_.element_level_;
    }

    void
    DetermineExecPath() override {
        if (CanUseJsonStatsAtInit()) {
            exec_path_ = ExprExecPath::JsonStats;
            return;
        }
        if (expr_->column_.data_type_ == DataType::JSON &&
            (!expr_->same_type_ ||
             std::any_of(expr_->vals_.begin(),
                         expr_->vals_.end(),
                         [](const auto& value) {
                             return value.val_case() ==
                                    proto::plan::GenericValue::kArrayVal;
                         }))) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }
        if (expr_->column_.data_type_ == DataType::ARRAY &&
            (expr_->column_.element_level_ || expr_->vals_.empty())) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }
        SegmentExpr::DetermineExecPath();
        if (exec_path_ != ExprExecPath::ScalarIndex ||
            expr_->column_.data_type_ != DataType::JSON ||
            value_type_ != DataType::INT64 || PinnedJsonIndexIsFlat()) {
            return;
        }
        const auto has_unsafe_int_literal = std::any_of(
            expr_->vals_.begin(),
            expr_->vals_.end(),
            [this](const auto& value) {
                return !IsInt64SafeForJsonDoubleIndex(value.int64_val());
            });
        if (has_unsafe_int_literal) {
            exec_path_ = ExprExecPath::RawData;
        }
    }

 private:
    VectorPtr
    EvalJsonContainsForDataSegment(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContains(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsByStats();

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContains(EvalCtx& context);

    template <typename ArrayType, typename ExprValueType, bool ElementLevel>
    VectorPtr
    ExecArrayContainsImpl(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsAll(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsAllByStats();

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContainsAll(EvalCtx& context);

    template <typename ArrayType, typename ExprValueType, bool ElementLevel>
    VectorPtr
    ExecArrayContainsAllImpl(EvalCtx& context);

    VectorPtr
    ExecJsonContainsArray(EvalCtx& context);

    VectorPtr
    ExecJsonContainsArrayByStats();

    VectorPtr
    ExecJsonContainsAllArray(EvalCtx& context);

    VectorPtr
    ExecJsonContainsAllArrayByStats();

    VectorPtr
    ExecJsonContainsAllWithDiffType(EvalCtx& context);

    VectorPtr
    ExecJsonContainsAllWithDiffTypeByStats();

    VectorPtr
    ExecJsonContainsWithDiffType(EvalCtx& context);

    VectorPtr
    ExecJsonContainsWithDiffTypeByStats();

    VectorPtr
    EvalArrayContainsForIndexSegment(DataType data_type);

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContainsForIndexSegmentImpl();

 private:
    std::shared_ptr<const milvus::expr::JsonContainsExpr> expr_;
    bool arg_inited_{false};
    std::shared_ptr<MultiElement> arg_set_;
    std::shared_ptr<void>
        arg_cached_set_;  // For caching std::set<T> or std::vector<T>
    PinWrapper<index::BsonInvertedIndex*> bson_index_{nullptr};
};
}  //namespace exec
}  // namespace milvus
