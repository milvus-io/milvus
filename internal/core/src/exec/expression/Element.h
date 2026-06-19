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
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>

#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Utils.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "query/PlanProto.h"
#include "ankerl/unordered_dense.h"

#include "exec/expression/SimdFilter.h"

namespace milvus {
namespace exec {

// Transparent string hash for heterogeneous lookup in unordered_dense::set
// See: https://github.com/martinus/unordered_dense#324-heterogeneous-overloads-using-is_transparent
struct StringHash {
    using is_transparent = void;  // enable heterogeneous overloads
    using is_avalanching = void;  // mark as high quality avalanching hash

    [[nodiscard]] auto
    operator()(std::string_view str) const noexcept -> uint64_t {
        return ankerl::unordered_dense::hash<std::string_view>{}(str);
    }
};

class BaseElement {
 public:
    virtual ~BaseElement() = default;
};

class SingleElement : public BaseElement {
 public:
    using ValueType = std::variant<std::monostate,
                                   bool,
                                   int8_t,
                                   uint8_t,
                                   int16_t,
                                   int32_t,
                                   int64_t,
                                   float,
                                   double,
                                   std::string,
                                   proto::plan::Array>;

    SingleElement() = default;
    virtual ~SingleElement() = default;

    template <typename T>
    void
    SetValue(const proto::plan::GenericValue& value) {
        value_ = GetValueWithCastNumber<T>(value);
    }

    template <typename T>
    void
    SetValue(const T& value) {
        if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, int8_t> ||
                      std::is_same_v<T, uint8_t> ||
                      std::is_same_v<T, int16_t> ||
                      std::is_same_v<T, int32_t> ||
                      std::is_same_v<T, int64_t> || std::is_same_v<T, float> ||
                      std::is_same_v<T, double> ||
                      std::is_same_v<T, std::string>) {
            value_ = value;
        } else {
            static_assert(sizeof(T) == 0,
                          "Type not supported in SingleElement");
        }
    }

    template <typename T>
    T
    GetValue() const {
        try {
            return std::get<T>(value_);
        } catch (const std::bad_variant_access& e) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "SingleElement GetValue() failed: {}",
                      e.what());
        }
    }

 public:
    ValueType value_;
};

class MultiElement : public BaseElement {
 public:
    using ValueType = std::variant<std::monostate,
                                   bool,
                                   int8_t,
                                   uint8_t,
                                   int16_t,
                                   int32_t,
                                   int64_t,
                                   float,
                                   double,
                                   std::string,
                                   std::string_view>;

    MultiElement() = default;
    virtual ~MultiElement() = default;

    virtual bool
    In(const ValueType& value) const = 0;

    virtual bool
    Empty() const = 0;

    virtual size_t
    Size() const = 0;
};

template <typename T>
class SortVectorElement : public MultiElement {
 public:
    explicit SortVectorElement(
        const std::vector<proto::plan::GenericValue>& values) {
        for (auto& value : values) {
            values_.push_back(GetValueWithCastNumber<T>(value));
        }
        std::sort(values_.begin(), values_.end());
        sorted_ = true;
    }

    explicit SortVectorElement(const std::vector<T>& values) {
        for (const auto& value : values) {
            values_.push_back(value);
        }
        std::sort(values_.begin(), values_.end());
        sorted_ = true;
    }

    bool
    Empty() const override {
        return values_.empty();
    }

    size_t
    Size() const override {
        return values_.size();
    }

    bool
    In(const ValueType& value) const override {
        AssertInfo(sorted_, "In() should be sorted before");
        if constexpr (std::is_same_v<T, std::string>) {
            if (std::holds_alternative<std::string>(value)) {
                return std::binary_search(values_.begin(),
                                          values_.end(),
                                          std::get<std::string>(value));
            } else if (std::holds_alternative<std::string_view>(value)) {
                return std::binary_search(
                    values_.begin(),
                    values_.end(),
                    std::string(std::get<std::string_view>(value)),
                    [](const std::string& a, std::string_view b) {
                        return a < b;
                    });
            }
        } else if (std::holds_alternative<T>(value)) {
            return std::binary_search(
                values_.begin(), values_.end(), std::get<T>(value));
        }
        return false;
    }

    void
    Sort() {
        std::sort(values_.begin(), values_.end());
        sorted_ = true;
    }

    void
    AddElement(const T& value) {
        values_.push_back(value);
    }

    std::vector<T>
    GetElements() const {
        return values_;
    }

 public:
    std::vector<T> values_;
    bool sorted_{false};
};

template <typename T>
class FlatVectorElement : public MultiElement {
 public:
    explicit FlatVectorElement(
        const std::vector<proto::plan::GenericValue>& values) {
        for (auto& value : values) {
            values_.push_back(GetValueWithCastNumber<T>(value));
        }
    }

    explicit FlatVectorElement(const std::vector<T>& values) {
        for (const auto& value : values) {
            values_.push_back(value);
        }
    }

    bool
    Empty() const override {
        return values_.empty();
    }

    bool
    In(const ValueType& value) const override {
        if (std::holds_alternative<T>(value)) {
            for (const auto& v : values_) {
                if (v == std::get<T>(value))
                    return true;
            }
        }
        // Handle string_view -> string comparison when T is std::string
        if constexpr (std::is_same_v<T, std::string>) {
            if (auto sv = std::get_if<std::string_view>(&value)) {
                return std::find(values_.begin(), values_.end(), *sv) !=
                       values_.end();
            }
        }
        return false;
    }

    size_t
    Size() const override {
        return values_.size();
    }

    void
    AddElement(const T& value) {
        values_.push_back(value);
    }

 public:
    std::vector<T> values_;
};

template <typename T>
class SetElement : public MultiElement {
    // Use transparent hash for std::string to enable heterogeneous lookup
    // This allows O(1) lookup with string_view without string copy
    using SetType = std::conditional_t<
        std::is_same_v<T, std::string>,
        ankerl::unordered_dense::set<T, StringHash, std::equal_to<>>,
        ankerl::unordered_dense::set<T>>;

 public:
    explicit SetElement(const std::vector<proto::plan::GenericValue>& values) {
        values_.max_load_factor(0.5f);
        for (auto& value : values) {
            values_.insert(GetValueWithCastNumber<T>(value));
        }
    }

    explicit SetElement(const std::vector<T>& values) {
        values_.max_load_factor(0.5f);
        for (const auto& value : values) {
            values_.insert(value);
        }
    }

    bool
    Empty() const override {
        return values_.empty();
    }

    bool
    In(const ValueType& value) const override {
        if (std::holds_alternative<T>(value)) {
            return values_.find(std::get<T>(value)) != values_.end();
        }
        // Handle string_view -> string comparison when T is std::string
        if constexpr (std::is_same_v<T, std::string>) {
            if (auto sv = std::get_if<std::string_view>(&value)) {
                return values_.find(*sv) != values_.end();
            }
        }
        return false;
    }

    void
    AddElement(const T& value) {
        values_.insert(value);
    }

    size_t
    Size() const override {
        return values_.size();
    }

    // Batch filter: bypass ValueType variant construction.
    // Looks up each data[i] directly in the hash set (zero-copy for strings
    // via transparent hash).
    void
    FilterChunk(const T* data, const int size, TargetBitmapView res) const {
        for (int i = 0; i < size; ++i) {
            if constexpr (std::is_same_v<T, std::string>) {
                // Use string_view to avoid copying into the hash function
                if (values_.find(std::string_view(data[i])) != values_.end()) {
                    res[i] = true;
                }
            } else {
                if (values_.find(data[i]) != values_.end()) {
                    res[i] = true;
                }
            }
        }
    }

    // string_view data overload for sealed/mmap segments (T=string only).
    template <typename U = T,
              typename = std::enable_if_t<std::is_same_v<U, std::string>>>
    void
    FilterChunk(const std::string_view* data,
                const int size,
                TargetBitmapView res) const {
        for (int i = 0; i < size; ++i) {
            if (values_.find(data[i]) != values_.end()) {
                res[i] = true;
            }
        }
    }

    std::vector<T>
    GetElements() const {
        return std::vector<T>(values_.begin(), values_.end());
    }

 public:
    SetType values_;
};

// SetElement<bool> specialization: avoids ankerl::unordered_dense::set<bool>
// whose wyhash reads 8 bytes from a 1-byte bool, causing ASAN
// stack-use-after-scope.  Bool has at most 2 distinct values, so two flags
// are both faster and correct.
template <>
class SetElement<bool> : public MultiElement {
 public:
    explicit SetElement(const std::vector<proto::plan::GenericValue>& values) {
        for (auto& value : values) {
            bool v = GetValueWithCastNumber<bool>(value);
            if (v) {
                has_true_ = true;
            } else {
                has_false_ = true;
            }
        }
    }

    explicit SetElement(const std::vector<bool>& values) {
        for (bool v : values) {
            if (v) {
                has_true_ = true;
            } else {
                has_false_ = true;
            }
        }
    }

    bool
    Empty() const override {
        return !has_true_ && !has_false_;
    }

    bool
    In(const ValueType& value) const override {
        if (auto* b = std::get_if<bool>(&value)) {
            return *b ? has_true_ : has_false_;
        }
        return false;
    }

    void
    AddElement(const bool& value) {
        if (value) {
            has_true_ = true;
        } else {
            has_false_ = true;
        }
    }

    size_t
    Size() const override {
        return static_cast<size_t>(has_true_) + static_cast<size_t>(has_false_);
    }

    std::vector<bool>
    GetElements() const {
        std::vector<bool> result;
        if (has_true_) {
            result.push_back(true);
        }
        if (has_false_) {
            result.push_back(false);
        }
        return result;
    }

 private:
    bool has_true_ = false;
    bool has_false_ = false;
};

// ═══════════════════════════════════════════════════════════════════════════
// SimdBatchElement: SIMD batch-data comparison for all numeric types.
// Supports: int8, int16, int32, int64, float, double.
//
// Uses runtime-dispatched SIMD — at startup selects the best instruction set
// available on the current CPU (AVX512, AVX2, SSE2, NEON).
//
// Two execution modes:
//   In()          — per-row fallback (linear scan, no hash overhead)
//   FilterChunk() — batch SIMD via simdFilterChunk() (preferred)
// ═══════════════════════════════════════════════════════════════════════════
template <typename T>
class SimdBatchElement : public MultiElement {
    static_assert(std::is_same_v<T, int8_t> || std::is_same_v<T, uint8_t> ||
                      std::is_same_v<T, int16_t> ||
                      std::is_same_v<T, int32_t> ||
                      std::is_same_v<T, int64_t> || std::is_same_v<T, float> ||
                      std::is_same_v<T, double>,
                  "SimdBatchElement supports numeric types only");

    // Sorted and deduplicated by the Go rewriter layer
    // (sortTermValues in rewriter/util.go).
    std::vector<T> vals_;

 public:
    explicit SimdBatchElement(
        const std::vector<proto::plan::GenericValue>& values) {
        vals_.reserve(values.size());
        for (auto& value : values) {
            vals_.push_back(GetValueWithCastNumber<T>(value));
        }
    }

    explicit SimdBatchElement(const std::vector<T>& values) : vals_(values) {
    }

    bool
    Empty() const override {
        return vals_.empty();
    }

    size_t
    Size() const override {
        return vals_.size();
    }

    // Per-row fallback: binary search (vals_ is pre-sorted by Go rewriter)
    // Callers should use std::in_place_type<T> when constructing ValueType
    // to avoid implicit integer promotion (e.g. int8_t -> int32_t).
    bool
    In(const ValueType& value) const override {
        T v = std::get<T>(value);
        return std::binary_search(vals_.begin(), vals_.end(), v);
    }

    // Batch SIMD filter — delegates to runtime-dispatched simdFilterChunk().
    void
    FilterChunk(const T* data, const int size, TargetBitmapView res) const {
        if (vals_.empty() || size <= 0) {
            return;
        }

        int offset = res.offset();
        int start = 0;

        // Head: scalar for unaligned leading bits (up to 7 rows)
        int bit_offset = offset % 8;
        if (bit_offset != 0) {
            int head = std::min(size, 8 - bit_offset);
            for (int i = 0; i < head; ++i) {
                if (std::binary_search(vals_.begin(), vals_.end(), data[i])) {
                    res[i] = true;
                }
            }
            start = head;
        }

        // Middle: SIMD for the byte-aligned bulk
        int remaining = size - start;
        if (remaining > 0) {
            uint8_t* bitmap =
                reinterpret_cast<uint8_t*>(res.data()) + (start + offset) / 8;
            simdFilterChunk<T>(data + start,
                               remaining,
                               bitmap,
                               vals_.data(),
                               static_cast<int>(vals_.size()));
        }
    }

    std::vector<T>
    GetElements() const {
        return vals_;
    }
};

// ═══════════════════════════════════════════════════════════════════════════
// GetElementValues: extract typed element vector from any MultiElement.
// Used by skip index to get IN values regardless of element type.
// ═══════════════════════════════════════════════════════════════════════════
template <typename T>
std::vector<T>
GetElementValues(const std::shared_ptr<MultiElement>& ptr) {
    if (auto p = std::dynamic_pointer_cast<SetElement<T>>(ptr)) {
        return p->GetElements();
    }
    if constexpr (!std::is_same_v<T, bool> && !std::is_same_v<T, std::string> &&
                  !std::is_same_v<T, std::string_view>) {
        if (auto p = std::dynamic_pointer_cast<SimdBatchElement<T>>(ptr)) {
            return p->GetElements();
        }
    }
    if (auto p = std::dynamic_pointer_cast<SortVectorElement<T>>(ptr)) {
        return p->GetElements();
    }
    return {};
}

}  //namespace exec
}  // namespace milvus
