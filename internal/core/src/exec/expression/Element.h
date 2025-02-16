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
#include <memory>
#include <string>

#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/VectorFunction.h"
#include "exec/expression/Utils.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "query/PlanProto.h"

namespace milvus {
namespace exec {

class BaseElement {
 public:
    virtual ~BaseElement() = default;
};

class SingleElement : public BaseElement {
 public:
    using ValueType = std::variant<std::monostate,
                                   bool,
                                   int8_t,
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
        value_ = GetValueFromProto<T>(value);
    }

    template <typename T>
    void
    SetValue(const T& value) {
        if constexpr (std::is_same_v<T, bool> || std::is_same_v<T, int8_t> ||
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
            PanicInfo(ErrorCode::UnexpectedError,
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
            values_.push_back(GetValueFromProto<T>(value));
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
        if (std::holds_alternative<T>(value)) {
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
            values_.push_back(GetValueFromProto<T>(value));
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
                if (v == value)
                    return true;
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
 public:
    explicit SetElement(const std::vector<proto::plan::GenericValue>& values) {
        for (auto& value : values) {
            values_.insert(GetValueFromProto<T>(value));
        }
    }

    explicit SetElement(const std::vector<T>& values) {
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
            return values_.count(std::get<T>(value)) > 0;
        }
    }

    void
    AddElement(const T& value) {
        values_.insert(value);
    }

    size_t
    Size() const override {
        return values_.size();
    }

 public:
    std::set<T> values_;
};

}  //namespace exec
}  // namespace milvus
