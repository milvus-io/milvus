// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/meta/MetaFieldValueHelper.h"
#include "db/meta/condition/MetaBaseFilter.h"
#include "db/meta/condition/MetaDeterminer.h"
#include "db/meta/condition/MetaFinder.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

////////////////////////////////////////
template <typename T>
class ComparableFilter : public MetaBaseFilter, public Determiner<T> {
 public:
    explicit ComparableFilter(const std::string& field) : MetaBaseFilter(field), Determiner<T>() {
    }

    ~ComparableFilter() override = default;

    bool
    StrFind(const std::string& s) const override {
        T v = Str2FieldValue<T>(s);
        return this->InRange(v);
    }
};

/***
 * Range Filter
 */

enum Range {
    LT,  /*less than*/
    LTE, /*less than or equal*/
    GT,  /*greater than*/
    GTE, /*greater than or equal*/
    EQ,  /*equal*/
    NE   /*not equal*/
};

template <typename R, typename F, typename T>
class MetaRangeFilter : public ComparableFilter<T> {
 public:
    MetaRangeFilter(Range range, const T& value)
        : ComparableFilter<T>(F::Name), res_name_(R::Name), range_(range), value_(value) {
    }

    ~MetaRangeFilter() override = default;

    bool
    InRange(const T& v) const override {
        switch (range_) {
            case LT: {
                return (FieldCompare(v, value_) == -1);
            }
            case LTE: {
                return (FieldCompare(v, value_) == -1) || (FieldCompare(v, value_) == 0);
            }
            case GT: {
                return (FieldCompare(v, value_) == 1);
            }
            case GTE: {
                return (FieldCompare(v, value_) == 1) || (FieldCompare(v, value_) == 0);
            }
            case EQ: {
                return (FieldCompare(v, value_) == 0);
            }
            case NE: {
                return (FieldCompare(v, value_) != 0);
            }
            default:
                break;
        }

        throw std::runtime_error("Unknown range type");
    }

    std::string
    Dump() const override {
        switch (range_) {
            case LT: {
                return this->Field() + " < " + FieldValue2Str(value_);
            }
            case LTE: {
                return this->Field() + " <= " + FieldValue2Str(value_);
            }
            case GT: {
                return this->Field() + " > " + FieldValue2Str(value_);
            }
            case GTE: {
                return this->Field() + " >= " + FieldValue2Str(value_);
            }
            case EQ: {
                return this->Field() + " = " + FieldValue2Str(value_);
            }
            case NE: {
                return this->Field() + " <> " + FieldValue2Str(value_);
            }
            default:
                // TODO(yhz): do something
                break;
        }

        throw std::runtime_error("Unknown range type");
    }

 private:
    const std::string res_name_;
    Range range_;
    const T value_;
};

/***
 * Between Filter
 */
template <typename R, typename F, typename T>
class MetaBetweenFilter : public ComparableFilter<T> {
 public:
    MetaBetweenFilter(const T& lvalue, const T& rvalue, bool in = true)
        : ComparableFilter<T>(F::Name), res_name_(R::Name), lvalue_(lvalue), rvalue_(rvalue), in_(in) {
    }

    ~MetaBetweenFilter() override = default;

    bool
    InRange(const T& v) const override {
        return ((FieldCompare(lvalue_, v) == -1) || (FieldCompare(lvalue_, v) == 0)) &&
               ((FieldCompare(v, rvalue_) == -1) || (FieldCompare(lvalue_, v) == 0));
    }

    std::string
    Dump() const override {
        std::string r = in_ ? " BETWEEN " : " NOT BETWEEN ";
        return this->Field() + r + FieldValue2Str(lvalue_) + " AND " + FieldValue2Str(rvalue_);
    }

 private:
    const std::string res_name_;
    const T lvalue_;
    const T rvalue_;
    bool in_;
};

template <typename R, typename F, typename T>
class MetaInFilter : public ComparableFilter<T> {
 public:
    explicit MetaInFilter(const std::vector<T>& values, bool in = true)
        : ComparableFilter<T>(F::Name), res_name_(R::Name), values_(values), in_(in) {
    }

    ~MetaInFilter() override = default;

    bool
    InRange(const T& v) const override {
        for (auto& vi : values_) {
            if (FieldCompare(vi, v) == 0) {
                return true;
            }
        }

        return false;
    }

    std::string
    Dump() const override {
        std::vector<std::string> svalues(values_.size());
        for (size_t i = 0; i < values_.size(); i++) {
            svalues[i] = FieldValue2Str(values_[i]);
        }

        std::string ms;
        StringHelpFunctions::MergeStringWithDelimeter(svalues, ", ", ms);
        std::string in_str = in_ ? " IN " : " NOT IN ";
        return this->Field() + in_str + "(" + ms + ")";
    }

 private:
    const std::string res_name_;
    const std::vector<T>& values_;
    bool in_;
};

/////////////////////////////////////////////////////////
template <typename R, typename F, typename T>
std::shared_ptr<MetaRangeFilter<R, F, T>>
Range_(Range range, const T& value) {
    return std::make_shared<MetaRangeFilter<R, F, T>>(range, value);
}

template <typename R, typename F, typename T>
std::shared_ptr<MetaBetweenFilter<R, F, T>>
Between_(const T& lvalue, const T& rvalue) {
    return std::make_shared<MetaBetweenFilter<R, F, T>>(lvalue, rvalue);
}

template <typename R, typename F, typename T>
std::shared_ptr<MetaInFilter<R, F, T>>
In_(const std::vector<T>& values) {
    return std::make_shared<MetaInFilter<R, F, T>>(values);
}

}  // namespace milvus::engine::meta
