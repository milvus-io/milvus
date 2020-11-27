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

#include "db/meta/condition/MetaBaseFilter.h"
#include "db/meta/condition/MetaConditionHelper.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

///////////////////////////////////////

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
class MetaRangeFilter : public MetaBaseFilter {
 public:
    MetaRangeFilter(Range range, const T& value)
        : res_name_(R::Name), field_name_(F::Name), range_(range), value_(value) {
    }

    ~MetaRangeFilter() override = default;

    std::string
    Dump() const override {
        switch (range_) {
            case LT: {
                return field_name_ + " < " + v2str();
            }
            case LTE: {
                return field_name_ + " <= " + v2str();
            }
            case GT: {
                return field_name_ + " > " + v2str();
            }
            case GTE: {
                return field_name_ + " >= " + v2str();
            }
            case EQ: {
                return field_name_ + " == " + v2str();
            }
            case NE: {
                return field_name_ + " <> " + v2str();
            }
            default:
                // TODO(yhz): do something
                break;
        }
    }

 protected:
    virtual std::string
    v2str() const {
        return FieldValue2Str(value_);
    }

 private:
    const std::string res_name_;
    const std::string field_name_;
    Range range_;
    const T value_;
};

/***
 * Between Filter
 */
template <typename R, typename F, typename T>
class MetaBetweenFilter : public MetaBaseFilter {
 public:
    MetaBetweenFilter(const T& lvalue, const T& rvalue, bool in = true)
        : res_name_(R::Name), field_name_(F::Name), lvalue_(lvalue), rvalue_(rvalue), in_(in) {
    }

    ~MetaBetweenFilter() override = default;

    std::string
    Dump() const override {
        std::string r = in_ ? " BETWEEN " : " NOT BETWEEN ";
        return field_name_ + r + FieldValue2Str(lvalue_) + " AND " + FieldValue2Str(rvalue_);
    }

 private:
    const std::string res_name_;
    const std::string field_name_;
    const T lvalue_;
    const T rvalue_;
    bool in_;
};

template <typename R, typename F, typename T>
class MetaInFilter : public MetaBaseFilter {
 public:
    explicit MetaInFilter(const std::vector<T>& values, bool in = true)
        : res_name_(R::Name), field_name_(F::Name), values_(values), in_(in) {
    }

    ~MetaInFilter() override = default;

    std::string
    Dump() const override {
        std::vector<std::string> svalues(values_.size());
        for (size_t i = 0; i < values_.size(); i++) {
            svalues[i] = FieldValue2Str(values_[i]);
        }

        std::string ms;
        StringHelpFunctions::MergeStringWithDelimeter(svalues, ", ", ms);
        std::string in_str = in_ ? " IN " : " NOT IN ";
        return field_name_ + in_str + "(" + ms + ")";
    }

 private:
    const std::string res_name_;
    const std::string field_name_;
    const std::vector<T>& values_;
    bool in_;
};

}  // namespace milvus::engine::meta
