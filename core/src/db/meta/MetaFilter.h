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

#include "db/meta/MetaResourceAttrs.h"
#include "db/snapshot/ResourceTypes.h"
#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

template <typename V>
inline std::string
FieldValue2Str(const V& v) {
    return std::to_string(v);
}

template <>
inline std::string
FieldValue2Str<std::string>(const std::string& v) {
    return v;
}

template <>
inline std::string
FieldValue2Str<snapshot::FTYPE_TYPE>(const snapshot::FTYPE_TYPE& v) {
    return std::to_string(static_cast<int>(v));
}

template <>
inline std::string
FieldValue2Str<snapshot::FETYPE_TYPE>(const snapshot::FETYPE_TYPE& v) {
    return std::to_string(static_cast<int>(v));
}

template <>
inline std::string
FieldValue2Str<snapshot::MappingT>(const snapshot::MappingT& v) {
    std::string value;
    mappings2str(v, value);
    return value;
}

///////////////////////////////////////
class MetaBaseCondition {
 public:
    virtual std::string
    Dump() const = 0;
};

using MetaConditionPtr = std::shared_ptr<MetaBaseCondition>;

class MetaBaseFilter : public MetaBaseCondition {};

enum Range {
    LT,  /*less than*/
    LTE, /*less than or equal*/
    GT,  /*greater than*/
    GTE, /*greater than or equal*/
    EQ,
    NE
};

template <typename R, typename F, typename T>
class MetaRangeFilter : public MetaBaseFilter {
 public:
    MetaRangeFilter(Range range, const T& value)
        : res_name_(R::Name), field_name_(F::Name), range_(range), value_(value) {
    }

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

template <typename R, typename F, typename T>
class MetaBetweenFilter : public MetaBaseFilter {
 public:
    MetaBetweenFilter(const T& lvalue, const T& rvalue, bool in = true)
        : res_name_(R::Name), field_name_(F::Name), lvalue_(lvalue), rvalue_(rvalue), in_(in) {
    }

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

////////////////////////////////////////////////////////////////////
using MetaFilterPtr = std::shared_ptr<MetaBaseFilter>;

enum Cond { and_, or_, one_ };

class MetaBaseCombination : public MetaBaseCondition {
 protected:
    using Ptr = std::shared_ptr<MetaBaseCombination>;

 public:
    explicit MetaBaseCombination(Cond cond) : cond_(cond) {
    }

 protected:
    std::string
    Relation() const {
        switch (cond_) {
            case and_:
                return "AND";
            case or_:
                return "OR";
            default:
                return "";
        }
    }

 protected:
    Cond cond_;
};

using MetaCombinationPtr = std::shared_ptr<MetaBaseCombination>;

class MetaFilterCombination : public MetaBaseCombination {
 public:
    explicit MetaFilterCombination(MetaFilterPtr filter) : MetaBaseCombination(one_), filter_(filter) {
    }

    std::string
    Dump() const override {
        return filter_->Dump();
    }

 private:
    MetaFilterPtr filter_;
};

class MetaRelationCombination : public MetaBaseCombination {
 public:
    MetaRelationCombination(Cond cond, MetaConditionPtr lcond, MetaConditionPtr rcond)
        : MetaBaseCombination(cond), lcond_(std::move(lcond)), rcond_(std::move(rcond)) {
        if (cond != and_ && cond != or_) {
            throw std::runtime_error("Invalid combination relation");
        }
    }

    std::string
    Dump() const override {
        std::string l_dump_str = lcond_->Dump();
        if (std::dynamic_pointer_cast<MetaRelationCombination>(lcond_)) {
            l_dump_str = "(" + l_dump_str + ")";
        }

        std::string r_dump_str = rcond_->Dump();
        if (std::dynamic_pointer_cast<MetaRelationCombination>(rcond_)) {
            r_dump_str = "(" + r_dump_str + ")";
        }
        return l_dump_str + " " + Relation() + " " + r_dump_str;
    }

 private:
    MetaConditionPtr lcond_;
    MetaConditionPtr rcond_;
};

////////////////////////////////////////////
MetaCombinationPtr
AND_(const MetaConditionPtr& lcond, const MetaConditionPtr& rcond);

MetaCombinationPtr
OR_(const MetaConditionPtr& lcond, const MetaConditionPtr& rcond);

MetaCombinationPtr
ONE_(const MetaFilterPtr& filter);

}  // namespace milvus::engine::meta
