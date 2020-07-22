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

#include <unistd.h>
#include <cstring>
#include <string>
#include <vector>

#include "utils/StringHelpFunctions.h"

namespace milvus::engine::meta {

class MetaField {
 public:
    MetaField(const std::string& name, const std::string& type, const std::string& setting)
        : name_(name), type_(type), setting_(setting) {
    }

    std::string
    name() const {
        return name_;
    }

    std::string
    ToString() const {
        return name_ + " " + type_ + " " + setting_;
    }

    // mysql field type has additional information. for instance, a filed type is defined as 'BIGINT'
    // we get the type from sql is 'bigint(20)', so we need to ignore the '(20)'
    bool
    IsEqual(const MetaField& field) const {
        size_t name_len_min = field.name_.length() > name_.length() ? name_.length() : field.name_.length();
        size_t type_len_min = field.type_.length() > type_.length() ? type_.length() : field.type_.length();

        // only check field type, don't check field width, for example: VARCHAR(255) and VARCHAR(100) is equal
        std::vector<std::string> type_split;
        milvus::StringHelpFunctions::SplitStringByDelimeter(type_, "(", type_split);
        if (!type_split.empty()) {
            type_len_min = type_split[0].length() > type_len_min ? type_len_min : type_split[0].length();
        }

        // field name must be equal, ignore type width
        return strncasecmp(field.name_.c_str(), name_.c_str(), name_len_min) == 0 &&
               strncasecmp(field.type_.c_str(), type_.c_str(), type_len_min) == 0;
    }

 private:
    std::string name_;
    std::string type_;
    std::string setting_;
};

using MetaFields = std::vector<MetaField>;

class MetaSchema {
 public:
    MetaSchema(const std::string& name, const MetaFields& fields) : name_(name), fields_(fields) {
    }

    std::string
    name() const {
        return name_;
    }

    std::string
    ToString() const {
        std::string result;
        for (auto& field : fields_) {
            if (!result.empty()) {
                result += ",";
            }
            result += field.ToString();
        }

//        std::string constraints;
//        for (auto& constraint : constraint_fields_) {
//            if (!constraints.empty()) {
//                constraints += ",";
//            }
//            constraints += constraint.name();
//        }
//
//        if (!constraints.empty()) {
//            result += ",constraint uq unique(" + constraints + ")";
//        }

        return result;
    }

    // if the outer fields contains all this MetaSchema fields, return true
    // otherwise return false
    bool
    IsEqual(const MetaFields& fields) const {
        std::vector<std::string> found_field;
        for (const auto& this_field : fields_) {
            for (const auto& outer_field : fields) {
                if (this_field.IsEqual(outer_field)) {
                    found_field.push_back(this_field.name());
                    break;
                }
            }
        }

        return found_field.size() == fields_.size();
    }

 private:
    std::string name_;
    MetaFields fields_;
};

}

