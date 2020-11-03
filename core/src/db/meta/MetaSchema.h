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

namespace milvus::engine::meta {

class MetaField {
 public:
    MetaField(const std::string& name, const std::string& type, const std::string& setting)
        : name_(name), type_(type), setting_(setting) {
    }

    const std::string&
    name() const {
        return name_;
    }

    const std::string&
    type() const {
        return type_;
    }

    const std::string&
    setting() const {
        return setting_;
    }

    std::string
    ToString() const {
        return name_ + " " + type_ + " " + setting_;
    }

    bool
    IsEqual(const MetaField& field) const;

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
    ToString() const;

    bool
    IsEqual(const MetaFields& fields) const;

    const MetaFields&
    Fields() const {
        return fields_;
    }

 private:
    std::string name_;
    MetaFields fields_;
};

}  // namespace milvus::engine::meta
