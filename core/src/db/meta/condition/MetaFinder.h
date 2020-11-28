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

#include <string>
#include <unordered_map>

namespace milvus::engine::meta {

class Finder {
 public:
    virtual ~Finder() = default;

    virtual bool
    StrFind(const std::string& v) const = 0;
};

using Fields = std::unordered_map<std::string, std::string>;

class FieldsFinder {
 public:
    virtual bool
    FieldsFind(const Fields& fields) const = 0;
};

// class MetaFinder {
// public:
//    virtual
//    ~MetaFinder() = default;
//
//    virtual bool
//    FieldFind(const std::string& field, const std::string& v) const = 0;
//
//};

}  // namespace milvus::engine::meta
