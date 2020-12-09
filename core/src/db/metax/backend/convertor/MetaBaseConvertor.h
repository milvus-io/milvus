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

#include <cstdint>
#include <memory>
#include <string>

#include "db/snapshot/ResourceTypes.h"
#include "utils/Json.h"

namespace milvus::engine::metax {

class MetaBaseConvertor {
 public:
    virtual ~MetaBaseConvertor() = default;

 public:
    virtual std::string
    int2str(const int64_t&) = 0;

    virtual std::string
    uint2str(const uint64_t&) = 0;

    virtual std::string
    str2str(const std::string&) = 0;

    virtual std::string
    json2str(const json&) = 0;

    virtual int64_t
    intfstr(const std::string&) = 0;

    virtual uint64_t
    uintfstr(const std::string&) = 0;

    virtual std::string
    strfstr(const std::string&) = 0;

    virtual json
    jsonfstr(const std::string&) = 0;
};

using MetaConvertorPtr = std::shared_ptr<MetaBaseConvertor>;

}  // namespace milvus::engine::metax
