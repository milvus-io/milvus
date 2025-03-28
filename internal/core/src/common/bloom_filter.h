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

#include <memory>
#include <string>
#include <string_view>
#include <cstddef>

#include <boost/dynamic_bitset.hpp>

#include "common/Json.h"

namespace milvus {

enum BloomFilterType{
    UNSUPPORTED = 1,
    ALWAYSTRUE=2,
    BASIC = 3,
    BLOCK = 4,
};

class BaseBloomFilter{
public:
    virtual 
    ~BaseBloomFilter() = default;

    // Read Functions

    // Test Return True if the bytes is in the filter
    virtual bool 
    test(const char* data, const size_t size) = 0;

    // TestString Return True if the string is in the filter
    virtual bool 
    test_string(const std::string& str) = 0;

    virtual bool 
    test_int64(const int64_t data) = 0;

    virtual void 
    from_json(const milvus::Json& json, const std::string& prefix) =0 ; 
    // TODO Write Functions
};

std::unique_ptr<BaseBloomFilter>
unmarshal_bloom_filter(const char* data, const size_t size,  const std::string& prefix);

std::string 
base64_decode(const std::string_view in);
} // namespace milvus

