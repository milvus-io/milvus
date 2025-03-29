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

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <cstddef>

#include "openssl/sha.h"
#include "simdjson/padded_string.h"
#include <boost/dynamic_bitset.hpp>

#include "common/Json.h"
#include "common/bloom_filter.h"


namespace milvus {

class BasicBloomFilter: public BaseBloomFilter{
    public:
        BasicBloomFilter() = default;
        BasicBloomFilter(const std::string_view data, const std::string& prefix){
            auto json= milvus::Json(simdjson::padded_string(data));
            from_json(json, prefix);
        }
    
        BasicBloomFilter(const milvus::Json& json, const std::string& prefix){
            from_json(json, prefix);
        }
    
        ~BasicBloomFilter() override = default;
    
        bool 
        test(const char* data, const size_t size) override;
    
        bool
        test_string(const std::string& str) override;
    
        bool
        test_int64(const int64_t data) override;
    
        void
        from_json(const milvus::Json& json, const std::string& prefix) override;
    
        uint
        basic_location(std::array<uint64_t, 4> h, uint ii);
    
    private:
        uint n;
        uint m;
        uint k;
        boost::dynamic_bitset<> b;
    };
    
}