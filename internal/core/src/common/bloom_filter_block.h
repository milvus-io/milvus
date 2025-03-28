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

#include "simdjson/padded_string.h"
#include "simdjson/ondemand.h"


#include "common/bloom_filter.h"

namespace milvus {

class BlockBloomFilter: public BaseBloomFilter{
public:
    BlockBloomFilter() = default;
    BlockBloomFilter(const std::string_view data, const std::string& prefix){
        auto json= milvus::Json(simdjson::padded_string(data));
        from_json(json, prefix);
    }

    explicit BlockBloomFilter(const milvus::Json& json, const std::string& prefix){
        from_json(json, prefix);
    }
    
    ~BlockBloomFilter() override = default;

    bool 
    test(const char* data, const size_t size) override;

    bool
    test_string(const std::string& str) override;

    bool
    test_int64(const int64_t data) override;

    bool
    block_location(uint64_t loc);

    void
    from_json(const milvus::Json& json, const std::string& prefix) override;
private:
    uint k;
    uint64_t n; // num blocks
    std::vector<boost::dynamic_bitset<>> blocks;
    };
}