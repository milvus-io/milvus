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

#include <xxhash.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string_view>

#include "simdjson/padded_string.h"
#include "simdjson/ondemand.h"
#include "common/EasyAssert.h"
#include "common/bloom_filter_block.h"
#include "common/Json.h"

#define BLOCK_BITS 512
#define BLOCK_BYTES 64
#define BLOCK_NUM 8 // 512 / 64

namespace milvus{

    void
    BlockBloomFilter::from_json(const milvus::Json& json, const std::string& prefix){
        this->k = json.at<int64_t>(prefix+"/k").value();

        auto blocks = json.array_at(prefix+"/b").value();
        this->blocks.resize(blocks.size());
        this->n = 0;

        for (auto block : blocks){
            auto buf = base64_decode(block.get_string());
            Assert(buf.size() == BLOCK_BYTES);
            auto ptr = reinterpret_cast<const uint64_t*>(buf.data());
            this->blocks[this->n++]=boost::dynamic_bitset<>(ptr, ptr + BLOCK_NUM);
        }

        Assert(this->n == this->blocks.size());
    }
    
    bool
    BlockBloomFilter::block_location(uint64_t loc){
        auto l1 = loc >> 32;
        auto l2 = loc & 0xFFFFFFFF;

        auto block = (this->n * l2) >> 32; // maps l2 to an integar in [0, n)

        uint32_t h1 = l1+l2, h2 = l2+1;
        for (uint i=1; i < this->k; i++, h1 += h2, h2 += i){
            if (!this->blocks[block][h1 % BLOCK_BITS]){
                return false;
            }
        }
        return true;
    }

    bool
    BlockBloomFilter::test(const char* data, const size_t size){
        auto hash = XXH3_64bits(data, size);    
        return this->block_location(hash);
    }

    bool
    BlockBloomFilter::test_int64(const int64_t data){
        auto bytes = std::array<char, 8>();
        std::memcpy(bytes.data(), &data, 8);
        return test(bytes.data(), 8);
    }

    bool
    BlockBloomFilter::test_string(const std::string& str){
        return test(str.data(), str.length());
    }
}