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

#include "common/bloom_filter.h"
#include <csignal>
#include <cstdint>
#include <iostream>
#include "bloom_filter_basic.h"
#include "bloom_filter_block.h"

namespace milvus{

    std::unique_ptr<BaseBloomFilter>
    unmarshal_bloom_filter(const char* data, const size_t size,  const std::string& prefix){
        auto json= milvus::Json(simdjson::padded_string(data, size));
        BloomFilterType type = static_cast<BloomFilterType>(json.at<int64_t >(prefix + "/bfType").value());

        switch (type){
            case BloomFilterType::BASIC:
                return std::make_unique<BasicBloomFilter>(json, prefix+"/bf");
            case BloomFilterType::BLOCK:
                return std::make_unique<BlockBloomFilter>(json, prefix+"/bf");
            default:
                throw std::runtime_error("Unsupported Bloom Filter Type");
        }
    }

    static const std::string base64_chars = 
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789-_";

    std::string 
    base64_decode(const std::string_view in){
        std::string out;
        std::vector<int> T(256, -1);
        unsigned int i;
        for (i =0; i < 64; i++) T[base64_chars[i]] = i;

        int val = 0, valb = -8;
        for (i = 0; i < in.length(); i++) {
            unsigned char c = in[i];
            if (T[c] == -1) break;
            val = (val<<6) + T[c];
            valb += 6;
            if (valb >= 0) {
                out.push_back(static_cast<char>((val>>valb)&0xFF));
                valb -= 8;
            }
        }
        return out;
    }
};