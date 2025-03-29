

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

#include <boost/dynamic_bitset.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "common/bloom_filter_basic.h"
#include "common/digest_128.h"
#include "common/EasyAssert.h"
#include "common/Json.h"


namespace milvus {

    #define UINT_CAP (~0u)

    std::array<uint64_t, 4>
    murmur_hash(const char* data, const size_t size) {
        Digest128 digest;

        return digest.sum256(data, size);
    };

    uint
    BasicBloomFilter::basic_location(std::array<uint64_t, 4> h, uint ii){
        // result = h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
        uint64_t result = h[ii&1] + h[2+(((ii>>1)^ii)&1)] * ii;

        return result % m;
    }

    uint64_t
    read_uint64(const char* data, int offset, int n){
        AssertInfo(offset + 8 <= n, "fetch uint64 failed, out of range");
        uint64_t result = 0;
        for (int i=0; i<8; i++){
            // std::cout << "data: " << data[offset+i] << "\n";
            result = (result << 8) + static_cast<uint8_t>(data[offset+i]);
        }
        return result;
    }

    
    int
    words_needed(uint i) {
        if (i > UINT_CAP - 63){
            return UINT_CAP >> 6;
        }

        return (i + 63) >> 6;
    }

    void
    BasicBloomFilter::from_json(const milvus::Json& json, const std::string& prefix){
        this->m = json.at<uint64_t>(prefix+"/m").value();
        this->k = json.at<uint64_t>(prefix+"/k").value();
        
        auto str = base64_decode(json.at<std::string_view>(prefix+"/b").value());
        auto bytes = str.data();
        int nbytes = str.length();
        
        auto length = read_uint64(bytes, 0, nbytes);
        this->n = static_cast<uint64_t>(length);
        AssertInfo(this->n == length, "unmarshalling error: type mismatch");
        
        auto nword = words_needed(this->n);
        auto words = new uint64_t[nword];
        for (int i=0; i<nword; i++){
            words[i] = read_uint64(bytes, (i+1)*8, nbytes);
        }

        this->b=boost::dynamic_bitset<>(words, words+nword);
    }
    
    bool 
    BasicBloomFilter::test(const char* data, const size_t size){  
        auto h = murmur_hash(data, size);
        uint64_t* uh = reinterpret_cast<uint64_t*>(h.data());

        for (uint i =0;i< k;i++){
            if (!b[basic_location(h, i)]){
                return false;
            }
        }
        return true;
    }

    bool
    BasicBloomFilter::test_int64(const int64_t data){
        auto bytes = std::array<char, 8>();
        std::memcpy(bytes.data(), &data, 8);
        return test(bytes.data(), 8);
    }

    bool
    BasicBloomFilter::test_string(const std::string& str){
        return test(str.data(), str.length());
    }
}