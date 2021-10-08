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

#include <boost/dynamic_bitset.hpp>
#include "ConcurrentBitset.h"

namespace faiss {
class BitsetView {
 public:
    BitsetView() = default;

    BitsetView(const uint8_t* blocks, int64_t size) : blocks_(blocks), size_(size) {
    }

    BitsetView(const ConcurrentBitset& bitset) : blocks_(bitset.data()), size_(bitset.count()) {
    }

    BitsetView(const ConcurrentBitsetPtr& bitset_ptr) {
        if(bitset_ptr) {
            *this = BitsetView(*bitset_ptr);
        }
    }

    BitsetView(const std::nullptr_t nullptr_value): BitsetView() {
        assert(nullptr_value == nullptr);
    }
    
    bool
    empty() const {
        return size_ == 0;
    }
    
    // return count of all bits
    int64_t 
    size() const {
        return size_;
    }

    // return sizeof bitmap in bytes
    int64_t
    u8size() const {
        return (size_ + 8 - 1) / 8;
    }

    const uint8_t*
    data() const {
        return blocks_;
    }

    operator bool() const {
        return !empty();
    }

    bool
    test(int64_t index) const {
        // assert(index < size_);
        auto block_id = index / 8;
        auto block_offset = index % 8;
        return (blocks_[block_id] >> block_offset) & 0x1;
    }

    uint64_t
    count_1() const {
        uint64_t ret = 0;
        auto p_data = reinterpret_cast<const uint64_t *>(blocks_);
        auto len = size_ >> 6;
        //auto remainder = size() % 8;
        auto popcount8 = [&](uint8_t x) -> int{
            x = (x & 0x55) + ((x >> 1) & 0x55);
            x = (x & 0x33) + ((x >> 2) & 0x33);
            x = (x & 0x0F) + ((x >> 4) & 0x0F);
            return x;
        };
        for (int64_t i = 0; i < len; ++ i) {
            ret += __builtin_popcountl(*p_data);
            p_data ++;
        }
        auto p_byte = blocks_ + (len << 3);
        for (auto i = (len << 3); i < u8size(); ++ i) {
            ret += popcount8(*p_byte);
            p_byte ++;
        }
        return ret;
    }

 private:
    const uint8_t* blocks_ = nullptr;
    int64_t size_ = 0;  // size of bits
};

}  // namespace faiss
