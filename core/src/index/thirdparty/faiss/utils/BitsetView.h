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

 private:
    const uint8_t* blocks_ = nullptr;
    int64_t size_ = 0;  // size of bits
};

}  // namespace faiss
