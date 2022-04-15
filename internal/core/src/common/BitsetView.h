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

#include <deque>
#include <boost_ext/dynamic_bitset_ext.hpp>

#include "common/Types.h"
#include "knowhere/utils/BitsetView.h"

namespace milvus {

class BitsetView : public faiss::BitsetView {
 public:
    BitsetView() = default;
    ~BitsetView() = default;

    BitsetView(const std::nullptr_t value) : faiss::BitsetView(value) {  // NOLINT
    }

    BitsetView(const uint8_t* data, size_t num_bits) : faiss::BitsetView(data, num_bits) {  // NOLINT
    }

    BitsetView(const BitsetType& bitset)  // NOLINT
        : BitsetView((uint8_t*)boost_ext::get_data(bitset), size_t(bitset.size())) {
    }

    BitsetView(const BitsetTypePtr& bitset_ptr) {  // NOLINT
        if (bitset_ptr) {
            *this = BitsetView(*bitset_ptr);
        }
    }

    BitsetView
    subview(size_t offset, size_t size) const {
        if (empty()) {
            return BitsetView();
        }
        assert((offset & 0x7) == 0);
        assert((offset + size) <= this->size());
        return BitsetView(data() + (offset >> 3), size);
    }
};

}  // namespace milvus
