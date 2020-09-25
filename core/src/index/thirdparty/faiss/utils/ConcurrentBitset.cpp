// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstring>
#include "ConcurrentBitset.h"

namespace faiss {

ConcurrentBitset::ConcurrentBitset(id_type_t capacity, uint8_t init_value) : capacity_(capacity), bitset_(((capacity + 8 - 1) >> 3)) {
    if (init_value) {
        memset(mutable_data(), init_value, (capacity + 8 - 1) >> 3);
    }
}

ConcurrentBitset&
ConcurrentBitset::operator&=(const ConcurrentBitset& bitset) {
    auto u8_1 = mutable_data();
    auto u8_2 = bitset.data();
    auto u64_1 = reinterpret_cast<uint64_t*>(u8_1);
    auto u64_2 = reinterpret_cast<const uint64_t*>(u8_2);

    size_t n8 = bitset_.size();
    size_t n64 = n8 / 8;

    for (size_t i = 0; i < n64; i++) {
        u64_1[i] &= u64_2[i];
    }

    size_t remain = n8 % 8;
    u8_1 += n64 * 8;
    u8_2 += n64 * 8;
    for (size_t i = 0; i < remain; i++) {
        u8_1[i] &= u8_2[i];
    }

    return *this;
}

std::shared_ptr<ConcurrentBitset>
ConcurrentBitset::operator&(const ConcurrentBitset& bitset) const {
    auto result_bitset = std::make_shared<ConcurrentBitset>(bitset.capacity());

    auto result_8 = result_bitset->mutable_data();
    auto result_64 = reinterpret_cast<uint64_t*>(result_8);

    auto u8_1 = data();
    auto u8_2 = bitset.data();
    auto u64_1 = reinterpret_cast<const uint64_t*>(u8_1);
    auto u64_2 = reinterpret_cast<const uint64_t*>(u8_2);

    size_t n8 = bitset_.size();
    size_t n64 = n8 / 8;

    for (size_t i = 0; i < n64; i++) {
        result_64[i] = u64_1[i] & u64_2[i];
    }

    size_t remain = n8 % 8;
    u8_1 += n64 * 8;
    u8_2 += n64 * 8;
    result_8 += n64 * 8;
    for (size_t i = 0; i < remain; i++) {
         result_8[i] = u8_1[i] & u8_2[i];
    }


    return result_bitset;
}

ConcurrentBitset&
ConcurrentBitset::operator|=(const ConcurrentBitset& bitset) {
    auto u8_1 = mutable_data();
    auto u8_2 = bitset.data();
    auto u64_1 = reinterpret_cast<uint64_t*>(u8_1);
    auto u64_2 = reinterpret_cast<const uint64_t*>(u8_2);

    size_t n8 = bitset_.size();
    size_t n64 = n8 / 8;

    for (size_t i = 0; i < n64; i++) {
        u64_1[i] |= u64_2[i];
    }

    size_t remain = n8 % 8;
    u8_1 += n64 * 8;
    u8_2 += n64 * 8;
    for (size_t i = 0; i < remain; i++) {
        u8_1[i] |= u8_2[i];
    }

    return *this;
}

std::shared_ptr<ConcurrentBitset>
ConcurrentBitset::operator|(const ConcurrentBitset& bitset) const {
    auto result_bitset = std::make_shared<ConcurrentBitset>(bitset.capacity());

    auto result_8 = result_bitset->mutable_data();
    auto result_64 = reinterpret_cast<uint64_t*>(result_8);

    auto u8_1 = data();
    auto u8_2 = bitset.data();
    auto u64_1 = reinterpret_cast<const uint64_t*>(u8_1);
    auto u64_2 = reinterpret_cast<const uint64_t*>(u8_2);

    size_t n8 = bitset_.size();
    size_t n64 = n8 / 8;

    for (size_t i = 0; i < n64; i++) {
        result_64[i] = u64_1[i] | u64_2[i];
    }

    size_t remain = n8 % 8;
    u8_1 += n64 * 8;
    u8_2 += n64 * 8;
    result_8 += n64 * 8;
    for (size_t i = 0; i < remain; i++) {
        result_8[i] = u8_1[i] | u8_2[i];
    }

    return result_bitset;
}

ConcurrentBitset&
ConcurrentBitset::operator^=(const ConcurrentBitset& bitset) {
    auto u8_1 = mutable_data();
    auto u8_2 = bitset.data();
    auto u64_1 = reinterpret_cast<uint64_t*>(u8_1);
    auto u64_2 = reinterpret_cast<const uint64_t*>(u8_2);

    size_t n8 = bitset_.size();
    size_t n64 = n8 / 8;

    for (size_t i = 0; i < n64; i++) {
        u64_1[i] &= u64_2[i];
    }

    size_t remain = n8 % 8;
    u8_1 += n64 * 8;
    u8_2 += n64 * 8;
    for (size_t i = 0; i < remain; i++) {
        u8_1[i] ^= u8_2[i];
    }

    return *this;
}

ConcurrentBitset&
ConcurrentBitset::negate() {
    auto u8_1 = mutable_data();
    auto u64_1 = reinterpret_cast<uint64_t*>(u8_1);

    size_t n8 = bitset_.size();
    size_t n64 = n8 / 8;

    for (size_t i = 0; i < n64; i++) {
        u64_1[i] = ~u64_1[i];
    }

    size_t remain = n8 % 8;
    u8_1 += n64 * 8;
    for (size_t i = 0; i < remain; i++) {
        u8_1[i] = ~u8_1[i];
    }

    return *this;
}

bool
ConcurrentBitset::test(id_type_t id) {
    return bitset_[id >> 3].load() & (0x1 << (id & 0x7));
}

void
ConcurrentBitset::set(id_type_t id) {
    bitset_[id >> 3].fetch_or(0x1 << (id & 0x7));
}

void
ConcurrentBitset::clear(id_type_t id) {
    bitset_[id >> 3].fetch_and(~(0x1 << (id & 0x7)));
}

size_t
ConcurrentBitset::capacity() const {
    return capacity_;
}

size_t
ConcurrentBitset::size() const {
    return ((capacity_ + 8 - 1) >> 3);
}

const uint8_t*
ConcurrentBitset::data() const {
    return reinterpret_cast<const uint8_t*>(bitset_.data());
}

uint8_t*
ConcurrentBitset::mutable_data() {
    return reinterpret_cast<uint8_t*>(bitset_.data());
}
}  // namespace faiss
