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

#include <atomic>
#include <memory>
#include <string.h>
#include <stdint.h>
#include <vector>

namespace faiss {

class ConcurrentBitset {
 public:
    using id_type_t = int64_t;

    explicit ConcurrentBitset(size_t count, uint8_t init_value = 0)
    : count_(count), bitset_(((count + 8 - 1) >> 3)) {
        if (init_value) {
            memset(mutable_data(), init_value, (count + 8 - 1) >> 3);
        }
    }

    explicit ConcurrentBitset(size_t count, const uint8_t* data) : bitset_(((count + 8 - 1) >> 3)) {
        memcpy(mutable_data(), data, (count + 8 - 1) >> 3);
    }

    ConcurrentBitset&
    operator&=(const ConcurrentBitset& bitset) {
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
    operator&(const ConcurrentBitset& bitset) const {
        auto result_bitset = std::make_shared<ConcurrentBitset>(bitset.count());

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
    operator|=(const ConcurrentBitset& bitset) {
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
    operator|(const ConcurrentBitset& bitset) const {
        auto result_bitset = std::make_shared<ConcurrentBitset>(bitset.count());

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
    negate() {
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
    test(id_type_t id) {
        unsigned char mask = (unsigned char)(0x01) << (id & 0x07);
        return (bitset_[id >> 3].load() & mask);
    }

    void
    set(id_type_t id) {
        unsigned char mask = (unsigned char)(0x01) << (id & 0x07);
        bitset_[id >> 3].fetch_or(mask);
    }


    void
    clear(id_type_t id) {
        unsigned char mask = (unsigned char)(0x01) << (id & 0x07);
        bitset_[id >> 3].fetch_and(~mask);
    }

    size_t
    count() const {
        return count_;
    }

    size_t
    size() const {
        return ((count_ + 8 - 1) >> 3);
    }

    const uint8_t*
    data() const {
        return reinterpret_cast<const uint8_t*>(bitset_.data());
    }

    uint8_t*
    mutable_data() {
        return reinterpret_cast<uint8_t*>(bitset_.data());
    }

    uint64_t
    count_1() {
        uint64_t ret = 0;
        auto p_data = reinterpret_cast<uint64_t *>(mutable_data());
        auto len = size() >> 3;
        //auto remainder = size() % 8;
        auto popcount8 = [&](uint8_t x) -> int{
            x = (x & 0x55) + ((x >> 1) & 0x55);
            x = (x & 0x33) + ((x >> 2) & 0x33);
            x = (x & 0x0F) + ((x >> 4) & 0x0F);
            return x;
        };
        for (size_t i = 0; i < len; ++ i) {
            ret += __builtin_popcountl(*p_data);
            p_data ++;
        }
        auto p_byte = data() + (len << 3);
        for (auto i = (len << 3); i < size(); ++ i) {
            ret += popcount8(*p_byte);
            p_byte ++;
        }
        return ret;
    }

 private:
    size_t count_;
    std::vector<std::atomic<uint8_t>> bitset_;
};


using ConcurrentBitsetPtr = std::shared_ptr<ConcurrentBitset>;

}  // namespace faiss
