// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include "nlohmann/json.hpp"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <stdexcept>
#include "log/Log.h"
#include "xxhash.h"  // from xxhash/xxhash

namespace milvus {

const std::string UNSUPPORTED_BF_NAME = "Unsupported BloomFilter";
const std::string BLOCKED_BF_NAME = "BlockedBloomFilter";
const std::string ALWAYS_TRUE_BF_NAME = "AlwaysTrueBloomFilter";

enum class BFType {
    Unsupported = 0,
    AlwaysTrue,  // empty bloom filter
    Blocked,
};

inline std::string
BFTypeToString(BFType type) {
    switch (type) {
        case BFType::Blocked:
            return BLOCKED_BF_NAME;
        case BFType::AlwaysTrue:
            return ALWAYS_TRUE_BF_NAME;
        default:
            return UNSUPPORTED_BF_NAME;
    }
}

inline BFType
StringToBFType(std::string_view name) {
    if (name == BLOCKED_BF_NAME) {
        return BFType::Blocked;
    }
    if (name == ALWAYS_TRUE_BF_NAME) {
        return BFType::AlwaysTrue;
    }
    return BFType::Unsupported;
}

class BloomFilter {
 public:
    virtual ~BloomFilter() = default;

    virtual BFType
    Type() const = 0;

    virtual uint64_t
    Cap() const = 0;

    virtual uint32_t
    K() const = 0;

    virtual void
    Add(const unsigned char* data, size_t len) = 0;

    virtual void
    Add(std::string_view data) = 0;

    virtual bool
    Test(const unsigned char* data, size_t len) const = 0;

    virtual bool
    Test(std::string_view data) const = 0;

    virtual bool
    TestLocations(const std::vector<uint64_t>& locs) const = 0;

    virtual std::vector<bool>
    BatchTestLocations(const std::vector<std::vector<uint64_t>>& locs,
                       const std::vector<bool>& hits) const = 0;

    virtual nlohmann::json
    ToJson() const = 0;
};
using BloomFilterPtr = std::shared_ptr<BloomFilter>;

class BlockedBloomFilter : public BloomFilter {
 public:
    BlockedBloomFilter(uint64_t capacity, double fp) {
        double m = -static_cast<double>(capacity) * std::log(fp) /
                   (std::log(2.0) * std::log(2.0));
        num_bits_ = static_cast<uint64_t>(std::ceil(m));

        double k = (m / capacity) * std::log(2.0);
        k_ = std::max(1u, static_cast<uint32_t>(std::round(k)));

        size_t num_blocks = (num_bits_ + 63) / 64;
        bits_.resize(num_blocks, 0);

        LOG_DEBUG(
            "Created BlockedBloomFilter: capacity={}, fp={}, bits={}, k={}",
            capacity,
            fp,
            num_bits_,
            k_);
    }

    explicit BlockedBloomFilter(const nlohmann::json& data) {
        if (!data.contains("bits") || !data.contains("num_bits") ||
            !data.contains("k")) {
            throw std::runtime_error(
                "Invalid JSON for BlockedBloomFilter: missing required fields");
        }

        bits_ = data["bits"].get<std::vector<uint64_t>>();
        num_bits_ = data["num_bits"].get<uint64_t>();
        k_ = data["k"].get<uint32_t>();
    }

    BFType
    Type() const override {
        return BFType::Blocked;
    }

    uint64_t
    Cap() const override {
        return num_bits_;
    }

    uint32_t
    K() const override {
        return k_;
    }

    void
    Add(const uint8_t* data, size_t len) override {
        uint64_t hash = XXH3_64bits(data, len);
        AddHash(hash);
    }

    void
    Add(std::string_view data) override {
        uint64_t hash = XXH3_64bits(data.data(), data.size());
        AddHash(hash);
    }

    bool
    Test(const uint8_t* data, size_t len) const override {
        uint64_t hash = XXH3_64bits(data, len);
        return TestHash(hash);
    }

    bool
    Test(std::string_view data) const override {
        uint64_t hash = XXH3_64bits(data.data(), data.size());
        return TestHash(hash);
    }

    bool
    TestLocations(const std::vector<uint64_t>& locs) const override {
        if (locs.size() != 1) {
            return true;
        }
        return TestHash(locs[0]);
    }

    std::vector<bool>
    BatchTestLocations(const std::vector<std::vector<uint64_t>>& locs,
                       const std::vector<bool>& hits) const override {
        std::vector<bool> ret(locs.size(), false);
        for (size_t i = 0; i < hits.size(); ++i) {
            if (!hits[i]) {
                if (locs[i].size() != 1) {
                    ret[i] = true;
                    continue;
                }
                ret[i] = TestHash(locs[i][0]);
            }
        }
        return ret;
    }

    nlohmann::json
    ToJson() const override {
        nlohmann::json data;
        data["type"] = BFTypeToString(Type());
        data["bits"] = bits_;
        data["num_bits"] = num_bits_;
        data["k"] = k_;
        return data;
    }

 private:
    void
    AddHash(uint64_t hash) {
        uint64_t h1 = hash;
        uint64_t h2 = hash >> 32;

        for (uint32_t i = 0; i < k_; ++i) {
            uint64_t combined_hash = h1 + i * h2;
            uint64_t bit_pos = combined_hash % num_bits_;
            uint64_t block_idx = bit_pos / 64;
            uint64_t bit_idx = bit_pos % 64;
            bits_[block_idx] |= (1ULL << bit_idx);
        }
    }

    bool
    TestHash(uint64_t hash) const {
        uint64_t h1 = hash;
        uint64_t h2 = hash >> 32;

        for (uint32_t i = 0; i < k_; ++i) {
            uint64_t combined_hash = h1 + i * h2;
            uint64_t bit_pos = combined_hash % num_bits_;
            uint64_t block_idx = bit_pos / 64;
            uint64_t bit_idx = bit_pos % 64;
            if ((bits_[block_idx] & (1ULL << bit_idx)) == 0) {
                return false;
            }
        }
        return true;
    }

 private:
    std::vector<uint64_t> bits_;
    uint64_t num_bits_;
    uint32_t k_;
};

class AlwaysTrueBloomFilter : public BloomFilter {
 public:
    BFType
    Type() const override {
        return BFType::AlwaysTrue;
    }

    uint64_t
    Cap() const override {
        return 0;
    }

    uint32_t
    K() const override {
        return 0;
    }

    void
    Add(const unsigned char* data, size_t len) override {
    }

    void
    Add(std::string_view data) override {
    }

    bool
    Test(const unsigned char* data, size_t len) const override {
        return true;
    }

    bool
    Test(std::string_view data) const override {
        return true;
    }

    bool
    TestLocations(const std::vector<uint64_t>& locs) const override {
        return true;
    }

    std::vector<bool>
    BatchTestLocations(const std::vector<std::vector<uint64_t>>& locs,
                       const std::vector<bool>& hits) const override {
        return std::vector<bool>(locs.size(), true);  // 全部返回 true
    }

    nlohmann::json
    ToJson() const override {
        nlohmann::json data;
        data["type"] = BFTypeToString(Type());
        return data;
    }
};

static const BloomFilterPtr g_always_true_bf =
    std::make_shared<AlwaysTrueBloomFilter>();

inline BloomFilterPtr
NewBloomFilterWithType(uint64_t capacity, double fp, BFType type) {
    switch (type) {
        case BFType::Blocked:
            return std::make_shared<BlockedBloomFilter>(capacity, fp);
        case BFType::AlwaysTrue:
            return g_always_true_bf;
        default:
            LOG_WARN(
                "Unsupported bloom filter type {}, falling back to BlockedBF",
                static_cast<int>(type));
            return std::make_shared<BlockedBloomFilter>(capacity, fp);
    }
}

inline BloomFilterPtr
NewBloomFilterWithType(uint64_t capacity,
                       double fp,
                       std::string_view type_name) {
    BFType type = StringToBFType(type_name);
    return NewBloomFilterWithType(capacity, fp, type);
}

inline BloomFilterPtr
BloomFilterFromJson(const nlohmann::json& data) {
    if (!data.contains("type")) {
        throw std::runtime_error(
            "JSON data for bloom filter missing 'type' field");
    }

    std::string type_str = data["type"].get<std::string>();
    BFType type = StringToBFType(type_str);

    switch (type) {
        case BFType::Blocked:
            return std::make_shared<BlockedBloomFilter>(data);
        case BFType::AlwaysTrue:
            return g_always_true_bf;
        default:
            throw std::runtime_error("Unsupported bloom filter type: " +
                                     type_str);
    }
}

inline std::vector<uint64_t>
Locations(const uint8_t* data, size_t len, uint32_t k, BFType bf_type) {
    switch (bf_type) {
        case BFType::Blocked:
            return {XXH3_64bits(data, len)};
        case BFType::AlwaysTrue:
            return {};
        default:
            LOG_WARN(
                "Unsupported bloom filter type in Locations, returning empty");
            return {};
    }
}

inline std::vector<uint64_t>
Locations(std::string_view data, uint32_t k, BFType bf_type) {
    return Locations(
        reinterpret_cast<const uint8_t*>(data.data()), data.size(), k, bf_type);
}

}  // namespace milvus