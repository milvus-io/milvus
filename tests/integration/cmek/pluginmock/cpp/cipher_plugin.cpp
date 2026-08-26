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

#include "storage/plugin/PluginInterface.h"

#include <atomic>
#include <cstdint>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace {

using milvus::storage::plugin::ICipherPlugin;
using milvus::storage::plugin::IDecryptor;
using milvus::storage::plugin::IEncryptor;
using milvus::storage::plugin::IPlugin;

struct ContextKey {
    int64_t ez_id;
    int64_t coll_id;

    bool
    operator==(const ContextKey& other) const {
        return ez_id == other.ez_id && coll_id == other.coll_id;
    }
};

struct ContextKeyHash {
    size_t
    operator()(const ContextKey& key) const {
        auto ez_hash = std::hash<int64_t>{}(key.ez_id);
        auto coll_hash = std::hash<int64_t>{}(key.coll_id);
        return ez_hash ^
               (coll_hash + 0x9e3779b9 + (ez_hash << 6) + (ez_hash >> 2));
    }
};

uint64_t
fnv1a(const std::string& value) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : value) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string
makeKey(const std::string& seed) {
    std::string key(32, '\0');
    auto hash = fnv1a(seed);
    for (size_t i = 0; i < key.size(); ++i) {
        hash ^= hash >> 12;
        hash ^= hash << 25;
        hash ^= hash >> 27;
        key[i] = static_cast<char>((hash * 2685821657736338717ULL) >> 56);
    }
    return key;
}

std::string
hexEncode(const std::string& value) {
    std::ostringstream encoded;
    encoded << std::hex << std::setfill('0');
    for (unsigned char byte : value) {
        encoded << std::setw(2) << static_cast<unsigned int>(byte);
    }
    return encoded.str();
}

std::string
xorBytes(const void* data, size_t len, const std::string& key) {
    if (key.empty()) {
        throw std::runtime_error("fixture cipher key is empty");
    }
    const auto* input = static_cast<const uint8_t*>(data);
    std::string output(len, '\0');
    for (size_t i = 0; i < len; ++i) {
        output[i] = static_cast<char>(
            input[i] ^ static_cast<uint8_t>(key[i % key.size()]));
    }
    return output;
}

class FixtureEncryptor final : public IEncryptor {
 public:
    explicit FixtureEncryptor(std::string key) : key_(std::move(key)) {
    }

    std::string
    Encrypt(const std::string& plaintext) const override {
        return xorBytes(plaintext.data(), plaintext.size(), key_);
    }

    std::string
    Encrypt(std::string_view plaintext) const override {
        return xorBytes(plaintext.data(), plaintext.size(), key_);
    }

    std::string
    Encrypt(const void* data, size_t len) const override {
        return xorBytes(data, len, key_);
    }

    std::string
    GetKey() const override {
        return key_;
    }

 private:
    std::string key_;
};

class FixtureDecryptor final : public IDecryptor {
 public:
    explicit FixtureDecryptor(std::string key) : key_(std::move(key)) {
    }

    std::string
    Decrypt(const std::string& ciphertext) const override {
        return xorBytes(ciphertext.data(), ciphertext.size(), key_);
    }

    std::string
    Decrypt(std::string_view ciphertext) const override {
        return xorBytes(ciphertext.data(), ciphertext.size(), key_);
    }

    std::string
    Decrypt(const void* data, size_t len) const override {
        return xorBytes(data, len, key_);
    }

    std::string
    GetKey() const override {
        return key_;
    }

 private:
    std::string key_;
};

class FixtureCipherPlugin final : public ICipherPlugin {
 public:
    std::string
    getPluginName() const override {
        return "CipherPlugin";
    }

    void
    Update(int64_t ez_id,
           int64_t coll_id,
           const std::string& root_key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        ContextKey context{ez_id, coll_id};
        if (root_key.empty()) {
            keys_.erase(context);
            return;
        }
        keys_[context] = root_key;
    }

    std::pair<std::shared_ptr<IEncryptor>, std::string>
    GetEncryptor(int64_t ez_id, int64_t coll_id) const override {
        auto root_key = getRootKey(ez_id, coll_id);
        auto sequence = sequence_.fetch_add(1, std::memory_order_relaxed);
        // The EDEK is embedded in V3 directory JSON, so keep it printable
        // while retaining the EDEK-dependent cipher key derivation.
        auto edek =
            hexEncode(makeKey(root_key + "/edek/" + std::to_string(sequence)));
        auto cipher_key = makeKey(root_key + "/" + edek);
        return {std::make_shared<FixtureEncryptor>(cipher_key), edek};
    }

    std::shared_ptr<IDecryptor>
    GetDecryptor(int64_t ez_id,
                 int64_t coll_id,
                 const std::string& safeKey) const override {
        auto root_key = getRootKey(ez_id, coll_id);
        return std::make_shared<FixtureDecryptor>(
            makeKey(root_key + "/" + safeKey));
    }

 private:
    std::string
    getRootKey(int64_t ez_id, int64_t coll_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = keys_.find({ez_id, coll_id});
        if (it != keys_.end()) {
            return it->second;
        }
        // Contexts are scoped by collection. Do not reuse an EZ-level key:
        // doing so would let a missing load-time collection context reuse a
        // key left behind by another storage operation.
        throw std::runtime_error(
            "fixture cipher key is not initialized for EZ " +
            std::to_string(ez_id) + " and collection " +
            std::to_string(coll_id));
    }

    mutable std::mutex mutex_;
    mutable std::atomic<uint64_t> sequence_{1};
    std::unordered_map<ContextKey, std::string, ContextKeyHash> keys_;
};

}  // namespace

extern "C" IPlugin*
CreatePlugin() {
    return new FixtureCipherPlugin();
}
