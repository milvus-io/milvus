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

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>

#include <array>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

using milvus::storage::plugin::ICipherPlugin;
using milvus::storage::plugin::IDecryptor;
using milvus::storage::plugin::IEncryptor;
using milvus::storage::plugin::IPlugin;

constexpr std::string_view kFixtureMasterKey = "milvus-cmek-fixture-master-v1";
constexpr std::string_view kEZKDomain{"ezk-v1\0", 7};
constexpr std::string_view kDEKDomain{"dek-v1\0", 7};
constexpr std::string_view kEDEKDomain{"edek-v1\0", 8};
constexpr std::string_view kEDEKVersion = "v1";
constexpr size_t kNonceSize = 16;
constexpr size_t kSHA256Size = 32;

void
appendInt64(std::string& output, int64_t value) {
    auto encoded = static_cast<uint64_t>(value);
    for (int shift = 56; shift >= 0; shift -= 8) {
        output.push_back(static_cast<char>((encoded >> shift) & 0xff));
    }
}

std::string
hmacSHA256(std::string_view key, std::string_view message) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int digest_len = 0;
    auto* result = HMAC(EVP_sha256(),
                        key.data(),
                        static_cast<int>(key.size()),
                        reinterpret_cast<const unsigned char*>(message.data()),
                        message.size(),
                        digest.data(),
                        &digest_len);
    if (result == nullptr || digest_len != kSHA256Size) {
        throw std::runtime_error("fixture cipher HMAC-SHA256 failed");
    }
    return {reinterpret_cast<const char*>(digest.data()), digest_len};
}

std::string
deriveEZKey(int64_t ez_id) {
    std::string message(kEZKDomain);
    appendInt64(message, ez_id);
    return hmacSHA256(kFixtureMasterKey, message);
}

std::string
deriveContextKey(std::string_view ezk,
                 std::string_view domain,
                 std::string_view nonce,
                 int64_t ez_id,
                 int64_t coll_id) {
    std::string message(domain);
    message.append(nonce);
    appendInt64(message, ez_id);
    appendInt64(message, coll_id);
    return hmacSHA256(ezk, message);
}

std::string
deriveDataKey(std::string_view ezk,
              std::string_view nonce,
              int64_t ez_id,
              int64_t coll_id) {
    return deriveContextKey(ezk, kDEKDomain, nonce, ez_id, coll_id);
}

std::string
deriveEDEKTag(std::string_view ezk,
              std::string_view nonce,
              int64_t ez_id,
              int64_t coll_id) {
    return deriveContextKey(ezk, kEDEKDomain, nonce, ez_id, coll_id);
}

std::string
hexEncode(std::string_view value) {
    std::ostringstream encoded;
    encoded << std::hex << std::setfill('0');
    for (unsigned char byte : value) {
        encoded << std::setw(2) << static_cast<unsigned int>(byte);
    }
    return encoded.str();
}

uint8_t
decodeHexDigit(char value) {
    if (value >= '0' && value <= '9') {
        return static_cast<uint8_t>(value - '0');
    }
    if (value >= 'a' && value <= 'f') {
        return static_cast<uint8_t>(value - 'a' + 10);
    }
    throw std::runtime_error("fixture cipher EDEK must use lowercase hex");
}

std::string
hexDecode(std::string_view value, size_t expected_size) {
    if (value.size() != expected_size * 2) {
        throw std::runtime_error(
            "fixture cipher EDEK field has invalid length");
    }
    std::string decoded(expected_size, '\0');
    for (size_t i = 0; i < expected_size; ++i) {
        decoded[i] = static_cast<char>((decodeHexDigit(value[i * 2]) << 4) |
                                       decodeHexDigit(value[i * 2 + 1]));
    }
    return decoded;
}

struct DecodedEDEK {
    std::string nonce;
    std::string tag;
};

DecodedEDEK
decodeEDEK(std::string_view edek) {
    auto first_separator = edek.find(':');
    auto second_separator = edek.find(':', first_separator + 1);
    if (first_separator == std::string_view::npos ||
        second_separator == std::string_view::npos ||
        edek.find(':', second_separator + 1) != std::string_view::npos) {
        throw std::runtime_error("fixture cipher EDEK has invalid field count");
    }
    if (edek.substr(0, first_separator) != kEDEKVersion) {
        throw std::runtime_error("fixture cipher EDEK has unsupported version");
    }
    return {
        hexDecode(edek.substr(first_separator + 1,
                              second_separator - first_separator - 1),
                  kNonceSize),
        hexDecode(edek.substr(second_separator + 1), kSHA256Size),
    };
}

bool
constantTimeEqual(std::string_view left, std::string_view right) {
    return left.size() == right.size() &&
           CRYPTO_memcmp(left.data(), right.data(), left.size()) == 0;
}

std::string
base64Encode(std::string_view value) {
    std::string encoded(4 * ((value.size() + 2) / 3), '\0');
    auto size =
        EVP_EncodeBlock(reinterpret_cast<unsigned char*>(encoded.data()),
                        reinterpret_cast<const unsigned char*>(value.data()),
                        static_cast<int>(value.size()));
    if (size < 0) {
        throw std::runtime_error("fixture cipher base64 encoding failed");
    }
    encoded.resize(static_cast<size_t>(size));
    return encoded;
}

std::string
newNonce() {
    std::string nonce(kNonceSize, '\0');
    if (RAND_bytes(reinterpret_cast<unsigned char*>(nonce.data()),
                   nonce.size()) != 1) {
        throw std::runtime_error("fixture cipher nonce generation failed");
    }
    return nonce;
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
        static_cast<void>(coll_id);
        if (root_key.empty()) {
            return;
        }
        auto expected_key = base64Encode(deriveEZKey(ez_id));
        if (!constantTimeEqual(root_key, expected_key)) {
            throw std::runtime_error(
                "fixture cipher received an unexpected EZ key for EZ " +
                std::to_string(ez_id));
        }
    }

    std::pair<std::shared_ptr<IEncryptor>, std::string>
    GetEncryptor(int64_t ez_id, int64_t coll_id) const override {
        auto ezk = deriveEZKey(ez_id);
        auto nonce = newNonce();
        auto tag = deriveEDEKTag(ezk, nonce, ez_id, coll_id);
        auto edek = std::string(kEDEKVersion) + ":" + hexEncode(nonce) + ":" +
                    hexEncode(tag);
        auto dek = deriveDataKey(ezk, nonce, ez_id, coll_id);
        return {std::make_shared<FixtureEncryptor>(std::move(dek)),
                std::move(edek)};
    }

    std::shared_ptr<IDecryptor>
    GetDecryptor(int64_t ez_id,
                 int64_t coll_id,
                 const std::string& safe_key) const override {
        auto decoded = decodeEDEK(safe_key);
        auto ezk = deriveEZKey(ez_id);
        auto expected_tag = deriveEDEKTag(ezk, decoded.nonce, ez_id, coll_id);
        if (!constantTimeEqual(decoded.tag, expected_tag)) {
            throw std::runtime_error(
                "fixture cipher EDEK authentication failed for EZ " +
                std::to_string(ez_id) + " and collection " +
                std::to_string(coll_id));
        }
        auto dek = deriveDataKey(ezk, decoded.nonce, ez_id, coll_id);
        return std::make_shared<FixtureDecryptor>(std::move(dek));
    }
};

}  // namespace

extern "C" IPlugin*
CreatePlugin() {
    return new FixtureCipherPlugin();
}
