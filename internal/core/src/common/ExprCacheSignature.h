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

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <fmt/format.h>
#include <xxhash.h>

namespace milvus {

// Hashes route lookups; the shared canonical input and derivation domains
// establish identity. A 128-bit collision must never reuse another predicate's
// result. Copies and derived signatures share the potentially large input.
struct ExprCacheSignature {
    uint64_t low{0};
    uint64_t high{0};

    ExprCacheSignature() = default;
    ExprCacheSignature(const std::string& text)
        : ExprCacheSignature(std::string_view(text)) {
    }
    ExprCacheSignature(std::string&& text)
        : ExprCacheSignature(
              std::make_shared<const std::string>(std::move(text))) {
    }
    ExprCacheSignature(const char* text)
        : ExprCacheSignature(std::string_view(text)) {
    }
    ExprCacheSignature(std::string_view text)
        : ExprCacheSignature(std::make_shared<const std::string>(text)) {
    }

    bool
    operator==(const ExprCacheSignature& other) const {
        if (low != other.low || high != other.high) {
            return false;
        }
        if (identity_ == other.identity_) {
            return true;
        }
        if (!identity_ || !other.identity_ ||
            identity_->domains != other.identity_->domains) {
            return false;
        }
        const auto& text = identity_->text;
        const auto& other_text = other.identity_->text;
        return text == other_text ||
               (text && other_text && *text == *other_text);
    }

    ExprCacheSignature
    Derive(uint64_t domain) const {
        // Hash only the fixed-width parent hash and new domain. Exact equality
        // also checks the original input and EVERY domain, including TTL time.
        const std::array<uint64_t, 3> input{low, high, domain};
        const auto hash = XXH3_128bits(input.data(), sizeof(input));
        auto identity = std::make_shared<Identity>();
        if (identity_) {
            identity->text = identity_->text;
            identity->domains = identity_->domains;
        }
        identity->domains.push_back(domain);
        ExprCacheSignature derived;
        derived.low = hash.low64;
        derived.high = hash.high64;
        derived.identity_ = std::move(identity);
        return derived;
    }

    // Conservative retained heap estimate for cache accounting. Although
    // signatures share this storage, each entry is charged for the full key
    // so eviction/replacement cannot leave unaccounted string ownership.
    size_t
    RetainedBytes() const {
        if (!identity_) {
            return 0;
        }
        const auto text_bytes = identity_->text
                                    ? sizeof(std::string) + 2 * sizeof(void*) +
                                          identity_->text->capacity() + 1
                                    : 0;
        return sizeof(Identity) + 2 * sizeof(void*) + text_bytes +
               identity_->domains.capacity() * sizeof(uint64_t);
    }

 private:
    friend struct ExprCacheSignatureTestAccess;

    struct Identity {
        std::shared_ptr<const std::string> text;
        std::vector<uint64_t> domains;
    };

    explicit ExprCacheSignature(std::shared_ptr<const std::string> text) {
        const auto hash = XXH3_128bits(text->data(), text->size());
        low = hash.low64;
        high = hash.high64;
        auto identity = std::make_shared<Identity>();
        identity->text = std::move(text);
        identity_ = std::move(identity);
    }

    std::shared_ptr<const Identity> identity_;
};

struct ExprCacheSignatureHasher {
    size_t
    operator()(const ExprCacheSignature& signature) const noexcept {
        return signature.low ^ signature.high;
    }
};
}  // namespace milvus

template <>
struct fmt::formatter<milvus::ExprCacheSignature>
    : fmt::formatter<std::string> {
    template <typename FormatContext>
    auto
    format(const milvus::ExprCacheSignature& value, FormatContext& ctx) const {
        return fmt::format_to(
            ctx.out(), "{:016x}{:016x}", value.high, value.low);
    }
};
