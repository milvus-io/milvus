// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#ifdef __SSE2__
#include <emmintrin.h>
#endif

namespace milvus {

/// Volnitsky substring search — O(n/m) average for needles >= 4 bytes.
///
/// Algorithm based on http://volnitsky.com/project/str_search/
/// Key optimizations inspired by ClickHouse's implementation:
/// - 64KB bigram hash table (fits L2 cache), heap-allocated
/// - Open-addressing linear probing for hash collisions
/// - Tail scanning after main loop to avoid missing matches
/// - SSE2 memchr-based fallback for short needles
/// - Word-aligned fast comparison before full memcmp
class VolnitskySearcher {
 public:
    static constexpr size_t kHashSize = 64 * 1024;  // 2^16
    static constexpr size_t kMinNeedleLen = 4;

    explicit VolnitskySearcher(std::string_view needle)
        : needle_(needle.data()),
          needle_size_(needle.size()),
          use_volnitsky_(false) {
        if (needle_size_ >= kMinNeedleLen && needle_size_ <= 255) {
            use_volnitsky_ = true;
            step_ = needle_size_ - 1;
            std::memset(hash_, 0, sizeof(hash_));

            // Insert bigrams from END to START so first (leftmost)
            // occurrence wins — it produces the largest skip distance.
            for (auto i = static_cast<ssize_t>(needle_size_ - 2); i >= 0; --i) {
                uint16_t bg = readBigram(needle_ + i);
                putBigram(bg, static_cast<uint8_t>(i + 1));
            }
        }
    }

    /// Returns true if haystack contains needle as a substring.
    bool
    contains(std::string_view haystack) const {
        if (needle_size_ == 0)
            return true;
        if (haystack.size() < needle_size_)
            return false;

        if (!use_volnitsky_) {
            return fallbackSearch(
                       reinterpret_cast<const uint8_t*>(haystack.data()),
                       reinterpret_cast<const uint8_t*>(haystack.data()) +
                           haystack.size()) != nullptr;
        }

        const auto* h = reinterpret_cast<const uint8_t*>(haystack.data());
        const auto* h_end = h + haystack.size();
        const auto* n = reinterpret_cast<const uint8_t*>(needle_);

        // Main Volnitsky loop: pos points to where we read the bigram,
        // aligned to where needle's last bigram would sit.
        const auto* pos = h + needle_size_ - 2;

        for (; pos <= h_end - 2; pos += step_) {
            uint16_t bg = readBigram(pos);

            // Walk the open-addressing chain
            for (size_t cell = bg % kHashSize; hash_[cell] != 0;
                 cell = (cell + 1) % kHashSize) {
                // hash_[cell] = position_in_needle + 1
                const auto* candidate = pos - (hash_[cell] - 1);
                if (candidate >= h && candidate + needle_size_ <= h_end &&
                    fastCompare(candidate, n, needle_size_)) {
                    return true;
                }
            }
        }

        // Tail scan: the main loop may have skipped past potential
        // matches near the end. Scan the remaining tail.
        const auto* tail_start = pos - step_ + 1;
        if (tail_start < h + needle_size_ - 2)
            tail_start = h + needle_size_ - 2;
        return fallbackSearch(tail_start - (needle_size_ - 2), h_end) !=
               nullptr;
    }

 private:
    static inline uint16_t
    readBigram(const void* p) {
        uint16_t v;
        std::memcpy(&v, p, 2);
        return v;
    }

    void
    putBigram(uint16_t bg, uint8_t offset) {
        size_t cell = bg % kHashSize;
        while (hash_[cell] != 0) cell = (cell + 1) % kHashSize;
        hash_[cell] = offset;
    }

    /// Fast comparison: check first 8 bytes with word compare, then
    /// full memcmp.  Avoids function call overhead for mismatches.
    static inline bool
    fastCompare(const uint8_t* a, const uint8_t* b, size_t len) {
        if (len >= 8) {
            uint64_t wa, wb;
            std::memcpy(&wa, a, 8);
            std::memcpy(&wb, b, 8);
            if (wa != wb)
                return false;
            if (len == 8)
                return true;
        }
        return std::memcmp(a, b, len) == 0;
    }

    /// Fallback search for short needles or tail scanning.
    /// Uses SSE2 memchr + memcmp when available.
    const uint8_t*
    fallbackSearch(const uint8_t* haystack, const uint8_t* h_end) const {
        if (needle_size_ == 0)
            return haystack;
        if (haystack + needle_size_ > h_end)
            return nullptr;

        const auto* n = reinterpret_cast<const uint8_t*>(needle_);

#ifdef __SSE2__
        // Use SSE2 to find first byte of needle, then memcmp
        const __m128i first = _mm_set1_epi8(n[0]);
        const auto* pos = haystack;

        while (pos + 16 <= h_end) {
            __m128i block =
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos));
            int mask = _mm_movemask_epi8(_mm_cmpeq_epi8(block, first));

            while (mask != 0) {
                int bit = __builtin_ctz(mask);
                const auto* candidate = pos + bit;
                if (candidate + needle_size_ <= h_end &&
                    std::memcmp(candidate, n, needle_size_) == 0) {
                    return candidate;
                }
                mask &= mask - 1;  // clear lowest set bit
            }
            pos += 16;
        }

        // Handle remaining bytes
        while (pos + needle_size_ <= h_end) {
            if (*pos == n[0] && std::memcmp(pos, n, needle_size_) == 0) {
                return pos;
            }
            ++pos;
        }
        return nullptr;
#else
        // Non-SSE2 fallback: use memchr + memcmp
        const auto* pos = haystack;
        while (pos + needle_size_ <= h_end) {
            pos = reinterpret_cast<const uint8_t*>(
                std::memchr(pos, n[0], h_end - pos - needle_size_ + 1));
            if (!pos)
                return nullptr;
            if (std::memcmp(pos, n, needle_size_) == 0)
                return pos;
            ++pos;
        }
        return nullptr;
#endif
    }

    const char* needle_;
    size_t needle_size_;
    bool use_volnitsky_;
    size_t step_ = 0;
    uint8_t hash_[kHashSize]{};
};

}  // namespace milvus
