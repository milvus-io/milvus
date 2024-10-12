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
#include <cstdint>

namespace milvus {
namespace bits {

template <typename T, typename U>
constexpr T
roundUp(T value, U factor) {
    return (value + (factor - 1)) / factor * factor;
}

constexpr uint64_t
nBytes(int32_t value) {
    return roundUp(value, 8) / 8;
}

constexpr inline uint64_t
lowMask(int32_t bits) {
    return (1UL << bits) - 1;
}

inline int32_t
getAndClearLastSetBit(uint16_t& bits) {
    int32_t trailingZeros = __builtin_ctz(bits);
    bits &= bits - 1;
    return trailingZeros;
}

constexpr inline uint64_t
highMask(int32_t bits) {
    return lowMask(bits) << (64 - bits);
}

/**
 * Invokes a function for each batch of bits (partial or full words)
 * in a given range.
 *
 * @param begin first bit to check (inclusive)
 * @param end last bit to check (exclusive)
 * @param partialWordFunc function to invoke for a partial word;
 *  takes index of the word and mask
 * @param fullWordFunc function to invoke for a full word;
 *  takes index of the word
 */
template <typename PartialWordFunc, typename FullWordFunc>
inline void
forEachWord(int32_t begin,
            int32_t end,
            PartialWordFunc partialWordFunc,
            FullWordFunc fullWordFunc) {
    if (begin >= end) {
        return;
    }
    int32_t firstWord = roundUp(begin, 64);
    int32_t lastWord = end & ~63L;
    if (lastWord < firstWord) {
        partialWordFunc(lastWord / 64,
                        lowMask(end - lastWord) & highMask(firstWord - begin));
        return;
    }
    if (begin != firstWord) {
        partialWordFunc(begin / 64, highMask(firstWord - begin));
    }
    for (int32_t i = firstWord; i + 64 <= lastWord; i += 64) {
        fullWordFunc(i / 64);
    }
    if (end != lastWord) {
        partialWordFunc(lastWord / 64, lowMask(end - lastWord));
    }
}

inline int32_t
countBits(const uint64_t* bits, int32_t begin, int32_t end) {
    int32_t count = 0;
    forEachWord(
        begin,
        end,
        [&count, bits](int32_t idx, uint64_t mask) {
            count += __builtin_popcountll(bits[idx] & mask);
        },
        [&count, bits](int32_t idx) {
            count += __builtin_popcountll(bits[idx]);
        });
    return count;
}

inline bool
isPowerOfTwo(uint64_t size) {
    return bits::countBits(&size, 0, sizeof(uint64_t) * 8) <= 1;
}

template <typename T = uint64_t>
inline int32_t
countLeadingZeros(T word) {
    static_assert(std::is_same_v<T, uint64_t> ||
                  std::is_same_v<T, __uint128_t>);
    /// Built-in Function: int __builtin_clz (unsigned int x) returns the number
    /// of leading 0-bits in x, starting at the most significant bit position. If
    /// x is 0, the result is undefined.
    if (word == 0) {
        return sizeof(T) * 8;
    }
    if constexpr (std::is_same_v<T, uint64_t>) {
        return __builtin_clzll(word);
    } else {
        uint64_t hi = word >> 64;
        uint64_t lo = static_cast<uint64_t>(word);
        return (hi == 0) ? 64 + __builtin_clzll(lo) : __builtin_clzll(hi);
    }
}

inline uint64_t
nextPowerOfTwo(uint64_t size) {
    if (size == 0) {
        return 0;
    }
    uint32_t bits = 63 - countLeadingZeros(size);
    uint64_t lower = 1ULL << bits;
    // Size is a power of 2.
    if (lower == size) {
        return size;
    }
    return 2 * lower;
}

// This is the Hash128to64 function from Google's cityhash (available
// under the MIT License).  We use it to reduce multiple 64 bit hashes
// into a single hash.
#if defined(FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER)
FOLLY_DISABLE_UNDEFINED_BEHAVIOR_SANITIZER("unsigned-integer-overflow")
#endif
inline uint64_t
hashMix(const uint64_t upper, const uint64_t lower) noexcept {
    // Murmur-inspired hashing.
    const uint64_t kMul = 0x9ddfea08eb382d69ULL;
    uint64_t a = (lower ^ upper) * kMul;
    a ^= (a >> 47);
    uint64_t b = (upper ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

/// Extract bits from integer 'a' at the corresponding bit locations specified
/// by 'mask' to contiguous low bits in return value; the remaining upper bits
/// in return value are set to zero.
template <typename T>
inline T
extractBits(T a, T mask);

#ifdef __BMI2__
template <>
inline uint32_t
extractBits(uint32_t a, uint32_t mask) {
    return _pext_u32(a, mask);
}
template <>
inline uint64_t
extractBits(uint64_t a, uint64_t mask) {
    return _pext_u64(a, mask);
}
#else
template <typename T>
T
extractBits(T a, T mask) {
    constexpr int kBitsCount = 8 * sizeof(T);
    T dst = 0;
    for (int i = 0, k = 0; i < kBitsCount; ++i) {
        if (mask & 1) {
            dst |= ((a & 1) << k);
            ++k;
        }
        a >>= 1;
        mask >>= 1;
    }
    return dst;
}
#endif
}  // namespace bits
}  // namespace milvus