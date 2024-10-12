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
#include <type_traits>

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
    if (bits >= 64)
        return ~0ULL;
    if (bits <= 0)
        return 0ULL;
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
    if (bits >= 64)
        return ~0ULL;
    if (bits <= 0)
        return 0ULL;
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
countBitsScalar(const uint64_t* bits, int32_t begin, int32_t end) {
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

#ifdef __AVX2__
#include <immintrin.h>
inline int32_t
countBitsAVX2(const uint64_t* bits, int32_t begin, int32_t end) {
    int32_t count = 0;

    // Convert bit indices to word-aligned bit indices (same logic as forEachWord)
    int32_t firstWordBit = roundUp(begin, 64);  // first word-aligned bit index
    int32_t lastWordBit = end & ~63L;           // last word-aligned bit index

    // Handle case where range spans less than one word
    if (lastWordBit < firstWordBit) {
        // begin and end are in the same word
        int32_t wordIdx = begin >> 6;
        uint64_t offset = begin & 63;
        uint64_t mask = highMask(64 - offset) & lowMask(end - (wordIdx << 6));
        return __builtin_popcountll(bits[wordIdx] & mask);
    }

    // Handle partial word at the beginning (if begin is not word-aligned)
    if (begin != firstWordBit) {
        int32_t wordIdx = begin >> 6;
        uint64_t offset = begin & 63;
        uint64_t mask = highMask(64 - offset);
        count += __builtin_popcountll(bits[wordIdx] & mask);
    }

    // Process full words using unrolled scalar loop
    // Note: Modern CPUs have fast scalar POPCNT (1 cycle latency, 1 per cycle throughput).
    // The previous AVX2 implementation only used SIMD for load/store, then did scalar
    // popcount anyway, adding overhead without benefit. This simple unrolled loop is
    // faster and more maintainable.
    int32_t wordIdx = firstWordBit >> 6;
    int32_t lastWordIdx = lastWordBit >> 6;

    for (; wordIdx + 4 <= lastWordIdx; wordIdx += 4) {
        count += __builtin_popcountll(bits[wordIdx]);
        count += __builtin_popcountll(bits[wordIdx + 1]);
        count += __builtin_popcountll(bits[wordIdx + 2]);
        count += __builtin_popcountll(bits[wordIdx + 3]);
    }

    // Handle remaining full words
    for (; wordIdx < lastWordIdx; ++wordIdx) {
        count += __builtin_popcountll(bits[wordIdx]);
    }

    // Handle partial word at the end (if end is not word-aligned)
    if (end != lastWordBit) {
        int32_t wordIdx = lastWordBit >> 6;
        uint64_t mask = lowMask(end & 63);
        count += __builtin_popcountll(bits[wordIdx] & mask);
    }

    return count;
}
#endif

#ifdef __AVX512F__
#include <immintrin.h>
inline int32_t
countBitsAVX512(const uint64_t* bits, int32_t begin, int32_t end) {
    if (begin >= end) {
        return 0;
    }

    int32_t count = 0;

    // Convert bit indices to word-aligned bit indices (same logic as forEachWord)
    int32_t firstWordBit = roundUp(begin, 64);  // first word-aligned bit index
    int32_t lastWordBit = end & ~63L;           // last word-aligned bit index

    // Handle case where range spans less than one word
    if (lastWordBit < firstWordBit) {
        // begin and end are in the same word
        int32_t wordIdx = begin >> 6;
        uint64_t offset = begin & 63;
        uint64_t mask = highMask(64 - offset) & lowMask(end - (wordIdx << 6));
        return __builtin_popcountll(bits[wordIdx] & mask);
    }

    // Handle partial word at the beginning (if begin is not word-aligned)
    if (begin != firstWordBit) {
        int32_t wordIdx = begin >> 6;
        uint64_t offset = begin & 63;
        uint64_t mask = highMask(64 - offset);
        count += __builtin_popcountll(bits[wordIdx] & mask);
    }

    // Process full words using AVX-512 (8 words at a time)
    int32_t wordIdx = firstWordBit >> 6;
    int32_t lastWordIdx = lastWordBit >> 6;

    for (; wordIdx + 8 <= lastWordIdx; wordIdx += 8) {
        __m512i v = _mm512_loadu_si512(bits + wordIdx);
        __m512i c = _mm512_popcnt_epi64(v);

        count += static_cast<int32_t>(_mm512_reduce_add_epi64(c));
    }

    // Handle remaining full words
    for (; wordIdx < lastWordIdx; ++wordIdx) {
        count += __builtin_popcountll(bits[wordIdx]);
    }

    // Handle partial word at the end (if end is not word-aligned)
    if (end != lastWordBit) {
        int32_t wordIdx = lastWordBit >> 6;
        uint64_t mask = lowMask(end & 63);
        count += __builtin_popcountll(bits[wordIdx] & mask);
    }

    return count;
}
#endif

inline int32_t
countBits(const uint64_t* bits, int32_t begin, int32_t end) {
    const int32_t range = end - begin;

    if (range < 256) {
        return countBitsScalar(bits, begin, end);
    }

#ifdef __AVX512F__
    if (range >= 2048) {
        return countBitsAVX512(bits, begin, end);
    }
#endif

#ifdef __AVX2__
    return countBitsAVX2(bits, begin, end);
#else
    return countBitsScalar(bits, begin, end);
#endif
}

inline bool
isPowerOfTwo(uint64_t size) {
    return (size > 0) && ((size & (size - 1)) == 0);
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
inline T
extractBits(T a, T mask) {
    static_assert(std::is_unsigned_v<T>, "extractBits requires unsigned type");

    // 1. first try to use BMI2 intrinsic
#ifdef __BMI2__
    if constexpr (sizeof(T) == 8) {
        return _pext_u64(static_cast<uint64_t>(a), static_cast<uint64_t>(mask));
    } else {
        // For uint32_t, uint16_t, uint8_t, use 32-bit PEXT
        return static_cast<T>(
            _pext_u32(static_cast<uint32_t>(a), static_cast<uint32_t>(mask)));
    }
#else
    // 2. Scalar fallback implementation
    T dst = 0;
    for (int k = 0; mask != 0; ++k) {
        // Use ctz implementation for different widths
        int shift;
        if constexpr (sizeof(T) <= sizeof(unsigned int)) {
            shift = __builtin_ctz(static_cast<unsigned int>(mask));
        } else {
            shift = __builtin_ctzll(static_cast<unsigned long long>(mask));
        }

        // Core extraction logic
        dst |= ((a >> shift) & 1) << k;

        // Clear the lowest set bit in mask (efficient clearing method)
        mask &= (mask - 1);
    }
    return dst;
#endif
}
#endif
}  // namespace bits
}  // namespace milvus