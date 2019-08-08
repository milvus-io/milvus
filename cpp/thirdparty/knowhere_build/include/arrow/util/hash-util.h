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

// From Apache Impala (incubating) as of 2016-02-22

#ifndef ARROW_UTIL_HASH_UTIL_H
#define ARROW_UTIL_HASH_UTIL_H

#include <cassert>
#include <cstdint>

#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/neon-util.h"
#include "arrow/util/sse-util.h"

static inline uint32_t HW_crc32_u8(uint32_t crc, uint8_t v) {
  DCHECK(false) << "Hardware CRC support is not enabled";
  return 0;
}

static inline uint32_t HW_crc32_u16(uint32_t crc, uint16_t v) {
  DCHECK(false) << "Hardware CRC support is not enabled";
  return 0;
}

static inline uint32_t HW_crc32_u32(uint32_t crc, uint32_t v) {
  DCHECK(false) << "Hardware CRC support is not enabled";
  return 0;
}

static inline uint32_t HW_crc32_u64(uint32_t crc, uint64_t v) {
  DCHECK(false) << "Hardware CRC support is not enabled";
  return 0;
}

#ifdef ARROW_HAVE_SSE4_2
#define HW_crc32_u8 SSE4_crc32_u8
#define HW_crc32_u16 SSE4_crc32_u16
#define HW_crc32_u32 SSE4_crc32_u32
#define HW_crc32_u64 SSE4_crc32_u64
#elif defined(ARROW_HAVE_ARM_CRC)
#define HW_crc32_u8 ARMCE_crc32_u8
#define HW_crc32_u16 ARMCE_crc32_u16
#define HW_crc32_u32 ARMCE_crc32_u32
#define HW_crc32_u64 ARMCE_crc32_u64
#endif

namespace arrow {

/// Utility class to compute hash values.
class HashUtil {
 public:
#if defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_ARM_CRC)
  static constexpr bool have_hardware_crc32 = true;
#else
  static constexpr bool have_hardware_crc32 = false;
#endif

  /// Compute the Crc32 hash for data using SSE4/ArmCRC instructions.  The input hash
  /// parameter is the current hash/seed value.
  /// This should only be called if SSE/ArmCRC is supported.
  /// This is ~4x faster than Fnv/Boost Hash.
  /// TODO: crc32 hashes with different seeds do not result in different hash functions.
  /// The resulting hashes are correlated.
  static uint32_t CrcHash(const void* data, int32_t nbytes, uint32_t hash) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(data);
    const uint8_t* end = p + nbytes;

#if ARROW_BITNESS >= 64
    while (p <= end - 8) {
      hash = HW_crc32_u64(hash, *reinterpret_cast<const uint64_t*>(p));
      p += 8;
    }
#endif

    while (p <= end - 4) {
      hash = HW_crc32_u32(hash, *reinterpret_cast<const uint32_t*>(p));
      p += 4;
    }
    while (p < end) {
      hash = HW_crc32_u8(hash, *p);
      ++p;
    }

    // The lower half of the CRC hash has has poor uniformity, so swap the halves
    // for anyone who only uses the first several bits of the hash.
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  /// A variant of CRC32 hashing that computes two independent running CRCs
  /// over interleaved halves of the input, giving out a 64-bit integer.
  /// The result's quality should be improved by a finalization step.
  ///
  /// In addition to producing more bits of output, this should be twice
  /// faster than CrcHash on CPUs that can overlap several independent
  /// CRC computations.
  static uint64_t DoubleCrcHash(const void* data, int32_t nbytes, uint64_t hash) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(data);

    uint32_t h1 = static_cast<uint32_t>(hash >> 32);
    uint32_t h2 = static_cast<uint32_t>(hash);

#if ARROW_BITNESS >= 64
    while (nbytes >= 16) {
      h1 = HW_crc32_u64(h1, *reinterpret_cast<const uint64_t*>(p));
      h2 = HW_crc32_u64(h2, *reinterpret_cast<const uint64_t*>(p + 8));
      nbytes -= 16;
      p += 16;
    }
    if (nbytes >= 8) {
      h1 = HW_crc32_u32(h1, *reinterpret_cast<const uint32_t*>(p));
      h2 = HW_crc32_u32(h2, *reinterpret_cast<const uint32_t*>(p + 4));
      nbytes -= 8;
      p += 8;
    }
#else
    while (nbytes >= 8) {
      h1 = HW_crc32_u32(h1, *reinterpret_cast<const uint32_t*>(p));
      h2 = HW_crc32_u32(h2, *reinterpret_cast<const uint32_t*>(p + 4));
      nbytes -= 8;
      p += 8;
    }
#endif

    if (nbytes >= 4) {
      h1 = HW_crc32_u16(h1, *reinterpret_cast<const uint16_t*>(p));
      h2 = HW_crc32_u16(h2, *reinterpret_cast<const uint16_t*>(p + 2));
      nbytes -= 4;
      p += 4;
    }
    switch (nbytes) {
      case 3:
        h1 = HW_crc32_u8(h1, p[2]);
        // fallthrough
      case 2:
        h2 = HW_crc32_u8(h2, p[1]);
        // fallthrough
      case 1:
        h1 = HW_crc32_u8(h1, p[0]);
        // fallthrough
      case 0:
        break;
      default:
        assert(0);
    }

    // A finalization step is recommended to mix up the result's bits
    return (static_cast<uint64_t>(h1) << 32) + h2;
  }

  static const uint64_t MURMUR_PRIME = 0xc6a4a7935bd1e995;
  static const int MURMUR_R = 47;

  /// Murmur2 hash implementation returning 64-bit hashes.
  static uint64_t MurmurHash2_64(const void* input, int len, uint64_t seed) {
    uint64_t h = seed ^ (len * MURMUR_PRIME);

    const uint64_t* data = reinterpret_cast<const uint64_t*>(input);
    const uint64_t* end = data + (len / sizeof(uint64_t));

    while (data != end) {
      uint64_t k = *data++;
      k *= MURMUR_PRIME;
      k ^= k >> MURMUR_R;
      k *= MURMUR_PRIME;
      h ^= k;
      h *= MURMUR_PRIME;
    }

    const uint8_t* data2 = reinterpret_cast<const uint8_t*>(data);
    switch (len & 7) {
      case 7:
        h ^= uint64_t(data2[6]) << 48;
      case 6:
        h ^= uint64_t(data2[5]) << 40;
      case 5:
        h ^= uint64_t(data2[4]) << 32;
      case 4:
        h ^= uint64_t(data2[3]) << 24;
      case 3:
        h ^= uint64_t(data2[2]) << 16;
      case 2:
        h ^= uint64_t(data2[1]) << 8;
      case 1:
        h ^= uint64_t(data2[0]);
        h *= MURMUR_PRIME;
    }

    h ^= h >> MURMUR_R;
    h *= MURMUR_PRIME;
    h ^= h >> MURMUR_R;
    return h;
  }

  /// default values recommended by http://isthe.com/chongo/tech/comp/fnv/
  static const uint32_t FNV_PRIME = 0x01000193;  //   16777619
  static const uint32_t FNV_SEED = 0x811C9DC5;   // 2166136261
  static const uint64_t FNV64_PRIME = 1099511628211UL;
  static const uint64_t FNV64_SEED = 14695981039346656037UL;

  /// Implementation of the Fowler-Noll-Vo hash function. This is not as performant
  /// as boost's hash on int types (2x slower) but has bit entropy.
  /// For ints, boost just returns the value of the int which can be pathological.
  /// For example, if the data is <1000, 2000, 3000, 4000, ..> and then the mod of 1000
  /// is taken on the hash, all values will collide to the same bucket.
  /// For string values, Fnv is slightly faster than boost.
  /// IMPORTANT: FNV hash suffers from poor diffusion of the least significant bit,
  /// which can lead to poor results when input bytes are duplicated.
  /// See FnvHash64to32() for how this can be mitigated.
  static uint64_t FnvHash64(const void* data, int32_t bytes, uint64_t hash) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
    while (bytes--) {
      hash = (*ptr ^ hash) * FNV64_PRIME;
      ++ptr;
    }
    return hash;
  }

  /// Return a 32-bit hash computed by invoking FNV-64 and folding the result to 32-bits.
  /// This technique is recommended instead of FNV-32 since the LSB of an FNV hash is the
  /// XOR of the LSBs of its input bytes, leading to poor results for duplicate inputs.
  /// The input seed 'hash' is duplicated so the top half of the seed is not all zero.
  /// Data length must be at least 1 byte: zero-length data should be handled separately,
  /// for example using CombineHash with a unique constant value to avoid returning the
  /// hash argument. Zero-length data gives terrible results: the initial hash value is
  /// xored with itself cancelling all bits.
  static uint32_t FnvHash64to32(const void* data, int32_t bytes, uint32_t hash) {
    // IMPALA-2270: this function should never be used for zero-byte inputs.
    DCHECK_GT(bytes, 0);
    uint64_t hash_u64 = hash | (static_cast<uint64_t>(hash) << 32);
    hash_u64 = FnvHash64(data, bytes, hash_u64);
    return static_cast<uint32_t>((hash_u64 >> 32) ^ (hash_u64 & 0xFFFFFFFF));
  }

  // Hash template
  template <bool hw>
  static inline int Hash(const void* data, int32_t bytes, uint32_t seed);

  /// The magic number (used in hash_combine()) 0x9e3779b9 = 2^32 / (golden ratio).
  static const uint32_t HASH_COMBINE_SEED = 0x9e3779b9;

  /// Combine hashes 'value' and 'seed' to get a new hash value.  Similar to
  /// boost::hash_combine(), but for uint32_t. This function should be used with a
  /// constant first argument to update the hash value for zero-length values such as
  /// NULL, boolean, and empty strings.
  static inline uint32_t HashCombine32(uint32_t value, uint32_t seed) {
    return seed ^ (HASH_COMBINE_SEED + value + (seed << 6) + (seed >> 2));
  }

  // Get 32 more bits of randomness from a 32-bit hash:
  static inline uint32_t Rehash32to32(const uint32_t hash) {
    // Constants generated by uuidgen(1) with the -r flag
    static const uint64_t m = 0x7850f11ec6d14889ull, a = 0x6773610597ca4c63ull;
    // This is strongly universal hashing following Dietzfelbinger's "Universal hashing
    // and k-wise independent random variables via integer arithmetic without primes". As
    // such, for any two distinct uint32_t's hash1 and hash2, the probability (over the
    // randomness of the constants) that any subset of bit positions of
    // Rehash32to32(hash1) is equal to the same subset of bit positions
    // Rehash32to32(hash2) is minimal.
    return static_cast<uint32_t>((static_cast<uint64_t>(hash) * m + a) >> 32);
  }

  static inline uint64_t Rehash32to64(const uint32_t hash) {
    static const uint64_t m1 = 0x47b6137a44974d91ull, m2 = 0x8824ad5ba2b7289cull,
                          a1 = 0x705495c62df1424aull, a2 = 0x9efc49475c6bfb31ull;
    const uint64_t hash1 = (static_cast<uint64_t>(hash) * m1 + a1) >> 32;
    const uint64_t hash2 = (static_cast<uint64_t>(hash) * m2 + a2) >> 32;
    return hash1 | (hash2 << 32);
  }
};

// HW Hash
template <>
inline int HashUtil::Hash<true>(const void* data, int32_t bytes, uint32_t seed) {
#ifdef ARROW_HAVE_ARM_CRC
  // Need run time check for Arm
  // if not support, fall back to Murmur
  if (!crc32c_runtime_check())
    return static_cast<int>(HashUtil::MurmurHash2_64(data, bytes, seed));
  else
#endif
    // Double CRC
    return static_cast<int>(HashUtil::DoubleCrcHash(data, bytes, seed));
}

// Murmur Hash
template <>
inline int HashUtil::Hash<false>(const void* data, int32_t bytes, uint32_t seed) {
  return static_cast<int>(HashUtil::MurmurHash2_64(data, bytes, seed));
}

}  // namespace arrow

#endif  // ARROW_UTIL_HASH_UTIL_H
