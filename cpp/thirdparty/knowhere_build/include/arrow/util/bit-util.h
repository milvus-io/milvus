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
#ifndef ARROW_UTIL_BIT_UTIL_H
#define ARROW_UTIL_BIT_UTIL_H

#ifdef _WIN32
#define ARROW_LITTLE_ENDIAN 1
#else
#ifdef __APPLE__
#include <machine/endian.h>
#else
#include <endian.h>
#endif
#
#ifndef __BYTE_ORDER__
#error "__BYTE_ORDER__ not defined"
#endif
#
#ifndef __ORDER_LITTLE_ENDIAN__
#error "__ORDER_LITTLE_ENDIAN__ not defined"
#endif
#
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define ARROW_LITTLE_ENDIAN 1
#else
#define ARROW_LITTLE_ENDIAN 0
#endif
#endif

#if defined(_MSC_VER)
#include <intrin.h>
#pragma intrinsic(_BitScanReverse)
#pragma intrinsic(_BitScanForward)
#define ARROW_BYTE_SWAP64 _byteswap_uint64
#define ARROW_BYTE_SWAP32 _byteswap_ulong
#else
#define ARROW_BYTE_SWAP64 __builtin_bswap64
#define ARROW_BYTE_SWAP32 __builtin_bswap32
#endif

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <type_traits>
#include <vector>

#include "arrow/util/macros.h"
#include "arrow/util/type_traits.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class Status;

namespace detail {

template <typename Integer>
typename std::make_unsigned<Integer>::type as_unsigned(Integer x) {
  return static_cast<typename std::make_unsigned<Integer>::type>(x);
}

}  // namespace detail

namespace BitUtil {

// The number of set bits in a given unsigned byte value, pre-computed
//
// Generated with the following Python code
// output = 'static constexpr uint8_t kBytePopcount[] = {{{0}}};'
// popcounts = [str(bin(i).count('1')) for i in range(0, 256)]
// print(output.format(', '.join(popcounts)))
static constexpr uint8_t kBytePopcount[] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3,
    4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4,
    4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4,
    5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5,
    4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2,
    3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5,
    5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4,
    5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6,
    4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};

//
// Bit-related computations on integer values
//

// Returns the ceil of value/divisor
constexpr int64_t CeilDiv(int64_t value, int64_t divisor) {
  return value / divisor + (value % divisor != 0);
}

constexpr int64_t BytesForBits(int64_t bits) { return (bits + 7) >> 3; }

// Returns the smallest power of two that contains v.  If v is already a
// power of two, it is returned as is.
static inline int64_t NextPower2(int64_t n) {
  // Taken from
  // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  n++;
  return n;
}

constexpr bool IsMultipleOf64(int64_t n) { return (n & 63) == 0; }

constexpr bool IsMultipleOf8(int64_t n) { return (n & 7) == 0; }

// Returns 'value' rounded up to the nearest multiple of 'factor'
constexpr int64_t RoundUp(int64_t value, int64_t factor) {
  return (value + (factor - 1)) / factor * factor;
}

// Returns 'value' rounded down to the nearest multiple of 'factor'
constexpr int64_t RoundDown(int64_t value, int64_t factor) {
  return (value / factor) * factor;
}

// Returns 'value' rounded up to the nearest multiple of 'factor' when factor
// is a power of two.
// The result is undefined on overflow, i.e. if `value > 2**64 - factor`,
// since we cannot return the correct result which would be 2**64.
constexpr int64_t RoundUpToPowerOf2(int64_t value, int64_t factor) {
  // DCHECK((factor > 0) && ((factor & (factor - 1)) == 0));
  return (value + (factor - 1)) & ~(factor - 1);
}

constexpr int64_t RoundUpToMultipleOf8(int64_t num) { return RoundUpToPowerOf2(num, 8); }

constexpr int64_t RoundUpToMultipleOf64(int64_t num) {
  return RoundUpToPowerOf2(num, 64);
}

// Returns the number of bytes covering a sliced bitmap. Find the length
// rounded to cover full bytes on both extremities.
//
// The following example represents a slice (offset=10, length=9)
//
// 0       8       16     24
// |-------|-------|------|
//           [       ]          (slice)
//         [             ]      (same slice aligned to bytes bounds, length=16)
//
// The covering bytes is the length (in bytes) of this new aligned slice.
constexpr int64_t CoveringBytes(int64_t offset, int64_t length) {
  return (BitUtil::RoundUp(length + offset, 8) - BitUtil::RoundDown(offset, 8)) / 8;
}

// Returns the 'num_bits' least-significant bits of 'v'.
static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
  if (ARROW_PREDICT_FALSE(num_bits == 0)) return 0;
  if (ARROW_PREDICT_FALSE(num_bits >= 64)) return v;
  int n = 64 - num_bits;
  return (v << n) >> n;
}

/// \brief Count the number of leading zeros in an unsigned integer.
static inline int CountLeadingZeros(uint32_t value) {
#if defined(__clang__) || defined(__GNUC__)
  if (value == 0) return 32;
  return static_cast<int>(__builtin_clz(value));
#elif defined(_MSC_VER)
  unsigned long index;                                               // NOLINT
  if (_BitScanReverse(&index, static_cast<unsigned long>(value))) {  // NOLINT
    return 31 - static_cast<int>(index);
  } else {
    return 32;
  }
#else
  int bitpos = 0;
  while (value != 0) {
    value >>= 1;
    ++bitpos;
  }
  return 32 - bitpos;
#endif
}

static inline int CountLeadingZeros(uint64_t value) {
#if defined(__clang__) || defined(__GNUC__)
  if (value == 0) return 64;
  return static_cast<int>(__builtin_clzll(value));
#elif defined(_MSC_VER)
  unsigned long index;                     // NOLINT
  if (_BitScanReverse64(&index, value)) {  // NOLINT
    return 63 - static_cast<int>(index);
  } else {
    return 64;
  }
#else
  int bitpos = 0;
  while (value != 0) {
    value >>= 1;
    ++bitpos;
  }
  return 64 - bitpos;
#endif
}

static inline int CountTrailingZeros(uint32_t value) {
#if defined(__clang__) || defined(__GNUC__)
  if (value == 0) return 32;
  return static_cast<int>(__builtin_ctzl(value));
#elif defined(_MSC_VER)
  unsigned long index;  // NOLINT
  if (_BitScanForward(&index, value)) {
    return static_cast<int>(index);
  } else {
    return 32;
  }
#else
  int bitpos = 0;
  if (value) {
    while (value & 1 == 0) {
      value >>= 1;
      ++bitpos;
    }
  } else {
    bitpos = 32;
  }
  return bitpos;
#endif
}

static inline int CountTrailingZeros(uint64_t value) {
#if defined(__clang__) || defined(__GNUC__)
  if (value == 0) return 64;
  return static_cast<int>(__builtin_ctzll(value));
#elif defined(_MSC_VER)
  unsigned long index;  // NOLINT
  if (_BitScanForward64(&index, value)) {
    return static_cast<int>(index);
  } else {
    return 64;
  }
#else
  int bitpos = 0;
  if (value) {
    while (value & 1 == 0) {
      value >>= 1;
      ++bitpos;
    }
  } else {
    bitpos = 64;
  }
  return bitpos;
#endif
}

// Returns the minimum number of bits needed to represent an unsigned value
static inline int NumRequiredBits(uint64_t x) { return 64 - CountLeadingZeros(x); }

// Returns ceil(log2(x)).
static inline int Log2(uint64_t x) {
  // DCHECK_GT(x, 0);
  return NumRequiredBits(x - 1);
}

//
// Byte-swap 16-bit, 32-bit and 64-bit values
//

// Swap the byte order (i.e. endianess)
static inline int64_t ByteSwap(int64_t value) { return ARROW_BYTE_SWAP64(value); }
static inline uint64_t ByteSwap(uint64_t value) {
  return static_cast<uint64_t>(ARROW_BYTE_SWAP64(value));
}
static inline int32_t ByteSwap(int32_t value) { return ARROW_BYTE_SWAP32(value); }
static inline uint32_t ByteSwap(uint32_t value) {
  return static_cast<uint32_t>(ARROW_BYTE_SWAP32(value));
}
static inline int16_t ByteSwap(int16_t value) {
  constexpr auto m = static_cast<int16_t>(0xff);
  return static_cast<int16_t>(((value >> 8) & m) | ((value & m) << 8));
}
static inline uint16_t ByteSwap(uint16_t value) {
  return static_cast<uint16_t>(ByteSwap(static_cast<int16_t>(value)));
}

// Write the swapped bytes into dst. Src and dst cannot overlap.
static inline void ByteSwap(void* dst, const void* src, int len) {
  switch (len) {
    case 1:
      *reinterpret_cast<int8_t*>(dst) = *reinterpret_cast<const int8_t*>(src);
      return;
    case 2:
      *reinterpret_cast<int16_t*>(dst) = ByteSwap(*reinterpret_cast<const int16_t*>(src));
      return;
    case 4:
      *reinterpret_cast<int32_t*>(dst) = ByteSwap(*reinterpret_cast<const int32_t*>(src));
      return;
    case 8:
      *reinterpret_cast<int64_t*>(dst) = ByteSwap(*reinterpret_cast<const int64_t*>(src));
      return;
    default:
      break;
  }

  auto d = reinterpret_cast<uint8_t*>(dst);
  auto s = reinterpret_cast<const uint8_t*>(src);
  for (int i = 0; i < len; ++i) {
    d[i] = s[len - i - 1];
  }
}

// Convert to little/big endian format from the machine's native endian format.
#if ARROW_LITTLE_ENDIAN
template <typename T, typename = internal::EnableIfIsOneOf<T, int64_t, uint64_t, int32_t,
                                                           uint32_t, int16_t, uint16_t>>
static inline T ToBigEndian(T value) {
  return ByteSwap(value);
}

template <typename T, typename = internal::EnableIfIsOneOf<T, int64_t, uint64_t, int32_t,
                                                           uint32_t, int16_t, uint16_t>>
static inline T ToLittleEndian(T value) {
  return value;
}
#else
template <typename T, typename = internal::EnableIfIsOneOf<T, int64_t, uint64_t, int32_t,
                                                           uint32_t, int16_t, uint16_t>>
static inline T ToBigEndian(T value) {
  return value;
}

template <typename T, typename = internal::EnableIfIsOneOf<T, int64_t, uint64_t, int32_t,
                                                           uint32_t, int16_t, uint16_t>>
static inline T ToLittleEndian(T value) {
  return ByteSwap(value);
}
#endif

// Convert from big/little endian format to the machine's native endian format.
#if ARROW_LITTLE_ENDIAN
template <typename T, typename = internal::EnableIfIsOneOf<T, int64_t, uint64_t, int32_t,
                                                           uint32_t, int16_t, uint16_t>>
static inline T FromBigEndian(T value) {
  return ByteSwap(value);
}

template <typename T, typename = internal::EnableIfIsOneOf<T, int64_t, uint64_t, int32_t,
                                                           uint32_t, int16_t, uint16_t>>
static inline T FromLittleEndian(T value) {
  return value;
}
#else
template <typename T, typename = internal::EnableIfIsOneOf<T, int64_t, uint64_t, int32_t,
                                                           uint32_t, int16_t, uint16_t>>
static inline T FromBigEndian(T value) {
  return value;
}

template <typename T, typename = internal::EnableIfIsOneOf<T, int64_t, uint64_t, int32_t,
                                                           uint32_t, int16_t, uint16_t>>
static inline T FromLittleEndian(T value) {
  return ByteSwap(value);
}
#endif

//
// Utilities for reading and writing individual bits by their index
// in a memory area.
//

// Bitmask selecting the k-th bit in a byte
static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

// the bitwise complement version of kBitmask
static constexpr uint8_t kFlippedBitmask[] = {254, 253, 251, 247, 239, 223, 191, 127};

// Bitmask selecting the (k - 1) preceding bits in a byte
static constexpr uint8_t kPrecedingBitmask[] = {0, 1, 3, 7, 15, 31, 63, 127};
static constexpr uint8_t kPrecedingWrappingBitmask[] = {255, 1, 3, 7, 15, 31, 63, 127};

// the bitwise complement version of kPrecedingBitmask
static constexpr uint8_t kTrailingBitmask[] = {255, 254, 252, 248, 240, 224, 192, 128};

static inline bool GetBit(const uint8_t* bits, uint64_t i) {
  return (bits[i >> 3] >> (i & 0x07)) & 1;
}

// Gets the i-th bit from a byte. Should only be used with i <= 7.
static inline bool GetBitFromByte(uint8_t byte, uint8_t i) { return byte & kBitmask[i]; }

static inline void ClearBit(uint8_t* bits, int64_t i) {
  bits[i / 8] &= kFlippedBitmask[i % 8];
}

static inline void SetBit(uint8_t* bits, int64_t i) { bits[i / 8] |= kBitmask[i % 8]; }

static inline void SetBitTo(uint8_t* bits, int64_t i, bool bit_is_set) {
  // https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  // NOTE: this seems to confuse Valgrind as it reads from potentially
  // uninitialized memory
  bits[i / 8] ^= static_cast<uint8_t>(-static_cast<uint8_t>(bit_is_set) ^ bits[i / 8]) &
                 kBitmask[i % 8];
}

/// \brief set or clear a range of bits quickly
static inline void SetBitsTo(uint8_t* bits, int64_t start_offset, int64_t length,
                             bool bits_are_set) {
  if (length == 0) return;

  const auto i_begin = start_offset;
  const auto i_end = start_offset + length;
  const uint8_t fill_byte = static_cast<uint8_t>(-static_cast<uint8_t>(bits_are_set));

  const auto bytes_begin = i_begin / 8;
  const auto bytes_end = i_end / 8 + 1;

  const auto first_byte_mask = kPrecedingBitmask[i_begin % 8];
  const auto last_byte_mask = kTrailingBitmask[i_end % 8];

  if (bytes_end == bytes_begin + 1) {
    // set bits within a single byte
    const auto only_byte_mask =
        i_end % 8 == 0 ? first_byte_mask
                       : static_cast<uint8_t>(first_byte_mask | last_byte_mask);
    bits[bytes_begin] &= only_byte_mask;
    bits[bytes_begin] |= static_cast<uint8_t>(fill_byte & ~only_byte_mask);
    return;
  }

  // set/clear trailing bits of first byte
  bits[bytes_begin] &= first_byte_mask;
  bits[bytes_begin] |= static_cast<uint8_t>(fill_byte & ~first_byte_mask);

  if (bytes_end - bytes_begin > 2) {
    // set/clear whole bytes
    std::memset(bits + bytes_begin + 1, fill_byte,
                static_cast<size_t>(bytes_end - bytes_begin - 2));
  }

  if (i_end % 8 == 0) return;

  // set/clear leading bits of last byte
  bits[bytes_end - 1] &= last_byte_mask;
  bits[bytes_end - 1] |= static_cast<uint8_t>(fill_byte & ~last_byte_mask);
}

/// \brief Convert vector of bytes to bitmap buffer
ARROW_EXPORT
Status BytesToBits(const std::vector<uint8_t>&, MemoryPool*, std::shared_ptr<Buffer>*);

}  // namespace BitUtil

namespace internal {

class BitmapReader {
 public:
  BitmapReader(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0), length_(length) {
    current_byte_ = 0;
    byte_offset_ = start_offset / 8;
    bit_offset_ = start_offset % 8;
    if (length > 0) {
      current_byte_ = bitmap[byte_offset_];
    }
  }

  bool IsSet() const { return (current_byte_ & (1 << bit_offset_)) != 0; }

  bool IsNotSet() const { return (current_byte_ & (1 << bit_offset_)) == 0; }

  void Next() {
    ++bit_offset_;
    ++position_;
    if (ARROW_PREDICT_FALSE(bit_offset_ == 8)) {
      bit_offset_ = 0;
      ++byte_offset_;
      if (ARROW_PREDICT_TRUE(position_ < length_)) {
        current_byte_ = bitmap_[byte_offset_];
      }
    }
  }

 private:
  const uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  int64_t byte_offset_;
  int64_t bit_offset_;
};

class BitmapWriter {
  // A sequential bitwise writer that preserves surrounding bit values.

 public:
  BitmapWriter(uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0), length_(length) {
    byte_offset_ = start_offset / 8;
    bit_mask_ = BitUtil::kBitmask[start_offset % 8];
    if (length > 0) {
      current_byte_ = bitmap[byte_offset_];
    } else {
      current_byte_ = 0;
    }
  }

  void Set() { current_byte_ |= bit_mask_; }

  void Clear() { current_byte_ &= bit_mask_ ^ 0xFF; }

  void Next() {
    bit_mask_ = static_cast<uint8_t>(bit_mask_ << 1);
    ++position_;
    if (bit_mask_ == 0) {
      // Finished this byte, need advancing
      bit_mask_ = 0x01;
      bitmap_[byte_offset_++] = current_byte_;
      if (ARROW_PREDICT_TRUE(position_ < length_)) {
        current_byte_ = bitmap_[byte_offset_];
      }
    }
  }

  void Finish() {
    // Store current byte if we didn't went past bitmap storage
    if (length_ > 0 && (bit_mask_ != 0x01 || position_ < length_)) {
      bitmap_[byte_offset_] = current_byte_;
    }
  }

  int64_t position() const { return position_; }

 private:
  uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  uint8_t bit_mask_;
  int64_t byte_offset_;
};

class FirstTimeBitmapWriter {
  // Like BitmapWriter, but any bit values *following* the bits written
  // might be clobbered.  It is hence faster than BitmapWriter, and can
  // also avoid false positives with Valgrind.

 public:
  FirstTimeBitmapWriter(uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap), position_(0), length_(length) {
    current_byte_ = 0;
    byte_offset_ = start_offset / 8;
    bit_mask_ = BitUtil::kBitmask[start_offset % 8];
    if (length > 0) {
      current_byte_ = bitmap[byte_offset_] & BitUtil::kPrecedingBitmask[start_offset % 8];
    } else {
      current_byte_ = 0;
    }
  }

  void Set() { current_byte_ |= bit_mask_; }

  void Clear() {}

  void Next() {
    bit_mask_ = static_cast<uint8_t>(bit_mask_ << 1);
    ++position_;
    if (bit_mask_ == 0) {
      // Finished this byte, need advancing
      bit_mask_ = 0x01;
      bitmap_[byte_offset_++] = current_byte_;
      current_byte_ = 0;
    }
  }

  void Finish() {
    // Store current byte if we didn't went past bitmap storage
    if (length_ > 0 && (bit_mask_ != 0x01 || position_ < length_)) {
      bitmap_[byte_offset_] = current_byte_;
    }
  }

  int64_t position() const { return position_; }

 private:
  uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;

  uint8_t current_byte_;
  uint8_t bit_mask_;
  int64_t byte_offset_;
};

// A std::generate() like function to write sequential bits into a bitmap area.
// Bits preceding the bitmap area are preserved, bits following the bitmap
// area may be clobbered.

template <class Generator>
void GenerateBits(uint8_t* bitmap, int64_t start_offset, int64_t length, Generator&& g) {
  if (length == 0) {
    return;
  }
  uint8_t* cur = bitmap + start_offset / 8;
  uint8_t bit_mask = BitUtil::kBitmask[start_offset % 8];
  uint8_t current_byte = *cur & BitUtil::kPrecedingBitmask[start_offset % 8];

  for (int64_t index = 0; index < length; ++index) {
    const bool bit = g();
    current_byte = bit ? (current_byte | bit_mask) : current_byte;
    bit_mask = static_cast<uint8_t>(bit_mask << 1);
    if (bit_mask == 0) {
      bit_mask = 1;
      *cur++ = current_byte;
      current_byte = 0;
    }
  }
  if (bit_mask != 1) {
    *cur++ = current_byte;
  }
}

// Like GenerateBits(), but unrolls its main loop for higher performance.

template <class Generator>
void GenerateBitsUnrolled(uint8_t* bitmap, int64_t start_offset, int64_t length,
                          Generator&& g) {
  if (length == 0) {
    return;
  }
  uint8_t current_byte;
  uint8_t* cur = bitmap + start_offset / 8;
  const uint64_t start_bit_offset = start_offset % 8;
  uint8_t bit_mask = BitUtil::kBitmask[start_bit_offset];
  int64_t remaining = length;

  if (bit_mask != 0x01) {
    current_byte = *cur & BitUtil::kPrecedingBitmask[start_bit_offset];
    while (bit_mask != 0 && remaining > 0) {
      current_byte = g() ? (current_byte | bit_mask) : current_byte;
      bit_mask = static_cast<uint8_t>(bit_mask << 1);
      --remaining;
    }
    *cur++ = current_byte;
  }

  int64_t remaining_bytes = remaining / 8;
  while (remaining_bytes-- > 0) {
    current_byte = 0;
    current_byte = g() ? current_byte | 0x01 : current_byte;
    current_byte = g() ? current_byte | 0x02 : current_byte;
    current_byte = g() ? current_byte | 0x04 : current_byte;
    current_byte = g() ? current_byte | 0x08 : current_byte;
    current_byte = g() ? current_byte | 0x10 : current_byte;
    current_byte = g() ? current_byte | 0x20 : current_byte;
    current_byte = g() ? current_byte | 0x40 : current_byte;
    current_byte = g() ? current_byte | 0x80 : current_byte;
    *cur++ = current_byte;
  }

  int64_t remaining_bits = remaining % 8;
  if (remaining_bits) {
    current_byte = 0;
    bit_mask = 0x01;
    while (remaining_bits-- > 0) {
      current_byte = g() ? (current_byte | bit_mask) : current_byte;
      bit_mask = static_cast<uint8_t>(bit_mask << 1);
    }
    *cur++ = current_byte;
  }
}

// A function that visits each bit in a bitmap and calls a visitor function with a
// boolean representation of that bit. This is intended to be analogous to
// GenerateBits.
template <class Visitor>
void VisitBits(const uint8_t* bitmap, int64_t start_offset, int64_t length,
               Visitor&& visit) {
  BitmapReader reader(bitmap, start_offset, length);
  for (int64_t index = 0; index < length; ++index) {
    visit(reader.IsSet());
    reader.Next();
  }
}

// Like VisitBits(), but unrolls its main loop for better performance.
template <class Visitor>
void VisitBitsUnrolled(const uint8_t* bitmap, int64_t start_offset, int64_t length,
                       Visitor&& visit) {
  if (length == 0) {
    return;
  }

  // Start by visiting any bits preceding the first full byte.
  int64_t num_bits_before_full_bytes =
      BitUtil::RoundUpToMultipleOf8(start_offset) - start_offset;
  // Truncate num_bits_before_full_bytes if it is greater than length.
  if (num_bits_before_full_bytes > length) {
    num_bits_before_full_bytes = length;
  }
  // Use the non loop-unrolled VisitBits since we don't want to add branches
  VisitBits<Visitor>(bitmap, start_offset, num_bits_before_full_bytes, visit);

  // Shift the start pointer to the first full byte and compute the
  // number of full bytes to be read.
  const uint8_t* first_full_byte = bitmap + BitUtil::CeilDiv(start_offset, 8);
  const int64_t num_full_bytes = (length - num_bits_before_full_bytes) / 8;

  // Iterate over each full byte of the input bitmap and call the visitor in
  // a loop-unrolled manner.
  for (int64_t byte_index = 0; byte_index < num_full_bytes; ++byte_index) {
    // Get the current bit-packed byte value from the bitmap.
    const uint8_t byte = *(first_full_byte + byte_index);

    // Execute the visitor function on each bit of the current byte.
    visit(BitUtil::GetBitFromByte(byte, 0));
    visit(BitUtil::GetBitFromByte(byte, 1));
    visit(BitUtil::GetBitFromByte(byte, 2));
    visit(BitUtil::GetBitFromByte(byte, 3));
    visit(BitUtil::GetBitFromByte(byte, 4));
    visit(BitUtil::GetBitFromByte(byte, 5));
    visit(BitUtil::GetBitFromByte(byte, 6));
    visit(BitUtil::GetBitFromByte(byte, 7));
  }

  // Write any leftover bits in the last byte.
  const int64_t num_bits_after_full_bytes = (length - num_bits_before_full_bytes) % 8;
  VisitBits<Visitor>(first_full_byte + num_full_bytes, 0, num_bits_after_full_bytes,
                     visit);
}

// ----------------------------------------------------------------------
// Bitmap utilities

/// Copy a bit range of an existing bitmap
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[out] out the resulting copy
///
/// \return Status message
ARROW_EXPORT
Status CopyBitmap(MemoryPool* pool, const uint8_t* bitmap, int64_t offset, int64_t length,
                  std::shared_ptr<Buffer>* out);

/// Copy a bit range of an existing bitmap into an existing bitmap
///
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[in] dest_offset bit offset into the destination
/// \param[in] restore_trailing_bits don't clobber bits outside the destination range
/// \param[out] dest the destination buffer, must have at least space for
/// (offset + length) bits
ARROW_EXPORT
void CopyBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                int64_t dest_offset, bool restore_trailing_bits = true);

/// Invert a bit range of an existing bitmap into an existing bitmap
///
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[in] dest_offset bit offset into the destination
/// \param[out] dest the destination buffer, must have at least space for
/// (offset + length) bits
ARROW_EXPORT
void InvertBitmap(const uint8_t* bitmap, int64_t offset, int64_t length, uint8_t* dest,
                  int64_t dest_offset);

/// Invert a bit range of an existing bitmap
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[out] out the resulting copy
///
/// \return Status message
ARROW_EXPORT
Status InvertBitmap(MemoryPool* pool, const uint8_t* bitmap, int64_t offset,
                    int64_t length, std::shared_ptr<Buffer>* out);

/// Compute the number of 1's in the given data array
///
/// \param[in] data a packed LSB-ordered bitmap as a byte array
/// \param[in] bit_offset a bitwise offset into the bitmap
/// \param[in] length the number of bits to inspect in the bitmap relative to
/// the offset
///
/// \return The number of set (1) bits in the range
ARROW_EXPORT
int64_t CountSetBits(const uint8_t* data, int64_t bit_offset, int64_t length);

ARROW_EXPORT
bool BitmapEquals(const uint8_t* left, int64_t left_offset, const uint8_t* right,
                  int64_t right_offset, int64_t bit_length);

/// \brief Do a "bitmap and" on right and left buffers starting at
/// their respective bit-offsets for the given bit-length and put
/// the results in out_buffer starting at the given bit-offset.
///
/// out_buffer will be allocated and initialized to zeros using pool before
/// the operation.
ARROW_EXPORT
Status BitmapAnd(MemoryPool* pool, const uint8_t* left, int64_t left_offset,
                 const uint8_t* right, int64_t right_offset, int64_t length,
                 int64_t out_offset, std::shared_ptr<Buffer>* out_buffer);

/// \brief Do a "bitmap and" on right and left buffers starting at
/// their respective bit-offsets for the given bit-length and put
/// the results in out starting at the given bit-offset.
ARROW_EXPORT
void BitmapAnd(const uint8_t* left, int64_t left_offset, const uint8_t* right,
               int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out);

/// \brief Do a "bitmap or" for the given bit length on right and left buffers
/// starting at their respective bit-offsets and put the results in out_buffer
/// starting at the given bit-offset.
///
/// out_buffer will be allocated and initialized to zeros using pool before
/// the operation.
ARROW_EXPORT
Status BitmapOr(MemoryPool* pool, const uint8_t* left, int64_t left_offset,
                const uint8_t* right, int64_t right_offset, int64_t length,
                int64_t out_offset, std::shared_ptr<Buffer>* out_buffer);

/// \brief Do a "bitmap or" for the given bit length on right and left buffers
/// starting at their respective bit-offsets and put the results in out
/// starting at the given bit-offset.
ARROW_EXPORT
void BitmapOr(const uint8_t* left, int64_t left_offset, const uint8_t* right,
              int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out);

/// \brief Do a "bitmap xor" for the given bit-length on right and left
/// buffers starting at their respective bit-offsets and put the results in
/// out_buffer starting at the given bit offset.
///
/// out_buffer will be allocated and initialized to zeros using pool before
/// the operation.
ARROW_EXPORT
Status BitmapXor(MemoryPool* pool, const uint8_t* left, int64_t left_offset,
                 const uint8_t* right, int64_t right_offset, int64_t length,
                 int64_t out_offset, std::shared_ptr<Buffer>* out_buffer);

/// \brief Do a "bitmap xor" for the given bit-length on right and left
/// buffers starting at their respective bit-offsets and put the results in
/// out starting at the given bit offset.
ARROW_EXPORT
void BitmapXor(const uint8_t* left, int64_t left_offset, const uint8_t* right,
               int64_t right_offset, int64_t length, int64_t out_offset, uint8_t* out);

/// \brief Generate Bitmap with all position to `value` except for one found
/// at `straggler_pos`.
ARROW_EXPORT
Status BitmapAllButOne(MemoryPool* pool, int64_t length, int64_t straggler_pos,
                       std::shared_ptr<Buffer>* output, bool value = true);

/// \brief Store a stack of bitsets efficiently. The top bitset may be
/// accessed and its bits may be modified, but it may not be resized.
class BitsetStack {
 public:
  using reference = typename std::vector<bool>::reference;

  /// \brief push a bitset onto the stack
  /// \param size number of bits in the next bitset
  /// \param value initial value for bits in the pushed bitset
  void Push(int size, bool value) {
    offsets_.push_back(bit_count());
    bits_.resize(bit_count() + size, value);
  }

  /// \brief number of bits in the bitset at the top of the stack
  int TopSize() const {
    if (offsets_.size() == 0) return 0;
    return bit_count() - offsets_.back();
  }

  /// \brief pop a bitset off the stack
  void Pop() {
    bits_.resize(offsets_.back());
    offsets_.pop_back();
  }

  /// \brief get the value of a bit in the top bitset
  /// \param i index of the bit to access
  bool operator[](int i) const { return bits_[offsets_.back() + i]; }

  /// \brief get a mutable reference to a bit in the top bitset
  /// \param i index of the bit to access
  reference operator[](int i) { return bits_[offsets_.back() + i]; }

 private:
  int bit_count() const { return static_cast<int>(bits_.size()); }
  std::vector<bool> bits_;
  std::vector<int> offsets_;
};

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_BIT_UTIL_H
