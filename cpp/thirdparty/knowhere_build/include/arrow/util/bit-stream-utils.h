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

// From Apache Impala (incubating) as of 2016-01-29

#ifndef ARROW_UTIL_BIT_STREAM_UTILS_H
#define ARROW_UTIL_BIT_STREAM_UTILS_H

#include <string.h>
#include <algorithm>
#include <cstdint>

#include "arrow/util/bit-util.h"
#include "arrow/util/bpacking.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace BitUtil {

/// Utility class to write bit/byte streams.  This class can write data to either be
/// bit packed or byte aligned (and a single stream that has a mix of both).
/// This class does not allocate memory.
class BitWriter {
 public:
  /// buffer: buffer to write bits to.  Buffer should be preallocated with
  /// 'buffer_len' bytes.
  BitWriter(uint8_t* buffer, int buffer_len) : buffer_(buffer), max_bytes_(buffer_len) {
    Clear();
  }

  void Clear() {
    buffered_values_ = 0;
    byte_offset_ = 0;
    bit_offset_ = 0;
  }

  /// The number of current bytes written, including the current byte (i.e. may include a
  /// fraction of a byte). Includes buffered values.
  int bytes_written() const {
    return byte_offset_ + static_cast<int>(BitUtil::BytesForBits(bit_offset_));
  }
  uint8_t* buffer() const { return buffer_; }
  int buffer_len() const { return max_bytes_; }

  /// Writes a value to buffered_values_, flushing to buffer_ if necessary.  This is bit
  /// packed.  Returns false if there was not enough space. num_bits must be <= 32.
  bool PutValue(uint64_t v, int num_bits);

  /// Writes v to the next aligned byte using num_bytes. If T is larger than
  /// num_bytes, the extra high-order bytes will be ignored. Returns false if
  /// there was not enough space.
  template <typename T>
  bool PutAligned(T v, int num_bytes);

  /// Write a Vlq encoded int to the buffer.  Returns false if there was not enough
  /// room.  The value is written byte aligned.
  /// For more details on vlq:
  /// en.wikipedia.org/wiki/Variable-length_quantity
  bool PutVlqInt(uint32_t v);

  // Writes an int zigzag encoded.
  bool PutZigZagVlqInt(int32_t v);

  /// Get a pointer to the next aligned byte and advance the underlying buffer
  /// by num_bytes.
  /// Returns NULL if there was not enough space.
  uint8_t* GetNextBytePtr(int num_bytes = 1);

  /// Flushes all buffered values to the buffer. Call this when done writing to
  /// the buffer.  If 'align' is true, buffered_values_ is reset and any future
  /// writes will be written to the next byte boundary.
  void Flush(bool align = false);

 private:
  uint8_t* buffer_;
  int max_bytes_;

  /// Bit-packed values are initially written to this variable before being memcpy'd to
  /// buffer_. This is faster than writing values byte by byte directly to buffer_.
  uint64_t buffered_values_;

  int byte_offset_;  // Offset in buffer_
  int bit_offset_;   // Offset in buffered_values_
};

/// Utility class to read bit/byte stream.  This class can read bits or bytes
/// that are either byte aligned or not.  It also has utilities to read multiple
/// bytes in one read (e.g. encoded int).
class BitReader {
 public:
  /// 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
  BitReader(const uint8_t* buffer, int buffer_len)
      : buffer_(buffer), max_bytes_(buffer_len), byte_offset_(0), bit_offset_(0) {
    int num_bytes = std::min(8, max_bytes_ - byte_offset_);
    memcpy(&buffered_values_, buffer_ + byte_offset_, num_bytes);
  }

  BitReader()
      : buffer_(NULL),
        max_bytes_(0),
        buffered_values_(0),
        byte_offset_(0),
        bit_offset_(0) {}

  void Reset(const uint8_t* buffer, int buffer_len) {
    buffer_ = buffer;
    max_bytes_ = buffer_len;
    byte_offset_ = 0;
    bit_offset_ = 0;
    int num_bytes = std::min(8, max_bytes_ - byte_offset_);
    memcpy(&buffered_values_, buffer_ + byte_offset_, num_bytes);
  }

  /// Gets the next value from the buffer.  Returns true if 'v' could be read or false if
  /// there are not enough bytes left. num_bits must be <= 32.
  template <typename T>
  bool GetValue(int num_bits, T* v);

  /// Get a number of values from the buffer. Return the number of values actually read.
  template <typename T>
  int GetBatch(int num_bits, T* v, int batch_size);

  /// Reads a 'num_bytes'-sized value from the buffer and stores it in 'v'. T
  /// needs to be a little-endian native type and big enough to store
  /// 'num_bytes'. The value is assumed to be byte-aligned so the stream will
  /// be advanced to the start of the next byte before 'v' is read. Returns
  /// false if there are not enough bytes left.
  template <typename T>
  bool GetAligned(int num_bytes, T* v);

  /// Reads a vlq encoded int from the stream.  The encoded int must start at
  /// the beginning of a byte. Return false if there were not enough bytes in
  /// the buffer.
  bool GetVlqInt(int32_t* v);

  // Reads a zigzag encoded int `into` v.
  bool GetZigZagVlqInt(int32_t* v);

  /// Returns the number of bytes left in the stream, not including the current
  /// byte (i.e., there may be an additional fraction of a byte).
  int bytes_left() {
    return max_bytes_ -
           (byte_offset_ + static_cast<int>(BitUtil::BytesForBits(bit_offset_)));
  }

  /// Maximum byte length of a vlq encoded int
  static const int MAX_VLQ_BYTE_LEN = 5;

 private:
  const uint8_t* buffer_;
  int max_bytes_;

  /// Bytes are memcpy'd from buffer_ and values are read from this variable. This is
  /// faster than reading values byte by byte directly from buffer_.
  uint64_t buffered_values_;

  int byte_offset_;  // Offset in buffer_
  int bit_offset_;   // Offset in buffered_values_
};

inline bool BitWriter::PutValue(uint64_t v, int num_bits) {
  // TODO: revisit this limit if necessary (can be raised to 64 by fixing some edge cases)
  DCHECK_LE(num_bits, 32);
  DCHECK_EQ(v >> num_bits, 0) << "v = " << v << ", num_bits = " << num_bits;

  if (ARROW_PREDICT_FALSE(byte_offset_ * 8 + bit_offset_ + num_bits > max_bytes_ * 8))
    return false;

  buffered_values_ |= v << bit_offset_;
  bit_offset_ += num_bits;

  if (ARROW_PREDICT_FALSE(bit_offset_ >= 64)) {
    // Flush buffered_values_ and write out bits of v that did not fit
    memcpy(buffer_ + byte_offset_, &buffered_values_, 8);
    buffered_values_ = 0;
    byte_offset_ += 8;
    bit_offset_ -= 64;
    buffered_values_ = v >> (num_bits - bit_offset_);
  }
  DCHECK_LT(bit_offset_, 64);
  return true;
}

inline void BitWriter::Flush(bool align) {
  int num_bytes = static_cast<int>(BitUtil::BytesForBits(bit_offset_));
  DCHECK_LE(byte_offset_ + num_bytes, max_bytes_);
  memcpy(buffer_ + byte_offset_, &buffered_values_, num_bytes);

  if (align) {
    buffered_values_ = 0;
    byte_offset_ += num_bytes;
    bit_offset_ = 0;
  }
}

inline uint8_t* BitWriter::GetNextBytePtr(int num_bytes) {
  Flush(/* align */ true);
  DCHECK_LE(byte_offset_, max_bytes_);
  if (byte_offset_ + num_bytes > max_bytes_) return NULL;
  uint8_t* ptr = buffer_ + byte_offset_;
  byte_offset_ += num_bytes;
  return ptr;
}

template <typename T>
inline bool BitWriter::PutAligned(T val, int num_bytes) {
  uint8_t* ptr = GetNextBytePtr(num_bytes);
  if (ptr == NULL) return false;
  memcpy(ptr, &val, num_bytes);
  return true;
}

inline bool BitWriter::PutVlqInt(uint32_t v) {
  bool result = true;
  while ((v & 0xFFFFFF80) != 0L) {
    result &= PutAligned<uint8_t>(static_cast<uint8_t>((v & 0x7F) | 0x80), 1);
    v >>= 7;
  }
  result &= PutAligned<uint8_t>(static_cast<uint8_t>(v & 0x7F), 1);
  return result;
}

namespace detail {

template <typename T>
inline void GetValue_(int num_bits, T* v, int max_bytes, const uint8_t* buffer,
                      int* bit_offset, int* byte_offset, uint64_t* buffered_values) {
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4800)
#endif
  *v = static_cast<T>(BitUtil::TrailingBits(*buffered_values, *bit_offset + num_bits) >>
                      *bit_offset);
#ifdef _MSC_VER
#pragma warning(pop)
#endif
  *bit_offset += num_bits;
  if (*bit_offset >= 64) {
    *byte_offset += 8;
    *bit_offset -= 64;

    int bytes_remaining = max_bytes - *byte_offset;
    if (ARROW_PREDICT_TRUE(bytes_remaining >= 8)) {
      memcpy(buffered_values, buffer + *byte_offset, 8);
    } else {
      memcpy(buffered_values, buffer + *byte_offset, bytes_remaining);
    }
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4800 4805)
#endif
    // Read bits of v that crossed into new buffered_values_
    *v = *v | static_cast<T>(BitUtil::TrailingBits(*buffered_values, *bit_offset)
                             << (num_bits - *bit_offset));
#ifdef _MSC_VER
#pragma warning(pop)
#endif
    DCHECK_LE(*bit_offset, 64);
  }
}

}  // namespace detail

template <typename T>
inline bool BitReader::GetValue(int num_bits, T* v) {
  return GetBatch(num_bits, v, 1) == 1;
}

template <typename T>
inline int BitReader::GetBatch(int num_bits, T* v, int batch_size) {
  DCHECK(buffer_ != NULL);
  // TODO: revisit this limit if necessary
  DCHECK_LE(num_bits, 32);
  DCHECK_LE(num_bits, static_cast<int>(sizeof(T) * 8));

  int bit_offset = bit_offset_;
  int byte_offset = byte_offset_;
  uint64_t buffered_values = buffered_values_;
  int max_bytes = max_bytes_;
  const uint8_t* buffer = buffer_;

  uint64_t needed_bits = num_bits * batch_size;
  uint64_t remaining_bits = (max_bytes - byte_offset) * 8 - bit_offset;
  if (remaining_bits < needed_bits) {
    batch_size = static_cast<int>(remaining_bits) / num_bits;
  }

  int i = 0;
  if (ARROW_PREDICT_FALSE(bit_offset != 0)) {
    for (; i < batch_size && bit_offset != 0; ++i) {
      detail::GetValue_(num_bits, &v[i], max_bytes, buffer, &bit_offset, &byte_offset,
                        &buffered_values);
    }
  }

  if (sizeof(T) == 4) {
    int num_unpacked =
        internal::unpack32(reinterpret_cast<const uint32_t*>(buffer + byte_offset),
                           reinterpret_cast<uint32_t*>(v + i), batch_size - i, num_bits);
    i += num_unpacked;
    byte_offset += num_unpacked * num_bits / 8;
  } else {
    const int buffer_size = 1024;
    uint32_t unpack_buffer[buffer_size];
    while (i < batch_size) {
      int unpack_size = std::min(buffer_size, batch_size - i);
      int num_unpacked =
          internal::unpack32(reinterpret_cast<const uint32_t*>(buffer + byte_offset),
                             unpack_buffer, unpack_size, num_bits);
      if (num_unpacked == 0) {
        break;
      }
      for (int k = 0; k < num_unpacked; ++k) {
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4800)
#endif
        v[i + k] = static_cast<T>(unpack_buffer[k]);
#ifdef _MSC_VER
#pragma warning(pop)
#endif
      }
      i += num_unpacked;
      byte_offset += num_unpacked * num_bits / 8;
    }
  }

  int bytes_remaining = max_bytes - byte_offset;
  if (bytes_remaining >= 8) {
    memcpy(&buffered_values, buffer + byte_offset, 8);
  } else {
    memcpy(&buffered_values, buffer + byte_offset, bytes_remaining);
  }

  for (; i < batch_size; ++i) {
    detail::GetValue_(num_bits, &v[i], max_bytes, buffer, &bit_offset, &byte_offset,
                      &buffered_values);
  }

  bit_offset_ = bit_offset;
  byte_offset_ = byte_offset;
  buffered_values_ = buffered_values;

  return batch_size;
}

template <typename T>
inline bool BitReader::GetAligned(int num_bytes, T* v) {
  DCHECK_LE(num_bytes, static_cast<int>(sizeof(T)));
  int bytes_read = static_cast<int>(BitUtil::BytesForBits(bit_offset_));
  if (ARROW_PREDICT_FALSE(byte_offset_ + bytes_read + num_bytes > max_bytes_))
    return false;

  // Advance byte_offset to next unread byte and read num_bytes
  byte_offset_ += bytes_read;
  memcpy(v, buffer_ + byte_offset_, num_bytes);
  byte_offset_ += num_bytes;

  // Reset buffered_values_
  bit_offset_ = 0;
  int bytes_remaining = max_bytes_ - byte_offset_;
  if (ARROW_PREDICT_TRUE(bytes_remaining >= 8)) {
    memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
  } else {
    memcpy(&buffered_values_, buffer_ + byte_offset_, bytes_remaining);
  }
  return true;
}

inline bool BitReader::GetVlqInt(int32_t* v) {
  *v = 0;
  int shift = 0;
  int num_bytes = 0;
  uint8_t byte = 0;
  do {
    if (!GetAligned<uint8_t>(1, &byte)) return false;
    *v |= (byte & 0x7F) << shift;
    shift += 7;
    DCHECK_LE(++num_bytes, MAX_VLQ_BYTE_LEN);
  } while ((byte & 0x80) != 0);
  return true;
}

inline bool BitWriter::PutZigZagVlqInt(int32_t v) {
  // Note negative left shift is undefined
  uint32_t u = (static_cast<uint32_t>(v) << 1) ^ (v >> 31);
  return PutVlqInt(u);
}

inline bool BitReader::GetZigZagVlqInt(int32_t* v) {
  int32_t u_signed;
  if (!GetVlqInt(&u_signed)) return false;
  uint32_t u = static_cast<uint32_t>(u_signed);
  *reinterpret_cast<uint32_t*>(v) = (u >> 1) ^ -(static_cast<int32_t>(u & 1));
  return true;
}

}  // namespace BitUtil
}  // namespace arrow

#endif  // ARROW_UTIL_BIT_STREAM_UTILS_H
