//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

/**
 * Helper functions which serialize and deserialize integers
 * into bytes in big endian.
 */

#pragma once

namespace rocksdb {
namespace cassandra {
namespace {
const int64_t kCharMask = 0xFFLL;
const int32_t kBitsPerByte = 8;
}

template<typename T>
void Serialize(T val, std::string* dest);

template<typename T>
T Deserialize(const char* src, std::size_t offset=0);

// Specializations
template<>
inline void Serialize<int8_t>(int8_t t, std::string* dest) {
  dest->append(1, static_cast<char>(t & kCharMask));
}

template<>
inline void Serialize<int32_t>(int32_t t, std::string* dest) {
  for (unsigned long i = 0; i < sizeof(int32_t); i++) {
     dest->append(1, static_cast<char>(
       (t >> (sizeof(int32_t) - 1 - i) * kBitsPerByte) & kCharMask));
  }
}

template<>
inline void Serialize<int64_t>(int64_t t, std::string* dest) {
  for (unsigned long i = 0; i < sizeof(int64_t); i++) {
     dest->append(
       1, static_cast<char>(
         (t >> (sizeof(int64_t) - 1 - i) * kBitsPerByte) & kCharMask));
  }
}

template<>
inline int8_t Deserialize<int8_t>(const char* src, std::size_t offset) {
  return static_cast<int8_t>(src[offset]);
}

template<>
inline int32_t Deserialize<int32_t>(const char* src, std::size_t offset) {
  int32_t result = 0;
  for (unsigned long i = 0; i < sizeof(int32_t); i++) {
    result |= static_cast<int32_t>(static_cast<unsigned char>(src[offset + i]))
        << ((sizeof(int32_t) - 1 - i) * kBitsPerByte);
  }
  return result;
}

template<>
inline int64_t Deserialize<int64_t>(const char* src, std::size_t offset) {
  int64_t result = 0;
  for (unsigned long i = 0; i < sizeof(int64_t); i++) {
    result |= static_cast<int64_t>(static_cast<unsigned char>(src[offset + i]))
        << ((sizeof(int64_t) - 1 - i) * kBitsPerByte);
  }
  return result;
}

} // namepsace cassandrda
} // namespace rocksdb
