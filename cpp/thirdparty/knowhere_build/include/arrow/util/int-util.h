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

#ifndef ARROW_UTIL_INT_UTIL_H
#define ARROW_UTIL_INT_UTIL_H

#include <cstdint>
#include <type_traits>

#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

ARROW_EXPORT
uint8_t DetectUIntWidth(const uint64_t* values, int64_t length, uint8_t min_width = 1);

ARROW_EXPORT
uint8_t DetectUIntWidth(const uint64_t* values, const uint8_t* valid_bytes,
                        int64_t length, uint8_t min_width = 1);

ARROW_EXPORT
uint8_t DetectIntWidth(const int64_t* values, int64_t length, uint8_t min_width = 1);

ARROW_EXPORT
uint8_t DetectIntWidth(const int64_t* values, const uint8_t* valid_bytes, int64_t length,
                       uint8_t min_width = 1);

ARROW_EXPORT
void DowncastInts(const int64_t* source, int8_t* dest, int64_t length);

ARROW_EXPORT
void DowncastInts(const int64_t* source, int16_t* dest, int64_t length);

ARROW_EXPORT
void DowncastInts(const int64_t* source, int32_t* dest, int64_t length);

ARROW_EXPORT
void DowncastInts(const int64_t* source, int64_t* dest, int64_t length);

ARROW_EXPORT
void DowncastUInts(const uint64_t* source, uint8_t* dest, int64_t length);

ARROW_EXPORT
void DowncastUInts(const uint64_t* source, uint16_t* dest, int64_t length);

ARROW_EXPORT
void DowncastUInts(const uint64_t* source, uint32_t* dest, int64_t length);

ARROW_EXPORT
void DowncastUInts(const uint64_t* source, uint64_t* dest, int64_t length);

template <typename InputInt, typename OutputInt>
ARROW_EXPORT void TransposeInts(const InputInt* source, OutputInt* dest, int64_t length,
                                const int32_t* transpose_map);

/// Signed addition with well-defined behaviour on overflow (as unsigned)
template <typename SignedInt>
SignedInt SafeSignedAdd(SignedInt u, SignedInt v) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) +
                                static_cast<UnsignedInt>(v));
}

/// Signed left shift with well-defined behaviour on negative numbers or overflow
template <typename SignedInt, typename Shift>
SignedInt SafeLeftShift(SignedInt u, Shift shift) {
  using UnsignedInt = typename std::make_unsigned<SignedInt>::type;
  return static_cast<SignedInt>(static_cast<UnsignedInt>(u) << shift);
}

/// Upcast an integer to the largest possible width (currently 64 bits)

template <typename Integer>
typename std::enable_if<
    std::is_integral<Integer>::value && std::is_signed<Integer>::value, int64_t>::type
UpcastInt(Integer v) {
  return v;
}

template <typename Integer>
typename std::enable_if<
    std::is_integral<Integer>::value && std::is_unsigned<Integer>::value, uint64_t>::type
UpcastInt(Integer v) {
  return v;
}

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_INT_UTIL_H
