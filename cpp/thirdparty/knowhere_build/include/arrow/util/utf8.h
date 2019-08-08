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

#ifndef ARROW_UTIL_UTF8_H
#define ARROW_UTIL_UTF8_H

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

// Convert a UTF8 string to a wstring (either UTF16 or UTF32, depending
// on the wchar_t width).
ARROW_EXPORT Status UTF8ToWideString(const std::string& source, std::wstring* out);

// Similarly, convert a wstring to a UTF8 string.
ARROW_EXPORT Status WideStringToUTF8(const std::wstring& source, std::string* out);

namespace internal {

// Copyright (c) 2008-2010 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

// A compact state table allowing UTF8 decoding using two dependent
// lookups per byte.  The first lookup determines the character class
// and the second lookup reads the next state.
// In this table states are multiples of 12.
ARROW_EXPORT extern const uint8_t utf8_small_table[256 + 9 * 12];

// Success / reject states when looked up in the small table
static constexpr uint8_t kUTF8DecodeAccept = 0;
static constexpr uint8_t kUTF8DecodeReject = 12;

// An expanded state table allowing transitions using a single lookup
// at the expense of a larger memory footprint (but on non-random data,
// not all the table will end up accessed and cached).
// In this table states are multiples of 256.
ARROW_EXPORT extern uint16_t utf8_large_table[9 * 256];

// Success / reject states when looked up in the large table
static constexpr uint16_t kUTF8ValidateAccept = 0;
static constexpr uint16_t kUTF8ValidateReject = 256;

static inline uint8_t DecodeOneUTF8Byte(uint8_t byte, uint8_t state, uint32_t* codep) {
  uint8_t type = utf8_small_table[byte];

  *codep = (state != kUTF8DecodeAccept) ? (byte & 0x3fu) | (*codep << 6)
                                        : (0xff >> type) & (byte);

  state = utf8_small_table[256 + state + type];
  return state;
}

static inline uint16_t ValidateOneUTF8Byte(uint8_t byte, uint16_t state) {
  return utf8_large_table[state + byte];
}

#ifndef NDEBUG
ARROW_EXPORT void CheckUTF8Initialized();
#endif

}  // namespace internal

// This function needs to be called before doing UTF8 validation.
ARROW_EXPORT void InitializeUTF8();

inline bool ValidateUTF8(const uint8_t* data, int64_t size) {
  static constexpr uint64_t high_bits_64 = 0x8080808080808080ULL;
  // For some reason, defining this variable outside the loop helps clang
  uint64_t mask;

#ifndef NDEBUG
  internal::CheckUTF8Initialized();
#endif

  while (size >= 8) {
    // XXX This is doing an unaligned access.  Contemporary architectures
    // (x86-64, AArch64, PPC64) support it natively and often have good
    // performance nevertheless.
    memcpy(&mask, data, 8);
    if (ARROW_PREDICT_TRUE((mask & high_bits_64) == 0)) {
      // 8 bytes of pure ASCII, move forward
      size -= 8;
      data += 8;
      continue;
    }
    // Non-ASCII run detected.
    // We process at least 4 bytes, to avoid too many spurious 64-bit reads
    // in case the non-ASCII bytes are at the end of the tested 64-bit word.
    // We also only check for rejection at the end since that state is stable
    // (once in reject state, we always remain in reject state).
    // It is guaranteed that size >= 8 when arriving here, which allows
    // us to avoid size checks.
    uint16_t state = internal::kUTF8ValidateAccept;
    // Byte 0
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 1
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 2
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 3
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 4
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 5
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 6
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 7
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // kUTF8ValidateAccept not reached along 4 transitions has to mean a rejection
    assert(state == internal::kUTF8ValidateReject);
    return false;
  }

  // Validate string tail one byte at a time
  // Note the state table is designed so that, once in the reject state,
  // we remain in that state until the end.  So we needn't check for
  // rejection at each char (we don't gain much by short-circuiting here).
  uint16_t state = internal::kUTF8ValidateAccept;
  while (size-- > 0) {
    state = internal::ValidateOneUTF8Byte(*data++, state);
  }
  return ARROW_PREDICT_TRUE(state == internal::kUTF8ValidateAccept);
}

inline bool ValidateUTF8(const util::string_view& str) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(str.data());
  const size_t length = str.size();

  return ValidateUTF8(data, length);
}

// Skip UTF8 byte order mark, if any.
ARROW_EXPORT
Status SkipUTF8BOM(const uint8_t* data, int64_t size, const uint8_t** out);

}  // namespace util
}  // namespace arrow

#endif
