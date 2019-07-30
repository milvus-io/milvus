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

#ifndef ARROW_UTIL_STRING_UTIL_H
#define ARROW_UTIL_STRING_UTIL_H

#include <algorithm>
#include <string>

#include "arrow/status.h"
#include "arrow/util/string_view.h"

namespace arrow {

static const char* kAsciiTable = "0123456789ABCDEF";

static inline std::string HexEncode(const uint8_t* data, size_t length) {
  std::string hex_string;
  hex_string.reserve(length * 2);
  for (size_t j = 0; j < length; ++j) {
    // Convert to 2 base16 digits
    hex_string.push_back(kAsciiTable[data[j] >> 4]);
    hex_string.push_back(kAsciiTable[data[j] & 15]);
  }
  return hex_string;
}

static inline std::string HexEncode(const char* data, size_t length) {
  return HexEncode(reinterpret_cast<const uint8_t*>(data), length);
}

static inline std::string HexEncode(util::string_view str) {
  return HexEncode(str.data(), str.size());
}

static inline Status ParseHexValue(const char* data, uint8_t* out) {
  char c1 = data[0];
  char c2 = data[1];

  const char* pos1 = std::lower_bound(kAsciiTable, kAsciiTable + 16, c1);
  const char* pos2 = std::lower_bound(kAsciiTable, kAsciiTable + 16, c2);

  // Error checking
  if (*pos1 != c1 || *pos2 != c2) {
    return Status::Invalid("Encountered non-hex digit");
  }

  *out = static_cast<uint8_t>((pos1 - kAsciiTable) << 4 | (pos2 - kAsciiTable));
  return Status::OK();
}

}  // namespace arrow

#endif  // ARROW_UTIL_STRING_UTIL_H
