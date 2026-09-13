// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "common/EasyAssert.h"
#include "fmt/core.h"

namespace milvus {

// Canonical Decimal wire encoding width (8-byte little-endian unscaled int64).
constexpr size_t kDecimalBytesLen = 8;

// Decodes canonical 8-byte wire form into an unscaled int64.
inline int64_t
DecodeDecimalBytes(std::string_view bytes) {
    AssertInfo(bytes.size() == kDecimalBytesLen,
               "decimal value must be exactly {} bytes (little-endian unscaled "
               "int64), got {}",
               kDecimalBytesLen,
               bytes.size());

    uint64_t raw = 0;
    for (size_t i = 0; i < kDecimalBytesLen; ++i) {
        raw |= static_cast<uint64_t>(static_cast<unsigned char>(bytes[i]))
               << (8 * i);
    }
    return static_cast<int64_t>(raw);
}

// Encodes an unscaled int64 back into the canonical wire form — the exact
// mirror of DecodeDecimalBytes, used when building query results.
inline std::string
EncodeDecimalBytes(int64_t unscaled) {
    auto raw = static_cast<uint64_t>(unscaled);
    std::string bytes(kDecimalBytesLen, '\0');
    for (size_t i = 0; i < kDecimalBytesLen; ++i) {
        bytes[i] = static_cast<char>((raw >> (8 * i)) & 0xFF);
    }
    return bytes;
}

}  // namespace milvus
