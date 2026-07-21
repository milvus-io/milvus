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
#include <string>
#include <string_view>

#include "common/EasyAssert.h"
#include "fmt/core.h"

namespace milvus {

// Decodes a decimal literal (e.g. "19.99") into its unscaled int64 representation
// at the given scale (e.g. scale=4 -> 199900), via pure string/integer arithmetic —
// never through a float — so it stays exact. This is the C++ mirror of Go's
// pkg/util/parameterutil.EncodeUnscaledInt64.
//
// The literal is expected to already be valid (Go's proxy-side ValidateDecimalString
// validated it before it was ever placed on the wire), so failures here indicate an
// internal inconsistency, not a user input error, and are treated as UnexpectedError.
inline int64_t
DecodeDecimalUnscaled(std::string_view literal, int64_t scale) {
    if (literal.empty()) {
        ThrowInfo(UnexpectedError, "empty decimal literal");
    }

    bool negative = false;
    std::string_view rest = literal;
    if (rest[0] == '-') {
        negative = true;
        rest = rest.substr(1);
    }

    auto dot_pos = rest.find('.');
    std::string int_part;
    std::string frac_part;
    if (dot_pos == std::string_view::npos) {
        int_part = std::string(rest);
    } else {
        int_part = std::string(rest.substr(0, dot_pos));
        frac_part = std::string(rest.substr(dot_pos + 1));
    }

    if (int_part.empty()) {
        ThrowInfo(UnexpectedError,
                  fmt::format("invalid decimal literal {}: malformed integer "
                              "part",
                              literal));
    }
    if (static_cast<int64_t>(frac_part.size()) > scale) {
        ThrowInfo(
            UnexpectedError,
            fmt::format("decimal literal {} exceeds scale {}", literal, scale));
    }

    // Pad the fractional part with trailing zeros up to `scale` digits, so
    // "19.9" and "19.90" both decode to the identical unscaled value.
    frac_part.append(scale - frac_part.size(), '0');

    std::string digits = int_part + frac_part;
    int64_t unscaled = 0;
    try {
        size_t consumed = 0;
        unscaled = std::stoll(digits, &consumed);
        if (consumed != digits.size()) {
            ThrowInfo(UnexpectedError,
                      fmt::format("invalid decimal literal {}: non-digit "
                                  "character",
                                  literal));
        }
    } catch (const std::exception& e) {
        ThrowInfo(UnexpectedError,
                  fmt::format("decimal literal {} could not be decoded: {}",
                              literal,
                              e.what()));
    }

    return negative ? -unscaled : unscaled;
}

// Encodes an unscaled int64 (see DecodeDecimalUnscaled) back into decimal-literal
// text at the given scale, e.g. (199900, 4) -> "19.9900". Callers should not expect
// this to reproduce the exact original insert text (e.g. "19.99") — it reproduces
// the canonical, fixed-scale form, exactly like a SQL NUMERIC(p,s) column would.
inline std::string
EncodeDecimalText(int64_t unscaled, int64_t scale) {
    bool negative = unscaled < 0;
    // Note: std::abs(INT64_MIN) is UB; unscaled is bounded well within int64
    // range by MaxDecimalPrecision (18 digits), so this is safe in practice.
    uint64_t magnitude = negative ? static_cast<uint64_t>(-unscaled)
                                  : static_cast<uint64_t>(unscaled);
    std::string digits = std::to_string(magnitude);

    if (static_cast<int64_t>(digits.size()) < scale + 1) {
        digits = std::string(scale + 1 - digits.size(), '0') + digits;
    }

    std::string result = negative ? "-" : "";
    if (scale == 0) {
        return result + digits;
    }
    auto split = digits.size() - scale;
    return result + digits.substr(0, split) + "." + digits.substr(split);
}

}  // namespace milvus
