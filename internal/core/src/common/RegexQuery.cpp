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

#include <mutex>

#include "common/RegexQuery.h"

namespace milvus {

bool
is_special(char c) {
    // initial special_bytes_bitmap only once.
    static std::once_flag _initialized;
    static std::string special_bytes(R"(\.+*?()|[]{}^$)");
    static std::vector<bool> special_bytes_bitmap;
    std::call_once(_initialized, []() -> void {
        special_bytes_bitmap.resize(256);
        for (char b : special_bytes) {
            special_bytes_bitmap[b + 128] = true;
        }
    });

    return special_bytes_bitmap[c + 128];
}

// Regex pattern to match a single UTF-8 character (1-4 bytes)
// Using RE2 hex escapes (\\xHH) for byte values in character classes
// Note: We use \\x (escaped backslash) so RE2 interprets the hex escape, not the C++ compiler
// UTF-8 encoding ranges:
//   1 byte:  0x00-0x7F (ASCII) - including NUL for consistency with LikePatternMatcher
//   2 bytes: 0xC2-0xDF followed by 0x80-0xBF
//   3 bytes: 0xE0-0xEF followed by 0x80-0xBF (x2)
//   4 bytes: 0xF0-0xF7 followed by 0x80-0xBF (x3)
static const char kUtf8CharPattern[] =
    "(?:[\\x00-\\x7f]"             // 1-byte ASCII (0x00-0x7F) including NUL
    "|[\\xc2-\\xdf][\\x80-\\xbf]"  // 2-byte sequences
    "|[\\xe0-\\xef][\\x80-\\xbf][\\x80-\\xbf]"  // 3-byte sequences
    "|[\\xf0-\\xf7][\\x80-\\xbf][\\x80-\\xbf][\\x80-\\xbf])";  // 4-byte sequences

// Pattern to match zero or more UTF-8 characters
static const char kUtf8CharsPattern[] =
    "(?:[\\x00-\\x7f]"             // 1-byte ASCII (0x00-0x7F) including NUL
    "|[\\xc2-\\xdf][\\x80-\\xbf]"  // 2-byte sequences
    "|[\\xe0-\\xef][\\x80-\\xbf][\\x80-\\xbf]"  // 3-byte sequences
    "|[\\xf0-\\xf7][\\x80-\\xbf][\\x80-\\xbf][\\x80-\\xbf])*";  // 4-byte sequences

std::string
translate_pattern_match_to_regex(const std::string& pattern) {
    std::string r;
    r.reserve(4 * pattern.size());  // UTF-8 patterns are longer
    bool escape_mode = false;
    for (char c : pattern) {
        if (escape_mode) {
            if (is_special(c)) {
                r += '\\';
            }
            r += c;
            escape_mode = false;
        } else {
            if (c == '\\') {
                escape_mode = true;
            } else if (c == '%') {
                r += kUtf8CharsPattern;
            } else if (c == '_') {
                r += kUtf8CharPattern;
            } else {
                if (is_special(c)) {
                    r += '\\';
                }
                r += c;
            }
        }
    }
    // Trailing backslash is a parse error - nothing to escape
    if (escape_mode) {
        ThrowInfo(ExprInvalid,
                  "Invalid LIKE pattern: trailing backslash with nothing "
                  "to escape");
    }
    return r;
}

std::string
extract_fixed_prefix_from_pattern(const std::string& pattern) {
    std::string prefix;
    prefix.reserve(pattern.size());
    bool escape_mode = false;

    for (char c : pattern) {
        if (escape_mode) {
            prefix += c;
            escape_mode = false;
        } else {
            if (c == '\\') {
                escape_mode = true;
            } else if (c == '%' || c == '_') {
                break;  // stop at first wildcard
            } else {
                prefix += c;
            }
        }
    }
    // Trailing backslash is a parse error - nothing to escape
    if (escape_mode) {
        ThrowInfo(ExprInvalid,
                  "Invalid LIKE pattern: trailing backslash with nothing "
                  "to escape");
    }
    return prefix;
}

}  // namespace milvus
