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

#include <re2/re2.h>

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

std::string
translate_pattern_match_to_regex(const std::string& pattern) {
    std::string r;
    r.reserve(2 * pattern.size());
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
                r += "[\\s\\S]*";
            } else if (c == '_') {
                r += "[\\s\\S]";
            } else {
                if (is_special(c)) {
                    r += '\\';
                }
                r += c;
            }
        }
    }
    return r;
}
}  // namespace milvus
