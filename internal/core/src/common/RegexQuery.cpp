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
#include <regex>

#include "common/RegexQuery.h"

namespace milvus {

bool
is_special(char c) {
    // initial special_bytes_bitmap only once.
    static std::string special_bytes(R"(\.+*?()|[]{}^$)");
    static char special_bytes_bitmap[16];
    static bool _ = []() -> bool {
        for (char b : special_bytes) {
            special_bytes_bitmap[b % 16] |= 1 << (b / 16);
        }
        return true;
    };
    return special_bytes_bitmap[c % 16] & (1 << (c / 16)) != 0;
}

std::string
quote_meta(const std::string& s) {
    size_t i;
    size_t l = s.length();
    std::string r;
    for (i = 0; i < l; i++) {
        if (is_special(s[i])) {
            r += '\\';
        }
        r += s[i];
    }
    return r;
}

std::string
replace_unescaped_chars(const std::string& input,
                        char src,
                        const std::string& replacement) {
    std::string result;
    bool escape_mode = false;

    for (char c : input) {
        if (escape_mode) {
            result += '\\';
            result += c;
            escape_mode = false;
        } else {
            if (c == '\\') {
                escape_mode = true;
            } else if (c == src) {
                result += replacement;
            } else {
                result += c;
            }
        }
    }

    return result;
}

std::string
translate_pattern_match_to_regex(const std::string& pattern) {
    auto r = quote_meta(pattern);
    r = replace_unescaped_chars(r, '%', "[\\s\\S]*");
    r = replace_unescaped_chars(r, '_', "[\\s\\S]");
    return r;
}
}  // namespace milvus
