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
std::string
ReplaceUnescapedChars(const std::string& input,
                      char src,
                      const std::string& replacement) {
    std::string result;
    bool escapeMode = false;

    for (char c : input) {
        if (escapeMode) {
            result += '\\';
            result += c;
            escapeMode = false;
        } else {
            if (c == '\\') {
                escapeMode = true;
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
TranslatePatternMatchToRegex(const std::string& pattern) {
    std::string regex_pattern;
#if 0
        regex_pattern = R"([\.\*\+\?\|\(\)\[\]\{\}\\])";
#else
    regex_pattern = R"([\.\*\+\?\|\(\)\[\]\{\}])";
#endif
    std::string regex =
        std::regex_replace(pattern, std::regex(regex_pattern), R"(\$&)");
    regex = ReplaceUnescapedChars(regex, '%', ".*");
    regex = ReplaceUnescapedChars(regex, '_', ".");
    return regex;
}
}  // namespace milvus
