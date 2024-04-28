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

#include <string>
#include <regex>
#include <boost/regex.hpp>
#include <utility>

#include "common/EasyAssert.h"

namespace milvus {
bool
is_special(char c);

std::string
translate_pattern_match_to_regex(const std::string& pattern);

struct PatternMatchTranslator {
    template <typename T>
    inline std::string
    operator()(const T& pattern) {
        PanicInfo(OpTypeInvalid,
                  "pattern matching is only supported on string type");
    }
};

template <>
inline std::string
PatternMatchTranslator::operator()<std::string>(const std::string& pattern) {
    return translate_pattern_match_to_regex(pattern);
}

struct RegexMatcher {
    template <typename T>
    inline bool
    operator()(const T& operand) {
        return false;
    }

    explicit RegexMatcher(const std::string& pattern) {
        r_ = boost::regex(pattern);
    }

 private:
    // avoid to construct the regex everytime.
    boost::regex r_;
};

template <>
inline bool
RegexMatcher::operator()(const std::string& operand) {
    // corner case:
    // . don't match \n, but .* match \n.
    // For example,
    // boost::regex_match("Hello\n", boost::regex("Hello.")) returns false
    // but
    // boost::regex_match("Hello\n", boost::regex("Hello.*")) returns true
    return boost::regex_match(operand, r_);
}

template <>
inline bool
RegexMatcher::operator()(const std::string_view& operand) {
    return boost::regex_match(operand.begin(), operand.end(), r_);
}
}  // namespace milvus
