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
#include <fnmatch.h>

#include "common/EasyAssert.h"

namespace milvus {
bool
is_special(char c);

// Check if the pattern is simple (only contains % and _ wildcards, no escape sequences)
bool
is_simple_pattern(const std::string& pattern);

// Translate SQL LIKE pattern (% and _) to fnmatch pattern (* and ?)
std::string
translate_pattern_match_to_fnmatch(const std::string& pattern);

std::string
translate_pattern_match_to_regex(const std::string& pattern);

struct PatternMatchTranslator {
    template <typename T>
    inline std::string
    operator()(const T& pattern) {
        ThrowInfo(OpTypeInvalid,
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

    // Constructor that accepts an already-translated regex pattern
    explicit RegexMatcher(const std::string& regex_pattern) {
        r_ = boost::regex(regex_pattern);
    }

    // Factory method for LIKE pattern matching with fnmatch optimization
    static RegexMatcher
    FromLikePattern(const std::string& like_pattern) {
        RegexMatcher matcher;
        matcher.use_fnmatch_ = is_simple_pattern(like_pattern);
        if (matcher.use_fnmatch_) {
            matcher.fnmatch_pattern_ =
                translate_pattern_match_to_fnmatch(like_pattern);
        } else {
            auto regex_pattern = translate_pattern_match_to_regex(like_pattern);
            matcher.r_ = boost::regex(regex_pattern);
        }
        return matcher;
    }

 private:
    RegexMatcher() = default;
    bool use_fnmatch_ = false;
    std::string fnmatch_pattern_;
    // avoid to construct the regex everytime.
    boost::regex r_;
};

template <>
inline bool
RegexMatcher::operator()(const std::string& operand) {
    if (use_fnmatch_) {
        // fnmatch returns 0 on match, FNM_NOMATCH on no match
        return fnmatch(fnmatch_pattern_.c_str(), operand.c_str(), 0) == 0;
    }
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
    if (use_fnmatch_) {
        // fnmatch requires null-terminated string, so convert string_view to string
        std::string str(operand);
        return fnmatch(fnmatch_pattern_.c_str(), str.c_str(), 0) == 0;
    }
    return boost::regex_match(operand.begin(), operand.end(), r_);
}
}  // namespace milvus
