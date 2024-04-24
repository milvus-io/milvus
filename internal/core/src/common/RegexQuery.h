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
std::string
ReplaceUnescapedChars(const std::string& input,
                      char src,
                      const std::string& replacement);

std::string
TranslatePatternMatchToRegex(const std::string& pattern);

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
    return TranslatePatternMatchToRegex(pattern);
}

struct RegexMatcher {
    virtual bool
    operator()(const std::string& operand) = 0;

    virtual bool
    operator()(const std::string_view& operand) = 0;
};

struct RegexMatcherHelper {
    explicit RegexMatcherHelper(std::shared_ptr<RegexMatcher> matcher)
        : matcher_(std::move(matcher)) {
    }

    template <typename T>
    inline bool
    operator()(const T& operand) {
        return false;
    }

 private:
    std::shared_ptr<RegexMatcher> matcher_;
};

template <>
inline bool
RegexMatcherHelper::operator()<std::string>(const std::string& operand) {
    return matcher_->operator()(operand);
}

template <>
inline bool
RegexMatcherHelper::operator()<std::string_view>(
    const std::string_view& operand) {
    return matcher_->operator()(operand);
}

template <bool newline_included = true>
struct BoostRegexMatcher : public RegexMatcher {
    bool
    operator()(const std::string& operand) override {
        return boost::regex_match(operand, r_);
    }

    bool
    operator()(const std::string_view& operand) override {
        return boost::regex_match(operand.begin(), operand.end(), r_);
    }

    explicit BoostRegexMatcher(const std::string& pattern) {
        if constexpr (newline_included) {
            r_ = boost::regex(pattern, boost::regex::mod_s);
        } else {
            r_ = boost::regex(pattern);
        }
    }

 private:
    // avoid to construct the regex everytime.
    boost::regex r_;
};

inline std::shared_ptr<RegexMatcher>
CreateDefaultRegexMatcher(const std::string& pattern) {
    return std::make_shared<BoostRegexMatcher<true>>(pattern);
}
}  // namespace milvus
