// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <string>
#include <vector>

namespace milvus {

struct TantivyRegexPattern {
    bool use_tantivy_regex;
    std::string pattern;
};

inline TantivyRegexPattern
TryTranslateRegexToTantivyPattern(const std::string& pattern,
                                  bool wrap_for_substring = true) {
    std::string result;
    result.reserve(pattern.size() * 2);
    bool in_char_class = false;
    bool unsupported_zero_width = false;

    // RE2 execution in Milvus uses dot_nl=true. Keep that as the default
    // while still respecting inline (?s)/(?-s) flag changes.
    bool dot_all = true;
    bool multiline = false;

    struct FlagScope {
        int depth;
        bool prev_dot_all;
        bool prev_multiline;
    };
    std::vector<FlagScope> flag_stack;
    int group_depth = 0;

    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];

        if (c == '\\' && i + 1 < pattern.size()) {
            char escaped = pattern[i + 1];
            if (!in_char_class &&
                (escaped == 'b' || escaped == 'B' || escaped == 'A' ||
                 escaped == 'z' || escaped == 'Z')) {
                unsupported_zero_width = true;
            }
            result += c;
            result += escaped;
            ++i;
        } else if (c == '[') {
            in_char_class = true;
            result += c;
        } else if (c == ']' && in_char_class) {
            in_char_class = false;
            result += c;
        } else if (in_char_class) {
            result += c;
        } else if (c == '(' && i + 1 < pattern.size() &&
                   pattern[i + 1] == '?') {
            size_t j = i + 2;
            bool setting = true;
            bool found_s_set = false;
            bool found_s_clear = false;
            bool found_m_set = false;
            bool found_m_clear = false;
            bool is_flag_group = true;

            while (j < pattern.size()) {
                char fc = pattern[j];
                if (fc == 'i' || fc == 'm' || fc == 's' || fc == 'U') {
                    if (fc == 's') {
                        if (setting) {
                            found_s_set = true;
                        } else {
                            found_s_clear = true;
                        }
                    } else if (fc == 'm') {
                        if (setting) {
                            found_m_set = true;
                        } else {
                            found_m_clear = true;
                        }
                    }
                    ++j;
                } else if (fc == '-') {
                    setting = false;
                    ++j;
                } else if (fc == ':' || fc == ')') {
                    break;
                } else {
                    is_flag_group = false;
                    break;
                }
            }

            if (is_flag_group && j < pattern.size() &&
                (pattern[j] == ':' || pattern[j] == ')') && j > i + 2) {
                bool new_dot_all = dot_all;
                if (found_s_set) {
                    new_dot_all = true;
                }
                if (found_s_clear) {
                    new_dot_all = false;
                }

                bool new_multiline = multiline;
                if (found_m_set) {
                    new_multiline = true;
                }
                if (found_m_clear) {
                    new_multiline = false;
                }

                if (pattern[j] == ':') {
                    ++group_depth;
                    flag_stack.push_back({group_depth, dot_all, multiline});
                    dot_all = new_dot_all;
                    multiline = new_multiline;
                } else {
                    dot_all = new_dot_all;
                    multiline = new_multiline;
                }
                result.append(pattern, i, j - i + 1);
                i = j;
            } else {
                ++group_depth;
                result += c;
            }
        } else if (c == '(') {
            ++group_depth;
            result += c;
        } else if (c == ')') {
            if (!flag_stack.empty() && flag_stack.back().depth == group_depth) {
                dot_all = flag_stack.back().prev_dot_all;
                multiline = flag_stack.back().prev_multiline;
                flag_stack.pop_back();
            }
            if (group_depth > 0) {
                --group_depth;
            }
            result += c;
        } else if (c == '.') {
            if (dot_all) {
                result += "[\\s\\S]";
            } else {
                result += '.';
            }
        } else if ((c == '^' || c == '$') &&
                   !(wrap_for_substring && c == '^' && i == 0 && !multiline) &&
                   !(wrap_for_substring && c == '$' &&
                     i + 1 == pattern.size() && !multiline)) {
            unsupported_zero_width = true;
            result += c;
        } else if (c == '?' && i > 0 &&
                   (pattern[i - 1] == '*' || pattern[i - 1] == '+' ||
                    pattern[i - 1] == '?' || pattern[i - 1] == '}')) {
            // Tantivy does not support lazy quantifiers. Dropping laziness is
            // safe because greedy and lazy quantifiers accept the same language.
        } else {
            result += c;
        }
    }

    if (!wrap_for_substring) {
        return {!unsupported_zero_width, result};
    }

    const bool anchored_at_start = !pattern.empty() && pattern.front() == '^';
    bool anchored_at_end = false;
    if (!pattern.empty() && pattern.back() == '$') {
        size_t slash_count = 0;
        for (size_t i = pattern.size() - 1; i > 0 && pattern[i - 1] == '\\';
             --i) {
            ++slash_count;
        }
        anchored_at_end = (slash_count % 2 == 0);
    }
    if (anchored_at_start && !result.empty() && result.front() == '^') {
        result.erase(result.begin());
    }
    if (anchored_at_end && !result.empty() && result.back() == '$') {
        result.pop_back();
    }

    if (anchored_at_start && anchored_at_end) {
        return {!unsupported_zero_width, result};
    }
    if (anchored_at_start) {
        return {!unsupported_zero_width, result + "[\\s\\S]*"};
    }
    if (anchored_at_end) {
        return {!unsupported_zero_width, "[\\s\\S]*(?:" + result + ")"};
    }
    return {!unsupported_zero_width, "[\\s\\S]*(?:" + result + ")[\\s\\S]*"};
}

inline std::string
regex_to_tantivy_pattern(const std::string& pattern,
                         bool wrap_for_substring = true) {
    return TryTranslateRegexToTantivyPattern(pattern, wrap_for_substring)
        .pattern;
}

}  // namespace milvus
