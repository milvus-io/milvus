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

// Try to translate the subset of Milvus regex semantics that can be safely
// expressed as a Tantivy RegexQuery pattern.
//
// Milvus evaluates RegexMatch as RE2::PartialMatch with dot_nl=true. Tantivy's
// RegexQuery matches the entire term and has different support/semantics for a
// few constructs. This helper therefore does only conservative rewrites:
//   * rewrite unescaped `.` to `[\s\S]` while dot-all is active, preserving
//     Milvus' dot_nl=true behavior;
//   * track inline `s` and `m` flags, including scoped groups like `(?-s:...)`;
//   * wrap unanchored patterns with `[\s\S]*(?:...)[\s\S]*` so Tantivy's
//     full-term regex behaves like RE2 PartialMatch;
//   * strip only safe outer `^`/`$` anchors before wrapping;
//   * normalize lazy quantifiers (`*?`, `+?`, `??`, `{m,n}?`) by dropping the
//     laziness marker, because greedy and lazy forms accept the same language.
//
// It is deliberately not a general RE2-to-Tantivy transpiler. Constructs whose
// semantics depend on RE2-specific zero-width assertions or character-class
// definitions are marked unsafe and must use the RE2 fallback path instead.
// This includes `^`/`$` anchors in non-outer or multiline-sensitive positions,
// word/text boundaries, and shorthand character classes like `\d`, `\w`, and
// `\s`, whose Unicode/ASCII behavior can differ between RE2 and Tantivy.
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
    bool previous_token_is_quantifier = false;

    auto is_regex_shorthand_or_boundary = [](char escaped) {
        switch (escaped) {
            case 'b':
            case 'B':
            case 'A':
            case 'z':
            case 'Z':
            case 'd':
            case 'D':
            case 's':
            case 'S':
            case 'w':
            case 'W':
            case 'p':
            case 'P':
                return true;
            default:
                return false;
        }
    };

    auto parse_quantifier_end = [&](size_t pos) -> size_t {
        if (pos >= pattern.size() || pattern[pos] != '{') {
            return std::string::npos;
        }
        size_t j = pos + 1;
        bool has_min = false;
        while (j < pattern.size() && pattern[j] >= '0' && pattern[j] <= '9') {
            has_min = true;
            ++j;
        }
        if (!has_min || j >= pattern.size()) {
            return std::string::npos;
        }
        if (pattern[j] == '}') {
            return j;
        }
        if (pattern[j] != ',') {
            return std::string::npos;
        }
        ++j;
        while (j < pattern.size() && pattern[j] >= '0' && pattern[j] <= '9') {
            ++j;
        }
        if (j < pattern.size() && pattern[j] == '}') {
            return j;
        }
        return std::string::npos;
    };

    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];

        if (c == '\\' && i + 1 < pattern.size()) {
            char escaped = pattern[i + 1];
            if (!in_char_class && is_regex_shorthand_or_boundary(escaped)) {
                unsupported_zero_width = true;
            }
            result += c;
            result += escaped;
            ++i;
            previous_token_is_quantifier = false;
        } else if (c == '[') {
            in_char_class = true;
            result += c;
            previous_token_is_quantifier = false;
        } else if (c == ']' && in_char_class) {
            in_char_class = false;
            result += c;
            previous_token_is_quantifier = false;
        } else if (in_char_class) {
            result += c;
            previous_token_is_quantifier = false;
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
                previous_token_is_quantifier = false;
            } else {
                ++group_depth;
                result += c;
                previous_token_is_quantifier = false;
            }
        } else if (c == '(') {
            ++group_depth;
            result += c;
            previous_token_is_quantifier = false;
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
            previous_token_is_quantifier = false;
        } else if (c == '.') {
            if (dot_all) {
                result += "[\\s\\S]";
            } else {
                result += '.';
            }
            previous_token_is_quantifier = false;
        } else if ((c == '^' || c == '$') &&
                   !(wrap_for_substring && c == '^' && i == 0 && !multiline) &&
                   !(wrap_for_substring && c == '$' &&
                     i + 1 == pattern.size() && !multiline)) {
            unsupported_zero_width = true;
            result += c;
            previous_token_is_quantifier = false;
        } else if (c == '?' && previous_token_is_quantifier) {
            // Tantivy does not support lazy quantifiers. Dropping laziness is
            // safe because greedy and lazy quantifiers accept the same language.
            previous_token_is_quantifier = false;
        } else if (c == '*' || c == '+' || c == '?') {
            result += c;
            previous_token_is_quantifier = true;
        } else if (c == '{') {
            auto quantifier_end = parse_quantifier_end(i);
            if (quantifier_end != std::string::npos) {
                result.append(pattern, i, quantifier_end - i + 1);
                i = quantifier_end;
                previous_token_is_quantifier = true;
            } else {
                result += c;
                previous_token_is_quantifier = false;
            }
        } else {
            result += c;
            previous_token_is_quantifier = false;
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

}  // namespace milvus
