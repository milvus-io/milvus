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
#include <string_view>
#include <boost/regex.hpp>
#include <re2/re2.h>
#include <utility>
#include <memory>
#include <vector>

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
        ThrowInfo(OpTypeInvalid,
                  "pattern matching is only supported on string type");
    }
};

template <>
inline std::string
PatternMatchTranslator::operator()<std::string>(const std::string& pattern) {
    return translate_pattern_match_to_regex(pattern);
}

// RegexMatcher using RE2 for high performance regex matching
// RE2 is 2-3x faster than boost::regex for most patterns
struct RegexMatcher {
    template <typename T>
    inline bool
    operator()(const T& operand) {
        return false;
    }

    explicit RegexMatcher(const std::string& pattern) {
        RE2::Options options;
        options.set_dot_nl(true);  // Make . match \n (like boost's [\s\S])
        options.set_log_errors(false);
        re2_ = std::make_unique<RE2>(pattern, options);
        AssertInfo(re2_->ok(),
                   "Failed to compile regex pattern: " + re2_->error());
    }

 private:
    std::unique_ptr<RE2> re2_;
};

template <>
inline bool
RegexMatcher::operator()(const std::string& operand) {
    return RE2::FullMatch(operand, *re2_);
}

template <>
inline bool
RegexMatcher::operator()(const std::string_view& operand) {
    re2::StringPiece sp(operand.data(), operand.size());
    return RE2::FullMatch(sp, *re2_);
}

// BoostRegexMatcher - kept for benchmark comparison
// This is the original implementation using boost::regex
struct BoostRegexMatcher {
    template <typename T>
    inline bool
    operator()(const T& operand) {
        return false;
    }

    explicit BoostRegexMatcher(const std::string& pattern) {
        r_ = boost::regex(pattern);
    }

 private:
    boost::regex r_;
};

template <>
inline bool
BoostRegexMatcher::operator()(const std::string& operand) {
    return boost::regex_match(operand, r_);
}

template <>
inline bool
BoostRegexMatcher::operator()(const std::string_view& operand) {
    return boost::regex_match(operand.begin(), operand.end(), r_);
}

// Extract fixed prefix from LIKE pattern (before first % or _)
// Examples: "abc%def" -> "abc", "ab_cd%" -> "ab", "%abc" -> ""
std::string
extract_fixed_prefix_from_pattern(const std::string& pattern);

// LikePatternMatcher - optimized for all LIKE patterns (both % and _)
// For patterns like "a%b_c%d", uses simple string operations instead of regex
// This is 5-15x faster than regex engines
class LikePatternMatcher {
 public:
    template <typename T>
    inline bool
    operator()(const T& operand) {
        return false;
    }

    explicit LikePatternMatcher(const std::string& pattern) {
        ParsePattern(pattern);
    }

 private:
    // A segment between % wildcards, may contain _ wildcards
    struct Segment {
        std::string text;  // The literal text (with _ removed)
        std::vector<size_t>
            underscore_positions;  // Positions of _ in original segment
        size_t total_length;       // Length including _ wildcards
    };

    void
    ParsePattern(const std::string& pattern) {
        Segment current_segment;
        current_segment.total_length = 0;
        bool escape_mode = false;
        size_t pos_in_segment = 0;

        // Track wildcards during parsing to handle escapes correctly
        leading_wildcard_ = false;
        trailing_wildcard_ = false;
        bool first_char_processed = false;

        for (size_t i = 0; i < pattern.size(); ++i) {
            char c = pattern[i];
            if (escape_mode) {
                current_segment.text += c;
                current_segment.total_length++;
                pos_in_segment++;
                escape_mode = false;
                trailing_wildcard_ = false;  // Escaped char, not a wildcard
            } else if (c == '\\') {
                escape_mode = true;
                trailing_wildcard_ = false;  // Escape sequence, not a wildcard
            } else if (c == '%') {
                segments_.push_back(std::move(current_segment));
                current_segment = Segment();
                current_segment.total_length = 0;
                pos_in_segment = 0;
                if (!first_char_processed) {
                    leading_wildcard_ = true;
                }
                trailing_wildcard_ =
                    true;  // May be trailing, will be reset if more chars follow
            } else if (c == '_') {
                current_segment.underscore_positions.push_back(pos_in_segment);
                current_segment.total_length++;
                pos_in_segment++;
                trailing_wildcard_ = false;  // Not a % wildcard
            } else {
                current_segment.text += c;
                current_segment.total_length++;
                pos_in_segment++;
                trailing_wildcard_ = false;  // Regular char, not a wildcard
            }
            first_char_processed = true;
        }
        // Trailing backslash is a parse error - nothing to escape
        if (escape_mode) {
            ThrowInfo(ExprInvalid,
                      "Invalid LIKE pattern: trailing backslash with nothing "
                      "to escape");
        }
        segments_.push_back(std::move(current_segment));

        // Precompute minimum required length for early rejection
        min_required_length_ = 0;
        for (const auto& seg : segments_) {
            min_required_length_ += seg.total_length;
        }
    }

    // Check if a segment matches at a specific position in the string
    template <typename StringType>
    bool
    SegmentMatchesAt(const Segment& seg,
                     const StringType& str,
                     size_t str_pos) const {
        if (str_pos + seg.total_length > str.size()) {
            return false;
        }

        // Check each character, skipping underscore positions
        size_t text_idx = 0;
        size_t underscore_idx = 0;
        size_t next_underscore = seg.underscore_positions.empty()
                                     ? std::string::npos
                                     : seg.underscore_positions[0];
        for (size_t i = 0; i < seg.total_length; ++i) {
            if (i == next_underscore) {
                ++underscore_idx;
                next_underscore =
                    (underscore_idx < seg.underscore_positions.size())
                        ? seg.underscore_positions[underscore_idx]
                        : std::string::npos;
                continue;
            }
            if (str[str_pos + i] != seg.text[text_idx]) {
                return false;
            }
            text_idx++;
        }
        return true;
    }

    // Find segment in string starting from pos
    template <typename StringType>
    size_t
    FindSegment(const Segment& seg,
                const StringType& str,
                size_t start_pos) const {
        if (seg.underscore_positions.empty()) {
            // No underscores - use fast string find
            size_t found = str.find(seg.text, start_pos);
            return found;
        }

        // Has underscores - need to check each position
        for (size_t pos = start_pos; pos + seg.total_length <= str.size();
             ++pos) {
            if (SegmentMatchesAt(seg, str, pos)) {
                return pos;
            }
        }
        return std::string::npos;
    }

    template <typename StringType>
    bool
    MatchImpl(const StringType& str) const {
        if (segments_.empty()) {
            return true;
        }

        size_t str_len = str.size();

        // Early rejection: string too short to match all segments
        if (str_len < min_required_length_) {
            return false;
        }

        size_t pos = 0;

        // If no wildcards at all, string length must match total pattern length
        if (!leading_wildcard_ && !trailing_wildcard_ &&
            segments_.size() == 1) {
            return str_len == segments_[0].total_length &&
                   (segments_[0].total_length == 0 ||
                    SegmentMatchesAt(segments_[0], str, 0));
        }

        for (size_t i = 0; i < segments_.size(); ++i) {
            const auto& seg = segments_[i];
            if (seg.total_length == 0) {
                continue;
            }

            bool is_first = (i == 0);
            bool is_last = (i == segments_.size() - 1);

            if (is_first && !leading_wildcard_) {
                // First segment must match at start
                if (!SegmentMatchesAt(seg, str, 0)) {
                    return false;
                }
                pos = seg.total_length;
                // If also last and no trailing wildcard, verify exact length
                if (is_last && !trailing_wildcard_) {
                    return str_len == seg.total_length;
                }
            } else if (is_last && !trailing_wildcard_) {
                // Last segment must match at end
                if (str_len < seg.total_length) {
                    return false;
                }
                size_t end_pos = str_len - seg.total_length;
                if (end_pos < pos || !SegmentMatchesAt(seg, str, end_pos)) {
                    return false;
                }
            } else {
                // Middle segment - find anywhere after current position
                size_t found = FindSegment(seg, str, pos);
                if (found == std::string::npos) {
                    return false;
                }
                // Use found + 1 instead of found + seg.total_length to allow
                // overlapping matches. The % wildcard between segments can match
                // zero characters, so consecutive segments can overlap.
                // Example: "%aa%aa%" vs "aaa" - first "aa" at 0-1, second at 1-2
                pos = found + 1;
            }
        }
        return true;
    }

    std::vector<Segment> segments_;
    bool leading_wildcard_ = false;
    bool trailing_wildcard_ = false;
    size_t min_required_length_ =
        0;  // Sum of all segment lengths for early rejection
};

template <>
inline bool
LikePatternMatcher::operator()(const std::string& operand) {
    return MatchImpl(operand);
}

template <>
inline bool
LikePatternMatcher::operator()(const std::string_view& operand) {
    return MatchImpl(operand);
}

// Backward compatibility alias
using MultiWildcardMatcher = LikePatternMatcher;

// SmartPatternMatcher - optimized LIKE pattern matching without regex
// Uses LikePatternMatcher for all standard LIKE patterns (% and _)
// This is faster than regex engines for typical LIKE patterns
class SmartPatternMatcher {
 public:
    template <typename T>
    inline bool
    operator()(const T& operand) {
        return false;
    }

    explicit SmartPatternMatcher(const std::string& pattern) {
        // LikePatternMatcher handles all standard LIKE patterns (% and _)
        // No need for regex in typical cases
        like_matcher_ = std::make_unique<LikePatternMatcher>(pattern);
    }

 private:
    std::unique_ptr<LikePatternMatcher> like_matcher_;
};

template <>
inline bool
SmartPatternMatcher::operator()(const std::string& operand) {
    return (*like_matcher_)(operand);
}

template <>
inline bool
SmartPatternMatcher::operator()(const std::string_view& operand) {
    return (*like_matcher_)(operand);
}

}  // namespace milvus
