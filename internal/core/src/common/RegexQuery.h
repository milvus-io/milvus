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
#include <string_view>
#include <optional>
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
        options.set_dot_nl(true);  // Make . match \n
        options.set_log_errors(false);
        // Use UTF-8 mode so regex engine natively understands Unicode
        // codepoints — consistent with Tantivy's Unicode regex engine.
        options.set_encoding(RE2::Options::EncodingUTF8);
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

// Adapt a Unicode regex pattern for Boost's byte-level regex engine.
// Replaces (.|\n) (single Unicode char in RE2 UTF-8 mode) with a
// byte-level pattern matching one complete UTF-8 character sequence.
inline std::string
adapt_regex_for_boost_utf8(const std::string& pattern) {
    // Byte-level pattern matching exactly one UTF-8 character (1-4 bytes)
    static const std::string kUtf8OneChar =
        "(?:[\\x00-\\x7F]"
        "|[\\xC0-\\xDF][\\x80-\\xBF]"
        "|[\\xE0-\\xEF][\\x80-\\xBF]{2}"
        "|[\\xF0-\\xF7][\\x80-\\xBF]{3})";

    // The needle is the literal 5-char string: ( . | \n )
    const std::string needle = "(.|\n)";

    std::string result;
    result.reserve(pattern.size() * 4);
    size_t pos = 0;
    while (pos < pattern.size()) {
        size_t found = pattern.find(needle, pos);
        if (found == std::string::npos) {
            result.append(pattern, pos, std::string::npos);
            break;
        }
        result.append(pattern, pos, found - pos);
        result.append(kUtf8OneChar);
        pos = found + needle.size();
    }
    return result;
}

// BoostRegexMatcher - kept for benchmark comparison
// Uses byte-level UTF-8 patterns so _ matches a full UTF-8 character
struct BoostRegexMatcher {
    template <typename T>
    inline bool
    operator()(const T& operand) {
        return false;
    }

    explicit BoostRegexMatcher(const std::string& pattern) {
        r_ = boost::regex(adapt_regex_for_boost_utf8(pattern));
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

// Get the byte length of a UTF-8 character from its first byte
// Returns 1-4 for valid UTF-8 lead bytes, 1 for invalid/continuation bytes
inline size_t
Utf8CharByteLen(unsigned char first_byte) {
    if ((first_byte & 0x80) == 0)
        return 1;  // ASCII: 0xxxxxxx
    if (first_byte >= 0xC2 && first_byte <= 0xDF)
        return 2;  // 2-byte: 110xxxxx
    if ((first_byte & 0xF0) == 0xE0)
        return 3;  // 3-byte: 1110xxxx
    if (first_byte >= 0xF0 && first_byte <= 0xF7)
        return 4;  // 4-byte: 11110xxx
    return 1;      // Invalid or continuation byte, treat as single byte
}

inline bool
IsUtf8ContinuationByte(unsigned char byte) {
    return (byte & 0xC0) == 0x80;
}

// Returns wildcard-consumable UTF-8 character length in [1,4].
// Returns 0 if bytes cannot be consumed by LIKE wildcards ('%' / '_'),
// which follows the regex translation rules.
inline size_t
Utf8WildcardCharByteLen(const char* str, size_t remaining) {
    if (remaining == 0) {
        return 0;
    }

    const auto first_byte = static_cast<unsigned char>(str[0]);
    if ((first_byte & 0x80) == 0) {
        return 1;
    }

    size_t char_len = 0;
    if (first_byte >= 0xC2 && first_byte <= 0xDF) {
        char_len = 2;
    } else if ((first_byte & 0xF0) == 0xE0) {
        char_len = 3;
    } else if (first_byte >= 0xF0 && first_byte <= 0xF7) {
        char_len = 4;
    } else {
        return 0;
    }

    if (char_len > remaining) {
        return 0;
    }
    for (size_t i = 1; i < char_len; ++i) {
        if (!IsUtf8ContinuationByte(static_cast<unsigned char>(str[i]))) {
            return 0;
        }
    }
    return char_len;
}

// Returns validated UTF-8 character length in [1,4].
// If the leading byte or continuation bytes are invalid (or truncated),
// this falls back to 1 so all matchers share the same byte-level behavior.
inline size_t
Utf8ValidatedCharByteLen(const char* str, size_t remaining) {
    if (remaining == 0) {
        return 0;
    }

    const auto first_byte = static_cast<unsigned char>(str[0]);
    const auto char_len = Utf8CharByteLen(first_byte);
    if (char_len == 1) {
        return 1;
    }
    if (char_len > remaining) {
        return 1;
    }
    for (size_t i = 1; i < char_len; ++i) {
        if (!IsUtf8ContinuationByte(static_cast<unsigned char>(str[i]))) {
            return 1;
        }
    }
    return char_len;
}

// Count the number of UTF-8 characters in a string
inline size_t
Utf8CharCount(const char* str, size_t byte_len) {
    size_t count = 0;
    size_t pos = 0;
    while (pos < byte_len) {
        pos += Utf8ValidatedCharByteLen(str + pos, byte_len - pos);
        ++count;
    }
    return count;
}

// LikePatternMatcher - optimized for all LIKE patterns (both % and _)
// For patterns like "a%b_c%d", uses simple string operations instead of regex
// This is 5-15x faster than regex engines
// Note: The _ wildcard matches one UTF-8 character (codepoint), following SQL standard
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
        std::string text;         // The literal text (with _ removed)
        size_t underscore_count;  // Number of _ wildcards in this segment
        // Positions (in character units) where underscores appear
        // E.g., for pattern "a_b_c", positions would be [1, 3]
        std::vector<size_t> underscore_char_positions;
        size_t char_count;  // Total character count including _ wildcards
    };

    void
    ParsePattern(const std::string& pattern) {
        Segment current_segment;
        current_segment.underscore_count = 0;
        current_segment.char_count = 0;
        bool escape_mode = false;
        size_t char_pos_in_segment = 0;

        // Track wildcards during parsing to handle escapes correctly
        leading_wildcard_ = false;
        trailing_wildcard_ = false;
        bool first_char_processed = false;

        for (size_t i = 0; i < pattern.size();) {
            char c = pattern[i];
            if (escape_mode) {
                // Add escaped character (may be multi-byte UTF-8)
                size_t char_len = Utf8ValidatedCharByteLen(pattern.data() + i,
                                                           pattern.size() - i);
                current_segment.text.append(pattern, i, char_len);
                current_segment.char_count++;
                char_pos_in_segment++;
                i += char_len;
                escape_mode = false;
                trailing_wildcard_ = false;
            } else if (c == '\\') {
                escape_mode = true;
                trailing_wildcard_ = false;
                ++i;
            } else if (c == '%') {
                segments_.push_back(std::move(current_segment));
                current_segment = Segment();
                current_segment.underscore_count = 0;
                current_segment.char_count = 0;
                char_pos_in_segment = 0;
                if (!first_char_processed) {
                    leading_wildcard_ = true;
                }
                trailing_wildcard_ = true;
                ++i;
            } else if (c == '_') {
                current_segment.underscore_char_positions.push_back(
                    char_pos_in_segment);
                current_segment.underscore_count++;
                current_segment.char_count++;
                char_pos_in_segment++;
                trailing_wildcard_ = false;
                ++i;
            } else {
                // Regular character (may be multi-byte UTF-8)
                size_t char_len = Utf8ValidatedCharByteLen(pattern.data() + i,
                                                           pattern.size() - i);
                current_segment.text.append(pattern, i, char_len);
                current_segment.char_count++;
                char_pos_in_segment++;
                i += char_len;
                trailing_wildcard_ = false;
            }
            first_char_processed = true;
        }
        if (escape_mode) {
            ThrowInfo(ExprInvalid,
                      "Invalid LIKE pattern: trailing backslash with nothing "
                      "to escape");
        }
        segments_.push_back(std::move(current_segment));

        // Precompute minimum required byte count for early rejection.
        // For literal text, use actual byte length; for each _ wildcard,
        // the minimum is 1 byte (a single-byte UTF-8/ASCII character).
        // This gives a precise lower bound in bytes.
        min_required_bytes_ = 0;
        for (const auto& seg : segments_) {
            min_required_bytes_ += seg.text.size() + seg.underscore_count;
        }
    }

    // Check if a segment matches at a specific byte position in the string
    // Returns the number of bytes consumed if match, std::nullopt if no match
    template <typename StringType>
    std::optional<size_t>
    SegmentMatchesAt(const Segment& seg,
                     const StringType& str,
                     size_t str_byte_pos) const {
        if (seg.char_count == 0) {
            return 0;  // Empty segment matches with 0 bytes consumed
        }

        size_t str_pos = str_byte_pos;
        size_t text_byte_pos = 0;
        size_t underscore_idx = 0;
        size_t next_underscore_char_pos =
            seg.underscore_char_positions.empty()
                ? std::string::npos
                : seg.underscore_char_positions[0];

        for (size_t char_idx = 0; char_idx < seg.char_count; ++char_idx) {
            if (str_pos >= str.size()) {
                return std::nullopt;  // String too short
            }

            if (char_idx == next_underscore_char_pos) {
                // Underscore: skip one UTF-8 character in the string
                size_t char_len = Utf8WildcardCharByteLen(str.data() + str_pos,
                                                          str.size() - str_pos);
                if (char_len == 0) {
                    return std::nullopt;
                }
                str_pos += char_len;
                ++underscore_idx;
                next_underscore_char_pos =
                    (underscore_idx < seg.underscore_char_positions.size())
                        ? seg.underscore_char_positions[underscore_idx]
                        : std::string::npos;
            } else {
                // Literal character: must match exactly
                if (text_byte_pos >= seg.text.size()) {
                    return std::nullopt;
                }
                size_t pattern_char_len =
                    Utf8ValidatedCharByteLen(seg.text.data() + text_byte_pos,
                                             seg.text.size() - text_byte_pos);
                size_t str_char_len = Utf8ValidatedCharByteLen(
                    str.data() + str_pos, str.size() - str_pos);

                if (pattern_char_len == 0 || str_char_len == 0 ||
                    pattern_char_len > seg.text.size() - text_byte_pos ||
                    pattern_char_len != str_char_len ||
                    str_pos + str_char_len > str.size()) {
                    return std::nullopt;
                }

                // Compare bytes of this character
                for (size_t b = 0; b < pattern_char_len; ++b) {
                    if (seg.text[text_byte_pos + b] != str[str_pos + b]) {
                        return std::nullopt;
                    }
                }
                text_byte_pos += pattern_char_len;
                str_pos += str_char_len;
            }
        }
        return str_pos - str_byte_pos;  // Bytes consumed
    }

    // Find segment in string starting from byte position
    // Returns the byte position where segment starts, or npos if not found
    template <typename StringType>
    std::pair<size_t, size_t>
    FindSegment(const Segment& seg,
                const StringType& str,
                size_t start_byte_pos) const {
        if (seg.underscore_count == 0) {
            // No underscores - use fast string find.
            // str.find() is byte-level, so it may land inside a multi-byte
            // UTF-8 character. We must validate that 'found' sits on a
            // character boundary by walking from start_byte_pos; if not,
            // retry the search from the next valid boundary.
            size_t search_from = start_byte_pos;
            size_t validated_pos = start_byte_pos;
            while (true) {
                size_t found = str.find(seg.text, search_from);
                if (found == std::string::npos) {
                    return {std::string::npos, 0};
                }
                // Walk validated_pos forward to reach or pass 'found'
                while (validated_pos < found) {
                    size_t step = Utf8WildcardCharByteLen(
                        str.data() + validated_pos, str.size() - validated_pos);
                    if (step == 0) {
                        return {std::string::npos, 0};
                    }
                    validated_pos += step;
                }
                if (validated_pos == found) {
                    return {found, seg.text.size()};
                }
                // validated_pos > found: match was mid-character, retry
                // from next valid boundary
                search_from = validated_pos;
            }
        }

        // Has underscores - need to check at each UTF-8 character boundary
        size_t pos = start_byte_pos;
        while (pos < str.size()) {
            auto bytes_matched = SegmentMatchesAt(seg, str, pos);
            if (bytes_matched.has_value()) {
                return {pos, *bytes_matched};
            }
            // Move to next UTF-8 character
            size_t step =
                Utf8WildcardCharByteLen(str.data() + pos, str.size() - pos);
            if (step == 0) {
                return {std::string::npos, 0};
            }
            pos += step;
        }
        return {std::string::npos, 0};
    }

    template <typename StringType>
    bool
    MatchImpl(const StringType& str) const {
        if (segments_.empty()) {
            return true;
        }

        size_t str_byte_len = str.size();

        // Early rejection: string byte length must be at least the minimum
        // required bytes (literal text bytes + 1 byte per _ wildcard).
        if (str_byte_len < min_required_bytes_) {
            return false;
        }

        size_t pos = 0;  // Current byte position in string

        // Special case: no wildcards at all (exact match required)
        if (!leading_wildcard_ && !trailing_wildcard_ &&
            segments_.size() == 1) {
            const auto& seg = segments_[0];
            // Empty pattern matches only empty string
            if (seg.char_count == 0) {
                return str_byte_len == 0;
            }
            auto bytes_matched = SegmentMatchesAt(seg, str, 0);
            return bytes_matched.has_value() && *bytes_matched == str_byte_len;
        }

        for (size_t i = 0; i < segments_.size(); ++i) {
            const auto& seg = segments_[i];
            if (seg.char_count == 0) {
                continue;
            }

            bool is_first = (i == 0);
            bool is_last = (i == segments_.size() - 1);

            if (is_first && !leading_wildcard_) {
                // First segment must match at start
                auto bytes_matched = SegmentMatchesAt(seg, str, 0);
                if (!bytes_matched.has_value()) {
                    return false;
                }
                pos = *bytes_matched;
                // If also last and no trailing wildcard, verify exact match
                if (is_last && !trailing_wildcard_) {
                    return pos == str_byte_len;
                }
            } else if (is_last && !trailing_wildcard_) {
                // Last segment must match at end
                size_t search_pos = pos;

                while (search_pos < str_byte_len) {
                    auto bytes_matched = SegmentMatchesAt(seg, str, search_pos);
                    if (bytes_matched.has_value() &&
                        search_pos + *bytes_matched == str_byte_len) {
                        return true;  // Found match at end
                    }
                    // Move to next UTF-8 character
                    size_t step = Utf8WildcardCharByteLen(
                        str.data() + search_pos, str.size() - search_pos);
                    if (step == 0) {
                        return false;
                    }
                    search_pos += step;
                }
                return false;
            } else {
                // Middle segment - find anywhere after current position
                auto [found_pos, bytes_matched] = FindSegment(seg, str, pos);
                if (found_pos == std::string::npos) {
                    return false;
                }
                // Move position past the matched segment (no overlapping)
                // SQL LIKE semantics require non-overlapping matches
                pos = found_pos + bytes_matched;
            }
        }

        // Validate that bytes consumed by trailing % are valid UTF-8,
        // aligning with RE2 UTF-8 mode where (.\n)* rejects invalid bytes.
        if (trailing_wildcard_ && pos < str_byte_len) {
            size_t vp = pos;
            while (vp < str_byte_len) {
                size_t step =
                    Utf8WildcardCharByteLen(str.data() + vp, str_byte_len - vp);
                if (step == 0) {
                    return false;
                }
                vp += step;
            }
        }
        return true;
    }

    std::vector<Segment> segments_;
    bool leading_wildcard_ = false;
    bool trailing_wildcard_ = false;
    size_t min_required_bytes_ = 0;
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

}  // namespace milvus
