// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/scalar/ngram/NgramIndexReader.h"

#include <algorithm>
#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "storage/artifact/LocalDirectory.h"
#include "index/scalar/ngram/NgramRegex.h"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

constexpr size_t kLargeRowThreshold = 5000;
constexpr size_t kMediumRowThreshold = 1000;
constexpr size_t kSmallRowThreshold = 100;
constexpr double kPreFilterHitRateThreshold = 0.20;
constexpr size_t kMaxIterations = 5;
constexpr double kBreakThreshold = 0.002;
constexpr size_t kMaxIterationsForMediumRow = 3;
constexpr double kBreakThresholdForSmallRow = 0.01;
constexpr size_t kMaxIterationsForSmallRow = 2;

size_t
Utf8LiteralLength(const std::string& literal) {
    return Utf8CharCount(literal.data(), literal.size());
}

std::vector<std::string>
split_by_wildcard(const std::string& literal) {
    std::vector<std::string> result;
    std::string r;
    r.reserve(literal.size());
    bool escape_mode = false;
    for (char c : literal) {
        if (escape_mode) {
            r += c;
            escape_mode = false;
        } else {
            if (c == '\\') {
                // consider case "\\%", we should reserve %
                escape_mode = true;
            } else if (c == '%' || c == '_') {
                if (r.length() > 0) {
                    result.push_back(std::move(r));
                    r.clear();
                }
            } else {
                r += c;
            }
        }
    }
    if (r.length() > 0) {
        result.push_back(std::move(r));
    }
    return result;
}

}  // namespace

// Extract runs of literal bytes from a regex pattern that are GUARANTEED to
// appear in any matching string.  Only these "required literals" are safe to
// use as AND-conditions in the ngram coarse filter.
//
// Key correctness rules:
//   1. Alternation `|` at any nesting level means we cannot determine which
//      branch will match, so no literal from either branch is guaranteed.
//      → Return empty immediately.
//   2. Quantifiers that allow zero occurrences (`?`, `*`, `{0,...}`) make the
//      preceding element optional.  The last character of the current literal
//      run must be removed before saving, since it might not be present.
//   3. `+` means "one or more" — the preceding element IS required, but the
//      repetition breaks contiguity with the following literal.  Save the
//      current run (including the repeated char) and start a new run.
//   4. Shorthand character classes (`\d`, `\w`, `\s`, etc.) are not literal.
//   5. `(?i)` or other inline flags containing `i` → return empty (case-
//      insensitive matching invalidates case-sensitive ngram lookups).
//
// This is deliberately conservative: returning fewer literals (or empty) is
// always safe — it just means less ngram filtering and more brute-force
// Phase-2 work.  Returning a wrong literal causes false negatives.
std::vector<std::string>
extract_literals_from_regex(const std::string& pattern) {
    constexpr int kMaxRequiredRepetitions = 1 << 20;
    auto is_metachar = [](char c) -> bool {
        return c == '.' || c == '+' || c == '*' || c == '?' || c == '^' ||
               c == '$' || c == '{' || c == '}' || c == '(' || c == ')' ||
               c == '|' || c == '[' || c == ']';
    };

    // WHITELIST approach: only escaped regex metacharacters are guaranteed
    // to produce a literal byte.  Everything else (\d, \w, \n, \t, \x,
    // \p, \0, etc.) is a character class, control char, or special escape
    // — NOT a guaranteed literal in the matched text.
    auto is_escaped_literal = [&](char next) -> bool {
        switch (next) {
            case '.':
            case '+':
            case '*':
            case '?':
            case '^':
            case '$':
            case '{':
            case '}':
            case '(':
            case ')':
            case '|':
            case '[':
            case ']':
            case '\\':
            case '/':
            case '-':
                return true;
            default:
                return false;
        }
    };

    // ── Pre-scan: bail out if any unescaped `|` exists (at any depth). ──
    {
        bool in_char_class = false;
        for (size_t i = 0; i < pattern.size(); ++i) {
            char c = pattern[i];
            if (c == '\\' && i + 1 < pattern.size()) {
                ++i;  // skip escaped char
                continue;
            }
            if (c == '[') {
                in_char_class = true;
                continue;
            }
            if (c == ']') {
                in_char_class = false;
                continue;
            }
            if (!in_char_class && c == '|') {
                return {};  // alternation → cannot safely AND literals
            }
        }
    }

    // Refuse a required repetition whose literal expansion would exceed the
    // bounded phase-1 parser. Returning no literals routes the caller to raw
    // refinement and cannot create a false negative by truncation.
    {
        bool in_char_class = false;
        for (size_t i = 0; i < pattern.size(); ++i) {
            const auto c = pattern[i];
            if (c == '\\' && i + 1 < pattern.size()) {
                ++i;
                continue;
            }
            if (c == '[') {
                in_char_class = true;
                continue;
            }
            if (c == ']') {
                in_char_class = false;
                continue;
            }
            if (in_char_class || c != '{') {
                continue;
            }
            int required = 0;
            for (size_t j = i + 1;
                 j < pattern.size() && pattern[j] >= '0' && pattern[j] <= '9';
                 ++j) {
                const auto digit = pattern[j] - '0';
                if (required > (kMaxRequiredRepetitions - digit) / 10) {
                    return {};
                }
                required = required * 10 + digit;
            }
        }
    }

    // ── Pre-scan: bail out on case-insensitive flag (?i), (?mi), etc. ──
    // RE2 flag groups are (?flags) or (?flags:...) where flags are only
    // [imsU-].  Named groups like (?P<id>...) or (?<name>...) must NOT
    // be mistaken for flag groups.
    for (size_t i = 0; i + 2 < pattern.size(); ++i) {
        if (pattern[i] == '(' && pattern[i + 1] == '?') {
            // Scan flag characters: only [imsU-] are valid flags
            for (size_t j = i + 2; j < pattern.size(); ++j) {
                char fc = pattern[j];
                if (fc == ')' || fc == ':')
                    break;
                if (fc == 'i')
                    return {};  // case-insensitive
                // If we hit a non-flag character, this is not a flag
                // group (e.g. (?P<...), (?<...), (?'...'))
                if (fc != 'm' && fc != 's' && fc != 'U' && fc != '-') {
                    break;
                }
            }
        }
    }

    // ── Parse {n}, {n,}, {n,m} quantifier ──
    // Returns (min, exact, end_pos). exact=true means {n} (min==max).
    // Returns (-1, false, pos) on parse failure.
    auto parse_quantifier = [=](const std::string& pat,
                                size_t pos) -> std::tuple<int, bool, size_t> {
        if (pos >= pat.size() || pat[pos] != '{')
            return {-1, false, pos};
        size_t j = pos + 1;
        int n = 0;
        bool has_n = false;
        while (j < pat.size() && pat[j] >= '0' && pat[j] <= '9') {
            const auto digit = pat[j] - '0';
            if (n > (kMaxRequiredRepetitions - digit) / 10) {
                while (j < pat.size() && pat[j] != '}') {
                    ++j;
                }
                return {-1, false, j < pat.size() ? j + 1 : j};
            }
            n = n * 10 + digit;
            has_n = true;
            ++j;
        }
        if (!has_n || j >= pat.size())
            return {-1, false, pos};
        if (pat[j] == '}')
            return {n, true, j + 1};  // {n} exact
        if (pat[j] == ',') {
            ++j;
            while (j < pat.size() && pat[j] >= '0' && pat[j] <= '9') ++j;
            if (j < pat.size() && pat[j] == '}')
                return {n, false, j + 1};  // {n,m} or {n,}
        }
        return {-1, false, pos};
    };

    // ── Main extraction loop ──
    std::vector<std::string> result;
    std::string current;
    std::vector<size_t> group_start_stack;  // tracks group start in `current`
    size_t expanded_bytes = 0;

    auto append = [&](std::string_view value, size_t copies = 1) {
        constexpr size_t kMaxExpandedBytes = 1 << 20;
        if (value.size() != 0 &&
            copies > (kMaxExpandedBytes - expanded_bytes) / value.size()) {
            return false;
        }
        for (size_t i = 0; i < copies; ++i) {
            current.append(value.data(), value.size());
        }
        expanded_bytes += value.size() * copies;
        return true;
    };

    auto flush = [&]() {
        if (!current.empty()) {
            result.push_back(std::move(current));
            current.clear();
        }
    };

    // Handle a variable-count quantifier on an element (char or group content).
    // Flushes current (including min copies), then starts new segment with
    // min copies so the next literal is contiguous with the last repetition.
    auto flush_variable_quantifier = [&](const std::string& element,
                                         int min_count) {
        // current already has one copy of element appended.
        // Add min_count-1 more copies.
        if (!append(element, static_cast<size_t>(min_count - 1))) {
            return false;
        }
        flush();
        // Start new segment with min_count copies for contiguity with what follows
        return append(element, static_cast<size_t>(min_count));
    };

    for (size_t i = 0; i < pattern.size();) {
        char c = pattern[i];

        // ── Escape sequence ──
        if (c == '\\' && i + 1 < pattern.size()) {
            char next = pattern[i + 1];
            if (is_escaped_literal(next)) {
                // Check for quantifier after the escaped char
                size_t after = i + 2;
                if (after < pattern.size()) {
                    char q = pattern[after];
                    if (q == '?' || q == '*') {
                        // Optional element → don't include
                        flush();
                        i = after + 1;
                        continue;
                    }
                    if (q == '{') {
                        auto [min_count, exact, end_pos] =
                            parse_quantifier(pattern, after);
                        if (min_count <= 0) {
                            flush();
                            i = end_pos;
                            continue;
                        }
                        std::string elem(1, next);
                        if (exact) {
                            // {n} exact: expand, keep contiguity
                            if (!append(elem, static_cast<size_t>(min_count))) {
                                return {};
                            }
                            i = end_pos;
                        } else {
                            // {n,m} or {n,}: expand min, break contiguity
                            if (!append(elem) ||
                                !flush_variable_quantifier(elem, min_count)) {
                                return {};
                            }
                            i = end_pos;
                        }
                        continue;
                    }
                    if (q == '+') {
                        // Required, variable — flush with repeat start
                        std::string elem(1, next);
                        if (!append(elem) ||
                            !flush_variable_quantifier(elem, 1)) {
                            return {};
                        }
                        i = after + 1;
                        continue;
                    }
                }
                if (!append(std::string_view(&next, 1))) {
                    return {};
                }
                i += 2;
            } else {
                // Shorthand class (\d, \w, \s, \p, \P, etc.) → split
                flush();
                i += 2;
                // \p{...} and \P{...} — consume the braced property name
                if ((next == 'p' || next == 'P') && i < pattern.size() &&
                    pattern[i] == '{') {
                    while (i < pattern.size() && pattern[i] != '}') ++i;
                    if (i < pattern.size())
                        ++i;  // skip '}'
                }
            }
            continue;
        }

        // ── Character class [...] ──
        if (c == '[') {
            flush();
            ++i;
            // Skip to closing ], handling \]
            while (i < pattern.size() && pattern[i] != ']') {
                if (pattern[i] == '\\' && i + 1 < pattern.size())
                    ++i;
                ++i;
            }
            if (i < pattern.size())
                ++i;  // skip ']'
            // Character class may be followed by a quantifier — skip it
            if (i < pattern.size() &&
                (pattern[i] == '?' || pattern[i] == '*' || pattern[i] == '+')) {
                ++i;
            } else if (i < pattern.size() && pattern[i] == '{') {
                while (i < pattern.size() && pattern[i] != '}') ++i;
                if (i < pattern.size())
                    ++i;
            }
            continue;
        }

        // ── Grouping (...) ──
        // We already bailed on `|`, so group content is sequential.
        // For non-optional groups, we can "penetrate" the parentheses
        // and continue accumulating literals from the group content.
        // For optional groups (followed by ?, *, {0,...}), we flush
        // and skip the entire group.
        if (c == '(') {
            ++i;
            // Skip group flags like (?:...), (?P<name>...), (?i...) etc.
            // These are non-content prefixes inside the group.
            bool is_flag_group = false;
            if (i < pattern.size() && pattern[i] == '?') {
                // (?:...) non-capturing, (?P<...) named, (?i...) flags
                // Skip to the actual content or end of flag-only group
                ++i;  // skip '?'
                // Skip flag chars and special prefixes
                while (i < pattern.size()) {
                    char fc = pattern[i];
                    if (fc == ':') {
                        ++i;  // skip ':', content follows
                        break;
                    }
                    if (fc == ')') {
                        // Flag-only group like (?i) — already consumed
                        // by pre-scan, just skip past ')'
                        ++i;
                        is_flag_group = true;
                        break;
                    }
                    if (fc == 'P' || fc == '<' || fc == '\'') {
                        // Named group — skip to '>' or '\'' then ':'
                        while (i < pattern.size() && pattern[i] != ')' &&
                               pattern[i] != ':') {
                            if (pattern[i] == '>' || pattern[i] == '\'') {
                                ++i;
                                break;
                            }
                            ++i;
                        }
                        if (i < pattern.size() && pattern[i] == ':')
                            ++i;
                        break;
                    }
                    // Flag character (i, m, s, U, etc.)
                    ++i;
                }
            }
            if (is_flag_group)
                continue;

            // Find the matching ')' to check the quantifier after it
            int depth = 1;
            size_t close_pos = i;
            while (close_pos < pattern.size() && depth > 0) {
                if (pattern[close_pos] == '\\' &&
                    close_pos + 1 < pattern.size()) {
                    close_pos += 2;
                    continue;
                }
                if (pattern[close_pos] == '(')
                    ++depth;
                if (pattern[close_pos] == ')')
                    --depth;
                if (depth > 0)
                    ++close_pos;
            }
            // close_pos now points at the matching ')'
            size_t after_close = close_pos + 1;

            // Check quantifier after ')'
            if (after_close < pattern.size()) {
                char q = pattern[after_close];
                if (q == '?' || q == '*') {
                    // Optional group — flush and skip
                    flush();
                    i = after_close + 1;
                    continue;
                }
                if (q == '{') {
                    auto [min_count, exact, end_pos] =
                        parse_quantifier(pattern, after_close);
                    if (min_count <= 0) {
                        // {0,...} optional — flush and skip
                        flush();
                        i = end_pos;
                        continue;
                    }
                    // Required group with {n} or {n,m}
                    // Penetrate: parse group content, then expand
                    group_start_stack.push_back(current.size());
                    // Store quantifier info for ')' handler
                    // We handle it when we reach ')' below
                    continue;
                }
                if (q == '+') {
                    // Required, variable — penetrate, expand at ')'
                    group_start_stack.push_back(current.size());
                    continue;
                }
            }

            // No quantifier — required group, just penetrate
            group_start_stack.push_back(current.size());
            continue;
        }

        // ── Close group ')' ──
        if (c == ')') {
            ++i;
            size_t group_start =
                group_start_stack.empty() ? 0 : group_start_stack.back();
            if (!group_start_stack.empty())
                group_start_stack.pop_back();
            std::string group_content = current.substr(group_start);

            if (i < pattern.size()) {
                char q = pattern[i];
                if (q == '+') {
                    // {1,} variable — expand
                    if (!flush_variable_quantifier(group_content, 1)) {
                        return {};
                    }
                    ++i;
                    continue;
                }
                if (q == '{') {
                    auto [min_count, exact, end_pos] =
                        parse_quantifier(pattern, i);
                    if (min_count > 0) {
                        if (exact) {
                            // {n} exact: expand, keep contiguity
                            if (!append(group_content,
                                        static_cast<size_t>(min_count - 1))) {
                                return {};
                            }
                        } else {
                            // {n,m} variable: expand min, break
                            if (!flush_variable_quantifier(group_content,
                                                           min_count)) {
                                return {};
                            }
                        }
                        i = end_pos;
                        continue;
                    }
                }
            }
            // No quantifier after ) — just continue (content already in current)
            continue;
        }

        // ── Other metacharacter ──
        if (is_metachar(c)) {
            flush();
            ++i;
            continue;
        }

        // ── Regular literal character ──
        // Peek ahead for quantifier
        if (i + 1 < pattern.size()) {
            char q = pattern[i + 1];
            if (q == '?' || q == '*') {
                // This character is optional
                flush();  // flush what we have (without this char)
                i += 2;
                continue;
            }
            if (q == '{') {
                auto [min_count, exact, end_pos] =
                    parse_quantifier(pattern, i + 1);
                if (min_count <= 0) {
                    // {0,...} optional
                    flush();
                    i = end_pos;
                    continue;
                }
                std::string elem(1, c);
                if (exact) {
                    // {n} exact: expand, keep contiguity
                    if (!append(elem, static_cast<size_t>(min_count))) {
                        return {};
                    }
                    i = end_pos;
                } else {
                    // {n,m} or {n,}: expand min, break contiguity
                    if (!append(elem) ||
                        !flush_variable_quantifier(elem, min_count)) {
                        return {};
                    }
                    i = end_pos;
                }
                continue;
            }
            if (q == '+') {
                // Required, variable — flush with repeat start
                std::string elem(1, c);
                if (!append(elem) || !flush_variable_quantifier(elem, 1)) {
                    return {};
                }
                i += 2;
                continue;
            }
        }

        if (!append(std::string_view(&c, 1))) {
            return {};
        }
        ++i;
    }
    flush();
    return result;
}

namespace {

int64_t
ToUsageBytes(size_t bytes) {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "NGRAM reader resource size exceeds int64 domain");
    }
    return static_cast<int64_t>(bytes);
}

std::string
OwnString(std::string_view value) {
    return value.empty() ? std::string{}
                         : std::string(value.data(), value.size());
}

}  // namespace

NgramIndexReader::NgramIndexReader(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    DataType value_type,
    uintptr_t min_gram,
    uintptr_t max_gram,
    size_t avg_row_size,
    bool mmap,
    size_t engine_bytes)
    : directory_(std::move(directory)),
      engine_(std::move(engine)),
      null_offsets_(std::move(null_offsets)),
      value_type_(value_type),
      min_gram_(min_gram),
      max_gram_(max_gram),
      avg_row_size_(avg_row_size),
      mmap_(mmap),
      engine_bytes_(engine_bytes) {
    AssertInfo(engine_ != nullptr, "NGRAM reader requires an engine");
    AssertInfo(null_offsets_ != nullptr,
               "NGRAM reader requires immutable null offsets");
    AssertInfo(IsStringDataType(value_type_),
               "NGRAM reader requires a string value type, got {}",
               static_cast<int>(value_type_));
    AssertInfo(min_gram_ > 0 && min_gram_ <= max_gram_,
               "NGRAM reader has invalid gram range {}..{}",
               min_gram_,
               max_gram_);
    AssertInfo(!mmap_ || directory_ != nullptr,
               "mmap NGRAM reader requires a directory owner");
    count_ = engine_->count();

    size_t previous = 0;
    bool first = true;
    for (const auto offset : *null_offsets_) {
        if ((!first && offset <= previous) || offset >= count_) {
            ThrowInfo(DataFormatBroken,
                      "invalid NGRAM null offset {} for count {}",
                      offset,
                      count_);
        }
        previous = offset;
        first = false;
    }
}

NgramIndexReader::~NgramIndexReader() = default;

ReaderCaps
NgramIndexReader::Caps() const {
    return ReaderCaps{.ngram_candidates = true, .exact = false};
}

Domain
NgramIndexReader::CoordDomain() const {
    return Domain::Row;
}

int64_t
NgramIndexReader::Count() const {
    return static_cast<int64_t>(count_);
}

DataType
NgramIndexReader::ValueType() const {
    return value_type_;
}

int64_t
NgramIndexReader::MemoryUsage() const {
    constexpr size_t kKnownMetadataBytes =
        sizeof(NgramIndexReader) +
        sizeof(milvus::tantivy::TantivyIndexWrapper) +
        sizeof(std::vector<size_t>);
    const auto offsets_bytes = null_offsets_->capacity() * sizeof(size_t);
    if (offsets_bytes >
        std::numeric_limits<size_t>::max() - kKnownMetadataBytes) {
        ThrowInfo(DataFormatBroken, "NGRAM reader memory size overflows");
    }
    auto total = kKnownMetadataBytes + offsets_bytes;
    if (directory_ != nullptr) {
        const auto directory_bytes = directory_->HeapBytes();
        if (directory_bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken, "NGRAM reader memory size overflows");
        }
        total += directory_bytes;
    }
    if (!mmap_) {
        if (engine_bytes_ > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken, "NGRAM reader memory size overflows");
        }
        total += engine_bytes_;
    }
    return ToUsageBytes(total);
}

cachinglayer::ResourceUsage
NgramIndexReader::CellByteSize() const {
    return {MemoryUsage(), mmap_ ? ToUsageBytes(engine_bytes_) : 0};
}

bool
NgramIndexReader::CanHandle(std::string_view literal, PatternOp op) const {
    const auto owned = OwnString(literal);
    switch (op) {
        case PatternOp::Match: {
            const auto literals = split_by_wildcard(owned);
            if (literals.empty()) {
                return false;
            }
            return std::all_of(
                literals.begin(), literals.end(), [this](const auto& part) {
                    return Utf8LiteralLength(part) >= min_gram_;
                });
        }
        case PatternOp::RegexMatch: {
            const auto literals = extract_literals_from_regex(owned);
            return std::any_of(
                literals.begin(), literals.end(), [this](const auto& part) {
                    return Utf8LiteralLength(part) >= min_gram_;
                });
        }
        case PatternOp::InnerMatch:
        case PatternOp::PrefixMatch:
        case PatternOp::PostfixMatch:
            return Utf8LiteralLength(owned) >= min_gram_;
    }
    return false;
}

void
NgramIndexReader::Candidates(std::string_view literal,
                             PatternOp op,
                             TargetBitmap& candidates) const {
    AssertInfo(candidates.size() == count_,
               "NGRAM candidates size {} disagrees with count {}",
               candidates.size(),
               count_);
    if (count_ == 0 || candidates.none()) {
        return;
    }

    const auto owned = OwnString(literal);
    std::vector<std::string> literals;
    switch (op) {
        case PatternOp::Match:
            literals = split_by_wildcard(owned);
            if (std::any_of(
                    literals.begin(), literals.end(), [this](const auto& part) {
                        return Utf8LiteralLength(part) < min_gram_;
                    })) {
                literals.clear();
            }
            break;
        case PatternOp::RegexMatch:
            for (auto&& part : extract_literals_from_regex(owned)) {
                if (Utf8LiteralLength(part) >= min_gram_) {
                    literals.push_back(std::move(part));
                }
            }
            break;
        case PatternOp::InnerMatch:
        case PatternOp::PrefixMatch:
        case PatternOp::PostfixMatch:
            if (Utf8LiteralLength(owned) >= min_gram_) {
                literals.push_back(owned);
            }
            break;
    }
    AssertInfo(!literals.empty(),
               "NGRAM cannot handle the requested literal and operation");

    const auto pre_count = candidates.count();
    const auto pre_filter_hit_rate =
        static_cast<double>(pre_count) / static_cast<double>(count_);
    if (ShouldUseBatchStrategy(pre_filter_hit_rate)) {
        for (const auto& part : literals) {
            TargetBitmap hits(count_);
            engine_->ngram_match_query(part, min_gram_, max_gram_, &hits);
            candidates &= hits;
            if (candidates.none()) {
                return;
            }
        }
        return;
    }

    const auto terms = engine_->ngram_tokenize(literals, min_gram_, max_gram_);
    AssertInfo(!terms.empty(),
               "NGRAM tokenizer returned no terms for a usable literal");
    ApplyIterativeNgramFilter(terms, count_, candidates);
}

TargetBitmap
NgramIndexReader::IsNull() const {
    TargetBitmap result(count_);
    for (const auto offset : *null_offsets_) {
        result.set(offset);
    }
    return result;
}

TargetBitmap
NgramIndexReader::IsNotNull() const {
    TargetBitmap result(count_, true);
    for (const auto offset : *null_offsets_) {
        result.reset(offset);
    }
    return result;
}

bool
NgramIndexReader::ShouldUseBatchStrategy(double pre_filter_hit_rate) const {
    return avg_row_size_ >= kLargeRowThreshold ||
           (avg_row_size_ >= kMediumRowThreshold &&
            pre_filter_hit_rate > kPreFilterHitRateThreshold);
}

void
NgramIndexReader::ApplyIterativeNgramFilter(
    const std::vector<std::string>& sorted_terms,
    size_t total_count,
    TargetBitmap& bitset) const {
    auto max_iterations = kMaxIterations;
    if (avg_row_size_ < kSmallRowThreshold) {
        max_iterations = kMaxIterationsForSmallRow;
    } else if (avg_row_size_ < kMediumRowThreshold) {
        max_iterations = kMaxIterationsForMediumRow;
    }

    const auto iterations = std::min(sorted_terms.size(), max_iterations);
    for (size_t i = 0; i < iterations; ++i) {
        TargetBitmap term_bitset(total_count);
        engine_->ngram_term_posting_list(sorted_terms[i], &term_bitset);
        bitset &= term_bitset;

        const auto current_hit_rate = static_cast<double>(bitset.count()) /
                                      static_cast<double>(total_count);
        if (current_hit_rate < kBreakThreshold ||
            (avg_row_size_ < kSmallRowThreshold &&
             current_hit_rate < kBreakThresholdForSmallRow)) {
            break;
        }
    }
}

}  // namespace milvus::index
