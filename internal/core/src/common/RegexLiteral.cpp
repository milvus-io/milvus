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

#include "common/RegexQuery.h"

#include <algorithm>
#include <optional>

namespace milvus {

namespace {

// Soundness contract for ALL matches of a subexpression:
// - prefix/suffix are mandatory at its ends; best is mandatory somewhere.
// - exact means every match is precisely prefix == suffix == best.
// - An empty exact summary is the identity; an unknown summary is not.
// Truncation must clear exact, or concatenation could join nonadjacent bytes.
// These summaries are only prefilters: the canonical RE2 matcher owns semantics.
struct RegexLiteralSummary {
    std::string prefix;
    std::string suffix;
    std::string best;
    bool exact = true;
};

class RegexLiteralAnalyzer {
 public:
    RegexLiteralAnalyzer(const std::string& pattern,
                         const RE2::Options& options,
                         size_t limit)
        : options_(options), limit_(limit), pattern_(pattern) {
    }

    std::optional<std::string>
    Extract() {
        auto result = Sequence(false, 0);
        if (!ok_ || pos_ != pattern_.size()) {
            return std::nullopt;
        }
        return std::move(result.best);
    }

 private:
    static RegexLiteralSummary
    Unknown() {
        return {{}, {}, {}, false};
    }

    RegexLiteralSummary
    Literal(const std::string& s, bool folded) const {
        if (folded) {
            return Unknown();
        }
        return {s, s, s, true};  // One decoded rune, never longer than limit_.
    }

    RegexLiteralSummary
    Concat(RegexLiteralSummary a, const RegexLiteralSummary& b) const {
        const bool exact =
            a.exact && b.exact && a.prefix.size() + b.prefix.size() <= limit_;
        const auto bridge_size =
            std::min(limit_, a.suffix.size() + b.prefix.size());
        // Reuse the sequence accumulator's buffers. Building a fresh summary
        // and temporary concatenations per rune allocates repeatedly on long
        // literal runs, even though the required strings only grow in place.
        if (bridge_size > std::max(a.best.size(), b.best.size())) {
            a.best = a.suffix;
            a.best.append(b.prefix, 0, bridge_size - a.suffix.size());
        } else if (b.best.size() > a.best.size()) {
            a.best = b.best;
        }
        if (a.exact) {
            a.prefix.append(b.prefix, 0, limit_ - a.prefix.size());
        }
        if (b.exact) {
            const auto keep = std::min(a.suffix.size(), limit_ - b.suffix.size());
            a.suffix.erase(0, a.suffix.size() - keep);
            a.suffix += b.suffix;
        } else {
            a.suffix = b.suffix;
        }
        a.exact = exact;
        return a;
    }

    RegexLiteralSummary
    Repeat(RegexLiteralSummary atom, int minimum, int maximum) const {
        if (maximum == 0 || (atom.exact && atom.prefix.empty())) {
            return {};
        }
        if (minimum == 0) {
            return Unknown();
        }
        if (minimum == 1) {
            atom.exact = atom.exact && maximum == 1;
            return atom;
        }
        RegexLiteralSummary result;
        // Bounded summaries and exponentiation avoid expanding nested repeats.
        for (int n = minimum; n != 0; n >>= 1) {
            if (n & 1) {
                result = Concat(std::move(result), atom);
            }
            if (n > 1) {
                atom = Concat(atom, atom);
            }
        }
        if (minimum != maximum) {
            result.exact = false;
        }
        return result;
    }

    int
    Count() {
        const auto start = pos_;
        int value = 0;
        while (pos_ < pattern_.size() && pattern_[pos_] >= '0' &&
               pattern_[pos_] <= '9') {
            value = value * 10 + pattern_[pos_++] - '0';
            if (value > 1000) {
                ok_ = false;
                return 0;
            }
        }
        // Noncanonical braces can be literal text in RE2. Decline rather than
        // interpreting, for example, {01} as repetition.
        if (pos_ == start || (pos_ > start + 1 && pattern_[start] == '0')) {
            ok_ = false;
        }
        return value;
    }

    // Empty quotes contribute no atom, so a following quantifier still binds
    // to the preceding atom. Never remove other quoting boundaries globally:
    // \0\Q\E12 must not become the different octal escape \012.
    void
    SkipEmptyQuotes() {
        while (pattern_.compare(pos_, 2, R"(\Q)") == 0) {
            if (pos_ + 2 == pattern_.size()) {
                pos_ += 2;
            } else if (pattern_.compare(pos_ + 2, 2, R"(\E)") == 0) {
                pos_ += 4;
            } else {
                break;
            }
        }
    }

    RegexLiteralSummary
    Quantify(RegexLiteralSummary atom) {
        SkipEmptyQuotes();
        if (pos_ == pattern_.size()) {
            return atom;
        }
        int minimum = 1, maximum = 1;
        const auto c = pattern_[pos_];
        if (c == '*' || c == '+' || c == '?') {
            ++pos_;
            minimum = c == '+' ? 1 : 0;
            maximum = c == '?' ? 1 : -1;
        } else if (c == '{') {
            ++pos_;
            minimum = maximum = Count();
            if (pos_ < pattern_.size() && pattern_[pos_] == ',') {
                ++pos_;
                maximum = pos_ < pattern_.size() && pattern_[pos_] == '}'
                              ? -1
                              : Count();
            }
            if (!ok_ || pos_ == pattern_.size() || pattern_[pos_++] != '}') {
                ok_ = false;
                return Unknown();
            }
        } else {
            return atom;
        }
        if (pos_ < pattern_.size() && pattern_[pos_] == '?') {
            ++pos_;  // Greediness changes preference, not mandatory bytes.
        }
        return Repeat(std::move(atom), minimum, maximum);
    }

    RegexLiteralSummary
    Escape(bool folded) {
        const auto start = pos_++;
        if (pos_ == pattern_.size()) {
            ok_ = false;
            return Unknown();
        }
        const char c = pattern_[pos_++];
        if (std::string_view("dDsSwWC").find(c) != std::string_view::npos) {
            return Unknown();
        }
        if (c == 'p' || c == 'P') {
            if (pos_ < pattern_.size() && pattern_[pos_] == '{') {
                const auto end = pattern_.find('}', pos_);
                if (end == std::string::npos) {
                    ok_ = false;
                    return Unknown();
                }
                pos_ = end + 1;
            } else if (pos_ < pattern_.size()) {
                ++pos_;
            } else {
                ok_ = false;
            }
            return Unknown();
        }
        if (std::string_view("bBAz").find(c) != std::string_view::npos) {
            return {};  // Zero width, including external word-boundary context.
        }
        if (c == 'x') {
            if (pos_ < pattern_.size() && pattern_[pos_] == '{') {
                const auto end = pattern_.find('}', pos_);
                if (end == std::string::npos) {
                    ok_ = false;
                    return Unknown();
                }
                pos_ = end + 1;
            } else {
                pos_ += 2;
            }
        } else if (c >= '0' && c <= '7') {
            while (pos_ < pattern_.size() && pos_ - start < 4 &&
                   pattern_[pos_] >= '0' && pattern_[pos_] <= '7') {
                ++pos_;
            }
        }
        if (pos_ > pattern_.size()) {
            ok_ = false;
            return Unknown();
        }
        // Let RE2 decode the complete escape; hex/octal/property payloads are
        // never mistaken for literal text. Only equal bounds for this isolated
        // consuming atom prove a fixed string. Assertions were handled above.
        RE2 atom(pattern_.substr(start, pos_ - start), options_);
        std::string lower, upper;
        if (!atom.ok() || !atom.PossibleMatchRange(&lower, &upper, limit_) ||
            lower != upper) {
            return Unknown();
        }
        return Literal(lower, folded);
    }

    RegexLiteralSummary
    CharacterClass() {
        ++pos_;
        if (pos_ < pattern_.size() && pattern_[pos_] == '^')
            ++pos_;
        if (pos_ < pattern_.size() && pattern_[pos_] == ']')
            ++pos_;
        while (ok_ && pos_ < pattern_.size() && pattern_[pos_] != ']') {
            if (pattern_[pos_] == '\\') {
                Escape(false);
            } else if (pattern_.compare(pos_, 2, "[:") == 0) {
                const auto end = pattern_.find(":]", pos_ + 2);
                if (end == std::string::npos) {
                    ok_ = false;
                    break;
                }
                pos_ = end + 2;
            } else {
                ++pos_;
            }
        }
        if (pos_ == pattern_.size()) {
            ok_ = false;
        } else {
            ++pos_;
        }
        return Unknown();
    }

    RegexLiteralSummary
    Group(bool& folded, size_t depth) {
        ++pos_;
        bool group_folded = folded;
        if (pos_ < pattern_.size() && pattern_[pos_] == '?') {
            ++pos_;
            if (pattern_.compare(pos_, 2, "P<") == 0) {
                const auto end = pattern_.find('>', pos_ + 2);
                if (end == std::string::npos) {
                    ok_ = false;
                    return Unknown();
                }
                pos_ = end + 1;
            } else {
                bool enabled = true;
                while (pos_ < pattern_.size() && pattern_[pos_] != ':' &&
                       pattern_[pos_] != ')') {
                    const auto flag = pattern_[pos_++];
                    if (flag == '-')
                        enabled = false;
                    else if (flag == 'i')
                        group_folded = enabled;
                    else if (std::string_view("msU").find(flag) ==
                             std::string_view::npos) {
                        ok_ = false;
                        return Unknown();
                    }
                }
                if (pos_ == pattern_.size()) {
                    ok_ = false;
                    return Unknown();
                }
                if (pattern_[pos_++] == ')') {
                    folded = group_folded;
                    SkipEmptyQuotes();
                    // A flag-only group is not a repeatable atom in RE2.
                    // Decline ambiguous quantifier attachment across it.
                    if (pos_ < pattern_.size() &&
                        std::string_view("*+?{").find(pattern_[pos_]) !=
                            std::string_view::npos)
                        ok_ = false;
                    return {};
                }
            }
        }
        auto result = Sequence(group_folded, depth + 1);
        if (pos_ == pattern_.size() || pattern_[pos_++] != ')')
            ok_ = false;
        return result;
    }

    RegexLiteralSummary
    Sequence(bool folded, size_t depth) {
        if (depth > 64) {
            ok_ = false;
            return Unknown();
        }
        RegexLiteralSummary result;
        while (ok_ && pos_ < pattern_.size()) {
            if (!quoted_) {
                SkipEmptyQuotes();
                if (pos_ == pattern_.size() || pattern_[pos_] == ')')
                    break;
                if (pattern_.compare(pos_, 2, R"(\Q)") == 0) {
                    pos_ += 2;
                    quoted_ = true;
                    continue;
                }
            } else {
                const auto len = Utf8ValidatedCharByteLen(
                    pattern_.data() + pos_, pattern_.size() - pos_);
                auto literal = Literal(pattern_.substr(pos_, len), folded);
                pos_ += len;
                if (pattern_.compare(pos_, 2, R"(\E)") == 0) {
                    pos_ += 2;
                    quoted_ = false;
                    literal = Quantify(std::move(literal));
                }
                result = Concat(std::move(result), literal);
                continue;
            }
            RegexLiteralSummary atom;
            const char c = pattern_[pos_];
            if (c == '(')
                atom = Group(folded, depth);
            else if (c == '[')
                atom = CharacterClass();
            else if (c == '\\')
                atom = Escape(folded);
            else if (c == '.') {
                ++pos_;
                atom = Unknown();
            } else if (c == '^' || c == '$') {
                ++pos_;
            } else if (std::string_view("|*+?{}").find(c) !=
                       std::string_view::npos) {
                ok_ = false;
                break;
            } else {
                const auto len = Utf8ValidatedCharByteLen(
                    pattern_.data() + pos_, pattern_.size() - pos_);
                atom = Literal(pattern_.substr(pos_, len), folded);
                pos_ += len;
            }
            if (!ok_)
                break;
            result = Concat(std::move(result), Quantify(std::move(atom)));
        }
        return result;
    }

    const RE2::Options& options_;
    const size_t limit_;
    const std::string& pattern_;
    size_t pos_ = 0;
    bool ok_ = true;
    bool quoted_ = false;
};

}  // namespace

std::string
PartialRegexMatcher::RequiredLiteral() const {
    if (!CanExtractLiteral()) {
        return {};
    }
    auto literal = RegexLiteralAnalyzer(
                       re2_->pattern(), re2_->options(), kMaxScanLiteralBytes)
                       .Extract();
    return literal.has_value() ? std::move(*literal) : RequiredPrefix();
}

std::vector<std::string>
PartialRegexMatcher::RequiredIndexLiterals() const {
    if (!CanExtractLiteral()) {
        return {};
    }
    auto prefix = RequiredPrefix();
    // Analyze directly so unsupported syntax reuses the prefix above rather
    // than computing RE2's bounds again through RequiredLiteral's fallback.
    auto literal = RegexLiteralAnalyzer(
                       re2_->pattern(), re2_->options(), kMaxScanLiteralBytes)
                       .Extract();
    std::vector<std::string> requirements;
    auto add = [&](std::string requirement) {
        if (requirement.empty() ||
            std::any_of(requirements.begin(),
                        requirements.end(),
                        [&](const auto& existing) {
                            return existing.find(requirement) !=
                                   std::string::npos;
                        })) {
            return;
        }
        // Remove weaker requirements implied by the new substring.
        std::erase_if(requirements, [&](const auto& existing) {
            return requirement.find(existing) != std::string::npos;
        });
        requirements.push_back(std::move(requirement));
    };
    if (literal.has_value()) {
        // Bound backward-search work, retaining both ends so either can supply
        // selectivity. Every substring of a mandatory literal is mandatory;
        // these fragments are independent requirements, never concatenated.
        add(literal->substr(0, kMaxPrefixBytes));
        if (literal->size() > kMaxPrefixBytes) {
            add(literal->substr(literal->size() - kMaxPrefixBytes));
        }
    }
    add(std::move(prefix));
    return requirements;
}

std::vector<std::string>
PartialRegexMatcher::PrepareIndexLiterals(const std::string& pattern) {
    struct Entry {
        std::string pattern;
        std::vector<std::string> requirements;
    };
    static thread_local std::optional<Entry> cache;
    if (cache.has_value() && cache->pattern == pattern) {
        return cache->requirements;
    }
    // Compile before publishing: invalid regexes still throw and cannot leave
    // a new key paired with an old result. Returned vectors are independent.
    auto requirements = PartialRegexMatcher(pattern).RequiredIndexLiterals();
    if (pattern.size() <= kMaxProgramSize) {
        cache.emplace(Entry{pattern, requirements});
    }
    return requirements;
}

}  // namespace milvus
