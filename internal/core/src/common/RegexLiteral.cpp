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
#include <cctype>
#include <functional>
#include <limits>
#include <set>
#include <optional>
#include <unordered_map>

#include <re2/filtered_re2.h>

namespace milvus {

namespace {

std::string
AsciiLower(std::string_view value) {
    std::string result(value);
    for (char& c : result) {
        const auto byte = static_cast<unsigned char>(c);
        if (byte < 0x80) {
            c = static_cast<char>(std::tolower(byte));
        }
    }
    return result;
}

void
CollectLiteralNodes(const RegexLiteralCondition& condition,
                    std::vector<std::string>& literals) {
    if (condition.op == RegexLiteralCondition::Op::Literal) {
        literals.push_back(condition.literal);
        return;
    }
    for (const auto& child : condition.children) {
        CollectLiteralNodes(child, literals);
    }
}

std::vector<std::string>
MapPrefilterAtomToOriginalBytes(const std::string& atom,
                                const RegexLiteralCondition& fallback) {
    std::vector<std::string> source_literals;
    CollectLiteralNodes(fallback, source_literals);
    std::set<std::string> mapped;
    for (const auto& source : source_literals) {
        if (source.size() < atom.size()) {
            continue;
        }
        for (size_t offset = 0; offset + atom.size() <= source.size();
             ++offset) {
            const auto candidate = source.substr(offset, atom.size());
            if (AsciiLower(candidate) == atom) {
                mapped.insert(candidate);
            }
        }
    }
    return {mapped.begin(), mapped.end()};
}

RegexLiteralCondition
BuildMappedAtomCondition(const std::vector<std::string>& literals) {
    RegexLiteralCondition result;
    bool initialized = false;
    for (const auto& literal : literals) {
        auto atom = RegexLiteralCondition::Literal(literal);
        result = initialized ? RegexLiteralCondition::Combine(
                                   RegexLiteralCondition::Op::Or,
                                   std::move(result),
                                   std::move(atom))
                             : std::move(atom);
        initialized = true;
    }
    return result;
}

// Prove that every row satisfying the original-byte condition also satisfies
// the mapped prefilter. Lowercased atoms lose branch provenance: a spelling
// found in one branch is not evidence for a folded occurrence in another.
// These implication rules are sufficient, not complete. Failure (including
// budget exhaustion) means keep the original condition, never trust a mapping.
bool
ConditionImplies(const RegexLiteralCondition& original,
                 const RegexLiteralCondition& mapped,
                 size_t& work_left) {
    if (work_left == 0) {
        return false;
    }
    --work_left;
    if (mapped.IsTrue() || original == mapped) {
        return true;
    }
    if (original.IsTrue()) {
        return false;
    }
    using Op = RegexLiteralCondition::Op;
    if (original.op == Op::Or) {
        return std::all_of(original.children.begin(),
                           original.children.end(),
                           [&](const auto& child) {
                               return ConditionImplies(
                                   child, mapped, work_left);
                           });
    }
    if (mapped.op == Op::And) {
        return std::all_of(mapped.children.begin(),
                           mapped.children.end(),
                           [&](const auto& child) {
                               return ConditionImplies(
                                   original, child, work_left);
                           });
    }
    if (original.op == Op::And) {
        return std::any_of(original.children.begin(),
                           original.children.end(),
                           [&](const auto& child) {
                               return ConditionImplies(
                                   child, mapped, work_left);
                           });
    }
    if (mapped.op == Op::Or) {
        return std::any_of(mapped.children.begin(),
                           mapped.children.end(),
                           [&](const auto& child) {
                               return ConditionImplies(
                                   original, child, work_left);
                           });
    }
    return original.literal.find(mapped.literal) != std::string::npos;
}

RegexLiteralCondition
ExtractPrefilterCondition(const std::string& pattern,
                          const RE2::Options& options,
                          const RegexLiteralCondition& fallback) {
    // FilteredRE2 is the public entry point to RE2's parsed Regexp walker and
    // Prefilter. Its atoms are lowercased by design, so map them back to the
    // exact byte spelling retained by the canonical structural summary before
    // they reach FMIndex. The original-byte condition is also the safety
    // fallback when a mapping cannot be proved to cover every viable branch.
    if (fallback.IsTrue()) {
        return fallback;
    }
    re2::FilteredRE2 filtered(2);
    int regexp_id = -1;
    if (filtered.Add(re2::StringPiece(pattern), options, &regexp_id) !=
        RE2::NoError) {
        return fallback;
    }

    std::vector<std::string> atoms;
    filtered.Compile(&atoms);
    if (atoms.empty()) {
        return fallback;
    }

    std::vector<RegexLiteralCondition> mapped_atoms;
    mapped_atoms.reserve(atoms.size());
    for (const auto& atom : atoms) {
        auto mapped = MapPrefilterAtomToOriginalBytes(atom, fallback);
        if (mapped.empty()) {
            // For example, a long RE2 atom may exceed the retained 64-byte
            // endpoints. Keep those endpoints and independent rare literals.
            return fallback;
        }
        mapped_atoms.push_back(BuildMappedAtomCondition(mapped));
        if (mapped_atoms.back().IsTrue()) {
            return fallback;
        }
    }

    const size_t max_probes = RegexLiteralCondition::kMaxNodes * 64;
    size_t probes = 0;
    std::vector<std::vector<int>> terms;
    std::vector<int> selected;
    bool exhausted = false;
    auto satisfies = [&](const std::vector<int>& ids) {
        if (++probes > max_probes) {
            exhausted = true;
            return false;
        }
        std::vector<int> potential;
        filtered.AllPotentials(ids, &potential);
        return std::find(potential.begin(), potential.end(), regexp_id) !=
               potential.end();
    };
    std::function<void(size_t)> visit = [&](size_t next) {
        if (exhausted) {
            return;
        }
        if (satisfies(selected)) {
            // The prefilter is monotone. A satisfying set is useful only when
            // none of its atoms can be removed while keeping it satisfying.
            for (size_t i = 0; i < selected.size(); ++i) {
                auto reduced = selected;
                reduced.erase(reduced.begin() + i);
                if (satisfies(reduced)) {
                    return;
                }
            }
            terms.push_back(selected);
            return;
        }
        if (next == atoms.size()) {
            return;
        }
        visit(next + 1);
        selected.push_back(static_cast<int>(next));
        visit(next + 1);
        selected.pop_back();
    };
    visit(0);
    if (exhausted || terms.empty()) {
        return fallback;
    }

    RegexLiteralCondition result;
    bool initialized = false;
    for (const auto& term : terms) {
        RegexLiteralCondition conjunction;
        for (const auto atom_id : term) {
            conjunction =
                RegexLiteralCondition::Combine(RegexLiteralCondition::Op::And,
                                               std::move(conjunction),
                                               mapped_atoms[atom_id]);
        }
        result = initialized ? RegexLiteralCondition::Combine(
                                   RegexLiteralCondition::Op::Or,
                                   std::move(result),
                                   std::move(conjunction))
                             : std::move(conjunction);
        initialized = true;
        if (result.IsTrue()) {
            return fallback;
        }
    }
    size_t work_left =
        RegexLiteralCondition::kMaxNodes * RegexLiteralCondition::kMaxNodes;
    return ConditionImplies(fallback, result, work_left) ? result : fallback;
}

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
    RegexLiteralCondition condition;
};

class RegexLiteralAnalyzer {
 public:
    RegexLiteralAnalyzer(const std::string& pattern,
                         const RE2::Options& options,
                         size_t limit,
                         bool conditions = false)
        : options_(options),
          limit_(limit),
          pattern_(pattern),
          conditions_(conditions) {
    }

    std::optional<std::string>
    Extract() {
        auto result = Sequence(false, 0);
        if (!ok_ || pos_ != pattern_.size()) {
            return std::nullopt;
        }
        return std::move(result.best);
    }

    std::optional<RegexLiteralCondition>
    ExtractCondition() {
        auto result = Sequence(false, 0);
        if (!ok_ || pos_ != pattern_.size()) {
            return std::nullopt;
        }
        return Condition(result);
    }

 private:
    static RegexLiteralCondition
    Condition(const RegexLiteralSummary& s) {
        if (s.exact) {
            return RegexLiteralCondition::Literal(s.prefix);
        }
        return RegexLiteralCondition::Combine(
            RegexLiteralCondition::Op::And,
            RegexLiteralCondition::Combine(
                RegexLiteralCondition::Op::And,
                s.condition,
                RegexLiteralCondition::Literal(s.prefix)),
            RegexLiteralCondition::Literal(s.suffix));
    }

    static RegexLiteralSummary
    Unknown() {
        return {{}, {}, {}, false, {}};
    }

    RegexLiteralSummary
    Literal(const std::string& s, bool folded) const {
        if (folded) {
            return Unknown();
        }
        return {
            s, s, s, true, {}};  // One decoded rune, never longer than limit_.
    }

    RegexLiteralSummary
    Concat(RegexLiteralSummary a, const RegexLiteralSummary& b) const {
        const bool exact =
            a.exact && b.exact && a.prefix.size() + b.prefix.size() <= limit_;
        const auto bridge_size =
            std::min(limit_, a.suffix.size() + b.prefix.size());
        RegexLiteralCondition condition;
        if (conditions_ && !exact) {
            using Op = RegexLiteralCondition::Op;
            // Keep a growing fixed suffix pending until a gap, so long runs
            // retain their endpoints without accumulating overlapping windows
            // (or spending the tree budget before a later rare condition).
            condition = a.condition;
            if (!b.exact) {
                condition = RegexLiteralCondition::Combine(
                    Op::And, Condition(a), Condition(b));
                if (!a.suffix.empty() && !b.prefix.empty()) {
                    auto bridge = a.suffix;
                    bridge.append(b.prefix, 0, limit_ - bridge.size());
                    condition = RegexLiteralCondition::Combine(
                        Op::And,
                        std::move(condition),
                        RegexLiteralCondition::Literal(bridge));
                }
            }
        }
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
            const auto keep =
                std::min(a.suffix.size(), limit_ - b.suffix.size());
            a.suffix.erase(0, a.suffix.size() - keep);
            a.suffix += b.suffix;
        } else {
            a.suffix = b.suffix;
        }
        a.exact = exact;
        a.condition = std::move(condition);
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
            if (conditions_) {
                atom.condition = Condition(atom);
            }
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
            if (conditions_) {
                result.condition = Condition(result);
            }
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
        std::optional<RegexLiteralCondition> alternatives;
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
            else if (c == '|' && conditions_) {
                auto branch = Condition(result);
                alternatives = alternatives.has_value()
                                   ? RegexLiteralCondition::Combine(
                                         RegexLiteralCondition::Op::Or,
                                         std::move(*alternatives),
                                         std::move(branch))
                                   : std::move(branch);
                result = {};
                ++pos_;
                continue;
            } else if (c == '[')
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
        if (alternatives.has_value()) {
            auto condition =
                RegexLiteralCondition::Combine(RegexLiteralCondition::Op::Or,
                                               std::move(*alternatives),
                                               Condition(result));
            result = Unknown();
            result.condition = std::move(condition);
        }
        return result;
    }

    const RE2::Options& options_;
    const size_t limit_;
    const std::string& pattern_;
    size_t pos_ = 0;
    bool ok_ = true;
    bool quoted_ = false;
    const bool conditions_;
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

RegexLiteralCondition
PartialRegexMatcher::RequiredIndexCondition() const {
    if (!CanExtractLiteral()) {
        return {};
    }
    auto fallback =
        RegexLiteralAnalyzer(
            re2_->pattern(), re2_->options(), kMaxScanLiteralBytes, true)
            .ExtractCondition();
    if (!fallback.has_value()) {
        return {};
    }
    return ExtractPrefilterCondition(
        re2_->pattern(), re2_->options(), *fallback);
}

RegexLiteralCondition
PartialRegexMatcher::PrepareIndexCondition(const std::string& pattern) {
    struct Entry {
        std::string pattern;
        RegexLiteralCondition condition;
    };
    static thread_local std::optional<Entry> cache;
    if (cache.has_value() && cache->pattern == pattern) {
        return cache->condition;
    }
    auto condition = PartialRegexMatcher(pattern).RequiredIndexCondition();
    if (pattern.size() <= kMaxProgramSize) {
        cache.emplace(Entry{pattern, condition});
    }
    return condition;
}

size_t
RegexLiteralCondition::NodeCount() const {
    size_t result = 1;
    for (const auto& child : children) {
        result += child.NodeCount();
    }
    return result;
}

RegexLiteralCondition
RegexLiteralCondition::Literal(const std::string& text) {
    if (text.size() < 2) {
        return {};
    }
    RegexLiteralCondition first{
        Op::Literal, text.substr(0, kMaxLiteralBytes), {}};
    if (text.size() <= kMaxLiteralBytes) {
        return first;
    }
    return Combine(
        Op::And,
        std::move(first),
        {Op::Literal, text.substr(text.size() - kMaxLiteralBytes), {}});
}

RegexLiteralCondition
RegexLiteralCondition::Combine(Op op,
                               RegexLiteralCondition a,
                               RegexLiteralCondition b) {
    if (a.IsTrue() || b.IsTrue()) {
        if (op == Op::Or) {
            return {};
        }
        return a.IsTrue() ? std::move(b) : std::move(a);
    }
    if (a == b) {
        return a;
    }
    if (op == Op::And) {
        // Flatten ANDs and drop weaker atoms implied by a longer atom. This
        // keeps incremental fixed runs from filling the budget with prefixes.
        if (a.op != op) {
            if (a.NodeCount() == kMaxNodes) {
                return a;
            }
            a = {op, {}, {std::move(a)}};
        }
        auto add = [&](RegexLiteralCondition child) {
            for (const auto& existing : a.children) {
                if (existing == child ||
                    (existing.op == Op::Literal && child.op == Op::Literal &&
                     existing.literal.find(child.literal) !=
                         std::string::npos)) {
                    return;
                }
            }
            if (child.op == Op::Literal) {
                std::erase_if(a.children, [&](const auto& existing) {
                    return existing.op == Op::Literal &&
                           child.literal.find(existing.literal) !=
                               std::string::npos;
                });
            }
            if (a.NodeCount() + child.NodeCount() <= kMaxNodes) {
                a.children.push_back(std::move(child));
            }
        };
        if (b.op == op) {
            for (auto& child : b.children) {
                add(std::move(child));
            }
        } else {
            add(std::move(b));
        }
        auto key = [](const RegexLiteralCondition& node,
                      const auto& self) -> std::string {
            if (node.op == Op::Literal) {
                return "L" + node.literal;
            }
            std::vector<std::string> children;
            children.reserve(node.children.size());
            for (const auto& child : node.children) {
                children.push_back(self(child, self));
            }
            std::sort(children.begin(), children.end());
            std::string result = node.op == Op::And ? "A" : "O";
            for (const auto& child : children) {
                result += "(" + child + ")";
            }
            return result;
        };
        std::sort(a.children.begin(),
                  a.children.end(),
                  [&](const auto& left, const auto& right) {
                      return key(left, key) < key(right, key);
                  });
        if (a.children.size() == 1) {
            return std::move(a.children.front());
        }
        return a;
    }
    // Never drop an OR branch on exhaustion: the entire OR becomes TRUE.
    if (a.NodeCount() + b.NodeCount() + 1 > kMaxNodes) {
        return {};
    }
    return {op, {}, {std::move(a), std::move(b)}};
}

RegexLiteralCondition
RegexLiteralCondition::Select(
    const std::function<size_t(const std::string&)>& count,
    double& cost) const {
    std::unordered_map<std::string, size_t> counts;
    std::function<RegexLiteralCondition(const RegexLiteralCondition&, double&)>
        select;
    select = [&](const RegexLiteralCondition& node, double& estimate) {
        if (node.IsTrue()) {
            estimate = std::numeric_limits<double>::infinity();
            return node;
        }
        if (node.op == Op::Literal) {
            auto [it, inserted] = counts.try_emplace(node.literal, 0);
            if (inserted) {
                it->second = count(node.literal);
            }
            estimate = static_cast<double>(it->second);
            return node;
        }
        RegexLiteralCondition result;
        estimate =
            node.op == Op::And ? std::numeric_limits<double>::infinity() : 0;
        for (const auto& child : node.children) {
            double child_cost;
            auto selected = select(child, child_cost);
            if (node.op == Op::And) {
                if (child_cost < estimate) {
                    estimate = child_cost;
                    result = std::move(selected);
                }
            } else {
                estimate += child_cost;
                if (selected.IsTrue()) {
                    estimate = std::numeric_limits<double>::infinity();
                    return selected;
                }
                // Start with the first child, not TRUE (which absorbs OR).
                result = result.IsTrue() ? std::move(selected)
                                         : Combine(Op::Or,
                                                   std::move(result),
                                                   std::move(selected));
            }
        }
        return result;
    };
    auto result = select(*this, cost);
    std::unordered_map<std::string, size_t> selected_counts;
    std::function<double(const RegexLiteralCondition&)> selected_cost =
        [&](const RegexLiteralCondition& node) {
            if (node.IsTrue()) {
                return std::numeric_limits<double>::infinity();
            }
            if (node.op == Op::Literal) {
                const auto [it, inserted] =
                    selected_counts.try_emplace(node.literal, 0);
                if (inserted) {
                    it->second = counts.at(node.literal);
                }
                return static_cast<double>(it->second);
            }
            double total = 0;
            for (const auto& child : node.children) {
                total += selected_cost(child);
            }
            return total;
        };
    cost = selected_cost(result);
    return result;
}

}  // namespace milvus
