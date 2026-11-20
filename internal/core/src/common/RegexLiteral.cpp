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
#include <functional>
#include <limits>
#include <optional>
#include <unordered_map>

namespace milvus {

namespace {

// Only public value types cross the RE2 boundary. Preserve every child,
// including TRUE branches, and keep the original bytes unchanged.
RegexLiteralCondition
FromRE2(const RE2::LiteralCondition& source) {
    using Op = RegexLiteralCondition::Op;
    RegexLiteralCondition result;
    switch (source.op) {
        case RE2::LiteralCondition::Op::True:
            result.op = Op::True;
            break;
        case RE2::LiteralCondition::Op::Literal:
            result.op = Op::Literal;
            break;
        case RE2::LiteralCondition::Op::And:
            result.op = Op::And;
            break;
        case RE2::LiteralCondition::Op::Or:
            result.op = Op::Or;
            break;
    }
    result.literal = source.literal;
    result.children.reserve(source.children.size());
    for (const auto& child : source.children) {
        result.children.push_back(FromRE2(child));
    }
    return result;
}

}  // namespace

const RE2::LiteralRequirements&
PartialRegexMatcher::RequiredLiterals() const {
    std::call_once(requirements_once_, [this] {
        if (CanExtractLiteral()) {
            requirements_ =
                re2_->RequiredLiterals(RegexLiteralCondition::kMaxNodes,
                                       RegexLiteralCondition::kMaxLiteralBytes,
                                       kMaxScanLiteralBytes,
                                       kMaxProgramSize);
        }
    });
    return requirements_;
}

std::string
PartialRegexMatcher::RequiredLiteral() const {
    return RequiredLiterals().literal;
}

RegexLiteralCondition
PartialRegexMatcher::RequiredIndexCondition() const {
    return FromRE2(RequiredLiterals().condition);
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
