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

#include "index/scalar/ngram/NgramRegex.h"

#include <algorithm>
#include <string>
#include <utility>

namespace milvus::index {
namespace {

// Length of the strictly valid UTF-8 (RFC 3629) character at `text`, or 0.
size_t
ValidUtf8CharLength(const unsigned char* text, size_t remaining) {
    const auto lead = text[0];
    if (lead < 0x80) {
        return 1;
    }
    size_t length = 0;
    unsigned char low = 0x80;
    unsigned char high = 0xBF;
    if (lead >= 0xC2 && lead <= 0xDF) {
        length = 2;
    } else if (lead >= 0xE0 && lead <= 0xEF) {
        length = 3;
        low = lead == 0xE0 ? 0xA0 : 0x80;
        high = lead == 0xED ? 0x9F : 0xBF;
    } else if (lead >= 0xF0 && lead <= 0xF4) {
        length = 4;
        low = lead == 0xF0 ? 0x90 : 0x80;
        high = lead == 0xF4 ? 0x8F : 0xBF;
    } else {
        return 0;
    }
    if (length > remaining || text[1] < low || text[1] > high) {
        return 0;
    }
    for (size_t i = 2; i < length; ++i) {
        if (!IsUtf8ContinuationByte(text[i])) {
            return 0;
        }
    }
    return length;
}

RegexLiteralCondition
Plan(const RegexLiteralCondition& condition, size_t min_gram) {
    using Op = RegexLiteralCondition::Op;
    switch (condition.op) {
        case Op::True:
            return {};
        case Op::Literal: {
            auto text = TrimToUtf8(condition.literal);
            if (text.empty() || Utf8CharCount(text.data(), text.size()) <
                                    std::max<size_t>(min_gram, 1)) {
                return {};
            }
            return {Op::Literal, std::move(text), {}};
        }
        case Op::And:
        case Op::Or: {
            RegexLiteralCondition result{condition.op, {}, {}};
            for (const auto& child : condition.children) {
                auto planned = Plan(child, min_gram);
                if (!planned.IsTrue()) {
                    result.children.push_back(std::move(planned));
                } else if (condition.op == Op::Or) {
                    // A branch without a usable literal can match any row.
                    return {};
                }
            }
            if (result.children.empty()) {
                return {};
            }
            if (result.children.size() == 1) {
                return std::move(result.children.front());
            }
            return result;
        }
    }
    return {};
}

}  // namespace

std::string
TrimToUtf8(const std::string& literal) {
    const auto* bytes = reinterpret_cast<const unsigned char*>(literal.data());
    size_t begin = 0;
    while (begin < literal.size() && IsUtf8ContinuationByte(bytes[begin])) {
        ++begin;
    }
    size_t end = begin;
    while (end < literal.size()) {
        const auto length =
            ValidUtf8CharLength(bytes + end, literal.size() - end);
        if (length == 0) {
            break;
        }
        end += length;
    }
    return literal.substr(begin, end - begin);
}

RegexLiteralCondition
PlanNgramRegexCondition(const RegexLiteralCondition& condition,
                        size_t min_gram) {
    if (condition.NodeCount() > RegexLiteralCondition::kMaxNodes) {
        return {};
    }
    return Plan(condition, min_gram);
}

RegexLiteralCondition
PlanNgramRegex(const std::string& pattern, size_t min_gram) {
    return PlanNgramRegexCondition(
        PartialRegexMatcher::PrepareIndexCondition(pattern), min_gram);
}

}  // namespace milvus::index
