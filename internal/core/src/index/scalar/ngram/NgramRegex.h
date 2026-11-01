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

#pragma once

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "common/RegexQuery.h"

namespace milvus::index {

// RegexMatch candidate planning reuses the bounded AND/OR condition from
// PartialRegexMatcher::PrepareIndexCondition (shared with FMINDEX). The plan
// is a necessary condition, never an answer: candidates are always rechecked
// with the canonical RE2 matcher. TRUE means no pruning (raw scan).

// Drops partial UTF-8 characters at either end, then keeps the longest valid
// UTF-8 prefix. RE2 may cut a 64-byte atom inside a character, and the NGRAM
// engine only accepts valid UTF-8; a substring of a required literal is still
// required, so trimming never loses a match.
std::string
TrimToUtf8(const std::string& literal);

// Restricts `condition` to lookups an index with `min_gram` can serve. A
// literal shorter than min_gram UTF-8 characters becomes TRUE; AND drops TRUE
// children; OR becomes TRUE when any branch is TRUE, so no branch is dropped.
// Conditions above RegexLiteralCondition::kMaxNodes are rejected as TRUE.
RegexLiteralCondition
PlanNgramRegexCondition(const RegexLiteralCondition& condition,
                        size_t min_gram);

// Compiles `pattern` (throws on invalid regex) and plans it for `min_gram`.
RegexLiteralCondition
PlanNgramRegex(const std::string& pattern, size_t min_gram);

// AND-merges a non-TRUE plan into `candidates`. `apply_literals(literals,
// bitmap)` must AND every literal's superset hits into `bitmap`. Literal
// children of one AND are applied together so the reader's cost policy sees
// all of them; every OR branch starts from the incoming candidates and the
// branches are unioned. Work is bounded by the plan's node count.
template <typename Bitmap, typename ApplyLiterals>
void
ApplyNgramRegexPlan(const RegexLiteralCondition& plan,
                    Bitmap& candidates,
                    const ApplyLiterals& apply_literals) {
    using Op = RegexLiteralCondition::Op;
    if (plan.IsTrue() || candidates.none()) {
        return;
    }
    switch (plan.op) {
        case Op::True:
            return;
        case Op::Literal:
            apply_literals(std::vector<std::string>{plan.literal}, candidates);
            return;
        case Op::And: {
            std::vector<std::string> literals;
            for (const auto& child : plan.children) {
                if (child.op == Op::Literal) {
                    literals.push_back(child.literal);
                }
            }
            if (!literals.empty()) {
                apply_literals(literals, candidates);
            }
            for (const auto& child : plan.children) {
                if (child.op != Op::Literal) {
                    ApplyNgramRegexPlan(child, candidates, apply_literals);
                }
            }
            return;
        }
        case Op::Or: {
            Bitmap merged(candidates.size(), false);
            for (const auto& child : plan.children) {
                auto branch = candidates.clone();
                ApplyNgramRegexPlan(child, branch, apply_literals);
                merged |= branch;
            }
            candidates = std::move(merged);
            return;
        }
    }
}

}  // namespace milvus::index
