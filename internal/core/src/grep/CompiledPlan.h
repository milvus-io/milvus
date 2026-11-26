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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "grep/Grep.h"
#include "pb/plan.pb.h"

namespace milvus::grep {

// Compiles the patterns of one request plan. Compiling a regex is a
// per-request cost, so it is done while the plan is parsed, under the
// segcore Grep limits, and the immutable result is kept in the plan: every
// segment of the request creates its own Session from it. A compile failure
// (for example a regex over the size limit) fails the plan.
//
// The compiled matcher depends on the patterns, the pattern kind, the case
// mode and the boundary only. Uses that agree on these four, such as a
// grep(...) predicate and a GREP_MATCHES(...) item written with the same
// pattern, get the same Plan; each creates its sessions with its own
// LineSelection. One instance serves the parsing of one plan and is not
// thread-safe.
class PlanCompiler {
 public:
    // The caller has validated the three option values.
    std::shared_ptr<const Plan>
    Compile(const std::vector<std::string_view>& patterns,
            uint32_t pattern_kind,
            uint32_t case_mode,
            uint32_t boundary);

    // Returns nullptr when the specification selects no row (a final
    // "-m 0"): like rg, the patterns are then not compiled.
    std::shared_ptr<const Plan>
    Compile(const proto::plan::GrepExpr& spec);

 private:
    std::map<std::string, std::shared_ptr<const Plan>> plans_;
};

// The predicate needs the row verdict only: each value stops at its first
// matching line, and no context is requested.
inline LineSelection
PredicateSelection() {
    return {1, 0, 0};
}

// The matching-line cap and context of a GREP_MATCHES(field, args) item.
inline LineSelection
MatchesSelection(const proto::plan::GrepMatches& item) {
    return {item.has_max_count() ? std::optional<uint64_t>(item.max_count())
                                 : std::nullopt,
            item.before_context(),
            item.after_context()};
}

}  // namespace milvus::grep
