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

#include "grep/CompiledPlan.h"

#include <string_view>

#include "common/EasyAssert.h"
#include "fmt/format.h"
#include "segcore/SegcoreConfig.h"

namespace milvus::grep {

std::shared_ptr<const Plan>
PlanCompiler::Compile(const std::vector<std::string_view>& patterns,
                      uint32_t pattern_kind,
                      uint32_t case_mode,
                      uint32_t boundary) {
    // Patterns are length-prefixed so that two different pattern lists never
    // render to the same key.
    auto key = fmt::format("{},{},{};", pattern_kind, case_mode, boundary);
    for (const auto& pattern : patterns) {
        key += fmt::format("{}:{},", pattern.size(), pattern);
    }
    auto& plan = plans_[key];
    if (plan == nullptr) {
        const auto& config = segcore::SegcoreConfig::default_config();
        plan = std::make_shared<const Plan>(
            Plan::Compile(patterns,
                          {static_cast<PatternKind>(pattern_kind),
                           static_cast<CaseMode>(case_mode),
                           static_cast<Boundary>(boundary)},
                          {config.get_grep_max_regex_bytes(),
                           config.get_grep_max_dfa_cache_bytes()}));
    }
    return plan;
}

std::shared_ptr<const Plan>
PlanCompiler::Compile(const proto::plan::GrepExpr& spec) {
    AssertInfo(spec.pattern_kind() <= 1 && spec.case_mode() <= 1 &&
                   spec.boundary() <= 2,
               "invalid normalized grep options");
    if (spec.select_none()) {
        return nullptr;
    }
    return Compile({spec.patterns().begin(), spec.patterns().end()},
                   spec.pattern_kind(),
                   spec.case_mode(),
                   spec.boundary());
}

}  // namespace milvus::grep
