// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <any>
#include <string_view>
#include <utility>
#include <vector>
#include "storage/IndexEntryTarget.h"

namespace milvus::index {

/**
 * @brief Per-load destinations and family initialization state for packed
 * input.
 *
 * Owns targets until successful reader initialization transfers their
 * resources. Destruction removes uncommitted files before releasing family
 * state and its directory leases; Commit marks successful ownership handoff
 * without I/O.
 * @pre Issued reads/writes must be drained before destruction. Destroy a failed
 * reader before its plan so mappings cannot outlive their backing files.
 * @note Async loading performs cleanup on the local-file executor; synchronous
 * loading keeps cleanup on the caller thread.
 */
struct IndexLoadPlan {
    std::vector<storage::EntryLoadPlan> entries;
    std::any load_context;

    IndexLoadPlan() = default;
    IndexLoadPlan(const IndexLoadPlan&) = delete;
    IndexLoadPlan&
    operator=(const IndexLoadPlan&) = delete;

    /** @brief Transfer cleanup responsibility, leaving the old plan empty. */
    IndexLoadPlan(IndexLoadPlan&& other) noexcept
        : entries(std::move(other.entries)),
          load_context(std::move(other.load_context)) {
        other.entries.clear();
    }

    IndexLoadPlan&
    operator=(IndexLoadPlan&&) = delete;

    ~IndexLoadPlan() {
        for (const auto& entry : entries) {
            const auto* target =
                std::get_if<storage::FileEntryTarget>(&entry.target);
            if (target && target->staging) {
                target->staging->Cleanup();
            }
        }
    }

    /**
     * @brief Look up a destination in this load's plan.
     * @return A reference owned by this plan.
     * @throws SegcoreError If the named entry was not planned.
     */
    const storage::EntryLoadPlan&
    At(std::string_view name) const {
        const auto it = std::find_if(
            entries.begin(), entries.end(), [name](const auto& entry) {
                return entry.name == name;
            });
        AssertInfo(it != entries.end(), "Planned entry not found: {}", name);
        return *it;
    }

    /**
     * @brief Hand off prepared files after successful reader initialization.
     * @pre All file targets are prepared and their writes have completed.
     * @note Does not perform I/O; call only after the final cancellation check.
     */
    void
    Commit() {
        for (auto& entry : entries) {
            if (auto* target =
                    std::get_if<storage::FileEntryTarget>(&entry.target)) {
                AssertInfo(target->staging && target->staging->Prepared(),
                           "Cannot commit unprepared file target for '{}'",
                           entry.name);
                target->staging->Commit();
            }
        }
    }
};

}  // namespace milvus::index
