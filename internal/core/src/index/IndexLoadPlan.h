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

// Owns destinations and engine context from planning through finalization.
// The caller must drain reads before destroying the plan. Plans containing
// files must be released on LocalFileIOPool; uncommitted files are removed
// before engine context (including directory leases) is released.
struct IndexLoadPlan {
    std::vector<storage::EntryLoadPlan> entries;
    std::any load_context;

    IndexLoadPlan() = default;
    IndexLoadPlan(const IndexLoadPlan&) = delete;
    IndexLoadPlan&
    operator=(const IndexLoadPlan&) = delete;

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
            if (target && target->staging && target->staging->file &&
                !target->staging->file->Committed()) {
                target->staging->file.reset();
            }
        }
    }

    const storage::EntryLoadPlan&
    At(std::string_view name) const {
        const auto it = std::find_if(
            entries.begin(), entries.end(), [name](const auto& entry) {
                return entry.name == name;
            });
        AssertInfo(it != entries.end(), "Planned entry not found: {}", name);
        return *it;
    }

    // Called only after successful engine finalization. Does not perform I/O.
    void
    Commit() {
        for (auto& entry : entries) {
            if (auto* target =
                    std::get_if<storage::FileEntryTarget>(&entry.target)) {
                AssertInfo(target->staging && target->staging->file,
                           "Cannot commit unprepared file target for '{}'",
                           entry.name);
                if (target->staging->retain_on_success) {
                    target->staging->file->Commit();
                }
            }
        }
    }
};
}  // namespace milvus::index
