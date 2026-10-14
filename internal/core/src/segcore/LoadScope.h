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

#include <unordered_set>
#include <vector>

#include "common/Schema.h"

namespace milvus::segcore {

// Immutable per-operation loading requirements. An empty explicit scope is
// empty. Compatibility callers resolve their legacy all-fields selector before
// constructing a scope; no collection Schema is changed by this value.
class LoadScope {
 public:
    explicit LoadScope(const std::vector<int64_t>& fields) {
        for (auto id : fields) {
            fields_.emplace(id);
        }
    }

    static LoadScope
    FromLegacySchema(const Schema& schema) {
        std::vector<int64_t> fields;
        for (auto id : schema.get_field_ids()) {
            if (schema.IsFieldInLoadScope(id)) {
                fields.push_back(id.get());
            }
        }
        auto scope = LoadScope(fields);
        scope.explicit_ = false;
        return scope;
    }

    bool
    Contains(FieldId id) const {
        return id.get() < START_USER_FIELDID || fields_.count(id.get()) != 0;
    }

    // Dependencies are physically loaded without granting logical access to
    // fields outside the View's explicit selection.
    LoadScope
    WithDependencies(const Schema& schema) const {
        auto scope = *this;
        if (auto pk = schema.get_primary_field_id()) {
            scope.dependencies_.emplace(pk->get());
        }
        if (auto ttl = schema.get_ttl_field_id()) {
            scope.dependencies_.emplace(ttl->get());
        }
        return scope;
    }

    bool
    Requires(FieldId id) const {
        return Contains(id) || dependencies_.count(id.get()) != 0;
    }

    bool
    IsExplicit() const {
        return explicit_;
    }

 private:
    bool explicit_ = true;
    std::unordered_set<int64_t> dependencies_;
    std::unordered_set<int64_t> fields_;
};

}  // namespace milvus::segcore
