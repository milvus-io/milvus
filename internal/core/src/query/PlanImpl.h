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

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "Plan.h"
#include "PlanNode.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Consts.h"
#include "common/Schema.h"

namespace milvus::query {

using Json = nlohmann::json;

struct ExtractedPlanInfo {
 public:
    explicit ExtractedPlanInfo(int64_t size) : involved_fields_(size) {
    }

    void
    add_involved_field(FieldId field_id) {
        auto pos = field_id.get() - START_USER_FIELDID;
        AssertInfo(0 <= pos && pos < involved_fields_.size(),
                   "invalid field id, field_id {}, schema size {}",
                   field_id,
                   involved_fields_.size());
        involved_fields_.set(pos);
    }

 public:
    BitsetType involved_fields_;
};

struct Plan {
 public:
    explicit Plan(const Schema& schema) : schema_(schema) {
    }

 public:
    const Schema& schema_;
    std::unique_ptr<VectorPlanNode> plan_node_;
    std::map<std::string, FieldId> tag2field_;  // PlaceholderName -> FieldId
    std::vector<FieldId> target_entries_;
    std::vector<std::string> target_dynamic_fields_;
    void
    check_identical(Plan& other);

 public:
    std::optional<ExtractedPlanInfo> extra_info_opt_;
    // TODO: add move extra info
};

struct Placeholder {
    std::string tag_;
    int64_t num_of_queries_;
    // TODO(SPARSE): add a dim_ field here, use the dim passed in search request
    // instead of the dim in schema, since the dim of sparse float column is
    // dynamic. This change will likely affect lots of code, thus I'll do it in
    // a separate PR, and use dim=0 for sparse float vector searches for now.

    // only one of blob_ and sparse_matrix_ should be set. blob_ is used for
    // dense vector search and sparse_matrix_ is for sparse vector search.
    aligned_vector<char> blob_;
    std::unique_ptr<knowhere::sparse::SparseRow<float>[]> sparse_matrix_;

    const void*
    get_blob() const {
        if (blob_.empty()) {
            return sparse_matrix_.get();
        }
        return blob_.data();
    }

    void*
    get_blob() {
        if (blob_.empty()) {
            return sparse_matrix_.get();
        }
        return blob_.data();
    }
};

struct RetrievePlan {
 public:
    explicit RetrievePlan(const Schema& schema) : schema_(schema) {
    }

 public:
    const Schema& schema_;
    std::unique_ptr<RetrievePlanNode> plan_node_;
    std::vector<FieldId> field_ids_;
    std::vector<std::string> target_dynamic_fields_;
};

using PlanPtr = std::unique_ptr<Plan>;

struct PlaceholderGroup : std::vector<Placeholder> {
    using std::vector<Placeholder>::vector;
};

}  // namespace milvus::query
