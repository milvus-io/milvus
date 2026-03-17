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

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldMeta.h"
#include "common/QueryInfo.h"

namespace milvus::plan {
class PlanNode;
};
namespace milvus::query {

class PlanNodeVisitor;

struct PlanOptions {
    bool expr_use_json_stats = true;
};

// Base of all Nodes
struct PlanNode {
 public:
    virtual ~PlanNode() = default;
    virtual void
    accept(PlanNodeVisitor&) = 0;

    PlanOptions plan_options_;
};

using PlanNodePtr = std::unique_ptr<PlanNode>;

struct VectorPlanNode : PlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;

    SearchInfo search_info_;
    std::string placeholder_tag_;
    std::shared_ptr<milvus::plan::PlanNode> plannodes_;
};

struct RetrievePlanNode : PlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;

    std::shared_ptr<milvus::plan::PlanNode> plannodes_;

    int64_t limit_;
    bool has_order_by_ = false;
    // Non-sort output fields deferred for late materialization (two-project mode).
    // Empty means single-project mode (all columns materialized in the first project).
    std::vector<FieldId> deferred_field_ids_;
    // Field IDs for pipeline columns in the same order as the ProjectNode output.
    // Used by FillOrderByResult to set field_id on DataArrays produced by the pipeline.
    std::vector<FieldId> pipeline_field_ids_;
};

}  // namespace milvus::query
