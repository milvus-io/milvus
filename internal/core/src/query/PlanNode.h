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

#include <any>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>
#include <string>

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

    // Shared-filter hybrid search, phase 2: `plannodes_` with the filter
    // prefix replaced by a PrecomputedBitsetNode. Built on first use and then
    // reused.
    //
    // Cached per parsed plan rather than per segment because one Plan is one
    // branch of the group and is executed once per segment of the search: the
    // rebound tree is identical for all of them and holds no bitset (the
    // bitset reaches PhyPrecomputedBitsetNode through the QueryContext), so
    // the segments can share it exactly as they already share `plannodes_`.
    // Building it per segment would allocate and re-id one tree per branch per
    // segment; with the cache it is one per branch, so phase 2 no longer
    // hammers the plan-node id generator (which stays atomic either way).
    //
    // Throws for a plan shape phase 2 cannot rebind. std::call_once leaves the
    // flag clear on an exception, so such a plan keeps throwing rather than
    // caching a half-built tree.
    const std::shared_ptr<milvus::plan::PlanNode>&
    shared_filter_plannodes();

    SearchInfo search_info_;
    std::string placeholder_tag_;
    std::shared_ptr<milvus::plan::PlanNode> plannodes_;

 private:
    std::once_flag shared_filter_once_;
    std::shared_ptr<milvus::plan::PlanNode> shared_filter_plannodes_;
};

struct RetrievePlanNode : PlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;

    std::shared_ptr<milvus::plan::PlanNode> plannodes_;

    bool is_count_;
    int64_t limit_;
};

}  // namespace milvus::query
