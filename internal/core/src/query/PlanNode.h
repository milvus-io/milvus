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
#include <optional>
#include <vector>
#include <string>

#include "common/QueryInfo.h"

namespace milvus::plan {
class PlanNode;
};
namespace milvus::query {

class PlanNodeVisitor;
// Base of all Nodes
struct PlanNode {
 public:
    virtual ~PlanNode() = default;
    virtual void
    accept(PlanNodeVisitor&) = 0;
};

using PlanNodePtr = std::unique_ptr<PlanNode>;

struct VectorPlanNode : PlanNode {
    SearchInfo search_info_;
    std::string placeholder_tag_;
    std::shared_ptr<milvus::plan::PlanNode> plannodes_;
};

struct FloatVectorANNS : VectorPlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;
};

struct BinaryVectorANNS : VectorPlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;
};

struct Float16VectorANNS : VectorPlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;
};

struct BFloat16VectorANNS : VectorPlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;
};

struct SparseFloatVectorANNS : VectorPlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;
};

struct Int8VectorANNS : VectorPlanNode {
 public:
    void
    accept(PlanNodeVisitor&) override;
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
