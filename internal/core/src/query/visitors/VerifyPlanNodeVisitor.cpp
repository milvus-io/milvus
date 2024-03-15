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

#include "query/generated/VerifyPlanNodeVisitor.h"

namespace milvus::query {

namespace impl {
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR UNDER suvlim/core_gen/
class VerifyPlanNodeVisitor : PlanNodeVisitor {
 public:
    using RetType = SearchResult;
    VerifyPlanNodeVisitor() = default;

 private:
    std::optional<RetType> ret_;
};
}  // namespace impl

void
VerifyPlanNodeVisitor::visit(FloatVectorANNS&) {
}

void
VerifyPlanNodeVisitor::visit(BinaryVectorANNS&) {
}

void
VerifyPlanNodeVisitor::visit(Float16VectorANNS&) {
}

void
VerifyPlanNodeVisitor::visit(BFloat16VectorANNS&) {
}

void
VerifyPlanNodeVisitor::visit(SparseFloatVectorANNS&) {
}

void
VerifyPlanNodeVisitor::visit(RetrievePlanNode&) {
}

}  // namespace milvus::query
