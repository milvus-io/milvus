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

// Generated File
// DO NOT EDIT
#include "query/PlanNode.h"
#include "PlanNodeVisitor.h"

namespace milvus::query {
void
FloatVectorANNS::accept(PlanNodeVisitor& visitor) {
    visitor.visit(*this);
}

void
BinaryVectorANNS::accept(PlanNodeVisitor& visitor) {
    visitor.visit(*this);
}

void
Float16VectorANNS::accept(PlanNodeVisitor& visitor) {
    visitor.visit(*this);
}

void
BFloat16VectorANNS::accept(PlanNodeVisitor& visitor) {
    visitor.visit(*this);
}

void
SparseFloatVectorANNS::accept(PlanNodeVisitor& visitor) {
    visitor.visit(*this);
}

void
RetrievePlanNode::accept(PlanNodeVisitor& visitor) {
    visitor.visit(*this);
}

}  // namespace milvus::query
