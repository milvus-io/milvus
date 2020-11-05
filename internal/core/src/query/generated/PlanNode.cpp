#pragma once
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

}  // namespace milvus::query
