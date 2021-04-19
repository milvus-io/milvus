#pragma once
// Generated File
// DO NOT EDIT
#include "query/PlanNode.h"
namespace milvus::query {
class PlanNodeVisitor {
 public:
    virtual ~PlanNodeVisitor() = 0;

 public:
    virtual void
    visit(FloatVectorANNS&) = 0;

    virtual void
    visit(BinaryVectorANNS&) = 0;
};
}  // namespace milvus::query
