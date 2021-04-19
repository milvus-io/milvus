#pragma once
// Generated File
// DO NOT EDIT
#include "PlanNodeVisitor.h"
namespace milvus::query {
class ExecPlanNodeVisitor : PlanNodeVisitor {
 public:
    virtual void
    visit(FloatVectorANNS& node) override;

    virtual void
    visit(BinaryVectorANNS& node) override;

 public:
};
}  // namespace milvus::query
