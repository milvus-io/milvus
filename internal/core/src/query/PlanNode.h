#pragma once
#include <memory>
#include <vector>
#include <any>
#include <string>
#include <optional>
#include "Expr.h"
namespace milvus::query {
class PlanNodeVisitor;

enum class PlanNodeType {
    kInvalid = 0,
    kScan,
    kANNS,
};

// Base of all Nodes
struct PlanNode {
    PlanNodeType node_type;

 public:
    virtual ~PlanNode() = default;
    virtual void
    accept(PlanNodeVisitor&) = 0;
};

using PlanNodePtr = std::unique_ptr<PlanNode>;

struct VectorPlanNode : PlanNode {
    std::optional<PlanNodePtr> child_;
    int64_t num_queries_;
    int64_t dim_;
    FieldId field_id_;

 public:
    virtual void
    accept(PlanNodeVisitor&) = 0;
};

struct FloatVectorANNS : VectorPlanNode {
    std::shared_ptr<float> data;
    std::string metric_type_;  // TODO: use enum
 public:
    void
    accept(PlanNodeVisitor&) override;
};

struct BinaryVectorANNS : VectorPlanNode {
    std::shared_ptr<uint8_t> data;
    std::string metric_type_;  // TODO: use enum
 public:
    void
    accept(PlanNodeVisitor&) override;
};

}  // namespace milvus::query
