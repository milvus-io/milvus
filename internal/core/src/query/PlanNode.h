#pragma once
#include <memory>
#include <vector>
#include <any>
#include <string>
#include <optional>
#include "Expr.h"
#include "utils/Json.h"
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

struct QueryInfo {
    int64_t topK_;
    FieldId field_id_;
    std::string metric_type_;  // TODO: use enum
    nlohmann::json search_params_;
};

struct VectorPlanNode : PlanNode {
    std::optional<ExprPtr> predicate_;
    QueryInfo query_info_;
    std::string placeholder_tag_;

 public:
    virtual void
    accept(PlanNodeVisitor&) = 0;
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

}  // namespace milvus::query
