#pragma once
#include <memory>
#include <vector>
#include <any>
#include <string>
#include <optional>
#include "Predicate.h"
namespace milvus::query {
class QueryNodeVisitor;

enum class QueryNodeType {
    kInvalid = 0, 
    kScan,  
    kANNS,
};

// Base of all Nodes
struct QueryNode {
    QueryNodeType node_type;
 public:
    virtual ~QueryNode() = default;
    virtual void
    accept(QueryNodeVisitor&) = 0;
};

using QueryNodePtr = std::unique_ptr<QueryNode>;


struct VectorQueryNode : QueryNode {
    std::optional<QueryNodePtr> child_;
    int64_t num_queries_;
    int64_t dim_;
    FieldId field_id_;
 public:
    virtual void
    accept(QueryNodeVisitor&) = 0;
};

struct FloatVectorANNS: VectorQueryNode {
    std::shared_ptr<float> data;
    std::string metric_type_; // TODO: use enum
 public:
    void
    accept(QueryNodeVisitor&) override;
};

struct BinaryVectorANNS: VectorQueryNode {
    std::shared_ptr<uint8_t> data;
    std::string metric_type_; // TODO: use enum
 public:
    void
    accept(QueryNodeVisitor&) override;
};

}  // namespace milvus::query
