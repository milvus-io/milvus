#include "common/Schema.h"
#include "query/PlanNode.h"
#include "pb/plan.pb.h"

namespace milvus::query {

ExprPtr
ExprFromProto(const Schema& schema, const proto::plan::Expr& expr_proto);

PlanNodePtr
PlanNodeFromProto(const Schema& schema, const proto::plan::PlanNode& plan_node_proto);
}