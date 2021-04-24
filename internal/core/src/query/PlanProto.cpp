#include "query/PlanProto.h"
#include "PlanNode.h"
#include "ExprImpl.h"
#include "pb/plan.pb.h"

namespace milvus::query {
namespace planpb = milvus::proto::plan;
ExprPtr
ExprFromProto(const Schema& schema, const planpb::Expr& expr_proto) {

    // TODO: make naive works
    Assert(expr_proto.has_range_expr());

    auto& range_expr = expr_proto.range_expr();
    auto field_id = FieldId(range_expr.field_id());
    auto field_offset = schema.get_offset(field_id);
    auto data_type = schema[field_offset].get_data_type();

    // auto& field_meta = schema[field_offset];
    ExprPtr result = [&]() {
        switch ((DataType)range_expr.data_type()) {
            case DataType::INT64: {
                auto result = std::make_unique<RangeExprImpl<int64_t>>();
                result->field_offset_ = field_offset;
                result->data_type_ = data_type;
                Assert(range_expr.ops_size() == range_expr.values_size());
                auto sz = range_expr.ops_size();
                // TODO simplify this
                for (int i = 0; i < sz; ++i) {
                    result->conditions_.emplace_back((RangeExpr::OpType)range_expr.ops(i),
                                                     range_expr.values(i).int64_val());
                }
                return result;
            }
            default: {
                PanicInfo("unsupported yet");
            }
        }
    }();
    return result;
}

PlanNodePtr
PlanNodeFromProto(const Schema& schema, const planpb::PlanNode& plan_node_proto) {
    // TODO: add more buffs
    Assert(plan_node_proto.has_vector_anns());
    auto& anns = plan_node_proto.vector_anns();
    AssertInfo(anns.is_binary() == false, "unimplemented");
    auto expr = ExprFromProto(schema, anns.predicates());
    planpb::QueryInfo query_info;
}

}  // namespace milvus::query
