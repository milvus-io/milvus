#include <gtest/gtest.h>
#include <google/protobuf/text_format.h>
#include "query/PlanProto.h"
#include "pb/plan.pb.h"
#include "query/generated/ShowPlanNodeVisitor.h"
#include <vector>
#include <queue>
#include <random>

using namespace milvus;
using namespace milvus::query;
namespace planpb = proto::plan;


TEST(PlanProto, Naive) {
    auto schema = std::make_unique<Schema>();
    schema->AddField(FieldName("vectorfield"), FieldId(101), DataType::VECTOR_FLOAT, 16, MetricType::METRIC_L2);
    schema->AddField(FieldName("int64field"), FieldId(100), DataType::INT64);
    std::string proto_text = R"(
vector_anns: <
  predicates: <
    range_expr: <
      field_id: 100
      data_type: Int64
      ops: GreaterThan
      values: <
        int64_val: 3
      >
    >
  >
  query_info: <
    topk: 10
    field_id: 101
    metric_type: "L2"
    search_params: "{\"nprobe\": 10}"
  >
  placeholder_tag: "$0"
>
)";
    planpb::PlanNode node_proto;
    google::protobuf::TextFormat::ParseFromString(proto_text, &node_proto);
    std::cout << node_proto.DebugString();

    auto plan_node = PlanNodeFromProto(*schema, node_proto);


    SUCCEED();
}
