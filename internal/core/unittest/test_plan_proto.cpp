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
  field_id: 101
  predicates: <
    range_expr: <
      column_info: <
        field_id: 100
        data_type: Int64
      >
      ops: GreaterThan
      values: <
        int64_val: 3
      >
    >
  >
  query_info: <
    topk: 10
    metric_type: "L2"
    search_params: "{\"nprobe\": 10}"
  >
  placeholder_tag: "$0"
>
)";
    planpb::PlanNode node_proto;
    google::protobuf::TextFormat::ParseFromString(proto_text, &node_proto);
    std::cout << node_proto.DebugString();
    auto plan = ProtoParser(*schema).CreatePlan(node_proto);

    ShowPlanNodeVisitor visitor;
    auto json = visitor.call_child(*plan->plan_node_);
    std::cout << json.dump(2);
    auto extra_info = plan->extra_info_opt_.value();

    std::string dsl_text = R"(
{
    "bool": {
        "must": [
            {
                "range": {
                    "int64field": {
                        "GT": 3
                    }
                }
            },
            {
                "vector": {
                    "vectorfield": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10
                    }
                }
            }
        ]
    }
}
)";
    auto ref_plan = CreatePlan(*schema, dsl_text);
    plan->check_identical(*ref_plan);
}
