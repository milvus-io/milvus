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

#include <boost/format.hpp>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <queue>
#include <random>
#include <vector>

#include "pb/plan.pb.h"
#include "query/PlanProto.h"
#include "query/generated/ShowPlanNodeVisitor.h"

using namespace milvus;
using namespace milvus::query;
namespace planpb = proto::plan;
using std::string;

namespace spb = proto::schema;
static SchemaPtr
getStandardSchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddField(FieldName("FloatVectorField"), FieldId(100 + spb::DataType::FloatVector), DataType::VECTOR_FLOAT,
                     16, MetricType::METRIC_L2);
    schema->AddField(FieldName("BinaryVectorField"), FieldId(100 + spb::DataType::BinaryVector),
                     DataType::VECTOR_BINARY, 16, MetricType::METRIC_Jaccard);
    schema->AddField(FieldName("Int64Field"), FieldId(100 + spb::DataType::Int64), DataType::INT64);
    schema->AddField(FieldName("Int32Field"), FieldId(100 + spb::DataType::Int32), DataType::INT32);
    schema->AddField(FieldName("Int16Field"), FieldId(100 + spb::DataType::Int16), DataType::INT16);
    schema->AddField(FieldName("Int8Field"), FieldId(100 + spb::DataType::Int8), DataType::INT8);
    schema->AddField(FieldName("DoubleField"), FieldId(100 + spb::DataType::Double), DataType::DOUBLE);
    schema->AddField(FieldName("FloatField"), FieldId(100 + spb::DataType::Float), DataType::FLOAT);
    return schema;
}

class PlanProtoTest : public ::testing::TestWithParam<std::tuple<spb::DataType>> {
 public:
    PlanProtoTest() {
        schema = getStandardSchema();
    }

 protected:
    SchemaPtr schema;
};

INSTANTIATE_TEST_CASE_P(InstName,
                        PlanProtoTest,
                        ::testing::Values(                           //
                            std::make_tuple(spb::DataType::Double),  //
                            std::make_tuple(spb::DataType::Float),   //
                            std::make_tuple(spb::DataType::Int64),   //
                            std::make_tuple(spb::DataType::Int32),   //
                            std::make_tuple(spb::DataType::Int16),   //
                            std::make_tuple(spb::DataType::Int8)     //
                            ));

TEST_P(PlanProtoTest, Range) {
    // xxx.query(predicates = "int64field > 3", topk = 10, ...)
    auto data_type = std::get<0>(GetParam());
    auto data_type_str = spb::DataType_Name(data_type);
    auto field_id = 100 + (int)data_type;
    auto field_name = data_type_str + "Field";
    string value_tag = "bool_val";
    if (datatype_is_floating((DataType)data_type)) {
        value_tag = "float_val";
    } else if (datatype_is_integer((DataType)data_type)) {
        value_tag = "int64_val";
    }

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: 201
  predicates: <
    unary_range_expr: <
      column_info: <
        field_id: %1%
        data_type: %2%
      >
      op: GreaterThan
      value: <
        %3%: 3
      >
    >
  >
  query_info: <
    topk: 10
    round_decimal: 3
    metric_type: "L2"
    search_params: "{\"nprobe\": 10}"
  >
  placeholder_tag: "$0"
>
)") % field_id % data_type_str %
                value_tag;

    auto proto_text = fmt1.str();
    planpb::PlanNode node_proto;
    google::protobuf::TextFormat::ParseFromString(proto_text, &node_proto);
    // std::cout << node_proto.DebugString();
    auto plan = ProtoParser(*schema).CreatePlan(node_proto);

    ShowPlanNodeVisitor visitor;
    auto json = visitor.call_child(*plan->plan_node_);
    // std::cout << json.dump(2);
    auto extra_info = plan->extra_info_opt_.value();

    std::string dsl_text = boost::str(boost::format(R"(
{
    "bool": {
        "must": [
            {
                "range": {
                    "%1%": {
                        "GT": 3
                    }
                }
            },
            {
                "vector": {
                    "FloatVectorField": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10,
                        "round_decimal": 3
                    }
                }
            }
        ]
    }
}
)") % field_name);

    auto ref_plan = CreatePlan(*schema, dsl_text);
    plan->check_identical(*ref_plan);
}

TEST_P(PlanProtoTest, TermExpr) {
    // xxx.query(predicates = "int64field in [1, 2, 3]", topk = 10, ...)
    auto data_type = std::get<0>(GetParam());
    auto data_type_str = spb::DataType_Name(data_type);
    auto field_id = 100 + (int)data_type;
    auto field_name = data_type_str + "Field";
    string value_tag = "bool_val";
    if (datatype_is_floating((DataType)data_type)) {
        value_tag = "float_val";
    } else if (datatype_is_integer((DataType)data_type)) {
        value_tag = "int64_val";
    }

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: 201
  predicates: <
    term_expr: <
      column_info: <
        field_id: %1%
        data_type: %2%
      >
      values: <
        %3%: 1
      >
      values: <
        %3%: 2
      >
      values: <
        %3%: 3
      >
    >
  >
  query_info: <
    topk: 10
    round_decimal: 3
    metric_type: "L2"
    search_params: "{\"nprobe\": 10}"
  >
  placeholder_tag: "$0"
>
)") % field_id % data_type_str %
                value_tag;

    auto proto_text = fmt1.str();
    planpb::PlanNode node_proto;
    google::protobuf::TextFormat::ParseFromString(proto_text, &node_proto);
    // std::cout << node_proto.DebugString();
    auto plan = ProtoParser(*schema).CreatePlan(node_proto);

    ShowPlanNodeVisitor visitor;
    auto json = visitor.call_child(*plan->plan_node_);
    // std::cout << json.dump(2);
    auto extra_info = plan->extra_info_opt_.value();

    std::string dsl_text = boost::str(boost::format(R"(
{
    "bool": {
        "must": [
            {
                "term": {
                    "%1%": {
                        "values": [1,2,3]
                    }
                }
            },
            {
                "vector": {
                    "FloatVectorField": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10,
                        "round_decimal": 3
                    }
                }
            }
        ]
    }
}
)") % field_name);

    auto ref_plan = CreatePlan(*schema, dsl_text);
    plan->check_identical(*ref_plan);
}

TEST(PlanProtoTest, NotExpr) {
    auto schema = getStandardSchema();
    // xxx.query(predicates = "not (int64field > 3)", topk = 10, ...)
    auto data_type = spb::DataType::Int64;
    auto data_type_str = spb::DataType_Name(data_type);
    auto field_id = 100 + (int)data_type;
    auto field_name = data_type_str + "Field";
    string value_tag = "bool_val";
    if (datatype_is_floating((DataType)data_type)) {
        value_tag = "float_val";
    } else if (datatype_is_integer((DataType)data_type)) {
        value_tag = "int64_val";
    }

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: 201
  predicates: <
    unary_expr: <
      op: Not
      child: <
        unary_range_expr: <
          column_info: <
            field_id: %1%
            data_type: %2%
          >
          op: GreaterThan
          value: <
            %3%: 3
          >
        >
      >
    >
  >
  query_info: <
    topk: 10
    round_decimal: 3
    metric_type: "L2"
    search_params: "{\"nprobe\": 10}"
  >
  placeholder_tag: "$0"
>
)") % field_id % data_type_str %
                value_tag;

    auto proto_text = fmt1.str();
    planpb::PlanNode node_proto;
    google::protobuf::TextFormat::ParseFromString(proto_text, &node_proto);
    // std::cout << node_proto.DebugString();
    auto plan = ProtoParser(*schema).CreatePlan(node_proto);

    ShowPlanNodeVisitor visitor;
    auto json = visitor.call_child(*plan->plan_node_);
    // std::cout << json.dump(2);
    auto extra_info = plan->extra_info_opt_.value();

    std::string dsl_text = boost::str(boost::format(R"(
{
    "bool": {
        "must": [
            {
                "must_not": [{
                    "range": {
                        "%1%": {
                            "GT": 3
                        }
                    }
                }]
            },
            {
                "vector": {
                    "FloatVectorField": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10,
                        "round_decimal": 3
                    }
                }
            }
        ]
    }
}
)") % field_name);

    auto ref_plan = CreatePlan(*schema, dsl_text);
    auto ref_json = ShowPlanNodeVisitor().call_child(*ref_plan->plan_node_);
    EXPECT_EQ(json.dump(2), ref_json.dump(2));
    plan->check_identical(*ref_plan);
}

TEST(PlanProtoTest, AndOrExpr) {
    auto schema = getStandardSchema();
    // xxx.query(predicates = "(int64field < 3) && (int64field > 2 || int64field == 1)", topk = 10, ...)
    auto data_type = spb::DataType::Int64;
    auto data_type_str = spb::DataType_Name(data_type);
    auto field_id = 100 + (int)data_type;
    auto field_name = data_type_str + "Field";
    string value_tag = "bool_val";
    if (datatype_is_floating((DataType)data_type)) {
        value_tag = "float_val";
    } else if (datatype_is_integer((DataType)data_type)) {
        value_tag = "int64_val";
    }

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: 201
  predicates: <
    binary_expr: <
      op: LogicalAnd
      left: <
        unary_range_expr: <
          column_info: <
            field_id: 105
            data_type: Int64
          >
          op: LessThan
          value: <
            int64_val: 3
          >
        >
      >
      right: <
        binary_expr: <
          op: LogicalOr
          left: <
            unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Int64
              >
              op: GreaterThan
              value: <
                int64_val: 2
              >
            >
          >
          right: <
            unary_range_expr: <
              column_info: <
                field_id: 105
                data_type: Int64
              >
              op: Equal
              value: <
                int64_val: 1
              >
            >
          >
        >
      >
    >
  >
  query_info: <
    topk: 10
    round_decimal: 3
    metric_type: "L2"
    search_params: "{\"nprobe\": 10}"
  >
  placeholder_tag: "$0"
>
)");

    auto proto_text = fmt1.str();
    planpb::PlanNode node_proto;
    google::protobuf::TextFormat::ParseFromString(proto_text, &node_proto);
    // std::cout << node_proto.DebugString();
    auto plan = ProtoParser(*schema).CreatePlan(node_proto);

    ShowPlanNodeVisitor visitor;
    auto json = visitor.call_child(*plan->plan_node_);
    // std::cout << json.dump(2);
    auto extra_info = plan->extra_info_opt_.value();

    std::string dsl_text = boost::str(boost::format(R"(
{
    "bool": {
        "must": [
            {
                "range": {
                    "%1%": {
                        "LT": 3
                    }
                }
            },

            {
                "should": [
                {
                    "range": {
                        "%1%": {
                            "GT": 2
                        }
                    }
                },
                {
                    "range": {
                        "%1%": {
                            "EQ": 1
                        }
                    }
                }
                ]
            },
            {
                "vector": {
                    "FloatVectorField": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10,
                        "round_decimal": 3
                    }
                }
            }
        ]
    }
}
)") % field_name);

    auto ref_plan = CreatePlan(*schema, dsl_text);
    auto ref_json = ShowPlanNodeVisitor().call_child(*ref_plan->plan_node_);
    EXPECT_EQ(json.dump(2), ref_json.dump(2));
    plan->check_identical(*ref_plan);
}

TEST_P(PlanProtoTest, CompareExpr) {
    auto schema = getStandardSchema();
    schema->AddField(FieldName("age1"), FieldId(128), DataType::INT64);
    // xxx.query(predicates = "int64field < int64field", topk = 10, ...)
    auto data_type = std::get<0>(GetParam());
    auto field_id = 100 + (int)data_type;
    auto data_type_str = spb::DataType_Name(data_type);
    auto field_name = data_type_str + "Field";

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: 201
  predicates: <
    compare_expr: <
      left_column_info: <
        field_id: 128
        data_type: Int64
      >
      right_column_info: <
        field_id: %1%
        data_type: %2%
      >
      op: LessThan
    >
  >
  query_info: <
    topk: 10
    round_decimal: 3
    metric_type: "L2"
    search_params: "{\"nprobe\": 10}"
  >
  placeholder_tag: "$0"
>
)") % field_id % data_type_str;

    auto proto_text = fmt1.str();
    planpb::PlanNode node_proto;
    google::protobuf::TextFormat::ParseFromString(proto_text, &node_proto);
    // std::cout << node_proto.DebugString();
    auto plan = ProtoParser(*schema).CreatePlan(node_proto);

    ShowPlanNodeVisitor visitor;
    auto json = visitor.call_child(*plan->plan_node_);
    // std::cout << json.dump(2);
    auto extra_info = plan->extra_info_opt_.value();

    std::string dsl_text = boost::str(boost::format(R"(
{
    "bool": {
        "must": [
            {
                "compare": {
                    "LT": [
                        "age1",
                        "%1%"
                    ]
                }
            },
            {
                "vector": {
                    "FloatVectorField": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 10,
                        "round_decimal": 3
                    }
                }
            }
        ]
    }
}
)") % field_name);

    auto ref_plan = CreatePlan(*schema, dsl_text);
    plan->check_identical(*ref_plan);
}