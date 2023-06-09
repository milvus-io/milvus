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
    schema->AddDebugField(
        "FloatVectorField", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("BinaryVectorField",
                          DataType::VECTOR_BINARY,
                          16,
                          knowhere::metric::JACCARD);
    schema->AddDebugField("Int64Field", DataType::INT64);
    schema->AddDebugField("Int32Field", DataType::INT32);
    schema->AddDebugField("Int16Field", DataType::INT16);
    schema->AddDebugField("Int8Field", DataType::INT8);
    schema->AddDebugField("DoubleField", DataType::DOUBLE);
    schema->AddDebugField("FloatField", DataType::FLOAT);
    return schema;
}

class PlanProtoTest : public ::testing::TestWithParam<std::tuple<std::string>> {
 public:
    PlanProtoTest() {
        schema = getStandardSchema();
    }

 protected:
    SchemaPtr schema;
};

INSTANTIATE_TEST_CASE_P(InstName,
                        PlanProtoTest,
                        ::testing::Values(                   //
                            std::make_tuple("DoubleField"),  //
                            std::make_tuple("FloatField"),   //
                            std::make_tuple("Int64Field"),   //
                            std::make_tuple("Int32Field"),   //
                            std::make_tuple("Int16Field"),   //
                            std::make_tuple("Int8Field")     //
                            ));

TEST_P(PlanProtoTest, Range) {
    // xxx.query(predicates = "int64field > 3", topk = 10, ...)
    FieldName vec_field_name = FieldName("FloatVectorField");
    FieldId vec_float_field_id = schema->get_field_id(vec_field_name);

    auto field_name = std::get<0>(GetParam());
    auto field_id = schema->get_field_id(FieldName(field_name));
    auto data_type = schema->operator[](field_id).get_data_type();
    auto data_type_str = spb::DataType_Name(int(data_type));

    string value_tag = "bool_val";
    if (datatype_is_floating(data_type)) {
        value_tag = "float_val";
    } else if (datatype_is_integer(data_type)) {
        value_tag = "int64_val";
    }

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: %1%
  predicates: <
    unary_range_expr: <
      column_info: <
        field_id: %2%
        data_type: %3%
      >
      op: GreaterThan
      value: <
        %4%: 3
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
)") % vec_float_field_id.get() %
                field_id.get() % data_type_str % value_tag;

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
    FieldName vec_field_name = FieldName("FloatVectorField");
    FieldId vec_float_field_id = schema->get_field_id(vec_field_name);

    auto field_name = std::get<0>(GetParam());
    auto field_id = schema->get_field_id(FieldName(field_name));
    auto data_type = schema->operator[](field_id).get_data_type();
    auto data_type_str = spb::DataType_Name(int(data_type));

    string value_tag = "bool_val";
    if (datatype_is_floating(data_type)) {
        value_tag = "float_val";
    } else if (datatype_is_integer(data_type)) {
        value_tag = "int64_val";
    }

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: %1%
  predicates: <
    term_expr: <
      column_info: <
        field_id: %2%
        data_type: %3%
      >
      values: <
        %4%: 1
      >
      values: <
        %4%: 2
      >
      values: <
        %4%: 3
      >
      is_in_field : false
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
)") % vec_float_field_id.get() %
                field_id.get() % data_type_str % value_tag;

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
                        "values": [1,2,3],
                        "is_in_field" : false
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
    FieldName vec_field_name = FieldName("FloatVectorField");
    FieldId vec_float_field_id = schema->get_field_id(vec_field_name);

    FieldName int64_field_name = FieldName("Int64Field");
    FieldId int64_field_id = schema->get_field_id(int64_field_name);
    string value_tag = "int64_val";

    auto data_type = spb::DataType::Int64;
    auto data_type_str = spb::DataType_Name(int(data_type));

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: %1%
  predicates: <
    unary_expr: <
      op: Not
      child: <
        unary_range_expr: <
          column_info: <
            field_id: %2%
            data_type: %3%
          >
          op: GreaterThan
          value: <
            %4%: 3
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
)") % vec_float_field_id.get() %
                int64_field_id.get() % data_type_str % value_tag;

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
)") % int64_field_name.get());

    auto ref_plan = CreatePlan(*schema, dsl_text);
    auto ref_json = ShowPlanNodeVisitor().call_child(*ref_plan->plan_node_);
    EXPECT_EQ(json.dump(2), ref_json.dump(2));
    plan->check_identical(*ref_plan);
}

TEST(PlanProtoTest, AndOrExpr) {
    auto schema = getStandardSchema();
    // xxx.query(predicates = "(int64field < 3) && (int64field > 2 || int64field == 1)", topk = 10, ...)
    FieldName vec_field_name = FieldName("FloatVectorField");
    FieldId vec_float_field_id = schema->get_field_id(vec_field_name);

    FieldName int64_field_name = FieldName("Int64Field");
    FieldId int64_field_id = schema->get_field_id(int64_field_name);
    string value_tag = "int64_val";

    auto data_type = spb::DataType::Int64;
    auto data_type_str = spb::DataType_Name(int(data_type));

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: %1%
  predicates: <
    binary_expr: <
      op: LogicalAnd
      left: <
        unary_range_expr: <
          column_info: <
            field_id: %2%
            data_type: %3%
          >
          op: LessThan
          value: <
            %4%: 3
          >
        >
      >
      right: <
        binary_expr: <
          op: LogicalOr
          left: <
            unary_range_expr: <
              column_info: <
                field_id: %2%
                data_type: %3%
              >
              op: GreaterThan
              value: <
                %4%: 2
              >
            >
          >
          right: <
            unary_range_expr: <
              column_info: <
                field_id: %2%
                data_type: %3%
              >
              op: Equal
              value: <
                %4%: 1
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
)") % vec_float_field_id.get() %
                int64_field_id.get() % data_type_str % value_tag;

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
)") % int64_field_name.get());

    auto ref_plan = CreatePlan(*schema, dsl_text);
    auto ref_json = ShowPlanNodeVisitor().call_child(*ref_plan->plan_node_);
    EXPECT_EQ(json.dump(2), ref_json.dump(2));
    plan->check_identical(*ref_plan);
}

TEST_P(PlanProtoTest, CompareExpr) {
    auto schema = getStandardSchema();
    auto age_fid = schema->AddDebugField("age1", DataType::INT64);
    // xxx.query(predicates = "int64field < int64field", topk = 10, ...)

    FieldName vec_field_name = FieldName("FloatVectorField");
    FieldId vec_float_field_id = schema->get_field_id(vec_field_name);

    auto field_name = std::get<0>(GetParam());
    auto field_id = schema->get_field_id(FieldName(field_name));
    auto data_type = schema->operator[](field_id).get_data_type();
    auto data_type_str = spb::DataType_Name(int(data_type));

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: %1%
  predicates: <
    compare_expr: <
      left_column_info: <
        field_id: %2%
        data_type: Int64
      >
      right_column_info: <
        field_id: %3%
        data_type: %4%
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
)") % vec_float_field_id.get() %
                age_fid.get() % field_id.get() % data_type_str;

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

TEST_P(PlanProtoTest, BinaryArithOpEvalRange) {
    // xxx.query(predicates = "int64field > 3", topk = 10, ...)
    //    auto data_type = std::get<0>(GetParam());
    //    auto data_type_str = spb::DataType_Name(data_type);
    //    auto field_id = 100 + (int)data_type;
    //    auto field_name = data_type_str + "Field";
    //    string value_tag = "bool_val";
    //    if (datatype_is_floating((DataType)data_type)) {
    //        value_tag = "float_val";
    //    } else if (datatype_is_integer((DataType)data_type)) {
    //        value_tag = "int64_val";
    //    }

    FieldName vec_field_name = FieldName("FloatVectorField");
    FieldId vec_float_field_id = schema->get_field_id(vec_field_name);

    auto field_name = std::get<0>(GetParam());
    auto field_id = schema->get_field_id(FieldName(field_name));
    auto data_type = schema->operator[](field_id).get_data_type();
    auto data_type_str = spb::DataType_Name(int(data_type));

    string value_tag = "bool_val";
    if (datatype_is_floating(data_type)) {
        value_tag = "float_val";
    } else if (datatype_is_integer(data_type)) {
        value_tag = "int64_val";
    }

    auto fmt1 = boost::format(R"(
vector_anns: <
  field_id: %1%
  predicates: <
    binary_arith_op_eval_range_expr: <
      column_info: <
        field_id: %2%
        data_type: %3%
      >
      arith_op: Add
      right_operand: <
        %4%: 1029
      >
      op: Equal
      value: <
        %4%: 2016
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
)") % vec_float_field_id.get() %
                field_id.get() % data_type_str % value_tag;

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
                        "EQ": {
                          "ADD": {
                            "right_operand": 1029,
                            "value": 2016
                          }
                        }
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

TEST(PlanProtoTest, Predicates) {
    auto schema = getStandardSchema();
    auto age_fid = schema->AddDebugField("age1", DataType::INT64);

    planpb::PlanNode plan_node_proto;
    auto expr =
        plan_node_proto.mutable_predicates()->mutable_unary_range_expr();
    expr->set_op(planpb::Equal);
    auto column_info = expr->mutable_column_info();
    column_info->set_data_type(proto::schema::DataType::Int64);
    column_info->set_field_id(age_fid.get());
    auto value = expr->mutable_value();
    value->set_int64_val(1000);

    std::string binary;
    plan_node_proto.SerializeToString(&binary);

    auto plan =
        CreateRetrievePlanByExpr(*schema, binary.c_str(), binary.size());
    ASSERT_TRUE(plan->plan_node_->predicate_.has_value());
    ASSERT_FALSE(plan->plan_node_->is_count);
}
