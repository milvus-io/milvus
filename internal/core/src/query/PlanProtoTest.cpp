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
#include <memory>
#include <string>

#include "common/Schema.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "query/PlanProto.h"

namespace {

milvus::SchemaPtr
BuildSchema() {
    auto schema = std::make_shared<milvus::Schema>();
    schema->AddDebugField(
        "fakevec", milvus::DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", milvus::DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    return schema;
}

milvus::proto::plan::PlanNode
BuildSearchPlanNode(float search_topk_ratio, float refine_topk_ratio) {
    milvus::proto::plan::PlanNode plan_node;
    auto* vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);

    auto* query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(-1);
    query_info->set_metric_type(knowhere::metric::L2);
    query_info->set_search_params("{}");
    query_info->set_search_topk_ratio(search_topk_ratio);
    query_info->set_refine_topk_ratio(refine_topk_ratio);

    return plan_node;
}

}  // namespace

TEST(PlanProto, NotSetUnsupported) {
    using namespace milvus;
    using namespace milvus::query;
    auto schema = BuildSchema();

    proto::plan::Expr expr_pb;
    ProtoParser parser(schema);
    ASSERT_ANY_THROW(parser.ParseExprs(expr_pb));
}

TEST(PlanProto, RejectsGlobalRefineRatiosBelowOne) {
    using namespace milvus::query;

    ProtoParser parser(BuildSchema());

    EXPECT_ANY_THROW(parser.PlanNodeFromProto(BuildSearchPlanNode(0.5f, 1.5f)));
    EXPECT_ANY_THROW(parser.PlanNodeFromProto(BuildSearchPlanNode(1.5f, 0.5f)));
}
