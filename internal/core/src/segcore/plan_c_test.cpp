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

#include "knowhere/comp/index_param.h"
#include "segcore/plan_c.h"
#include "test_utils/c_api_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

template <class TraitType>
void
Test_CPlan(const knowhere::MetricType& metric_type) {
    std::string schema_string =
        generate_collection_schema<TraitType>(knowhere::metric::JACCARD, DIM);
    auto collection = NewCollection(schema_string.c_str());

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(TraitType::vector_type);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    void* plan = nullptr;
    auto status = CreateSearchPlanByExpr(
        collection, plan_str.data(), plan_str.size(), &plan);
    ASSERT_EQ(status.error_code, Success);

    int64_t field_id = -1;
    status = GetFieldID(plan, &field_id);
    ASSERT_EQ(status.error_code, Success);

    auto col = static_cast<Collection*>(collection);
    for (auto& [target_field_id, field_meta] :
         col->get_schema()->get_fields()) {
        if (field_meta.is_vector()) {
            ASSERT_EQ(field_id, target_field_id.get());
        }
    }
    ASSERT_NE(field_id, -1);

    DeleteSearchPlan(plan);
    DeleteCollection(collection);
}

TEST(CApiTest, CPlan) {
    Test_CPlan<milvus::BinaryVector>(knowhere::metric::JACCARD);
    Test_CPlan<milvus::FloatVector>(knowhere::metric::L2);
    Test_CPlan<milvus::Float16Vector>(knowhere::metric::L2);
    Test_CPlan<milvus::BFloat16Vector>(knowhere::metric::L2);
    Test_CPlan<milvus::Int8Vector>(knowhere::metric::L2);
}