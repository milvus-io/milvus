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
#include <type_traits>
#include <unordered_map>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "query/PlanImpl.h"
#include "segcore/Collection.h"
#include "segcore/plan_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/indexbuilder_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

std::string
encode_segment_pk_hint_list(
    const std::vector<milvus::proto::plan::SegmentPkHint>& hints) {
    std::string buffer;
    google::protobuf::io::StringOutputStream output_stream(&buffer);
    google::protobuf::io::CodedOutputStream output(&output_stream);
    for (const auto& hint : hints) {
        auto data = hint.SerializeAsString();
        output.WriteTag((1 << 3) | 2);  // field 1, wire type length-delimited
        output.WriteVarint32(static_cast<uint32_t>(data.size()));
        output.WriteRaw(data.data(), data.size());
    }
    return buffer;
}

}  // namespace

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
        collection, plan_str.data(), plan_str.size(), nullptr, 0, &plan);
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

TEST(CApiTest, CPlanWithSegmentHints) {
    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::FloatVector::vector_type);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(100);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(3);
    query_info->set_metric_type("L2");
    query_info->set_search_params(R"({"nprobe": 10})");
    auto plan_str = plan_node.SerializeAsString();

    milvus::proto::plan::SegmentPkHint hint_1;
    hint_1.set_segment_id(1001);
    hint_1.add_filtered_pk_values()->set_int64_val(11);
    hint_1.add_filtered_pk_values()->set_int64_val(22);

    milvus::proto::plan::SegmentPkHint hint_2;
    hint_2.set_segment_id(1002);

    auto hints_data = encode_segment_pk_hint_list({hint_1, hint_2});

    void* plan = nullptr;
    auto status = CreateSearchPlanByExpr(collection,
                                         plan_str.data(),
                                         plan_str.size(),
                                         hints_data.data(),
                                         hints_data.size(),
                                         &plan);
    ASSERT_EQ(status.error_code, Success);

    auto* parsed_plan = static_cast<milvus::query::Plan*>(plan);
    ASSERT_EQ(parsed_plan->segment_hints_.size(), 2);
    ASSERT_EQ(parsed_plan->segment_hints_[1001].size(), 2);
    ASSERT_EQ(parsed_plan->segment_hints_[1001][0].int64_val(), 11);
    ASSERT_EQ(parsed_plan->segment_hints_[1001][1].int64_val(), 22);
    ASSERT_NE(parsed_plan->segment_hints_.find(1002),
              parsed_plan->segment_hints_.end());
    ASSERT_TRUE(parsed_plan->segment_hints_[1002].empty());

    DeleteSearchPlan(plan);
    DeleteCollection(collection);
}

TEST(CApiTest, CRetrievePlanWithSegmentHints) {
    std::string schema_string = generate_collection_schema<milvus::FloatVector>(
        knowhere::metric::L2, DIM);
    auto collection = NewCollection(schema_string.c_str());

    milvus::proto::plan::PlanNode plan_node;
    auto query = plan_node.mutable_query();
    query->set_limit(100);
    auto term_expr = query->mutable_predicates()->mutable_term_expr();
    auto column_info = term_expr->mutable_column_info();
    column_info->set_field_id(101);
    column_info->set_data_type(milvus::proto::schema::DataType::Int64);
    column_info->set_is_primary_key(true);
    term_expr->add_values()->set_int64_val(11);
    auto plan_str = plan_node.SerializeAsString();

    milvus::proto::plan::SegmentPkHint hint;
    hint.set_segment_id(2001);
    hint.add_filtered_pk_values()->set_int64_val(11);
    auto hints_data = encode_segment_pk_hint_list({hint});

    void* plan = nullptr;
    auto status = CreateRetrievePlanByExpr(collection,
                                           plan_str.data(),
                                           plan_str.size(),
                                           hints_data.data(),
                                           hints_data.size(),
                                           &plan);
    ASSERT_EQ(status.error_code, Success);

    auto* parsed_plan = static_cast<milvus::query::RetrievePlan*>(plan);
    ASSERT_EQ(parsed_plan->segment_hints_.size(), 1);
    ASSERT_EQ(parsed_plan->segment_hints_[2001].size(), 1);
    ASSERT_EQ(parsed_plan->segment_hints_[2001][0].int64_val(), 11);

    DeleteRetrievePlan(plan);
    DeleteCollection(collection);
}
