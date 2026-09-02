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
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
// RoaringMembership.h only pulls in roaring.hh (32-bit Roaring); the 64-bit
// map used to build MRB1 test blobs needs its own header.
#include <roaring/roaring64map.hh>

#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/BloomFilterEnvelope.h"
#include "common/RoaringMembership.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/Plan.h"
#include "query/PlanProto.h"

namespace {

void
PutU16(std::string& out, size_t offset, uint16_t value) {
    out[offset] = static_cast<char>(value & 0xff);
    out[offset + 1] = static_cast<char>((value >> 8) & 0xff);
}

void
PutU32(std::string& out, size_t offset, uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        out[offset + i] = static_cast<char>((value >> (8 * i)) & 0xff);
    }
}

void
PutU64(std::string& out, size_t offset, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out[offset + i] = static_cast<char>((value >> (8 * i)) & 0xff);
    }
}

std::string
BuildEmptyMbf1() {
    std::string blob(milvus::bloom_envelope::kHeaderSize +
                         milvus::bloom_envelope::kBytesPerBlock,
                     '\0');
    std::memcpy(blob.data(), "MBF1", 4);
    PutU16(blob, 4, milvus::bloom_envelope::kVersion);
    PutU16(blob, 6, milvus::bloom_envelope::kAlgoParquetSbbfXxh64);
    PutU32(blob, 24, 1);
    return blob;
}

std::string
BuildMrb1(std::initializer_list<int64_t> values) {
    roaring::Roaring64Map bitmap;
    for (auto value : values) {
        bitmap.add(static_cast<uint64_t>(value));
    }
    bitmap.runOptimize();

    std::string body(bitmap.getSizeInBytes(true), '\0');
    EXPECT_EQ(bitmap.write(body.data(), true), body.size());

    std::string blob(milvus::RoaringMembership::kHeaderSize + body.size(),
                     '\0');
    std::memcpy(blob.data(),
                milvus::RoaringMembership::kMagic.data(),
                milvus::RoaringMembership::kMagic.size());
    PutU16(blob, 4, milvus::RoaringMembership::kVersion);
    PutU16(blob, 6, milvus::RoaringMembership::kFormatPortableRoaring64);
    PutU64(blob, 8, bitmap.cardinality());
    PutU64(blob, 16, body.size());
    std::memcpy(blob.data() + milvus::RoaringMembership::kHeaderSize,
                body.data(),
                body.size());
    return blob;
}

milvus::SchemaPtr
BuildSchema() {
    auto schema = std::make_shared<milvus::Schema>();
    schema->AddDebugField(
        "fakevec", milvus::DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", milvus::DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    schema->AddDebugField("score", milvus::DataType::INT64);
    return schema;
}

milvus::proto::plan::PlanNode
BuildSearchPlanNode(float search_topk_ratio,
                    float refine_topk_ratio,
                    milvus::FieldId vector_field_id) {
    milvus::proto::plan::PlanNode plan_node;
    auto* vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(vector_field_id.get());

    auto* query_info = vector_anns->mutable_query_info();
    query_info->set_topk(10);
    query_info->set_round_decimal(-1);
    query_info->set_metric_type(knowhere::metric::L2);
    query_info->set_search_params("{}");
    query_info->set_search_topk_ratio(search_topk_ratio);
    query_info->set_refine_topk_ratio(refine_topk_ratio);

    return plan_node;
}

std::shared_ptr<milvus::plan::FilterBitsNode>
FindFilterBitsNode(const std::shared_ptr<milvus::plan::PlanNode>& node) {
    if (node == nullptr) {
        return nullptr;
    }
    if (auto filter =
            std::dynamic_pointer_cast<milvus::plan::FilterBitsNode>(node)) {
        return filter;
    }
    for (const auto& source : node->sources()) {
        if (auto filter = FindFilterBitsNode(source)) {
            return filter;
        }
    }
    return nullptr;
}

}  // namespace

TEST(PlanProto, NotSetUnsupported) {
    using namespace milvus;
    using namespace milvus::query;
    auto schema = BuildSchema();

    proto::plan::Expr expr_pb;
    query::ProtoParser parser(schema);
    ASSERT_ANY_THROW(parser.ParseExprs(expr_pb));
}

TEST(PlanProto, DebugStringRedactsMembershipBlobs) {
    namespace planpb = milvus::proto::plan;
    using milvus::query::PlanProtoDebugString;

    const std::string roaring_secret = "MRB1-exact-member-set";
    const std::string bloom_secret = "MBF1-approximate-member-set";

    planpb::PlanNode plan_node;
    auto* query = plan_node.mutable_query();
    query->set_limit(10);
    query->mutable_predicates()->mutable_roaring_filter_expr()->set_bitmap_blob(
        roaring_secret);
    plan_node.add_scorers()
        ->mutable_filter()
        ->mutable_bloom_filter_expr()
        ->set_filter_blob(bloom_secret);

    const auto debug = PlanProtoDebugString(plan_node);
    EXPECT_EQ(debug.find(roaring_secret), std::string::npos);
    EXPECT_EQ(debug.find(bloom_secret), std::string::npos);
    EXPECT_NE(debug.find("bytes elided"), std::string::npos);
    EXPECT_EQ(
        plan_node.query().predicates().roaring_filter_expr().bitmap_blob(),
        roaring_secret);
    EXPECT_EQ(plan_node.scorers(0).filter().bloom_filter_expr().filter_blob(),
              bloom_secret);

    planpb::PlanNode oversized;
    oversized.mutable_query()
        ->mutable_predicates()
        ->mutable_roaring_filter_expr()
        ->set_bitmap_blob("MRB1" + std::string(5000, 'x'));
    const auto oversized_debug = PlanProtoDebugString(oversized);
    EXPECT_EQ(oversized_debug.find("MRB1"), std::string::npos);
    EXPECT_NE(oversized_debug.find("bytes, elided"), std::string::npos);
}

// A node whose descriptor pool predates a membership field keeps the blob in
// the UnknownFieldSet, where per-field redaction cannot reach it and
// ShortDebugString() would print it as raw bytes. Simulated by appending a
// field number this build does not know to a serialized plan.
TEST(PlanProto, DebugStringDropsUnknownFieldBlobs) {
    namespace planpb = milvus::proto::plan;
    using milvus::query::PlanProtoDebugString;

    const std::string secret = "MRB1-secret-member-bytes";

    planpb::PlanNode plan_node;
    plan_node.mutable_query()->set_limit(10);
    std::string wire;
    ASSERT_TRUE(plan_node.SerializeToString(&wire));

    // Field 4095, length-delimited (wire type 2), carrying the blob. No such
    // field exists in PlanNode, so it parses into the UnknownFieldSet exactly
    // as a newer peer's field would on an older node.
    {
        google::protobuf::io::StringOutputStream out(&wire);
        google::protobuf::io::CodedOutputStream coded(&out);
        coded.WriteVarint32((4095u << 3) | 2u);
        coded.WriteVarint32(static_cast<uint32_t>(secret.size()));
        coded.WriteString(secret);
    }

    planpb::PlanNode unaware;
    ASSERT_TRUE(unaware.ParseFromString(wire));
    ASSERT_GT(unaware.GetReflection()->GetUnknownFields(unaware).field_count(),
              0)
        << "precondition: the blob must land in unknown fields";
    ASSERT_NE(unaware.ShortDebugString().find("MRB1"), std::string::npos)
        << "precondition: an unredacted dump would leak the blob";

    const auto debug = PlanProtoDebugString(unaware);
    EXPECT_EQ(debug.find(secret), std::string::npos);
    EXPECT_EQ(debug.find("MRB1"), std::string::npos);
    // The content is gone but the fact is kept: a version skew is exactly what
    // someone reading this log line needs to see.
    EXPECT_NE(debug.find("1 unknown fields"), std::string::npos) << debug;
    EXPECT_NE(debug.find("bytes elided>"), std::string::npos) << debug;

    // The caller's plan is untouched; only the rendered copy is scrubbed.
    EXPECT_GT(unaware.GetReflection()->GetUnknownFields(unaware).field_count(),
              0);
}

// The membership branches used to return right after eliding the blob, so a
// nested message under them -- column_info -- was never walked and kept its
// unknown fields. Exercises a deeper path than the root-level test above.
TEST(PlanProto, DebugStringDropsUnknownFieldsNestedUnderMembershipExpr) {
    namespace planpb = milvus::proto::plan;
    using milvus::query::PlanProtoDebugString;

    const std::string secret = "future-sensitive-secret";

    planpb::PlanNode plan_node;
    plan_node.mutable_query()->set_limit(10);
    auto* roaring = plan_node.mutable_query()
                        ->mutable_predicates()
                        ->mutable_roaring_filter_expr();
    roaring->set_bitmap_blob("MRB1-blob-bytes");
    roaring->mutable_column_info()->set_field_id(101);

    // Append an unknown field to column_info, nested two levels below the
    // membership expression whose branch used to return early.
    {
        std::string column_wire;
        ASSERT_TRUE(roaring->column_info().SerializeToString(&column_wire));
        google::protobuf::io::StringOutputStream out(&column_wire);
        google::protobuf::io::CodedOutputStream coded(&out);
        coded.WriteVarint32((4095u << 3) | 2u);
        coded.WriteVarint32(static_cast<uint32_t>(secret.size()));
        coded.WriteString(secret);
        coded.Trim();
        ASSERT_TRUE(
            roaring->mutable_column_info()->ParseFromString(column_wire));
    }
    ASSERT_GT(roaring->column_info()
                  .GetReflection()
                  ->GetUnknownFields(roaring->column_info())
                  .field_count(),
              0)
        << "precondition: the secret must live in a nested unknown field";
    ASSERT_NE(plan_node.ShortDebugString().find(secret), std::string::npos)
        << "precondition: an unredacted dump would leak it";

    const auto debug = PlanProtoDebugString(plan_node);
    EXPECT_EQ(debug.find(secret), std::string::npos) << debug;
    EXPECT_EQ(debug.find("MRB1"), std::string::npos) << debug;
    EXPECT_NE(debug.find("bytes elided"), std::string::npos) << debug;
    EXPECT_NE(debug.find("unknown fields"), std::string::npos) << debug;
}

TEST(PlanProto, SupportsMembershipFiltersInScorers) {
    using namespace milvus;
    namespace planpb = milvus::proto::plan;

    auto schema = BuildSchema();
    query::ProtoParser parser(schema);
    const auto vector_field_id = schema->get_field_id(FieldName("fakevec"));
    const auto scalar_field_id = schema->get_field_id(FieldName("age"));

    for (const auto membership_type :
         {planpb::Expr::kBloomFilterExpr, planpb::Expr::kRoaringFilterExpr}) {
        planpb::ScoreFunction scorer;
        scorer.set_type(planpb::FunctionTypeWeight);
        scorer.set_weight(2.0F);
        if (membership_type == planpb::Expr::kBloomFilterExpr) {
            auto* bloom = scorer.mutable_filter()->mutable_bloom_filter_expr();
            bloom->mutable_column_info()->set_field_id(scalar_field_id.get());
            bloom->mutable_column_info()->set_data_type(
                proto::schema::DataType::Int64);
            bloom->set_filter_blob(BuildEmptyMbf1());
        } else {
            auto* roaring =
                scorer.mutable_filter()->mutable_roaring_filter_expr();
            roaring->mutable_column_info()->set_field_id(scalar_field_id.get());
            roaring->mutable_column_info()->set_data_type(
                proto::schema::DataType::Int64);
            roaring->set_bitmap_blob(BuildMrb1({1, 2, 3}));
        }
        auto parsed_scorer = parser.ParseScorer(scorer);
        ASSERT_NE(parsed_scorer, nullptr);

        auto plan_node = BuildSearchPlanNode(1.0f, 1.0f, vector_field_id);
        *plan_node.add_scorers() = scorer;
        auto parsed_plan = parser.CreatePlan(plan_node);
        ASSERT_NE(parsed_plan, nullptr);
    }
}

TEST(PlanProto, BloomBlobMovesFromOwnedSearchPlanNode) {
    using namespace milvus;
    namespace planpb = milvus::proto::plan;

    auto schema = BuildSchema();
    const auto vector_field_id = schema->get_field_id(FieldName("fakevec"));
    const auto scalar_field_id = schema->get_field_id(FieldName("age"));
    auto plan_node = std::make_unique<planpb::PlanNode>(
        BuildSearchPlanNode(1.0F, 1.0F, vector_field_id));
    auto* bloom = plan_node->mutable_vector_anns()
                      ->mutable_predicates()
                      ->mutable_bloom_filter_expr();
    bloom->mutable_column_info()->set_field_id(scalar_field_id.get());
    bloom->mutable_column_info()->set_data_type(
        proto::schema::DataType::Int64);
    bloom->set_filter_blob(BuildEmptyMbf1());

    const auto protobuf_blob_object = reinterpret_cast<uintptr_t>(
        std::addressof(bloom->filter_blob()));
    const auto* protobuf_blob_data = bloom->filter_blob().data();
    auto parsed_plan =
        query::ProtoParser(schema).CreatePlan(std::move(plan_node));
    auto filter_node = FindFilterBitsNode(parsed_plan->plan_node_->plannodes_);
    ASSERT_NE(filter_node, nullptr);
    auto bloom_expr =
        std::dynamic_pointer_cast<const expr::BloomFilterExpr>(
            filter_node->filter());
    ASSERT_NE(bloom_expr, nullptr);
    EXPECT_NE(reinterpret_cast<uintptr_t>(bloom_expr->filter_blob_.get()),
              protobuf_blob_object);
    EXPECT_EQ(bloom_expr->filter_blob_->data(), protobuf_blob_data);
    std::weak_ptr<const std::string> weak_blob = bloom_expr->filter_blob_;

    EXPECT_EQ(plan_node, nullptr);
    EXPECT_FALSE(weak_blob.expired());
    EXPECT_EQ(*bloom_expr->filter_blob_, BuildEmptyMbf1());

    parsed_plan.reset();
    filter_node.reset();
    bloom_expr.reset();
    EXPECT_TRUE(weak_blob.expired());
}

TEST(PlanProto, BloomBlobMovesFromOwnedRetrievePlanNode) {
    using namespace milvus;
    namespace planpb = milvus::proto::plan;

    auto schema = BuildSchema();
    const auto scalar_field_id = schema->get_field_id(FieldName("age"));
    auto plan_node = std::make_unique<planpb::PlanNode>();
    auto* bloom = plan_node->mutable_query()
                      ->mutable_predicates()
                      ->mutable_bloom_filter_expr();
    bloom->mutable_column_info()->set_field_id(scalar_field_id.get());
    bloom->mutable_column_info()->set_data_type(
        proto::schema::DataType::Int64);
    bloom->set_filter_blob(BuildEmptyMbf1());

    const auto protobuf_blob_object = reinterpret_cast<uintptr_t>(
        std::addressof(bloom->filter_blob()));
    const auto* protobuf_blob_data = bloom->filter_blob().data();
    auto parsed_plan =
        query::ProtoParser(schema).CreateRetrievePlan(std::move(plan_node));
    auto filter_node = FindFilterBitsNode(parsed_plan->plan_node_->plannodes_);
    ASSERT_NE(filter_node, nullptr);
    auto bloom_expr =
        std::dynamic_pointer_cast<const expr::BloomFilterExpr>(
            filter_node->filter());
    ASSERT_NE(bloom_expr, nullptr);
    EXPECT_NE(reinterpret_cast<uintptr_t>(bloom_expr->filter_blob_.get()),
              protobuf_blob_object);
    EXPECT_EQ(bloom_expr->filter_blob_->data(), protobuf_blob_data);
    std::weak_ptr<const std::string> weak_blob = bloom_expr->filter_blob_;

    EXPECT_EQ(plan_node, nullptr);
    EXPECT_FALSE(weak_blob.expired());
    EXPECT_EQ(*bloom_expr->filter_blob_, BuildEmptyMbf1());

    parsed_plan.reset();
    filter_node.reset();
    bloom_expr.reset();
    EXPECT_TRUE(weak_blob.expired());
}

TEST(PlanProto, BloomBlobMovesFromOwnedScoreFunction) {
    using namespace milvus;
    namespace planpb = milvus::proto::plan;

    auto schema = BuildSchema();
    const auto scalar_field_id = schema->get_field_id(FieldName("age"));
    auto function = std::make_unique<planpb::ScoreFunction>();
    function->set_type(planpb::FunctionTypeWeight);
    function->set_weight(2.0F);
    auto* bloom = function->mutable_filter()->mutable_bloom_filter_expr();
    bloom->mutable_column_info()->set_field_id(scalar_field_id.get());
    bloom->mutable_column_info()->set_data_type(
        proto::schema::DataType::Int64);
    bloom->set_filter_blob(BuildEmptyMbf1());

    const auto protobuf_blob_object = reinterpret_cast<uintptr_t>(
        std::addressof(bloom->filter_blob()));
    const auto* protobuf_blob_data = bloom->filter_blob().data();
    auto scorer =
        query::ProtoParser(schema).ParseScorer(std::move(function));
    auto bloom_expr =
        std::dynamic_pointer_cast<const expr::BloomFilterExpr>(scorer->filter());
    ASSERT_NE(bloom_expr, nullptr);
    EXPECT_NE(reinterpret_cast<uintptr_t>(bloom_expr->filter_blob_.get()),
              protobuf_blob_object);
    EXPECT_EQ(bloom_expr->filter_blob_->data(), protobuf_blob_data);
    std::weak_ptr<const std::string> weak_blob = bloom_expr->filter_blob_;

    EXPECT_EQ(function, nullptr);
    EXPECT_FALSE(weak_blob.expired());
    EXPECT_EQ(*bloom_expr->filter_blob_, BuildEmptyMbf1());

    scorer.reset();
    bloom_expr.reset();
    EXPECT_TRUE(weak_blob.expired());
}

TEST(PlanProto, RejectsGlobalRefineRatiosBelowOne) {
    using namespace milvus::query;

    auto schema = BuildSchema();
    auto vector_field_id = schema->get_field_id(milvus::FieldName("fakevec"));
    ProtoParser parser(schema);

    EXPECT_ANY_THROW(parser.PlanNodeFromProto(
        BuildSearchPlanNode(0.5f, 1.5f, vector_field_id)));
    EXPECT_ANY_THROW(parser.PlanNodeFromProto(
        BuildSearchPlanNode(1.5f, 0.5f, vector_field_id)));
}

TEST(PlanProto, VectorArrayFieldIdGapInStructArray) {
    namespace planpb = milvus::proto::plan;
    namespace schemapb = milvus::proto::schema;

    schemapb::CollectionSchema schema_proto;
    auto pk = schema_proto.add_fields();
    pk->set_name("id");
    pk->set_fieldid(100);
    pk->set_is_primary_key(true);
    pk->set_data_type(schemapb::DataType::Int64);

    auto struct_array = schema_proto.add_struct_array_fields();
    struct_array->set_name("evidence");
    struct_array->set_fieldid(146);

    auto evidence_item = struct_array->add_fields();
    evidence_item->set_name("evidence[evidence_item]");
    evidence_item->set_fieldid(147);
    evidence_item->set_data_type(schemapb::DataType::Array);
    evidence_item->set_element_type(schemapb::DataType::VarChar);
    auto max_length = evidence_item->add_type_params();
    max_length->set_key("max_length");
    max_length->set_value("512");
    auto max_capacity = evidence_item->add_type_params();
    max_capacity->set_key("max_capacity");
    max_capacity->set_value("200");

    auto evidence_vector = struct_array->add_fields();
    evidence_vector->set_name("evidence[evidence_vector]");
    evidence_vector->set_fieldid(148);
    evidence_vector->set_data_type(schemapb::DataType::ArrayOfVector);
    evidence_vector->set_element_type(schemapb::DataType::FloatVector);
    auto dim = evidence_vector->add_type_params();
    dim->set_key("dim");
    dim->set_value("1024");
    auto vector_max_capacity = evidence_vector->add_type_params();
    vector_max_capacity->set_key("max_capacity");
    vector_max_capacity->set_value("200");

    auto schema = milvus::Schema::ParseFrom(schema_proto);
    ASSERT_EQ(schema->size(), 3);
    ASSERT_EQ(schema->get_field_id_bitset_size(), 49);

    planpb::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(planpb::VectorType::EmbListFloatVector);
    vector_anns->set_field_id(148);
    vector_anns->set_placeholder_tag("$0");
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_metric_type("MAX_SIM_COSINE");
    query_info->set_topk(10);
    query_info->set_round_decimal(-1);
    query_info->set_search_params(R"({"ef": 200})");

    auto plan = milvus::query::CreateSearchPlanFromPlanNode(schema, plan_node);
    ASSERT_TRUE(plan->extra_info_opt_.has_value());
    const auto& involved_fields = plan->extra_info_opt_->involved_fields_;
    ASSERT_EQ(involved_fields.size(), 49);
    EXPECT_TRUE(involved_fields[48]);
}

TEST(PlanProto, SearchPlanCollectsFieldAccessInfo) {
    namespace planpb = milvus::proto::plan;

    auto schema = BuildSchema();
    auto vector_field_id = schema->get_field_id(milvus::FieldName("fakevec"));
    auto predicate_field_id = schema->get_field_id(milvus::FieldName("age"));
    auto output_field_id = schema->get_field_id(milvus::FieldName("score"));
    auto plan_node = BuildSearchPlanNode(0.0f, 0.0f, vector_field_id);
    plan_node.add_output_field_ids(output_field_id.get());

    auto* query_info = plan_node.mutable_vector_anns()->mutable_query_info();
    query_info->set_query_field_id(predicate_field_id.get());
    query_info->add_group_by_field_ids(predicate_field_id.get());

    auto* predicates = plan_node.mutable_vector_anns()->mutable_predicates();
    auto* term_expr = predicates->mutable_term_expr();
    auto* column_info = term_expr->mutable_column_info();
    column_info->set_field_id(predicate_field_id.get());
    column_info->set_data_type(milvus::proto::schema::DataType::Int64);

    auto plan = milvus::query::CreateSearchPlanFromPlanNode(schema, plan_node);

    EXPECT_EQ(plan->target_entries_,
              std::vector<milvus::FieldId>({output_field_id}));
    EXPECT_EQ(plan->access_entries_,
              std::vector<milvus::FieldId>(
                  {vector_field_id, predicate_field_id, output_field_id}));
}

TEST(PlanProto, RetrievePlanCollectsFieldAccessInfo) {
    namespace planpb = milvus::proto::plan;

    auto schema = BuildSchema();
    auto predicate_field_id = schema->get_field_id(milvus::FieldName("age"));
    auto output_field_id = schema->get_field_id(milvus::FieldName("score"));

    planpb::PlanNode plan_node;
    plan_node.add_output_field_ids(output_field_id.get());

    auto* query = plan_node.mutable_query();
    query->set_limit(10);
    query->add_group_by_field_ids(predicate_field_id.get());
    auto* aggregate = query->add_aggregates();
    aggregate->set_op(planpb::sum);
    aggregate->set_field_id(predicate_field_id.get());
    auto* order_by = query->add_order_by_fields();
    order_by->set_field_id(predicate_field_id.get());

    auto* predicates = query->mutable_predicates();
    auto* term_expr = predicates->mutable_term_expr();
    auto* column_info = term_expr->mutable_column_info();
    column_info->set_field_id(predicate_field_id.get());
    column_info->set_data_type(milvus::proto::schema::DataType::Int64);

    auto plan = milvus::query::CreateRetrievePlanByExpr(
        schema, plan_node.SerializeAsString().data(), plan_node.ByteSizeLong());

    EXPECT_EQ(plan->field_ids_,
              std::vector<milvus::FieldId>({output_field_id}));
    EXPECT_EQ(
        plan->access_entries_,
        std::vector<milvus::FieldId>({predicate_field_id, output_field_id}));
}
