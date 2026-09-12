// Copyright (C) 2019-2025 Zilliz. All rights reserved.
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
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "query/PlanProto.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus::query;
using namespace milvus::segcore;

using namespace milvus;

// Verify the DataType enum value for UUID.
TEST(UuidTest, EnumValue) {
    ASSERT_EQ(static_cast<int>(DataType::UUID), 31);
}

// Verify that UUID is not classified as a generic variable-width string.
TEST(UuidTest, IsNotStringDataType) {
    ASSERT_FALSE(IsStringDataType(DataType::UUID));
}

// Verify that UUID has a logical data size of 16 bytes and is a fixed-width type.
TEST(UuidTest, DataTypeSize) {
    ASSERT_TRUE(IsFixedSizeType(DataType::UUID));
    ASSERT_EQ(GetDataTypeSize(DataType::UUID), 16);
}

// Verify ToProtoDataType maps internal DataType::UUID to proto DataType::UUID.
TEST(UuidTest, ToProtoDataTypeMapping) {
    auto proto_type = ToProtoDataType(DataType::UUID);
    ASSERT_EQ(proto_type, proto::schema::DataType::UUID);
}

// Verify that InitScalarFieldData creates a FixedSizeBinary(16)-backed FieldData
// for UUID (IsFixedSizeType true, not VarChar-backed std::string).
TEST(UuidTest, InitScalarFieldData) {
    auto field_data = InitScalarFieldData(DataType::UUID, false, 10);
    ASSERT_NE(field_data, nullptr);
    ASSERT_EQ(field_data->get_data_type(), DataType::UUID);
}

// Verify that InitScalarFieldDataWithLength creates a FixedSizeBinary(16)-backed
// FieldData for UUID at a given capacity.
TEST(UuidTest, InitScalarFieldDataWithLength) {
    constexpr int64_t kLength = 100;
    auto field_data = InitScalarFieldDataWithLength(DataType::UUID, kLength);
    ASSERT_NE(field_data, nullptr);
    ASSERT_EQ(field_data->get_data_type(), DataType::UUID);
    ASSERT_EQ(field_data->Length(), kLength);
}

// Verify that a UUID field can be added to a Schema via AddDebugField.
// IsStringDataType(UUID) is false (IsFixedSizeType true, 16B), UUID carries
// no max_length type param; FieldMeta defaults canonical input to 36-char.
TEST(UuidTest, SchemaAddField) {
    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("uuid_field", DataType::UUID);
    ASSERT_NE(field_id.get(), -1);

    auto& field_meta = schema->operator[](field_id);
    ASSERT_EQ(field_meta.get_data_type(), DataType::UUID);
    ASSERT_EQ(field_meta.get_name().get(), "uuid_field");
}

// Retrieve helper: term `id == <uuid_str>` (and the absent-value negative
// control) against a segment. Silent 0-row matches on sealed segments are
// the regression under test: the predicate must match exactly the rows whose
// stored 16B value equals the literal, on both growing and sealed.
namespace {
constexpr int64_t kUuidTestRows = 100;

std::shared_ptr<Schema>
GenUuidPkSchema() {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto uuid_fid = schema->AddDebugField("id", DataType::UUID);
    schema->set_primary_field_id(uuid_fid);
    return schema;
}

std::unique_ptr<milvus::query::RetrievePlan>
GenUuidTermRetrievePlan(const std::shared_ptr<Schema>& schema,
                        FieldId uuid_fid,
                        const std::string& uuid_str) {
    auto column_info = new proto::plan::ColumnInfo();
    column_info->set_field_id(uuid_fid.get());
    column_info->set_data_type(proto::schema::DataType::UUID);
    column_info->set_is_primary_key(true);
    auto term_expr = new proto::plan::TermExpr();
    term_expr->add_values()->set_string_val(uuid_str);
    term_expr->set_allocated_column_info(column_info);
    auto expr = std::make_unique<proto::plan::Expr>();
    expr->set_allocated_term_expr(term_expr);
    auto plan_node = std::make_unique<proto::plan::PlanNode>();
    plan_node->mutable_query()->set_allocated_predicates(expr.release());
    return ProtoParser(schema).CreateRetrievePlan(*plan_node);
}

std::unique_ptr<milvus::query::RetrievePlan>
GenUuidUnaryRangeRetrievePlan(const std::shared_ptr<Schema>& schema,
                              FieldId uuid_fid,
                              proto::plan::OpType op,
                              const std::string& uuid_str) {
    auto column_info = new proto::plan::ColumnInfo();
    column_info->set_field_id(uuid_fid.get());
    column_info->set_data_type(proto::schema::DataType::UUID);
    column_info->set_is_primary_key(true);
    auto value = new proto::plan::GenericValue();
    value->set_string_val(uuid_str);
    auto unary_range_expr = new proto::plan::UnaryRangeExpr();
    unary_range_expr->set_op(op);
    unary_range_expr->set_allocated_value(value);
    unary_range_expr->set_allocated_column_info(column_info);
    auto expr = std::make_unique<proto::plan::Expr>();
    expr->set_allocated_unary_range_expr(unary_range_expr);
    auto plan_node = std::make_unique<proto::plan::PlanNode>();
    plan_node->mutable_query()->set_allocated_predicates(expr.release());
    return ProtoParser(schema).CreateRetrievePlan(*plan_node);
}

void
AssertUuidRangeRetrieveCount(segcore::SegmentInterface* segment,
                             const std::shared_ptr<Schema>& schema,
                             FieldId uuid_fid,
                             proto::plan::OpType op,
                             const std::string& uuid_str,
                             int64_t expected_count) {
    auto plan = GenUuidUnaryRangeRetrievePlan(schema, uuid_fid, op, uuid_str);
    auto retrieved = segment->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);
    ASSERT_EQ(retrieved->offset().size(), expected_count)
        << "range over UUID PK returned wrong row count";
    ASSERT_EQ(retrieved->ids().uuid_id().data_size(), expected_count);
}

void
AssertUuidTermRetrieve(segcore::SegmentInterface* segment,
                       const std::shared_ptr<Schema>& schema,
                       FieldId uuid_fid,
                       const std::string& present_uuid,
                       const std::string& absent_uuid) {
    auto plan = GenUuidTermRetrievePlan(schema, uuid_fid, present_uuid);
    auto retrieved = segment->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);
    ASSERT_EQ(retrieved->offset().size(), 1)
        << "present UUID must match exactly one row";
    ASSERT_EQ(retrieved->ids().uuid_id().data_size(), 1);
    const auto expected = UUID::FromString(present_uuid);
    EXPECT_EQ(retrieved->ids().uuid_id().data(0),
              std::string(reinterpret_cast<const char*>(expected.data.data()),
                          expected.data.size()));

    auto absent_plan = GenUuidTermRetrievePlan(schema, uuid_fid, absent_uuid);
    auto absent = segment->Retrieve(nullptr,
                                    absent_plan.get(),
                                    MAX_TIMESTAMP,
                                    DEFAULT_MAX_OUTPUT_SIZE,
                                    false);
    ASSERT_EQ(absent->offset().size(), 0) << "absent UUID must match no rows";
}

// get_col<UUID> cannot compile: the harness switch instantiates every scalar
// arm, including int32 -> UUID assignment. Read the generated 16B entries
// straight from the raw proto instead.
std::string
GetFirstUuidCanonical(const segcore::GeneratedData& dataset, FieldId uuid_fid) {
    for (auto i = 0; i < dataset.raw_->fields_data_size(); ++i) {
        const auto& field_data = dataset.raw_->fields_data(i);
        if (field_data.field_id() != uuid_fid.get()) {
            continue;
        }
        const auto& bytes_data = field_data.scalars().bytes_data();
        if (bytes_data.data_size() == 0) {
            break;
        }
        UUID uuid{};
        memcpy(uuid.data.data(), bytes_data.data(0).data(), sizeof(uuid.data));
        return uuid.ToString();
    }
    return "";
}
}  // namespace

TEST(UuidTest, GrowingTermQueryMatchesInsertedUuid) {
    auto schema = GenUuidPkSchema();
    auto uuid_fid = schema->get_primary_field_id().value();

    auto dataset = DataGen(schema, kUuidTestRows);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(kUuidTestRows);
    segment->Insert(0,
                    kUuidTestRows,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    const auto present_uuid = GetFirstUuidCanonical(dataset, uuid_fid);
    ASSERT_EQ(present_uuid.size(), 36);
    // Well-formed but absent value: valid RFC-4122 form, never inserted.
    AssertUuidTermRetrieve(segment.get(),
                           schema,
                           uuid_fid,
                           present_uuid,
                           "12345678-1234-4234-8234-123456789abc");
}

TEST(UuidTest, SealedTermQueryMatchesLoadedUuid) {
    auto schema = GenUuidPkSchema();
    auto uuid_fid = schema->get_primary_field_id().value();

    auto dataset = DataGen(schema, kUuidTestRows);
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);

    const auto present_uuid = GetFirstUuidCanonical(dataset, uuid_fid);
    ASSERT_EQ(present_uuid.size(), 36);
    AssertUuidTermRetrieve(sealed_segment.get(),
                           schema,
                           uuid_fid,
                           present_uuid,
                           "12345678-1234-4234-8234-123456789abc");
}

TEST(UuidTest, SealedRangeQueryMatchesLoadedUuid) {
    auto schema = GenUuidPkSchema();
    auto uuid_fid = schema->get_primary_field_id().value();

    auto dataset = DataGen(schema, kUuidTestRows);
    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, dataset);

    AssertUuidRangeRetrieveCount(sealed_segment.get(),
                                 schema,
                                 uuid_fid,
                                 proto::plan::OpType::GreaterEqual,
                                 "00000000-0000-4000-8000-000000000000",
                                 kUuidTestRows);
    AssertUuidRangeRetrieveCount(sealed_segment.get(),
                                 schema,
                                 uuid_fid,
                                 proto::plan::OpType::GreaterThan,
                                 "ffffffff-ffff-ffff-ffff-ffffffffffff",
                                 0);
}

TEST(UuidTest, GrowingRangeQueryMatchesInsertedUuid) {
    auto schema = GenUuidPkSchema();
    auto uuid_fid = schema->get_primary_field_id().value();

    auto dataset = DataGen(schema, kUuidTestRows);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(kUuidTestRows);
    segment->Insert(0,
                    kUuidTestRows,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    AssertUuidRangeRetrieveCount(segment.get(),
                                 schema,
                                 uuid_fid,
                                 proto::plan::OpType::GreaterEqual,
                                 "00000000-0000-4000-8000-000000000000",
                                 kUuidTestRows);
    AssertUuidRangeRetrieveCount(segment.get(),
                                 schema,
                                 uuid_fid,
                                 proto::plan::OpType::GreaterThan,
                                 "ffffffff-ffff-ffff-ffff-ffffffffffff",
                                 0);
}
