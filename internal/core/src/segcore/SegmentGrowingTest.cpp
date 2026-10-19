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

#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <stddef.h>
#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <shared_mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "bitset/bitset.h"
#include "bitset/detail/element_vectorized.h"
#include "cachinglayer/Utils.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/IndexMeta.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/object.h"
#include "knowhere/sparse_utils.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "query/SearchOnGrowing.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/ManifestTestUtil.h"
#include "test_utils/SegcoreConfigUtils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus::segcore;
using namespace milvus;

namespace {

void
AddStorageV3SystemFields(const SchemaPtr& schema) {
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
}

bool
ManifestHasField(const milvus::test::V3SegmentTestData& test_data,
                 FieldId field_id) {
    auto field_column = std::to_string(field_id.get());
    auto column_groups = test_data.GetColumnGroups();
    for (size_t i = 0; i < column_groups->size(); ++i) {
        const auto& columns = column_groups->at(i)->columns;
        if (std::find(columns.begin(), columns.end(), field_column) !=
            columns.end()) {
            return true;
        }
    }
    return false;
}

}  // namespace

TEST(Growing, DeleteCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    int64_t c = 10;
    auto offset = 0;

    auto dataset = DataGen(schema, c);
    auto pks = dataset.get_col<int64_t>(pk);
    segment->Insert(offset,
                    c,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    Timestamp begin_ts = 100;
    auto tss = GenTss(c, begin_ts);
    auto del_pks = GenPKs(pks.begin(), pks.end());
    auto status = segment->Delete(c, del_pks.get(), tss.data());
    ASSERT_TRUE(status.ok());

    auto cnt = segment->get_deleted_count();
    ASSERT_EQ(cnt, c);
}

TEST(Growing, RealCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    int64_t c = 10;
    auto offset = 0;
    auto dataset = DataGen(schema, c);
    auto pks = dataset.get_col<int64_t>(pk);
    segment->Insert(offset,
                    c,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    // no delete.
    ASSERT_EQ(c, segment->get_real_count());

    // delete half.
    auto half = c / 2;
    auto del_ids1 = GenPKs(pks.begin(), pks.begin() + half);
    auto del_tss1 = GenTss(half, c);
    auto status = segment->Delete(half, del_ids1.get(), del_tss1.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(c - half, segment->get_real_count());

    // delete duplicate.
    auto del_offset2 = segment->get_deleted_count();
    ASSERT_EQ(del_offset2, half);
    auto del_tss2 = GenTss(half, c + half);
    status = segment->Delete(half, del_ids1.get(), del_tss2.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(c - half, segment->get_real_count());

    // delete all.
    auto del_offset3 = segment->get_deleted_count();
    ASSERT_EQ(del_offset3, half);
    auto del_ids3 = GenPKs(pks.begin(), pks.end());
    auto del_tss3 = GenTss(c, c + half * 2);
    status = segment->Delete(c, del_ids3.get(), del_tss3.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(0, segment->get_real_count());
}

TEST(Growing, LoadStorageV3ManifestCapsRowsAtCheckpoint) {
    auto schema = std::make_shared<Schema>();
    AddStorageV3SystemFields(schema);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    constexpr int64_t dim = 4;
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);

    std::map<std::string, std::string> analyzer_params;
    auto text = schema->AddDebugVarcharField(FieldName("text"),
                                             DataType::VARCHAR,
                                             65535,
                                             false,
                                             true,
                                             true,
                                             analyzer_params,
                                             std::nullopt);
    schema->set_primary_field_id(pk);

    auto base_path = (std::filesystem::path(TestLocalPath) /
                      "growing_recovery_checkpoint_row_cap")
                         .string();
    std::filesystem::remove_all(base_path);
    milvus::test::V3SegmentTestData test_data(
        schema, 2, 3, dim, TestLocalPath, base_path);
    ASSERT_EQ(test_data.NumColumnGroups(), 2);
    ASSERT_TRUE(ManifestHasField(test_data, RowFieldID));
    ASSERT_TRUE(ManifestHasField(test_data, TimestampFieldID));

    constexpr int64_t checkpoint_rows = 4;
    milvus::proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(1);
    load_info.set_partitionid(2);
    load_info.set_segmentid(3);
    load_info.set_storageversion(STORAGE_V3);
    load_info.set_num_of_rows(checkpoint_rows);
    load_info.set_manifest_path(test_data.ManifestPathJson());
    load_info.set_insert_channel("by-dev-rootcoord-dml_0_1v0");

    auto segment =
        CreateGrowingSegment(schema, empty_index_meta, load_info.segmentid());
    segment->SetLoadInfo(load_info);
    milvus::tracer::TraceContext trace_ctx;
    segment->Load(trace_ctx, nullptr);
    ASSERT_EQ(segment->get_row_count(), checkpoint_rows);
    EXPECT_EQ(segment->get_real_count(), checkpoint_rows);

    std::vector<int64_t> row_ids = {checkpoint_rows};
    std::vector<Timestamp> timestamps = {100};
    std::vector<int64_t> pks = {100000};
    std::vector<std::string> texts = {"text after recovery checkpoint"};
    std::vector<float> vectors(dim, 1.0F);

    auto insert_data = std::make_shared<InsertRecordProto>();
    insert_data->set_num_rows(1);
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, 1, (*schema)[pk]).release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(texts.data(), nullptr, 1, (*schema)[text])
            .release());
    insert_data->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(vectors.data(), nullptr, 1, (*schema)[vec])
            .release());

    auto offset = segment->PreInsert(1);
    ASSERT_EQ(offset, checkpoint_rows);
    ASSERT_NO_THROW(segment->Insert(
        offset, 1, row_ids.data(), timestamps.data(), insert_data));
    EXPECT_EQ(segment->get_row_count(), checkpoint_rows + 1);

    std::filesystem::remove_all(base_path);
}

TEST(Growing, LoadStorageV3ManifestRejectsShortRequiredRows) {
    auto schema = std::make_shared<Schema>();
    AddStorageV3SystemFields(schema);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);

    auto base_path = (std::filesystem::path(TestLocalPath) /
                      "growing_recovery_short_required_rows")
                         .string();
    std::filesystem::remove_all(base_path);
    milvus::test::V3SegmentTestData test_data(
        schema, 1, 2, 1, TestLocalPath, base_path);
    ASSERT_TRUE(ManifestHasField(test_data, RowFieldID));
    ASSERT_TRUE(ManifestHasField(test_data, TimestampFieldID));

    milvus::proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(1);
    load_info.set_partitionid(2);
    load_info.set_segmentid(4);
    load_info.set_storageversion(STORAGE_V3);
    load_info.set_num_of_rows(test_data.TotalRows() + 1);
    load_info.set_manifest_path(test_data.ManifestPathJson());
    load_info.set_insert_channel("by-dev-rootcoord-dml_0_1v0");

    auto segment =
        CreateGrowingSegment(schema, empty_index_meta, load_info.segmentid());
    segment->SetLoadInfo(load_info);
    milvus::tracer::TraceContext trace_ctx;
    ASSERT_ANY_THROW(segment->Load(trace_ctx, nullptr));
    EXPECT_EQ(segment->get_row_count(), 0);

    std::filesystem::remove_all(base_path);
}

TEST(Growing, InsertSkipsMissingFunctionOutputField) {
    auto schema = std::make_shared<Schema>();
    schema->set_schema_version(2);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto sparse = FieldId(pk.get() + 1);
    schema->AddField(FieldName("sparse_out"),
                     sparse,
                     DataType::VECTOR_SPARSE_U32_F32,
                     0,
                     std::nullopt,
                     false);
    schema->add_function_output_field_id(sparse);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    auto insert_schema = std::make_shared<Schema>();
    insert_schema->set_schema_version(1);
    insert_schema->AddField(
        FieldName("pk"), pk, DataType::INT64, false, std::nullopt);
    insert_schema->set_primary_field_id(pk);

    constexpr int64_t row_count = 5;
    auto dataset = DataGen(insert_schema, row_count);
    segment->PreInsert(row_count);
    ASSERT_NO_THROW(
        segment->Insert(0,
                        row_count,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        std::make_shared<InsertRecordProto>(*dataset.raw_)));
    EXPECT_EQ(segment->get_row_count(), row_count);
    EXPECT_TRUE(segment->FieldAccessible(pk));
    EXPECT_FALSE(segment->HasFieldData(sparse));
    EXPECT_FALSE(segment->FieldAccessible(sparse));
}

TEST(Growing, AllNullVectorFieldIsAccessibleWithoutRawChunks) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2, true);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    constexpr int64_t row_count = 5;
    auto dataset =
        DataGen(schema, row_count, 42, 0, 1, 10, 1, false, true, false, 100);
    EXPECT_FALSE(segment->FieldAccessible(vec));
    auto offset = segment->PreInsert(row_count);
    segment->Insert(offset,
                    row_count,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    EXPECT_EQ(segment->get_row_count(), row_count);
    EXPECT_EQ(segment_impl->get_insert_record().get_data_base(vec)->num_chunk(),
              0);
    EXPECT_FALSE(segment->HasIndex(vec));
    EXPECT_TRUE(segment->HasFieldData(vec));
    EXPECT_TRUE(segment->FieldAccessible(vec));

    const std::array<int64_t, row_count> offsets = {0, 1, 2, 3, 4};
    auto actual =
        segment->bulk_subscript(nullptr, vec, offsets.data(), row_count);
    const auto& valid_data = GetFieldDataRowValidData(*actual);
    ASSERT_EQ(valid_data.size(), row_count);
    for (bool valid : valid_data) {
        EXPECT_FALSE(valid);
    }
    EXPECT_TRUE(actual->vectors().float_vector().data().empty());
}

TEST(Growing, InsertRejectsTruncatedGeometryBeforeWritingSegmentData) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto geometry = schema->AddDebugField("geometry", DataType::GEOMETRY, true);
    schema->set_primary_field_id(pk);

    constexpr int64_t row_count = 2;
    std::array<int64_t, row_count> row_ids = {10, 11};
    std::array<Timestamp, row_count> timestamps = {100, 101};
    std::array<int64_t, row_count> pks = {1, 2};
    std::array<bool, row_count> valid = {true, true};

    auto geos_ctx = GEOS_init_r();
    std::array<std::string, row_count> geometries = {
        Geometry(geos_ctx, "POINT (1 1)").to_wkb_string(),
        Geometry(geos_ctx, "POINT (2 2)").to_wkb_string()};
    GEOS_finish_r(geos_ctx);

    auto make_record = [&]() {
        auto record = std::make_shared<InsertRecordProto>();
        record->set_num_rows(row_count);
        record->mutable_fields_data()->AddAllocated(
            CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk])
                .release());
        record->mutable_fields_data()->AddAllocated(
            CreateDataArrayFrom(
                geometries.data(), valid.data(), row_count, (*schema)[geometry])
                .release());
        return record;
    };

    auto expect_rejected_without_ack = [&](auto mutate) {
        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        auto record = make_record();
        mutate(record->mutable_fields_data(1));
        auto offset = segment->PreInsert(row_count);

        try {
            segment->Insert(
                offset, row_count, row_ids.data(), timestamps.data(), record);
            FAIL() << "expected malformed geometry insert to be rejected";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
        }
        EXPECT_EQ(segment->get_row_count(), 0);
    };

    expect_rejected_without_ack([](DataArray* field_data) {
        MutableFieldDataRowValidData(field_data)->RemoveLast();
    });
    expect_rejected_without_ack([](DataArray* field_data) {
        field_data->mutable_scalars()
            ->mutable_geometry_data()
            ->mutable_data()
            ->RemoveLast();
    });
}

TEST(Growing, MissingStructElementOffsetsReturnsEmptyForOldRows) {
    auto old_schema = std::make_shared<Schema>();
    old_schema->set_schema_version(1);
    auto pk = old_schema->AddDebugField("pk", DataType::INT64);
    old_schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(old_schema, empty_index_meta);

    constexpr int64_t row_count = 5;
    auto dataset = DataGen(old_schema, row_count);
    segment->Insert(0,
                    row_count,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    auto new_schema = std::make_shared<Schema>();
    new_schema->set_schema_version(2);
    new_schema->AddField(
        FieldName("pk"), pk, DataType::INT64, false, std::nullopt);
    new_schema->set_primary_field_id(pk);
    auto label = FieldId(pk.get() + 1);
    new_schema->AddField(FieldName("chunks[label]"),
                         label,
                         DataType::ARRAY,
                         DataType::VARCHAR,
                         true,
                         false);
    segment->Reopen(new_schema);

    auto offsets = segment->GetStructElementOffsets(label);
    ASSERT_NE(offsets, nullptr);
    EXPECT_EQ(offsets->GetRowCount(), row_count);
    EXPECT_EQ(offsets->GetTotalElementCount(), 0);
    for (int64_t i = 0; i <= row_count; ++i) {
        auto [start, end] = offsets->ElementIDRangeOfRow(i);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 0);
    }
}

TEST(Growing, StructArrayRepresentativeFollowsSchemaOrder) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto first =
        schema->AddDebugArrayField("items[first]", DataType::INT32, false);
    auto second =
        schema->AddDebugArrayField("items[second]", DataType::INT32, false);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    InsertRecordProto insert;
    insert.set_num_rows(1);
    auto* pk_data = insert.add_fields_data();
    pk_data->set_field_id(pk.get());
    pk_data->set_type(proto::schema::DataType::Int64);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(1);
    // Send the later schema field first and give it a different length so the
    // provider is observable in this test.
    for (auto [id, count] : {std::pair{second, 3}, std::pair{first, 2}}) {
        auto* data = insert.add_fields_data();
        data->set_field_id(id.get());
        data->set_type(proto::schema::DataType::Array);
        auto* rows = data->mutable_scalars()->mutable_array_data();
        rows->set_element_type(proto::schema::DataType::Int32);
        auto* row = rows->add_data();
        for (int i = 0; i < count; ++i) {
            row->mutable_int_data()->add_data(i);
        }
    }
    const int64_t row_id = 1;
    const Timestamp timestamp = 1;
    segment->Insert(segment->PreInsert(1),
                    1,
                    &row_id,
                    &timestamp,
                    std::make_shared<InsertRecordProto>(insert));
    auto offsets = segment->GetStructElementOffsets(first);
    ASSERT_NE(offsets, nullptr);
    EXPECT_EQ(offsets.get(), segment->GetStructElementOffsets(second).get());
    EXPECT_EQ(offsets->ElementIDRangeOfRow(0), std::make_pair(0, 2));
}

TEST(Growing, ReopenBackfillsStructChildBeforeInsertLengths) {
    const FieldId pk(100);
    const FieldId source(101);
    const FieldId added(102);
    auto make_schema = [&](int64_t version, bool include_added) {
        auto schema = std::make_shared<Schema>();
        schema->set_schema_version(version);
        schema->AddField(
            FieldName("pk"), pk, DataType::INT64, false, std::nullopt);
        schema->set_primary_field_id(pk);
        schema->AddField(FieldName("items[source]"),
                         source,
                         DataType::ARRAY,
                         DataType::INT32,
                         false,
                         false);
        if (include_added) {
            schema->AddField(FieldName("items[added]"),
                             added,
                             DataType::ARRAY,
                             DataType::INT32,
                             false,
                             true);
        }
        return schema;
    };
    auto segment =
        CreateGrowingSegment(make_schema(1, false), empty_index_meta);
    auto append = [&](int64_t row_id, int length) {
        InsertRecordProto insert;
        insert.set_num_rows(1);
        auto* pk_data = insert.add_fields_data();
        pk_data->set_field_id(pk.get());
        pk_data->set_type(proto::schema::DataType::Int64);
        pk_data->mutable_scalars()->mutable_long_data()->add_data(row_id);
        auto* source_data = insert.add_fields_data();
        source_data->set_field_id(source.get());
        source_data->set_type(proto::schema::DataType::Array);
        auto* rows = source_data->mutable_scalars()->mutable_array_data();
        rows->set_element_type(proto::schema::DataType::Int32);
        auto* row = rows->add_data();
        for (int i = 0; i < length; ++i) {
            row->mutable_int_data()->add_data(i);
        }
        const Timestamp timestamp = row_id;
        segment->Insert(segment->PreInsert(1),
                        1,
                        &row_id,
                        &timestamp,
                        std::make_shared<InsertRecordProto>(insert));
    };
    append(1, 2);
    auto next = make_schema(2, true);
    segment->Reopen(next);
    append(2, 3);

    auto offsets = segment->GetStructElementOffsets(added);
    ASSERT_NE(offsets, nullptr);
    EXPECT_EQ(offsets->ElementIDRangeOfRow(0), std::make_pair(0, 2));
    EXPECT_EQ(offsets->ElementIDRangeOfRow(1), std::make_pair(2, 5));
    const int64_t rows[] = {0, 1};
    milvus::OpContext op_ctx;
    auto result = segment->bulk_subscript(&op_ctx, added, rows, 2);
    ASSERT_EQ(result->scalars().array_data().data_size(), 2);
    EXPECT_EQ(result->scalars().array_data().data(0).valid_data_size(), 2);
    EXPECT_EQ(result->scalars().array_data().data(1).valid_data_size(), 3);
    auto fallback =
        segment->bulk_subscript_not_exist_field((*next)[added], 0, 2, *offsets);
    EXPECT_EQ(fallback->scalars().array_data().data(0).valid_data_size(), 2);
    EXPECT_EQ(fallback->scalars().array_data().data(1).valid_data_size(), 3);
}

TEST(Growing, ReopenBackfillsNullableStructRows) {
    auto make_schema = [](int64_t version, bool include_added) {
        auto schema = std::make_shared<Schema>();
        schema->set_schema_version(version);
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        schema->AddDebugArrayField("items[source]", DataType::INT32, true);
        if (include_added) {
            schema->AddDebugArrayField(
                "items[added]", DataType::INT32, true, true);
        }
        return schema;
    };
    auto schema = make_schema(1, false);
    auto source = schema->get_field_ids().back();
    auto dataset = DataGen(schema, 3, 42, 0, 1, 2);
    for (auto& field : *dataset.raw_->mutable_fields_data()) {
        if (field.field_id() == source.get()) {
            MutableFieldDataRowValidData(&field)->Set(1, false);
            auto* null_row =
                field.mutable_scalars()->mutable_array_data()->mutable_data(1);
            null_row->Clear();
            null_row->mutable_int_data();
        }
    }
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->Insert(segment->PreInsert(3),
                    3,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));
    auto next = make_schema(2, true);
    auto added = next->get_field_ids().back();
    segment->Reopen(next);

    auto offsets = segment->GetStructElementOffsets(added);
    ASSERT_NE(offsets, nullptr);
    EXPECT_EQ(offsets->GetTotalElementCount(), 4);
    const int64_t row_ids[] = {0, 1, 2};
    milvus::OpContext op_ctx;
    auto result = segment->bulk_subscript(&op_ctx, added, row_ids, 3);
    const auto& validity = GetFieldDataRowValidData(*result);
    ASSERT_EQ(validity.size(), 3);
    EXPECT_TRUE(validity[0]);
    EXPECT_FALSE(validity[1]);
    EXPECT_TRUE(validity[2]);
    ASSERT_EQ(result->scalars().array_data().data_size(), 3);
    EXPECT_EQ(result->scalars().array_data().data(0).valid_data_size(), 2);
    EXPECT_EQ(result->scalars().array_data().data(1).valid_data_size(), 0);
    EXPECT_EQ(result->scalars().array_data().data(2).valid_data_size(), 2);
}

TEST(Growing, DroppingStructSubFieldsKeepsOffsetsContinuous) {
    const FieldId pk(100);
    const FieldId first(101);
    const FieldId middle(102);
    const FieldId last(103);
    const FieldId readded_first(104);
    auto make_schema =
        [&](int64_t version,
            std::initializer_list<std::pair<FieldId, const char*>> children) {
            auto schema = std::make_shared<Schema>();
            schema->set_schema_version(version);
            schema->AddField(
                FieldName("pk"), pk, DataType::INT64, false, std::nullopt);
            schema->set_primary_field_id(pk);
            for (auto [id, name] : children) {
                schema->AddField(FieldName(name),
                                 id,
                                 DataType::ARRAY,
                                 DataType::INT32,
                                 true,
                                 false);
            }
            return schema;
        };
    auto initial = make_schema(1,
                               {{first, "items[first]"},
                                {middle, "items[middle]"},
                                {last, "items[last]"}});
    auto segment = CreateGrowingSegment(initial, empty_index_meta);
    auto append = [&](int64_t row_id,
                      int count,
                      std::initializer_list<FieldId> children) {
        InsertRecordProto insert;
        insert.set_num_rows(1);
        auto* pk_data = insert.add_fields_data();
        pk_data->set_field_id(pk.get());
        pk_data->set_type(proto::schema::DataType::Int64);
        pk_data->mutable_scalars()->mutable_long_data()->add_data(row_id);
        for (auto id : children) {
            auto* data = insert.add_fields_data();
            data->set_field_id(id.get());
            data->set_type(proto::schema::DataType::Array);
            auto* scalar = data->mutable_scalars();
            scalar->add_valid_data(true);
            auto* rows = scalar->mutable_array_data();
            rows->set_element_type(proto::schema::DataType::Int32);
            auto* row = rows->add_data();
            for (int i = 0; i < count; ++i) {
                row->mutable_int_data()->add_data(i);
            }
        }
        const Timestamp timestamp = row_id;
        segment->Insert(segment->PreInsert(1),
                        1,
                        &row_id,
                        &timestamp,
                        std::make_shared<InsertRecordProto>(insert));
    };

    append(1, 2, {first, middle, last});
    auto original_offsets = segment->GetStructElementOffsets(first);
    ASSERT_NE(original_offsets, nullptr);
    segment->Reopen(
        make_schema(2, {{middle, "items[middle]"}, {last, "items[last]"}}));
    // The dropped sub-field's alias stays and keeps pointing to the struct's
    // offsets. Reopen no longer erases it: erasing it before the new schema
    // is published opened a window (and, if Reopen failed, a lasting state)
    // in which a reader on the old schema, which resolves the struct through
    // this first sub-field, found no offsets.
    EXPECT_EQ(segment->GetStructElementOffsets(first).get(),
              original_offsets.get());
    EXPECT_EQ(segment->GetStructElementOffsets(middle).get(),
              original_offsets.get());
    append(2, 3, {middle, last});
    EXPECT_EQ(original_offsets->ElementIDRangeOfRow(1), std::make_pair(2, 5));

    segment->Reopen(make_schema(3,
                                {{middle, "items[middle]"},
                                 {last, "items[last]"},
                                 {readded_first, "items[first]"}}));
    EXPECT_EQ(segment->GetStructElementOffsets(readded_first).get(),
              original_offsets.get());
    append(3, 1, {middle, last, readded_first});
    EXPECT_EQ(original_offsets->ElementIDRangeOfRow(2), std::make_pair(5, 6));

    segment->Reopen(make_schema(
        4, {{middle, "items[middle]"}, {readded_first, "items[first]"}}));
    EXPECT_EQ(segment->GetStructElementOffsets(last).get(),
              original_offsets.get());
    append(4, 2, {middle, readded_first});
    EXPECT_EQ(original_offsets->ElementIDRangeOfRow(3), std::make_pair(6, 8));
}

// A growing Reopen that fails partway leaves the old schema published. Every
// struct offsets alias of that schema must still resolve, so a query on the
// old schema keeps working: MATCH_ANY resolves the struct through its first
// sub-field, which the failed Reopen was dropping. A retry then publishes the
// new schema, and inserts keep appending to the same offsets.
TEST(Growing, FailedReopenKeepsStructOffsetsOfPublishedSchema) {
    const FieldId pk(100);
    const FieldId first(101);
    const FieldId second(102);
    const FieldId required(103);
    auto make_schema =
        [&](int64_t version, bool with_first, bool with_required) {
            auto schema = std::make_shared<Schema>();
            schema->set_schema_version(version);
            schema->AddField(
                FieldName("pk"), pk, DataType::INT64, false, std::nullopt);
            schema->set_primary_field_id(pk);
            if (with_first) {
                schema->AddField(FieldName("items[first]"),
                                 first,
                                 DataType::ARRAY,
                                 DataType::INT32,
                                 true,
                                 false);
            }
            schema->AddField(FieldName("items[second]"),
                             second,
                             DataType::ARRAY,
                             DataType::INT32,
                             true,
                             false);
            if (with_required) {
                // Not nullable and no default: the backfill of the existing rows
                // throws, after Reopen has started on the struct sub-fields.
                schema->AddField(FieldName("required"),
                                 required,
                                 DataType::INT64,
                                 false,
                                 std::nullopt);
            }
            return schema;
        };
    auto segment =
        CreateGrowingSegment(make_schema(1, true, false), empty_index_meta);
    auto append = [&](int64_t row_id,
                      int count,
                      std::initializer_list<FieldId> children) {
        InsertRecordProto insert;
        insert.set_num_rows(1);
        auto* pk_data = insert.add_fields_data();
        pk_data->set_field_id(pk.get());
        pk_data->set_type(proto::schema::DataType::Int64);
        pk_data->mutable_scalars()->mutable_long_data()->add_data(row_id);
        for (auto id : children) {
            auto* data = insert.add_fields_data();
            data->set_field_id(id.get());
            data->set_type(proto::schema::DataType::Array);
            auto* scalar = data->mutable_scalars();
            scalar->add_valid_data(true);
            auto* rows = scalar->mutable_array_data();
            rows->set_element_type(proto::schema::DataType::Int32);
            auto* row = rows->add_data();
            for (int i = 0; i < count; ++i) {
                row->mutable_int_data()->add_data(i);
            }
        }
        const Timestamp timestamp = row_id;
        segment->Insert(segment->PreInsert(1),
                        1,
                        &row_id,
                        &timestamp,
                        std::make_shared<InsertRecordProto>(insert));
    };

    append(1, 2, {first, second});
    auto offsets = segment->GetStructElementOffsets(first);
    ASSERT_NE(offsets, nullptr);

    EXPECT_ANY_THROW(segment->Reopen(make_schema(2, false, true)));
    auto schema = segment->get_schema_snapshot();
    EXPECT_EQ(schema->get_schema_version(), 1);
    const auto head = schema->GetFirstArrayFieldInStruct("items").get_id();
    EXPECT_EQ(head, first);
    EXPECT_EQ(segment->GetStructElementOffsets(head).get(), offsets.get());
    EXPECT_EQ(segment->GetStructElementOffsets(second).get(), offsets.get());

    segment->Reopen(make_schema(2, false, false));
    schema = segment->get_schema_snapshot();
    EXPECT_EQ(schema->get_schema_version(), 2);
    EXPECT_EQ(schema->GetFirstArrayFieldInStruct("items").get_id(), second);
    EXPECT_EQ(segment->GetStructElementOffsets(second).get(), offsets.get());
    append(2, 3, {second});
    EXPECT_EQ(offsets->ElementIDRangeOfRow(1), std::make_pair(2, 5));
}

TEST(Growing, ElementNullableStructSubFieldsHaveLogicalOffsets) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto scalar = schema->AddDebugArrayField(
        "scalar[items]", DataType::INT64, false, true);
    proto::schema::TypeSchema nested_type;
    nested_type.set_nullable(false);
    auto* inner = nested_type.mutable_array_element();
    inner->set_nullable(true);
    inner->mutable_array_element()->set_leaf_type(
        proto::schema::DataType::Int32);
    auto nested = FieldId(scalar.get() + 1);
    schema->AddField(FieldMeta(FieldName("nested[items]"),
                               nested,
                               DataType::ARRAY,
                               DataType::ARRAY,
                               false,
                               true,
                               std::nullopt,
                               "",
                               LOCAL_FORMAT_RAW,
                               nested_type));
    auto vectors = FieldId(nested.get() + 1);
    schema->AddField(FieldMeta(FieldName("vectors[items]"),
                               vectors,
                               DataType::VECTOR_ARRAY,
                               DataType::VECTOR_FLOAT,
                               2,
                               knowhere::metric::L2,
                               false,
                               true));
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    InsertRecordProto insert;
    insert.set_num_rows(1);
    auto* pk_data = insert.add_fields_data();
    pk_data->set_field_id(pk.get());
    pk_data->set_type(proto::schema::DataType::Int64);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(1);

    auto* scalar_data = insert.add_fields_data();
    scalar_data->set_field_id(scalar.get());
    scalar_data->set_type(proto::schema::DataType::Array);
    auto* scalar_rows = scalar_data->mutable_scalars()->mutable_array_data();
    scalar_rows->set_element_type(proto::schema::DataType::Int64);
    auto* scalar_row = scalar_rows->add_data();
    for (int i = 0; i < 3; ++i) {
        scalar_row->mutable_long_data()->add_data(0);
        scalar_row->add_valid_data(false);
    }

    auto* nested_data = insert.add_fields_data();
    nested_data->set_field_id(nested.get());
    nested_data->set_type(proto::schema::DataType::Array);
    auto* nested_rows = nested_data->mutable_scalars()->mutable_array_data();
    nested_rows->set_element_type(proto::schema::DataType::Array);
    auto* nested_row = nested_rows->add_data();
    nested_row->mutable_array_data()->set_element_type(
        proto::schema::DataType::Int32);
    for (int i = 0; i < 2; ++i) {
        nested_row->mutable_array_data()->add_data()->mutable_int_data();
        nested_row->add_valid_data(false);
    }

    auto* vector_data = insert.add_fields_data();
    vector_data->set_field_id(vectors.get());
    vector_data->set_type(proto::schema::DataType::ArrayOfVector);
    vector_data->mutable_vectors()->set_dim(2);
    auto* vector_rows = vector_data->mutable_vectors()->mutable_vector_array();
    vector_rows->set_dim(2);
    vector_rows->set_element_type(proto::schema::DataType::FloatVector);
    auto* vector_row = vector_rows->add_data();
    vector_row->set_dim(2);
    vector_row->mutable_float_vector();
    for (int i = 0; i < 4; ++i) {
        vector_row->add_valid_data(false);
    }

    const int64_t row_id = 1;
    const Timestamp timestamp = 1;
    segment->Insert(segment->PreInsert(1),
                    1,
                    &row_id,
                    &timestamp,
                    std::make_shared<InsertRecordProto>(insert));
    for (auto [field_id, count] :
         {std::pair{scalar, 3}, std::pair{nested, 2}, std::pair{vectors, 4}}) {
        auto offsets = segment->GetStructElementOffsets(field_id);
        ASSERT_NE(offsets, nullptr);
        EXPECT_EQ(offsets->ElementIDRangeOfRow(0), std::make_pair(0, count));
    }
}

TEST(Growing, AddNativeListAndVectorArrayFieldsBackfillsNullRows) {
    auto old_schema = std::make_shared<Schema>();
    old_schema->set_schema_version(1);
    auto pk = old_schema->AddDebugField("pk", DataType::INT64);
    old_schema->set_primary_field_id(pk);

    auto new_schema = std::make_shared<Schema>();
    new_schema->set_schema_version(2);
    new_schema->AddField(
        FieldName("pk"), pk, DataType::INT64, false, std::nullopt);
    new_schema->set_primary_field_id(pk);
    auto vec = FieldId(pk.get() + 1);
    new_schema->AddField(FieldMeta(FieldName("vectors[items]"),
                                   vec,
                                   DataType::VECTOR_ARRAY,
                                   DataType::VECTOR_FLOAT,
                                   4,
                                   knowhere::metric::L2,
                                   true,
                                   true));
    auto native = FieldId(pk.get() + 2);
    new_schema->AddField(FieldName("scalars[items]"),
                         native,
                         DataType::ARRAY,
                         DataType::INT64,
                         true,
                         true);
    proto::schema::TypeSchema nested_type;
    nested_type.set_nullable(true);
    auto* inner = nested_type.mutable_array_element();
    inner->set_nullable(true);
    auto* leaf = inner->mutable_array_element();
    leaf->set_nullable(true);
    leaf->set_leaf_type(proto::schema::DataType::Int64);
    auto nested = FieldId(pk.get() + 3);
    new_schema->AddField(FieldMeta(FieldName("nested[items]"),
                                   nested,
                                   DataType::ARRAY,
                                   DataType::ARRAY,
                                   true,
                                   true,
                                   std::nullopt,
                                   "",
                                   LOCAL_FORMAT_RAW,
                                   nested_type));

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    const bool previous_mmap = mmap_config.GetEnableGrowingMmap();
    auto restore_mmap = std::shared_ptr<void>(nullptr, [&](void*) {
        mmap_config.growing_enable_mmap = previous_mmap;
    });
    std::array<int64_t, 3> offsets{0, 1, 2};
    for (bool enable_mmap : {false, true}) {
        mmap_config.growing_enable_mmap = enable_mmap;
        auto config = SegcoreConfig::default_config();
        config.set_chunk_rows(2);
        config.set_enable_interim_segment_index(false);
        auto segment =
            CreateGrowingSegment(old_schema, empty_index_meta, 1, config);
        auto dataset = DataGen(old_schema, offsets.size());
        auto start = segment->PreInsert(offsets.size());
        segment->Insert(start,
                        offsets.size(),
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        std::make_shared<InsertRecordProto>(*dataset.raw_));
        ASSERT_NO_THROW(segment->Reopen(new_schema));
        auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
        ASSERT_NE(growing, nullptr);
        EXPECT_EQ(growing->get_insert_record()
                      .get_data<milvus::VectorArray>(vec)
                      ->is_mmap(),
                  enable_mmap);
        EXPECT_EQ(growing->get_insert_record()
                      .get_data<ArrayValue>(native)
                      ->is_mmap(),
                  enable_mmap);
        EXPECT_TRUE(growing->get_insert_record()
                        .get_data<ArrayValue>(native)
                        ->view_element(0)
                        .is_null());
        EXPECT_TRUE(growing->get_insert_record()
                        .get_data<ArrayValue>(nested)
                        ->view_element(0)
                        .is_null());

        auto vector_result = segment->bulk_subscript(
            nullptr, vec, offsets.data(), offsets.size());
        auto scalar_result = segment->bulk_subscript(
            nullptr, native, offsets.data(), offsets.size());
        auto nested_result = segment->bulk_subscript(
            nullptr, nested, offsets.data(), offsets.size());
        for (const auto* result :
             {vector_result.get(), scalar_result.get(), nested_result.get()}) {
            const auto& valid = GetFieldDataRowValidData(*result);
            ASSERT_EQ(valid.size(), offsets.size());
            for (bool row_valid : valid) {
                EXPECT_FALSE(row_valid);
            }
        }
        const auto& vector_rows = vector_result->vectors().vector_array();
        ASSERT_EQ(vector_rows.data_size(), offsets.size());
        for (const auto& row : vector_rows.data()) {
            EXPECT_EQ(row.dim(), 4);
            EXPECT_EQ(row.data_case(), VectorFieldProto::kFloatVector);
            EXPECT_EQ(row.valid_data_size(), 0);
            EXPECT_TRUE(row.float_vector().data().empty());
            EXPECT_NO_THROW(milvus::VectorArray(row, true));
        }
        for (const auto* result : {scalar_result.get(), nested_result.get()}) {
            const auto& rows = result->scalars().array_data().data();
            ASSERT_EQ(rows.size(), offsets.size());
            for (const auto& row : rows) {
                EXPECT_EQ(row.data_case(), ScalarFieldProto::DATA_NOT_SET);
            }
        }
    }
}

TEST(Growing, LoadMissingStructElementOffsetsReturnsEmptyForOldRows) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto label =
        schema->AddDebugArrayField("chunks[label]", DataType::VARCHAR, true);
    auto score =
        schema->AddDebugArrayField("chunks[score]", DataType::INT32, true);

    constexpr int64_t row_count = 5;
    auto dataset = DataGen(schema, row_count);
    auto config = SegcoreConfig::default_config();
    auto segment = CreateGrowingWithFieldDataLoaded(
        schema,
        empty_index_meta,
        config,
        dataset,
        false,
        std::vector<int64_t>{label.get(), score.get()});

    auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing, nullptr);
    growing->FillAbsentFields();
    ASSERT_EQ(growing->get_row_count(), row_count);

    auto offsets = growing->GetStructElementOffsets(label);
    ASSERT_NE(offsets, nullptr);
    auto score_offsets = growing->GetStructElementOffsets(score);
    ASSERT_NE(score_offsets, nullptr);
    EXPECT_EQ(offsets.get(), score_offsets.get());
    EXPECT_EQ(offsets->GetRowCount(), row_count);
    EXPECT_EQ(offsets->GetTotalElementCount(), 0);
    for (int64_t i = 0; i <= row_count; ++i) {
        auto [start, end] = offsets->ElementIDRangeOfRow(i);
        EXPECT_EQ(start, 0);
        EXPECT_EQ(end, 0);
    }
}

TEST(Growing, LoadSelectsRealStructProviderBeforeBackfill) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto added = schema->AddDebugArrayField(
        "items[added]", DataType::INT32, false, true);
    auto source =
        schema->AddDebugArrayField("items[source]", DataType::INT32, false);
    // Cross the 4096-row recovery batch boundary.
    constexpr int64_t rows = 4097;
    auto dataset = DataGen(schema, rows, 42, 0, 1, 2);
    auto config = SegcoreConfig::default_config();
    auto segment =
        CreateGrowingWithFieldDataLoaded(schema,
                                         empty_index_meta,
                                         config,
                                         dataset,
                                         false,
                                         std::vector<int64_t>{added.get()});
    auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing, nullptr);
    growing->FillAbsentFields();
    auto offsets = growing->GetStructElementOffsets(added);
    ASSERT_NE(offsets, nullptr);
    EXPECT_EQ(offsets.get(), growing->GetStructElementOffsets(source).get());
    EXPECT_EQ(offsets->GetTotalElementCount(), rows * 2);
    for (int64_t i : {int64_t{0}, int64_t{4095}, int64_t{4096}}) {
        auto range = offsets->ElementIDRangeOfRow(i);
        EXPECT_EQ(range.second - range.first, 2);
    }
    const int64_t row_ids[] = {0, 4095, 4096};
    auto result = growing->bulk_subscript(nullptr, added, row_ids, 3);
    ASSERT_EQ(result->scalars().array_data().data_size(), 3);
    for (const auto& row : result->scalars().array_data().data()) {
        EXPECT_EQ(row.valid_data_size(), 2);
    }
}

class GrowingTest
    : public ::testing::TestWithParam<
          std::tuple</*index type*/ std::string, knowhere::MetricType>> {
 public:
    void
    SetUp() override {
        index_type = std::get<0>(GetParam());
        metric_type = std::get<1>(GetParam());
        if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT ||
            index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC) {
            data_type = DataType::VECTOR_FLOAT;
        } else if (index_type ==
                       knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
                   index_type == knowhere::IndexEnum::INDEX_SPARSE_WAND) {
            data_type = DataType::VECTOR_SPARSE_U32_F32;
        } else {
            ASSERT_TRUE(false);
        }
    }
    knowhere::MetricType metric_type;
    std::string index_type;
    DataType data_type;
};

INSTANTIATE_TEST_SUITE_P(
    FloatGrowingTest,
    GrowingTest,
    ::testing::Combine(
        ::testing::Values(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                          knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC),
        ::testing::Values(knowhere::metric::L2,
                          knowhere::metric::IP,
                          knowhere::metric::COSINE)));

INSTANTIATE_TEST_SUITE_P(
    SparseFloatGrowingTest,
    GrowingTest,
    ::testing::Combine(
        ::testing::Values(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                          knowhere::IndexEnum::INDEX_SPARSE_WAND),
        ::testing::Values(knowhere::metric::IP)));

TEST_P(GrowingTest, FillData) {
    auto schema = std::make_shared<Schema>();
    auto bool_field = schema->AddDebugField("bool", DataType::BOOL);
    auto int8_field = schema->AddDebugField("int8", DataType::INT8);
    auto int16_field = schema->AddDebugField("int16", DataType::INT16);
    auto int32_field = schema->AddDebugField("int32", DataType::INT32);
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto timestamptz_field =
        schema->AddDebugField("timestamptz", DataType::TIMESTAMPTZ);
    auto float_field = schema->AddDebugField("float", DataType::FLOAT);
    auto double_field = schema->AddDebugField("double", DataType::DOUBLE);
    auto varchar_field = schema->AddDebugField("varchar", DataType::VARCHAR);
    auto json_field = schema->AddDebugField("json", DataType::JSON);
    auto geometry_field = schema->AddDebugField("geometry", DataType::GEOMETRY);
    auto int_array_field =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_field =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_field =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto string_array_field = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    auto double_array_field = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto float_array_field =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto vec = schema->AddDebugField("embeddings", data_type, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", index_type},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);

        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        std::make_shared<InsertRecordProto>(*dataset.raw_));
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto bool_result = segment->bulk_subscript(
            nullptr, bool_field, ids_ds->GetIds(), num_inserted);
        auto int8_result = segment->bulk_subscript(
            nullptr, int8_field, ids_ds->GetIds(), num_inserted);
        auto int16_result = segment->bulk_subscript(
            nullptr, int16_field, ids_ds->GetIds(), num_inserted);
        auto int32_result = segment->bulk_subscript(
            nullptr, int32_field, ids_ds->GetIds(), num_inserted);
        auto int64_result = segment->bulk_subscript(
            nullptr, int64_field, ids_ds->GetIds(), num_inserted);
        auto float_result = segment->bulk_subscript(
            nullptr, float_field, ids_ds->GetIds(), num_inserted);
        auto double_result = segment->bulk_subscript(
            nullptr, double_field, ids_ds->GetIds(), num_inserted);
        auto timestamptz_result = segment->bulk_subscript(
            nullptr, timestamptz_field, ids_ds->GetIds(), num_inserted);
        auto varchar_result = segment->bulk_subscript(
            nullptr, varchar_field, ids_ds->GetIds(), num_inserted);
        auto json_result = segment->bulk_subscript(
            nullptr, json_field, ids_ds->GetIds(), num_inserted);
        auto geometry_result = segment->bulk_subscript(
            nullptr, geometry_field, ids_ds->GetIds(), num_inserted);
        auto int_array_result = segment->bulk_subscript(
            nullptr, int_array_field, ids_ds->GetIds(), num_inserted);
        auto long_array_result = segment->bulk_subscript(
            nullptr, long_array_field, ids_ds->GetIds(), num_inserted);
        auto bool_array_result = segment->bulk_subscript(
            nullptr, bool_array_field, ids_ds->GetIds(), num_inserted);
        auto string_array_result = segment->bulk_subscript(
            nullptr, string_array_field, ids_ds->GetIds(), num_inserted);
        auto double_array_result = segment->bulk_subscript(
            nullptr, double_array_field, ids_ds->GetIds(), num_inserted);
        auto float_array_result = segment->bulk_subscript(
            nullptr, float_array_field, ids_ds->GetIds(), num_inserted);
        auto vec_result = segment->bulk_subscript(
            nullptr, vec, ids_ds->GetIds(), num_inserted);
        // checking result data
        EXPECT_EQ(bool_result->scalars().bool_data().data_size(), num_inserted);
        EXPECT_EQ(int8_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int16_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int32_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int64_result->scalars().long_data().data_size(),
                  num_inserted);
        EXPECT_EQ(timestamptz_result->scalars().timestamptz_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_result->scalars().float_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_result->scalars().double_data().data_size(),
                  num_inserted);
        EXPECT_EQ(varchar_result->scalars().string_data().data_size(),
                  num_inserted);
        EXPECT_EQ(json_result->scalars().json_data().data_size(), num_inserted);
        EXPECT_EQ(geometry_result->scalars().geometry_data().data_size(),
                  num_inserted);
        if (data_type == DataType::VECTOR_FLOAT) {
            EXPECT_EQ(vec_result->vectors().float_vector().data_size(),
                      num_inserted * dim);
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            EXPECT_EQ(
                vec_result->vectors().sparse_float_vector().contents_size(),
                num_inserted);
        } else {
            ASSERT_TRUE(false);
        }
        EXPECT_EQ(int_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(long_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(bool_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(string_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_array_result->scalars().array_data().data_size(),
                  num_inserted);

        EXPECT_EQ(GetFieldDataRowValidData(*bool_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*int8_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*int16_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*int32_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*int64_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*float_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*double_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*timestamptz_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*varchar_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*json_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*int_array_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*long_array_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*bool_array_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*string_array_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*double_array_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*float_array_result).size(), 0);
    }
}

// load_field_data_common addresses column data, the interim index, timestamps
// and row ids by the offset PreInsert reserved. Validity has to use the same
// offset: ConcurrentVectorImpl::set_data_raw reads the range back through
// bulk_is_valid_range(element_offset, ...) to lay out a compact nullable
// column, so the two schemes disagreeing puts the null flags on the wrong rows.
//
// Appending at the bitmap's current length happens to agree with the reserved
// offset whenever the two coincide, which is every production caller today
// (Load runs one PreInsert against an empty bitmap). These two cases are the
// ones where they do not.
TEST(Growing, LoadFieldDataAddressesValidityByReservedOffset) {
    constexpr int64_t row_count = 4;

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto nullable = schema->AddDebugField("nullable", DataType::INT64, true);
    const auto& field_meta = (*schema)[nullable];

    const std::array<int64_t, row_count> values = {10, 11, 12, 13};
    auto make_batch = [&](const std::array<bool, row_count>& valid) {
        auto array = CreateDataArrayFrom(
            values.data(), valid.data(), row_count, field_meta);
        return CreateFieldDataFromDataArray(row_count, array.get(), field_meta);
    };
    const std::array<bool, row_count> first = {true, false, true, false};
    const std::array<bool, row_count> second = {false, true, false, true};

    auto bits_of = [](const SegmentGrowingImpl& segment, FieldId field_id) {
        auto bits =
            segment.get_insert_record().get_valid_data(field_id)->get_data();
        return std::vector<bool>(bits.begin(), bits.end());
    };

    // A reload of a range already written rewrites it in place. Appending would
    // put the second batch at [row_count, 2 * row_count) instead, doubling the
    // bitmap and leaving the rows themselves flagged by the stale batch.
    {
        auto owned = CreateGrowingSegment(schema, empty_index_meta);
        auto* segment = dynamic_cast<SegmentGrowingImpl*>(owned.get());
        ASSERT_NE(segment, nullptr);

        ASSERT_EQ(segment->PreInsert(row_count), 0);
        segment->load_field_data_common(
            nullable, 0, {make_batch(first)}, pk, row_count);
        EXPECT_EQ(bits_of(*segment, nullable),
                  (std::vector<bool>{true, false, true, false}));

        segment->load_field_data_common(
            nullable, 0, {make_batch(second)}, pk, row_count);
        EXPECT_EQ(bits_of(*segment, nullable),
                  (std::vector<bool>{false, true, false, true}));
    }

    // A batch whose reserved offset runs past the bitmap fails where the
    // misalignment happens. Appending would silently write this batch's flags
    // over the earlier rows and shift every row after them.
    {
        auto owned = CreateGrowingSegment(schema, empty_index_meta);
        auto* segment = dynamic_cast<SegmentGrowingImpl*>(owned.get());
        ASSERT_NE(segment, nullptr);

        // Reserve a range and never write it, so the bitmap falls behind.
        ASSERT_EQ(segment->PreInsert(row_count), 0);
        ASSERT_EQ(segment->PreInsert(row_count), row_count);
        EXPECT_THROW(
            segment->load_field_data_common(
                nullable, row_count, {make_batch(first)}, pk, row_count),
            SegcoreError);
        EXPECT_TRUE(
            segment->get_insert_record().get_valid_data(nullable)->empty());
    }
}

// pk2offset_ maps a primary key to the row offset every other growing
// structure addresses that row by. A batch loaded at a reserved offset has to
// register its rows there. Indexing from 0 instead would point queries and
// deletes at rows belonging to an earlier batch -- silently, with no assert to
// catch it, which is why this one is worth a test of its own.
TEST(Growing, LoadFieldDataAddressesPkOffsetsByReservedOffset) {
    constexpr int64_t batch_rows = 2;

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);

    auto owned = CreateGrowingSegment(schema, empty_index_meta);
    auto* segment = dynamic_cast<SegmentGrowingImpl*>(owned.get());
    ASSERT_NE(segment, nullptr);

    auto make_int64_batch = [](const std::vector<int64_t>& values) {
        auto field_data = milvus::storage::CreateFieldData(
            DataType::INT64,
            DataType::NONE,
            false,
            1,
            static_cast<int64_t>(values.size()));
        field_data->FillFieldData(values.data(),
                                  static_cast<ssize_t>(values.size()));
        return field_data;
    };

    // Two loads, each reserving its own logical range. Timestamps go through
    // the same entry point so search_pk has something to compare against.
    const std::vector<std::vector<int64_t>> pk_batches = {{100, 101},
                                                          {200, 201}};
    for (int64_t batch = 0; batch < 2; ++batch) {
        auto reserved = segment->PreInsert(batch_rows);
        ASSERT_EQ(reserved, batch * batch_rows);
        segment->load_field_data_common(TimestampFieldID,
                                        reserved,
                                        {make_int64_batch({1, 1})},
                                        pk,
                                        batch_rows);
        segment->load_field_data_common(pk,
                                        reserved,
                                        {make_int64_batch(pk_batches[batch])},
                                        pk,
                                        batch_rows);
    }

    auto offsets_for = [&](int64_t pk_value) {
        std::vector<int64_t> out;
        for (auto offset : segment->get_insert_record().search_pk(
                 PkType(pk_value), Timestamp(100))) {
            out.push_back(offset.get());
        }
        return out;
    };

    // The second batch owns rows 2 and 3. Appending from 0 would hand it rows
    // 0 and 1, which the first batch already owns.
    EXPECT_EQ(offsets_for(100), (std::vector<int64_t>{0}));
    EXPECT_EQ(offsets_for(101), (std::vector<int64_t>{1}));
    EXPECT_EQ(offsets_for(200), (std::vector<int64_t>{2}));
    EXPECT_EQ(offsets_for(201), (std::vector<int64_t>{3}));
}

TEST(Growing, FillNullableData) {
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
    auto bool_field = schema->AddDebugField("bool", DataType::BOOL, true);
    auto int8_field = schema->AddDebugField("int8", DataType::INT8, true);
    auto int16_field = schema->AddDebugField("int16", DataType::INT16, true);
    auto int32_field = schema->AddDebugField("int32", DataType::INT32, true);
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto float_field = schema->AddDebugField("float", DataType::FLOAT, true);
    auto double_field = schema->AddDebugField("double", DataType::DOUBLE, true);
    auto timestamptz_field =
        schema->AddDebugField("timestamptz", DataType::TIMESTAMPTZ, true);
    auto varchar_field =
        schema->AddDebugField("varchar", DataType::VARCHAR, true);
    auto json_field = schema->AddDebugField("json", DataType::JSON, true);
    auto int_array_field = schema->AddDebugField(
        "int_array", DataType::ARRAY, DataType::INT8, true);
    auto long_array_field = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    auto bool_array_field = schema->AddDebugField(
        "bool_array", DataType::ARRAY, DataType::BOOL, true);
    auto string_array_field = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR, true);
    auto double_array_field = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE, true);
    auto float_array_field = schema->AddDebugField(
        "float_array", DataType::ARRAY, DataType::FLOAT, true);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);

        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        std::make_shared<InsertRecordProto>(*dataset.raw_));
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto bool_result = segment->bulk_subscript(
            nullptr, bool_field, ids_ds->GetIds(), num_inserted);
        auto int8_result = segment->bulk_subscript(
            nullptr, int8_field, ids_ds->GetIds(), num_inserted);
        auto int16_result = segment->bulk_subscript(
            nullptr, int16_field, ids_ds->GetIds(), num_inserted);
        auto int32_result = segment->bulk_subscript(
            nullptr, int32_field, ids_ds->GetIds(), num_inserted);
        auto int64_result = segment->bulk_subscript(
            nullptr, int64_field, ids_ds->GetIds(), num_inserted);
        auto float_result = segment->bulk_subscript(
            nullptr, float_field, ids_ds->GetIds(), num_inserted);
        auto double_result = segment->bulk_subscript(
            nullptr, double_field, ids_ds->GetIds(), num_inserted);
        auto timestamptz_result = segment->bulk_subscript(
            nullptr, timestamptz_field, ids_ds->GetIds(), num_inserted);
        auto varchar_result = segment->bulk_subscript(
            nullptr, varchar_field, ids_ds->GetIds(), num_inserted);
        auto json_result = segment->bulk_subscript(
            nullptr, json_field, ids_ds->GetIds(), num_inserted);
        auto int_array_result = segment->bulk_subscript(
            nullptr, int_array_field, ids_ds->GetIds(), num_inserted);
        auto long_array_result = segment->bulk_subscript(
            nullptr, long_array_field, ids_ds->GetIds(), num_inserted);
        auto bool_array_result = segment->bulk_subscript(
            nullptr, bool_array_field, ids_ds->GetIds(), num_inserted);
        auto string_array_result = segment->bulk_subscript(
            nullptr, string_array_field, ids_ds->GetIds(), num_inserted);
        auto double_array_result = segment->bulk_subscript(
            nullptr, double_array_field, ids_ds->GetIds(), num_inserted);
        auto float_array_result = segment->bulk_subscript(
            nullptr, float_array_field, ids_ds->GetIds(), num_inserted);
        auto vec_result = segment->bulk_subscript(
            nullptr, vec, ids_ds->GetIds(), num_inserted);

        EXPECT_EQ(bool_result->scalars().bool_data().data_size(), num_inserted);
        EXPECT_EQ(int8_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int16_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int32_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int64_result->scalars().long_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_result->scalars().float_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_result->scalars().double_data().data_size(),
                  num_inserted);
        EXPECT_EQ(timestamptz_result->scalars().timestamptz_data().data_size(),
                  num_inserted);
        EXPECT_EQ(varchar_result->scalars().string_data().data_size(),
                  num_inserted);
        EXPECT_EQ(json_result->scalars().json_data().data_size(), num_inserted);
        EXPECT_EQ(vec_result->vectors().float_vector().data_size(),
                  num_inserted * dim);
        EXPECT_EQ(int_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(long_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(bool_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(string_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*bool_result).size(), num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*int8_result).size(), num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*int16_result).size(), num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*int32_result).size(), num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*float_result).size(), num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*double_result).size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*timestamptz_result).size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*varchar_result).size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*json_result).size(), num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*int_array_result).size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*long_array_result).size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*bool_array_result).size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*string_array_result).size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*double_array_result).size(),
                  num_inserted);
        EXPECT_EQ(GetFieldDataRowValidData(*float_array_result).size(),
                  num_inserted);
    }
}

class GrowingNullableTest : public ::testing::TestWithParam<
                                std::tuple</*data_type*/ DataType,
                                           /*metric_type*/ knowhere::MetricType,
                                           /*index_type*/ std::string,
                                           /*null_percent*/ int,
                                           /*enable_interim_index*/ bool,
                                           /*use_iterator*/ bool>> {
 public:
    void
    SetUp() override {
        std::tie(data_type,
                 metric_type,
                 index_type,
                 null_percent,
                 enable_interim_index,
                 use_iterator) = GetParam();
    }

    DataType data_type;
    knowhere::MetricType metric_type;
    std::string index_type;
    int null_percent;
    bool enable_interim_index;
    bool use_iterator;
};

static std::vector<
    std::tuple<DataType, knowhere::MetricType, std::string, int, bool, bool>>
GenerateGrowingNullableTestParams() {
    std::vector<
        std::
            tuple<DataType, knowhere::MetricType, std::string, int, bool, bool>>
        params;

    // Dense float vectors with IVF_FLAT
    std::vector<std::tuple<DataType, knowhere::MetricType, std::string>>
        base_configs = {
            {DataType::VECTOR_FLOAT,
             knowhere::metric::L2,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
            {DataType::VECTOR_FLOAT,
             knowhere::metric::IP,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
            {DataType::VECTOR_FLOAT,
             knowhere::metric::COSINE,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
            {DataType::VECTOR_SPARSE_U32_F32,
             knowhere::metric::IP,
             knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX},
        };

    std::vector<int> null_percents = {0, 20, 100};

    std::vector<bool> interim_index_configs = {true, false};

    std::vector<bool> iterator_configs = {false, true};

    for (const auto& [dtype, metric, idx_type] : base_configs) {
        for (int null_pct : null_percents) {
            for (bool enable_interim : interim_index_configs) {
                for (bool use_iter : iterator_configs) {
                    // Skip iterator for sparse vectors (not supported)
                    if (use_iter && dtype == DataType::VECTOR_SPARSE_U32_F32) {
                        continue;
                    }
                    params.push_back({dtype,
                                      metric,
                                      idx_type,
                                      null_pct,
                                      enable_interim,
                                      use_iter});
                }
            }
        }
    }
    return params;
}

INSTANTIATE_TEST_SUITE_P(
    NullableVectorParameters,
    GrowingNullableTest,
    ::testing::ValuesIn(GenerateGrowingNullableTestParams()));

TEST_P(GrowingNullableTest, SearchAndQueryNullableVectors) {
    using namespace milvus::query;

    bool nullable = true;

    auto schema = std::make_shared<Schema>();
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    int64_t dim = 8;
    auto vec = schema->AddDebugField(
        "embeddings", data_type, dim, metric_type, nullable);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params;
    std::map<std::string, std::string> type_params;
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        index_params = {{"index_type", index_type},
                        {"metric_type", metric_type}};
        type_params = {};
    } else {
        index_params = {{"index_type", index_type},
                        {"metric_type", metric_type},
                        {"nlist", "128"}};
        type_params = {{"dim", std::to_string(dim)}};
    }
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(enable_interim_index);
    // Explicitly set interim index type to avoid contamination from other tests
    config.set_dense_vector_intermin_index_type(
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t batch_size = 2000;
    int64_t num_rounds = 10;
    int64_t topk = 5;
    // Iterator only supports single query
    int64_t num_queries = use_iterator ? 1 : 2;
    Timestamp timestamp = 10000000;

    // Prepare search plan using ScopedSchemaHandle
    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    std::vector<char> plan_str;
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        plan_str = schema_handle.ParseSearch(
            "",                               // expression (no filter)
            "embeddings",                     // vector field name
            topk,                             // topk
            metric_type,                      // metric_type
            R"({"drop_ratio_search": 0.1})",  // search_params
            3);                               // round_decimal
    } else if (use_iterator) {
        plan_str = schema_handle.ParseSearchIterator(
            "",                           // expression (no filter)
            "embeddings",                 // vector field name
            topk,                         // topk
            metric_type,                  // metric_type
            R"({"nprobe": 10})",          // search_params
            static_cast<uint32_t>(topk),  // batch_size
            "",                           // token (empty)
            std::nullopt,                 // last_bound (none)
            3);                           // round_decimal
    } else {
        plan_str =
            schema_handle.ParseSearch("",            // expression (no filter)
                                      "embeddings",  // vector field name
                                      topk,          // topk
                                      metric_type,   // metric_type
                                      R"({"nprobe": 10})",  // search_params
                                      3);                   // round_decimal
    }
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    // Create query vectors
    proto::common::PlaceholderGroup ph_group_raw;
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        ph_group_raw = CreateSparseFloatPlaceholderGroup(num_queries, 42);
    } else {
        auto query_data = generate_float_vector(num_queries, dim);
        ph_group_raw =
            CreatePlaceholderGroupFromBlob(num_queries, dim, query_data.data());
    }

    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    // Store all inserted data for verification
    // For nullable vectors, data is stored sparsely (only valid vectors)
    // We need a mapping from logical offset to physical offset
    std::vector<float> all_float_vectors;  // Physical storage (only valid)
    std::vector<knowhere::sparse::SparseRow<float>> all_sparse_vectors;
    std::vector<bool> all_valid_data;  // Logical storage (all rows)
    std::vector<int64_t>
        logical_to_physical;  // Maps logical offset to physical

    // Insert data in multiple rounds and test after each round
    for (int64_t round = 0; round < num_rounds; round++) {
        int64_t total_rows = (round + 1) * batch_size;
        int64_t expected_valid_count =
            total_rows - (total_rows * null_percent / 100);

        auto dataset = DataGen(schema,
                               batch_size,
                               42 + round,
                               0,
                               1,
                               10,
                               1,
                               false,
                               true,
                               false,
                               null_percent);

        // Build logical to physical mapping for this batch
        int64_t base_physical = all_float_vectors.size() / dim;
        if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            base_physical = all_sparse_vectors.size();
        }

        auto valid_data_from_dataset = dataset.get_col_valid(vec);
        int64_t physical_idx = base_physical;
        for (size_t i = 0; i < valid_data_from_dataset.size(); i++) {
            if (valid_data_from_dataset[i]) {
                logical_to_physical.push_back(physical_idx);
                physical_idx++;
            } else {
                logical_to_physical.push_back(-1);  // null
            }
        }

        // Get original data directly from proto (sparse storage for nullable)
        // Data is stored sparsely - only valid vectors are in the proto
        if (data_type == DataType::VECTOR_FLOAT) {
            auto field_data = dataset.get_col(vec);
            auto& float_data = field_data->vectors().float_vector().data();
            all_float_vectors.insert(
                all_float_vectors.end(), float_data.begin(), float_data.end());
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            auto field_data = dataset.get_col(vec);
            auto& sparse_array = field_data->vectors().sparse_float_vector();
            for (int i = 0; i < sparse_array.contents_size(); i++) {
                auto& content = sparse_array.contents(i);
                auto row = CopyAndWrapSparseRow(content.data(), content.size());
                all_sparse_vectors.push_back(std::move(row));
            }
        }
        all_valid_data.insert(all_valid_data.end(),
                              valid_data_from_dataset.begin(),
                              valid_data_from_dataset.end());

        auto offset = segment->PreInsert(batch_size);
        segment->Insert(offset,
                        batch_size,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        std::make_shared<InsertRecordProto>(*dataset.raw_));

        auto& insert_record = segment->get_insert_record();
        ASSERT_TRUE(insert_record.is_valid_data_exist(vec));

        auto valid_data_ptr = insert_record.get_data_base(vec);
        auto valid_data = valid_data_ptr->get_valid_data();

        // Test search
        auto sr =
            segment_growing->Search(plan.get(), ph_group.get(), timestamp);

        ASSERT_EQ(sr->total_nq_, num_queries);
        ASSERT_EQ(sr->unity_topK_, topk);

        if (expected_valid_count == 0) {
            auto total_results = sr->get_total_result_count();
            EXPECT_EQ(total_results, 0)
                << "Round " << round
                << ": 100% null should return 0 results, but got "
                << total_results;
        } else {
            // Verify search results don't contain null vectors
            for (size_t i = 0; i < sr->seg_offsets_.size(); i++) {
                auto seg_offset = sr->seg_offsets_[i];
                if (seg_offset < 0) {
                    continue;
                }
                ASSERT_TRUE(valid_data[seg_offset])
                    << "Round " << round
                    << ": Search returned null vector at offset " << seg_offset;
            }
        }

        auto vec_result = segment->bulk_subscript(
            nullptr, vec, sr->seg_offsets_.data(), sr->seg_offsets_.size());
        ASSERT_TRUE(vec_result != nullptr);

        if (data_type == DataType::VECTOR_FLOAT) {
            auto& float_data = vec_result->vectors().float_vector();
            size_t valid_idx = 0;
            for (size_t i = 0; i < sr->seg_offsets_.size(); i++) {
                auto offset = sr->seg_offsets_[i];
                if (offset < 0) {
                    continue;  // Skip invalid offsets
                }
                auto physical_idx = logical_to_physical[offset];
                for (int d = 0; d < dim; d++) {
                    float expected_val =
                        all_float_vectors[physical_idx * dim + d];
                    float actual_val = float_data.data(valid_idx * dim + d);
                    ASSERT_FLOAT_EQ(expected_val, actual_val)
                        << "Round " << round << ": Mismatch at logical offset "
                        << offset << " dim " << d;
                }
                valid_idx++;
            }
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            auto& sparse_data = vec_result->vectors().sparse_float_vector();
            size_t valid_idx = 0;
            for (size_t i = 0; i < sr->seg_offsets_.size(); i++) {
                auto offset = sr->seg_offsets_[i];
                if (offset < 0) {
                    continue;  // Skip invalid offsets
                }
                auto physical_idx = logical_to_physical[offset];
                auto& content = sparse_data.contents(valid_idx);
                auto retrieved_row =
                    CopyAndWrapSparseRow(content.data(), content.size());
                const auto& expected_row = all_sparse_vectors[physical_idx];
                ASSERT_EQ(retrieved_row.size(), expected_row.size())
                    << "Round " << round
                    << ": Sparse vector size mismatch at logical offset "
                    << offset;
                for (size_t j = 0; j < retrieved_row.size(); j++) {
                    ASSERT_EQ(retrieved_row[j].id, expected_row[j].id)
                        << "Round " << round
                        << ": Sparse vector id mismatch at logical offset "
                        << offset << " element " << j;
                    ASSERT_FLOAT_EQ(retrieved_row[j].val, expected_row[j].val)
                        << "Round " << round
                        << ": Sparse vector val mismatch at logical offset "
                        << offset << " element " << j;
                }
                valid_idx++;
            }
        }
    }
}

TEST_P(GrowingTest, FillVectorArrayData) {
    auto schema = std::make_shared<Schema>();
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto array_float_vector = schema->AddDebugVectorArrayField(
        "array_float_vector", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);

        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        std::make_shared<InsertRecordProto>(*dataset.raw_));
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto int64_result = segment->bulk_subscript(
            nullptr, int64_field, ids_ds->GetIds(), num_inserted);
        auto array_float_vector_result = segment->bulk_subscript(
            nullptr, array_float_vector, ids_ds->GetIds(), num_inserted);

        EXPECT_EQ(int64_result->scalars().long_data().data_size(),
                  num_inserted);
        EXPECT_EQ(
            array_float_vector_result->vectors().vector_array().data_size(),
            num_inserted);

        if (i == 0) {
            // Verify vector array data
            auto verify_float_vectors = [](auto arr1, auto arr2) {
                static constexpr float EPSILON = 1e-6;
                EXPECT_EQ(arr1.size(), arr2.size());
                for (int64_t i = 0; i < arr1.size(); ++i) {
                    EXPECT_NEAR(arr1[i], arr2[i], EPSILON);
                }
            };

            auto array_vec_values =
                dataset.get_col<VectorFieldProto>(array_float_vector);
            for (int64_t i = 0; i < per_batch; ++i) {
                auto arrow_array = array_float_vector_result->vectors()
                                       .vector_array()
                                       .data()[i]
                                       .float_vector()
                                       .data();
                auto expected_array =
                    array_vec_values[ids_ds->GetIds()[i]].float_vector().data();
                verify_float_vectors(arrow_array, expected_array);
            }
        }

        EXPECT_EQ(GetFieldDataRowValidData(*int64_result).size(), 0);
        EXPECT_EQ(GetFieldDataRowValidData(*array_float_vector_result).size(),
                  0);
    }
}

TEST(GrowingTest, EmptyVectorArrayRowsInitializeElementOneof) {
    auto schema = std::make_shared<Schema>();
    auto array_float_vector =
        schema->AddDebugVectorArrayField("array_float_vector",
                                         DataType::VECTOR_FLOAT,
                                         8,
                                         knowhere::metric::L2,
                                         true);

    auto data =
        CreateEmptyVectorDataArray(2, schema->operator[](array_float_vector));

    ASSERT_EQ(data->vectors().vector_array().data_size(), 2);
    for (const auto& row : data->vectors().vector_array().data()) {
        EXPECT_EQ(row.dim(), 8);
        EXPECT_EQ(row.data_case(),
                  proto::schema::VectorField::DataCase::kFloatVector);
        EXPECT_TRUE(row.float_vector().data().empty());
        EXPECT_NO_THROW(milvus::VectorArray(row, false));
    }
}

TEST(GrowingTest, QueryNullableVectorArrayUsesPhysicalOffsets) {
    auto schema = std::make_shared<Schema>();
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto array_float_vector =
        schema->AddDebugVectorArrayField("array_float_vector",
                                         DataType::VECTOR_FLOAT,
                                         4,
                                         knowhere::metric::L2,
                                         true);
    schema->set_primary_field_id(int64_field);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(false);
    auto segment_growing =
        CreateGrowingSegment(schema, empty_index_meta, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    ASSERT_NE(segment, nullptr);

    auto insert_record_proto = std::make_shared<InsertRecordProto>();
    insert_record_proto->set_num_rows(2);

    auto pk_data = insert_record_proto->add_fields_data();
    pk_data->set_field_id(int64_field.get());
    pk_data->set_type(proto::schema::DataType::Int64);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(10);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(11);

    auto array_data = insert_record_proto->add_fields_data();
    array_data->set_field_id(array_float_vector.get());
    array_data->set_type(proto::schema::DataType::ArrayOfVector);
    array_data->mutable_vectors()->set_dim(4);
    array_data->mutable_vectors()->add_valid_data(false);
    array_data->mutable_vectors()->add_valid_data(true);
    auto vector_array = array_data->mutable_vectors()->mutable_vector_array();
    vector_array->set_dim(4);
    vector_array->set_element_type(proto::schema::DataType::FloatVector);
    auto valid_row = vector_array->add_data();
    valid_row->set_dim(4);
    for (auto value : {1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F}) {
        valid_row->mutable_float_vector()->add_data(value);
    }

    std::vector<int64_t> row_ids = {0, 1};
    std::vector<Timestamp> timestamps = {100, 101};
    auto offset = segment->PreInsert(2);
    segment->Insert(
        offset, 2, row_ids.data(), timestamps.data(), insert_record_proto);

    std::array<int64_t, 2> offsets = {0, 1};
    std::unique_ptr<DataArray> result;
    EXPECT_NO_THROW(
        result = segment->bulk_subscript(
            nullptr, array_float_vector, offsets.data(), offsets.size()));
    ASSERT_NE(result, nullptr);

    const auto& result_valid_data = GetFieldDataRowValidData(*result);
    ASSERT_EQ(result_valid_data.size(), 2);
    EXPECT_FALSE(result_valid_data[0]);
    EXPECT_TRUE(result_valid_data[1]);
    ASSERT_EQ(result->vectors().vector_array().data_size(), 2);
    EXPECT_TRUE(
        result->vectors().vector_array().data(0).float_vector().data().empty());
    const auto& row = result->vectors().vector_array().data(1);
    ASSERT_EQ(row.float_vector().data_size(), 8);
    EXPECT_FLOAT_EQ(row.float_vector().data(0), 1.0F);
    EXPECT_FLOAT_EQ(row.float_vector().data(7), 8.0F);
}

TEST(GrowingTest, QueryNullableVectorArrayStoresRowDenseInputByValidData) {
    auto schema = std::make_shared<Schema>();
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto array_float_vector =
        schema->AddDebugVectorArrayField("array_float_vector",
                                         DataType::VECTOR_FLOAT,
                                         4,
                                         knowhere::metric::L2,
                                         true);
    schema->set_primary_field_id(int64_field);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(false);
    auto segment_growing =
        CreateGrowingSegment(schema, empty_index_meta, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    ASSERT_NE(segment, nullptr);

    auto insert_record_proto = std::make_shared<InsertRecordProto>();
    insert_record_proto->set_num_rows(2);

    auto pk_data = insert_record_proto->add_fields_data();
    pk_data->set_field_id(int64_field.get());
    pk_data->set_type(proto::schema::DataType::Int64);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(10);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(11);

    auto array_data = insert_record_proto->add_fields_data();
    array_data->set_field_id(array_float_vector.get());
    array_data->set_type(proto::schema::DataType::ArrayOfVector);
    array_data->mutable_vectors()->set_dim(4);
    array_data->mutable_vectors()->add_valid_data(false);
    array_data->mutable_vectors()->add_valid_data(true);
    auto vector_array = array_data->mutable_vectors()->mutable_vector_array();
    vector_array->set_dim(4);
    vector_array->set_element_type(proto::schema::DataType::FloatVector);
    auto null_row = vector_array->add_data();
    null_row->set_dim(4);
    null_row->mutable_float_vector();
    auto valid_row = vector_array->add_data();
    valid_row->set_dim(4);
    for (auto value : {1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F, 7.0F, 8.0F}) {
        valid_row->mutable_float_vector()->add_data(value);
    }

    std::vector<int64_t> row_ids = {0, 1};
    std::vector<Timestamp> timestamps = {100, 101};
    auto offset = segment->PreInsert(2);
    segment->Insert(
        offset, 2, row_ids.data(), timestamps.data(), insert_record_proto);

    std::array<int64_t, 2> offsets = {0, 1};
    std::unique_ptr<DataArray> result;
    EXPECT_NO_THROW(
        result = segment->bulk_subscript(
            nullptr, array_float_vector, offsets.data(), offsets.size()));
    ASSERT_NE(result, nullptr);

    const auto& result_valid_data = GetFieldDataRowValidData(*result);
    ASSERT_EQ(result_valid_data.size(), 2);
    EXPECT_FALSE(result_valid_data[0]);
    EXPECT_TRUE(result_valid_data[1]);
    ASSERT_EQ(result->vectors().vector_array().data_size(), 2);
    EXPECT_TRUE(
        result->vectors().vector_array().data(0).float_vector().data().empty());
    const auto& row = result->vectors().vector_array().data(1);
    ASSERT_EQ(row.float_vector().data_size(), 8);
    EXPECT_FLOAT_EQ(row.float_vector().data(0), 1.0F);
    EXPECT_FLOAT_EQ(row.float_vector().data(7), 8.0F);
}

TEST(GrowingTest, VectorArrayGrowingMmapMatchesHeap) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto plain = schema->AddDebugVectorArrayField(
        "chunks[plain]", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto nullable = schema->AddDebugVectorArrayField("other[nullable]",
                                                     DataType::VECTOR_FLOAT,
                                                     4,
                                                     knowhere::metric::L2,
                                                     true,
                                                     true);
    schema->set_primary_field_id(pk);

    auto insert = std::make_unique<InsertRecordProto>();
    insert->set_num_rows(5);
    auto* pk_data = insert->add_fields_data();
    pk_data->set_field_id(pk.get());
    pk_data->set_type(proto::schema::DataType::Int64);
    for (int64_t i = 0; i < 5; ++i) {
        pk_data->mutable_scalars()->mutable_long_data()->add_data(i);
    }
    auto* plain_data = insert->add_fields_data();
    plain_data->set_field_id(plain.get());
    plain_data->set_type(proto::schema::DataType::ArrayOfVector);
    plain_data->mutable_vectors()->set_dim(4);
    auto* plain_array = plain_data->mutable_vectors()->mutable_vector_array();
    plain_array->set_dim(4);
    plain_array->set_element_type(proto::schema::DataType::FloatVector);
    auto* nullable_data = insert->add_fields_data();
    nullable_data->set_field_id(nullable.get());
    nullable_data->set_type(proto::schema::DataType::ArrayOfVector);
    nullable_data->mutable_vectors()->set_dim(4);
    auto* nullable_array =
        nullable_data->mutable_vectors()->mutable_vector_array();
    nullable_array->set_dim(4);
    nullable_array->set_element_type(proto::schema::DataType::FloatVector);

    const std::array<std::vector<float>, 5> plain_values{
        {{1, 0, 0, 0, 0, 1, 0, 0},
         {},
         {0, 0, 1, 0},
         {0, 0, 0, 1},
         {1, 1, 0, 0}}};
    const std::array<std::vector<float>, 5> nullable_values{
        {{1, 0, 0, 0, 0, 1, 0, 0}, {}, {}, {}, {0, 0, 1, 0}}};
    const std::array<std::vector<bool>, 5> element_valid{
        {{true, false, true}, {}, {}, {false}, {true}}};
    for (int i = 0; i < 5; ++i) {
        auto* plain_row = plain_array->add_data();
        plain_row->set_dim(4);
        plain_row->mutable_float_vector()->mutable_data()->Add(
            plain_values[i].begin(), plain_values[i].end());

        nullable_data->mutable_vectors()->add_valid_data(i != 1);
        auto* nullable_row = nullable_array->add_data();
        nullable_row->set_dim(4);
        nullable_row->mutable_float_vector()->mutable_data()->Add(
            nullable_values[i].begin(), nullable_values[i].end());
        for (bool valid : element_valid[i]) {
            nullable_row->add_valid_data(valid);
        }
    }

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    const bool previous_mmap = mmap_config.GetEnableGrowingMmap();
    auto restore_mmap = std::shared_ptr<void>(nullptr, [&](void*) {
        mmap_config.growing_enable_mmap = previous_mmap;
    });
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(2);
    config.set_enable_interim_segment_index(false);

    std::array<int64_t, 5> offsets{0, 1, 2, 3, 4};
    std::array<int64_t, 5> row_ids{0, 1, 2, 3, 4};
    std::array<Timestamp, 5> timestamps{100, 100, 100, 100, 100};
    std::string heap_plain;
    std::string heap_nullable;
    std::vector<int64_t> heap_element_offsets;
    std::vector<int32_t> heap_element_indices;
    std::vector<float> heap_distances;
    for (bool enable_mmap : {false, true}) {
        mmap_config.growing_enable_mmap = enable_mmap;
        auto segment =
            CreateGrowingSegment(schema, empty_index_meta, 1, config);
        auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
        ASSERT_NE(growing, nullptr);
        auto offset = segment->PreInsert(5);
        segment->Insert(offset,
                        5,
                        row_ids.data(),
                        timestamps.data(),
                        std::make_shared<InsertRecordProto>(*insert));
        EXPECT_EQ(growing->get_insert_record()
                      .get_data<milvus::VectorArray>(plain)
                      ->is_mmap(),
                  enable_mmap);
        EXPECT_EQ(growing->get_insert_record()
                      .get_data<milvus::VectorArray>(nullable)
                      ->is_mmap(),
                  enable_mmap);

        auto plain_result = segment->bulk_subscript(
            nullptr, plain, offsets.data(), offsets.size());
        auto nullable_result = segment->bulk_subscript(
            nullptr, nullable, offsets.data(), offsets.size());
        ASSERT_EQ(plain_result->vectors().vector_array().data_size(), 5);
        ASSERT_EQ(nullable_result->vectors().vector_array().data_size(), 5);
        const auto& valid = GetFieldDataRowValidData(*nullable_result);
        ASSERT_EQ(valid.size(), 5);
        EXPECT_TRUE(valid[0]);
        EXPECT_FALSE(valid[1]);
        EXPECT_TRUE(valid[2]);
        EXPECT_TRUE(valid[3]);
        EXPECT_TRUE(valid[4]);
        const auto& rows = nullable_result->vectors().vector_array();
        EXPECT_EQ(rows.data(0).valid_data_size(), 3);
        EXPECT_FALSE(rows.data(0).valid_data(1));
        EXPECT_EQ(rows.data(0).float_vector().data_size(), 8);
        EXPECT_EQ(rows.data(1).data_case(), VectorFieldProto::kFloatVector);
        EXPECT_EQ(rows.data(2).valid_data_size(), 0);
        EXPECT_EQ(rows.data(3).valid_data_size(), 1);
        EXPECT_FALSE(rows.data(3).valid_data(0));
        EXPECT_EQ(rows.data(3).float_vector().data_size(), 0);

        auto views = segment->chunk_view<VectorArrayView>(
            nullptr, nullable, 0, std::make_pair(0, 2));
        const auto& [chunk_rows, chunk_valid] = views.get();
        ASSERT_EQ(chunk_rows.size(), 2);
        EXPECT_TRUE(chunk_valid[0]);
        EXPECT_FALSE(chunk_valid[1]);
        EXPECT_EQ(chunk_rows[0].length(), 3);
        EXPECT_EQ(chunk_rows[0].physical_length(), 2);
        EXPECT_EQ(chunk_rows[0].output_data().SerializeAsString(),
                  rows.data(0).SerializeAsString());

        SearchInfo info;
        info.field_id_ = plain;
        info.topk_ = 2;
        info.round_decimal_ = -1;
        info.metric_type_ = knowhere::metric::L2;
        info.search_params_ =
            knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2}};
        info.active_count_ = 5;
        info.struct_element_offsets_ = segment->GetStructElementOffsets(plain);
        ASSERT_NE(info.struct_element_offsets_, nullptr);
        float query_vec[4]{1, 0, 0, 0};
        SearchResult search_result;
        query::SearchOnGrowing(*growing,
                               info,
                               query_vec,
                               nullptr,
                               1,
                               MAX_TIMESTAMP,
                               BitsetView{},
                               nullptr,
                               search_result);
        EXPECT_TRUE(search_result.element_level_);
        ASSERT_FALSE(search_result.seg_offsets_.empty());
        EXPECT_EQ(search_result.seg_offsets_.front(), 0);
        EXPECT_EQ(search_result.element_indices_.front(), 0);
        info.field_id_ = nullable;
        info.struct_element_offsets_.reset();
        SearchResult unsupported_result;
        try {
            query::SearchOnGrowing(*growing,
                                   info,
                                   query_vec,
                                   nullptr,
                                   1,
                                   MAX_TIMESTAMP,
                                   BitsetView{},
                                   nullptr,
                                   unsupported_result);
            FAIL() << "embedding-list search on element-nullable "
                      "VECTOR_ARRAY must reject";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::NotImplemented);
        }

        // Element-level search on the element-nullable field returns
        // logical element positions and never a null element. Logical ids:
        // row 0 -> 0..2 (1 null), row 3 -> 3 (null), row 4 -> 4.
        info.topk_ = 3;
        info.struct_element_offsets_ =
            segment->GetStructElementOffsets(nullable);
        ASSERT_NE(info.struct_element_offsets_, nullptr);
        ASSERT_EQ(info.struct_element_offsets_->GetTotalElementCount(), 5);
        SearchResult nullable_search;
        query::SearchOnGrowing(*growing,
                               info,
                               query_vec,
                               nullptr,
                               1,
                               MAX_TIMESTAMP,
                               BitsetView{},
                               nullptr,
                               nullable_search);
        EXPECT_TRUE(nullable_search.element_level_);
        ASSERT_EQ(nullable_search.seg_offsets_.size(), 3);
        EXPECT_EQ(nullable_search.seg_offsets_[0], 0);
        EXPECT_EQ(nullable_search.element_indices_[0], 0);
        EXPECT_FLOAT_EQ(nullable_search.distances_[0], 0.0f);
        std::set<std::pair<int64_t, int32_t>> tail{
            {nullable_search.seg_offsets_[1],
             nullable_search.element_indices_[1]},
            {nullable_search.seg_offsets_[2],
             nullable_search.element_indices_[2]}};
        EXPECT_EQ(tail,
                  (std::set<std::pair<int64_t, int32_t>>{{0, 2}, {4, 0}}));
        info.topk_ = 2;
        if (enable_mmap) {
            EXPECT_EQ(plain_result->SerializeAsString(), heap_plain);
            EXPECT_EQ(nullable_result->SerializeAsString(), heap_nullable);
            EXPECT_EQ(search_result.seg_offsets_, heap_element_offsets);
            EXPECT_EQ(search_result.element_indices_, heap_element_indices);
            EXPECT_EQ(search_result.distances_, heap_distances);
        } else {
            heap_plain = plain_result->SerializeAsString();
            heap_nullable = nullable_result->SerializeAsString();
            heap_element_offsets = search_result.seg_offsets_;
            heap_element_indices = search_result.element_indices_;
            heap_distances = search_result.distances_;
        }
    }
}

TEST(GrowingTest, VectorArraySearchIteratorMmapMatchesHeap) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vectors = schema->AddDebugVectorArrayField(
        "items[vec]", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    schema->set_primary_field_id(pk);

    auto insert = std::make_unique<InsertRecordProto>();
    insert->set_num_rows(3);
    auto* pk_data = insert->add_fields_data();
    pk_data->set_field_id(pk.get());
    pk_data->set_type(proto::schema::DataType::Int64);
    auto* vector_data = insert->add_fields_data();
    vector_data->set_field_id(vectors.get());
    vector_data->set_type(proto::schema::DataType::ArrayOfVector);
    vector_data->mutable_vectors()->set_dim(4);
    auto* vector_array = vector_data->mutable_vectors()->mutable_vector_array();
    vector_array->set_dim(4);
    vector_array->set_element_type(proto::schema::DataType::FloatVector);
    const std::array<std::vector<float>, 3> values{
        {{1, 0, 0, 0, 0, 1, 0, 0}, {}, {0.5F, 0, 0, 0}}};
    for (int i = 0; i < 3; ++i) {
        pk_data->mutable_scalars()->mutable_long_data()->add_data(i);
        auto* row = vector_array->add_data();
        row->set_dim(4);
        row->mutable_float_vector()->mutable_data()->Add(values[i].begin(),
                                                         values[i].end());
    }

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    const bool previous_mmap = mmap_config.GetEnableGrowingMmap();
    auto restore_mmap = std::shared_ptr<void>(nullptr, [&](void*) {
        mmap_config.growing_enable_mmap = previous_mmap;
    });
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(2);
    config.set_enable_interim_segment_index(false);
    std::array<int64_t, 3> row_ids{0, 1, 2};
    std::array<Timestamp, 3> timestamps{100, 100, 100};
    std::vector<int64_t> heap_offsets;
    std::vector<int32_t> heap_indices;
    std::vector<float> heap_distances;
    float query_vec[4]{1, 0, 0, 0};
    for (bool enable_mmap : {false, true}) {
        mmap_config.growing_enable_mmap = enable_mmap;
        auto segment =
            CreateGrowingSegment(schema, empty_index_meta, 1, config);
        auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
        ASSERT_NE(growing, nullptr);
        auto start = segment->PreInsert(3);
        segment->Insert(start,
                        3,
                        row_ids.data(),
                        timestamps.data(),
                        std::make_shared<InsertRecordProto>(*insert));
        EXPECT_EQ(growing->get_insert_record()
                      .get_data<milvus::VectorArray>(vectors)
                      ->is_mmap(),
                  enable_mmap);

        SearchInfo info;
        info.field_id_ = vectors;
        info.topk_ = 3;
        info.round_decimal_ = -1;
        info.metric_type_ = knowhere::metric::L2;
        info.search_params_ =
            knowhere::Json{{knowhere::meta::METRIC_TYPE, knowhere::metric::L2}};
        info.active_count_ = 3;
        info.struct_element_offsets_ =
            segment->GetStructElementOffsets(vectors);
        ASSERT_NE(info.struct_element_offsets_, nullptr);
        SearchIteratorV2Info iterator_info;
        iterator_info.batch_size = 2;
        info.iterator_v2_info_ = iterator_info;

        SearchResult result;
        query::SearchOnGrowing(*growing,
                               info,
                               query_vec,
                               nullptr,
                               1,
                               MAX_TIMESTAMP,
                               BitsetView{},
                               nullptr,
                               result);
        ASSERT_EQ(result.unity_topK_, iterator_info.batch_size);
        ASSERT_EQ(result.seg_offsets_.size(), 2);
        ASSERT_EQ(result.element_indices_.size(), 2);
        EXPECT_TRUE(result.element_level_);
        EXPECT_EQ(result.seg_offsets_[0], 0);
        EXPECT_EQ(result.element_indices_[0], 0);
        EXPECT_EQ(result.seg_offsets_[1], 2);
        EXPECT_EQ(result.element_indices_[1], 0);
        if (enable_mmap) {
            EXPECT_EQ(result.seg_offsets_, heap_offsets);
            EXPECT_EQ(result.element_indices_, heap_indices);
            EXPECT_EQ(result.distances_, heap_distances);
        } else {
            heap_offsets = result.seg_offsets_;
            heap_indices = result.element_indices_;
            heap_distances = result.distances_;
        }
    }
}

TEST(GrowingTest, LoadVectorArrayData) {
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::MAX_SIM;
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto array_float_vector = schema->AddDebugVectorArrayField(
        "array_vec", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));

    int64_t dataset_size = 1000;
    auto dataset = DataGen(schema, dataset_size);
    auto segment_growing =
        CreateGrowingWithFieldDataLoaded(schema, metaPtr, config, dataset);
    auto segment = segment_growing.get();

    // Verify data
    auto int64_values = dataset.get_col<int64_t>(int64_field);
    auto array_vec_values =
        dataset.get_col<VectorFieldProto>(array_float_vector);

    auto ids_ds = GenRandomIds(dataset_size);
    auto int64_result = segment->bulk_subscript(
        nullptr, int64_field, ids_ds->GetIds(), dataset_size);
    auto array_float_vector_result = segment->bulk_subscript(
        nullptr, array_float_vector, ids_ds->GetIds(), dataset_size);

    EXPECT_EQ(int64_result->scalars().long_data().data_size(), dataset_size);
    EXPECT_EQ(array_float_vector_result->vectors().vector_array().data_size(),
              dataset_size);

    auto verify_float_vectors = [](auto arr1, auto arr2) {
        static constexpr float EPSILON = 1e-6;
        EXPECT_EQ(arr1.size(), arr2.size());
        for (int64_t i = 0; i < arr1.size(); ++i) {
            EXPECT_NEAR(arr1[i], arr2[i], EPSILON);
        }
    };

    for (int64_t i = 0; i < dataset_size; ++i) {
        auto arrow_array = array_float_vector_result->vectors()
                               .vector_array()
                               .data()[i]
                               .float_vector()
                               .data();
        auto expected_array =
            array_vec_values[ids_ds->GetIds()[i]].float_vector().data();
        verify_float_vectors(arrow_array, expected_array);
    }
}

TEST(GrowingTest, SearchVectorArray) {
    using namespace milvus::query;

    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::MAX_SIM;

    auto dim = 32;

    // Add fields
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto array_vec = schema->AddDebugVectorArrayField(
        "array_vec", DataType::VECTOR_FLOAT, dim, metric_type);
    schema->set_primary_field_id(int64_field);

    // Configure segment
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);

    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_HNSW},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta fieldIndexMeta(
        array_vec, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> fieldMap = {{array_vec, fieldIndexMeta}};

    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(fieldMap));

    // Insert data
    int64_t N = 100;
    uint64_t seed = 42;
    int emb_list_len = 5;  // Each row contains 5 vectors
    auto dataset = DataGen(schema, N, seed, 0, 1, emb_list_len);

    // Prepare search query
    int vec_num = 10;  // Total number of query vectors
    std::vector<float> query_vec = generate_float_vector(vec_num, dim);

    // Create query dataset with offsets for VectorArray
    std::vector<size_t> query_vec_offsets;
    query_vec_offsets.push_back(0);  // First query has 3 vectors
    query_vec_offsets.push_back(3);
    query_vec_offsets.push_back(10);  // Second query has 7 vectors

    // Create search plan using ScopedSchemaHandle
    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str =
        schema_handle.ParseSearch("",           // expression (no filter)
                                  "array_vec",  // vector field name
                                  5,            // topk
                                  "MAX_SIM",    // metric_type
                                  R"({"nprobe": 10})",  // search_params
                                  3);                   // round_decimal
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    // Use CreatePlaceholderGroupFromBlob for VectorArray
    auto ph_group_raw = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
        vec_num, dim, query_vec.data(), query_vec_offsets);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    const bool previous_mmap = mmap_config.GetEnableGrowingMmap();
    auto restore_mmap = std::shared_ptr<void>(nullptr, [&](void*) {
        mmap_config.growing_enable_mmap = previous_mmap;
    });
    std::optional<nlohmann::json> heap_result;
    for (bool enable_mmap : {false, true}) {
        mmap_config.growing_enable_mmap = enable_mmap;
        auto segment = CreateGrowingSegment(schema, metaPtr, 1, config);
        auto offset = segment->PreInsert(N);
        segment->Insert(offset,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        std::make_shared<InsertRecordProto>(*dataset.raw_));
        auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
        ASSERT_NE(growing, nullptr);
        EXPECT_EQ(growing->get_insert_record()
                      .get_data<milvus::VectorArray>(array_vec)
                      ->is_mmap(),
                  enable_mmap);
        auto sr = segment->Search(plan.get(), ph_group.get(), 10000000);
        ASSERT_NE(sr, nullptr);
        ASSERT_FALSE(sr->seg_offsets_.empty());
        auto result = SearchResultToJson(*sr);
        if (enable_mmap) {
            ASSERT_TRUE(heap_result.has_value());
            EXPECT_EQ(result, *heap_result);
        } else {
            heap_result = std::move(result);
        }
    }
}

TEST(Growing, TestMaskWithTTLField) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64, false);
    auto ttl_fid =
        schema->AddDebugField("ttl_field", DataType::TIMESTAMPTZ, false);
    schema->set_primary_field_id(pk_fid);
    schema->set_ttl_field_id(ttl_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    int64_t test_data_count = 100;

    uint64_t base_ts = 1000000000ULL << 18;
    std::vector<Timestamp> ts_data(test_data_count);
    for (int i = 0; i < test_data_count; i++) {
        ts_data[i] = base_ts + i;
    }

    std::vector<idx_t> row_ids(test_data_count);
    std::iota(row_ids.begin(), row_ids.end(), 0);

    std::vector<int64_t> pk_data(test_data_count);
    std::iota(pk_data.begin(), pk_data.end(), 0);

    uint64_t base_physical_us = (base_ts >> 18) * 1000;
    std::vector<int64_t> ttl_data(test_data_count);
    for (int i = 0; i < test_data_count; i++) {
        if (i < test_data_count / 2) {
            ttl_data[i] = static_cast<int64_t>(base_physical_us - 10);
        } else {
            ttl_data[i] = static_cast<int64_t>(base_physical_us + 10);
        }
    }

    auto insert_record_proto = std::make_shared<InsertRecordProto>();
    insert_record_proto->set_num_rows(test_data_count);

    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(pk_fid.get());
        field_data->set_type(proto::schema::DataType::Int64);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_long_data();
        for (auto v : pk_data) {
            data->add_data(v);
        }
    }

    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(ttl_fid.get());
        field_data->set_type(proto::schema::DataType::Timestamptz);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_timestamptz_data();
        for (auto v : ttl_data) {
            data->add_data(v);
        }
    }

    auto offset = segment->PreInsert(test_data_count);
    segment->Insert(offset,
                    test_data_count,
                    row_ids.data(),
                    ts_data.data(),
                    insert_record_proto);

    // Test TTL field filtering using CompileExpressions pathway
    Timestamp query_ts = base_ts + test_data_count;
    int64_t active_count = segment->get_active_count(query_ts);

    // Create an expression list with AlwaysTrueExpr - CompileExpressions will
    // automatically add TTL field filtering expression
    auto always_true_expr = std::make_shared<expr::AlwaysTrueExpr>();
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       always_true_expr);

    // Execute query expression - this will trigger CompileExpressions which
    // automatically adds TTL field filtering expression
    BitsetType bitset =
        query::ExecuteQueryExpr(plan, segment.get(), active_count, query_ts);

    // Note: ExecuteQueryExpr already flips the bitset, so bitset[i] = true
    // means the row matches (not expired), bitset[i] = false means expired
    BitsetTypeView bitset_view(bitset);

    // Verify results:
    // After ExecuteQueryExpr, bitset[i] = true means row matches (not expired)
    // bitset[i] = false means row is filtered out (expired)
    // - i < test_data_count / 2: expired (TTL < current time), should be expired (bitset_view[i] = false)
    // - i >= test_data_count / 2: not expired, should NOT be expired (bitset_view[i] = true)
    int expired_count = 0;
    for (int i = 0; i < test_data_count; i++) {
        if (!bitset_view[i]) {
            expired_count++;
        }
    }

    EXPECT_EQ(expired_count, test_data_count / 2);
    for (int i = 0; i < test_data_count / 2; i++) {
        EXPECT_FALSE(bitset_view[i]) << "Row " << i << " should be expired";
    }
    for (int i = test_data_count / 2; i < test_data_count; i++) {
        EXPECT_TRUE(bitset_view[i]) << "Row " << i << " should not be expired";
    }
}

// Test TTL field filtering with nullable field for Growing segment
TEST(Growing, TestMaskWithNullableTTLField) {
    // Create schema with nullable TTL field
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64, false);
    auto ttl_fid = schema->AddDebugField(
        "ttl_field", DataType::TIMESTAMPTZ, true);  // nullable
    schema->set_primary_field_id(pk_fid);
    schema->set_ttl_field_id(ttl_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    int64_t test_data_count = 100;

    // Generate timestamp data
    uint64_t base_ts = 1000000000ULL << 18;
    std::vector<Timestamp> ts_data(test_data_count);
    for (int i = 0; i < test_data_count; i++) {
        ts_data[i] = base_ts + i;
    }

    // Generate row IDs
    std::vector<idx_t> row_ids(test_data_count);
    std::iota(row_ids.begin(), row_ids.end(), 0);

    // Generate PK data
    std::vector<int64_t> pk_data(test_data_count);
    std::iota(pk_data.begin(), pk_data.end(), 0);

    // Generate TTL data with some nulls
    uint64_t base_physical_us = (base_ts >> 18) * 1000;
    std::vector<int64_t> ttl_data(test_data_count);
    std::vector<bool> valid_data(test_data_count);
    for (int i = 0; i < test_data_count; i++) {
        if (i % 4 == 0) {
            // Null value - should not expire
            ttl_data[i] = 0;
            valid_data[i] = false;
        } else if (i % 4 == 1) {
            // Expired
            ttl_data[i] = static_cast<int64_t>(base_physical_us - 10);
            valid_data[i] = true;
        } else {
            // Not expired
            ttl_data[i] = static_cast<int64_t>(base_physical_us + 10);
            valid_data[i] = true;
        }
    }

    // Create insert record proto
    auto insert_record_proto = std::make_shared<InsertRecordProto>();
    insert_record_proto->set_num_rows(test_data_count);

    // Add PK field data
    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(pk_fid.get());
        field_data->set_type(proto::schema::DataType::Int64);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_long_data();
        for (auto v : pk_data) {
            data->add_data(v);
        }
    }

    // Add nullable TTL field data
    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(ttl_fid.get());
        field_data->set_type(proto::schema::DataType::Timestamptz);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_timestamptz_data();
        for (auto v : ttl_data) {
            data->add_data(v);
        }
        // Add valid_data for nullable field
        // Note: valid_data[i] = false means null, valid_data[i] = true means non-null
        for (size_t i = 0; i < valid_data.size(); ++i) {
            scalars->add_valid_data(valid_data[i]);
        }
        // Verify valid_data was added correctly
        ASSERT_EQ(scalars->valid_data_size(), test_data_count)
            << "valid_data size mismatch: expected " << test_data_count
            << ", got " << scalars->valid_data_size();
    }

    // Insert data
    auto offset = segment->PreInsert(test_data_count);
    segment->Insert(offset,
                    test_data_count,
                    row_ids.data(),
                    ts_data.data(),
                    insert_record_proto);

    // Test TTL field filtering using CompileExpressions pathway
    Timestamp query_ts = base_ts + test_data_count;
    int64_t active_count = segment->get_active_count(query_ts);

    // Create an expression list with AlwaysTrueExpr - CompileExpressions will
    // automatically add TTL field filtering expression
    auto always_true_expr = std::make_shared<expr::AlwaysTrueExpr>();
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       always_true_expr);

    // Execute query expression - this will trigger CompileExpressions which
    // automatically adds TTL field filtering expression
    BitsetType bitset =
        query::ExecuteQueryExpr(plan, segment.get(), active_count, query_ts);

    // Note: ExecuteQueryExpr already flips the bitset, so bitset[i] = true
    // means the row matches (not expired), bitset[i] = false means expired
    BitsetTypeView bitset_view(bitset);

    // Verify results:
    // After ExecuteQueryExpr, bitset[i] = true means row matches (not expired)
    // bitset[i] = false means row is filtered out (expired)
    // - i % 4 == 0: null, should NOT be expired (bitset_view[i] = true)
    // - i % 4 == 1: expired (TTL < current time), should be expired (bitset_view[i] = false)
    // - i % 4 == 2 or 3: not expired, should NOT be expired (bitset_view[i] = true)
    int expired_count = 0;
    for (int i = 0; i < test_data_count; i++) {
        if (i % 4 == 0) {
            // Null value should not be expired (should match)
            EXPECT_TRUE(bitset_view[i])
                << "Row " << i << " (null) should not be expired";
        } else if (i % 4 == 1) {
            // Should be expired (should be filtered out)
            EXPECT_FALSE(bitset_view[i]) << "Row " << i << " should be expired";
            expired_count++;
        } else {
            // Should not be expired (should match)
            EXPECT_TRUE(bitset_view[i])
                << "Row " << i << " should not be expired";
        }
    }

    EXPECT_EQ(expired_count, test_data_count / 4);
}

TEST(Growing, FilterOnlySearchUsesEntityTTLPhysicalTime) {
    using namespace milvus::query;

    auto schema = std::make_shared<Schema>();
    auto dim = 4;
    auto metric_type = knowhere::metric::L2;
    schema->AddDebugField("vec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64, false);
    auto ttl_fid =
        schema->AddDebugField("ttl_field", DataType::TIMESTAMPTZ, false);
    schema->set_primary_field_id(pk_fid);
    schema->set_ttl_field_id(ttl_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(dynamic_cast<SegmentGrowingImpl*>(segment.get()), nullptr);

    constexpr int64_t test_data_count = 100;
    // 90/10 split: first 10 rows expire before the TTL cutoff,
    // remaining 90 are still alive.  An inverted-bitset bug would
    // report 10 instead of 90.
    constexpr int64_t expired_count = 10;
    constexpr int64_t expected_valid_count =
        test_data_count - expired_count;  // 90
    auto dataset = DataGen(schema, test_data_count);

    constexpr int64_t entity_ttl_physical_time_us = 1770026400000000LL;
    for (auto& field_data : *dataset.raw_->mutable_fields_data()) {
        if (field_data.field_id() != ttl_fid.get()) {
            continue;
        }

        auto* ttl_values =
            field_data.mutable_scalars()->mutable_timestamptz_data();
        ttl_values->clear_data();
        for (int i = 0; i < test_data_count; ++i) {
            ttl_values->add_data(i < expired_count
                                     ? entity_ttl_physical_time_us - 10
                                     : entity_ttl_physical_time_us + 10);
        }
    }

    segment->PreInsert(test_data_count);
    segment->Insert(0,
                    test_data_count,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    Timestamp query_ts = test_data_count + 10;
    int64_t active_count = segment->get_active_count(query_ts);
    ASSERT_EQ(active_count, test_data_count);

    // Cross-check via ExecuteQueryExpr to confirm the expected count.
    auto always_true_expr = std::make_shared<expr::AlwaysTrueExpr>();
    auto expected_plan = std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, always_true_expr);
    auto expected_bitset = query::ExecuteQueryExpr(expected_plan,
                                                   segment.get(),
                                                   active_count,
                                                   query_ts,
                                                   entity_ttl_physical_time_us);
    BitsetTypeView expected_matches(expected_bitset);
    int64_t cross_check_count = 0;
    for (int i = 0; i < test_data_count; ++i) {
        if (expected_matches[i]) {
            ++cross_check_count;
        }
    }
    ASSERT_EQ(cross_check_count, expected_valid_count);

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.ParseSearch(
        "pk >= 0", "vec", test_data_count, metric_type, R"({"nprobe": 10})", 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    ASSERT_NE(plan, nullptr);

    auto query_data = generate_float_vector(1, dim);
    auto ph_group_raw =
        CreatePlaceholderGroupFromBlob(1, dim, query_data.data());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    ASSERT_NE(ph_group, nullptr);

    auto search_result = segment->Search(plan.get(),
                                         ph_group.get(),
                                         query_ts,
                                         folly::CancellationToken(),
                                         0,
                                         0,
                                         entity_ttl_physical_time_us,
                                         true);

    EXPECT_EQ(search_result->valid_count_, expected_valid_count);
}

// Resource tracking tests for growing segments
TEST(Growing, EmptySegmentResourceEstimation) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metric_type = knowhere::metric::L2;
    schema->AddDebugField("vec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    // Empty segment should have zero resource usage
    auto resource = segment_impl->EstimateSegmentResourceUsage();
    EXPECT_EQ(resource.memory_bytes, 0);
    EXPECT_EQ(resource.file_bytes, 0);
}

TEST(Growing, ResourceEstimationAfterInsert) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metric_type = knowhere::metric::L2;
    schema->AddDebugField("vec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    // Insert some data
    const int64_t N = 1000;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    // After insert, resource usage should be positive
    auto resource = segment_impl->EstimateSegmentResourceUsage();
    EXPECT_GT(resource.memory_bytes, 0);

    // Memory should include at least:
    // - Vector data: N * dim * sizeof(float) = 1000 * 128 * 4 = 512000 bytes
    // - Timestamps: N * sizeof(Timestamp) = 1000 * 8 = 8000 bytes
    // - RowIDs: N * sizeof(int64_t) = 1000 * 8 = 8000 bytes
    // - PK field: N * sizeof(int64_t) = 1000 * 8 = 8000 bytes
    // Plus safety margin of 1.2x
    int64_t expected_min_size = N * dim * sizeof(float) +
                                N * sizeof(Timestamp) + N * sizeof(int64_t) +
                                N * sizeof(int64_t);
    EXPECT_GE(resource.memory_bytes, expected_min_size);
}

TEST(Growing, ResourceIncrementsWithMoreInserts) {
    auto schema = std::make_shared<Schema>();
    auto dim = 128;
    auto metric_type = knowhere::metric::L2;
    schema->AddDebugField("vec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    // First insert
    const int64_t N1 = 500;
    auto dataset1 = DataGen(schema, N1, 42, 0);
    segment->PreInsert(N1);
    segment->Insert(0,
                    N1,
                    dataset1.row_ids_.data(),
                    dataset1.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset1.raw_));
    auto resource1 = segment_impl->EstimateSegmentResourceUsage();

    // Second insert
    const int64_t N2 = 500;
    auto dataset2 = DataGen(schema, N2, 43, N1);
    segment->PreInsert(N2);
    segment->Insert(N1,
                    N2,
                    dataset2.row_ids_.data(),
                    dataset2.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset2.raw_));
    auto resource2 = segment_impl->EstimateSegmentResourceUsage();

    // Resource should increase after second insert
    EXPECT_GT(resource2.memory_bytes, resource1.memory_bytes);
}

TEST(Growing, ResourceTrackingAfterDelete) {
    auto schema = std::make_shared<Schema>();
    auto dim = 64;
    auto metric_type = knowhere::metric::L2;
    schema->AddDebugField("vec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    // Insert data first
    const int64_t N = 100;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    auto resource_before_delete = segment_impl->EstimateSegmentResourceUsage();
    EXPECT_GT(resource_before_delete.memory_bytes, 0);

    // Delete some rows
    auto pks = dataset.get_col<int64_t>(pk_fid);
    auto del_pks = GenPKs(pks.begin(), pks.begin() + 5);
    auto del_tss = GenTss(5, N);
    auto status = segment->Delete(5, del_pks.get(), del_tss.data());
    EXPECT_TRUE(status.ok());

    // Resource estimation should still work after delete
    auto resource_after_delete = segment_impl->EstimateSegmentResourceUsage();
    EXPECT_GT(resource_after_delete.memory_bytes, 0);
}

TEST(Growing, ConcurrentInsertResourceTracking) {
    auto schema = std::make_shared<Schema>();
    auto dim = 32;
    auto metric_type = knowhere::metric::L2;
    schema->AddDebugField("vec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    const int num_threads = 4;
    const int64_t rows_per_thread = 100;
    std::vector<std::thread> threads;

    // Reserve space for all rows upfront
    int64_t total_rows = num_threads * rows_per_thread;
    segment->PreInsert(total_rows);

    // Concurrent inserts from multiple threads
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto dataset =
                DataGen(schema, rows_per_thread, 42 + t, t * rows_per_thread);
            segment->Insert(t * rows_per_thread,
                            rows_per_thread,
                            dataset.row_ids_.data(),
                            dataset.timestamps_.data(),
                            std::make_shared<InsertRecordProto>(*dataset.raw_));
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify total row count
    EXPECT_EQ(segment->get_row_count(), total_rows);

    // Verify resource estimation is consistent and positive
    auto resource = segment_impl->EstimateSegmentResourceUsage();
    EXPECT_GT(resource.memory_bytes, 0);
}

TEST(Growing, NullableVectorInsertBuildsMonotonicOffsetMapping) {
    auto schema = std::make_shared<Schema>();
    constexpr int64_t dim = 8;
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2, true);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
        {"metric_type", knowhere::metric::L2},
        {"nlist", "1"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta field_index_meta(
        vec, std::move(index_params), std::move(type_params));

    constexpr int64_t rows_per_batch = 40;
    constexpr int64_t total_rows = rows_per_batch * 2;
    constexpr int64_t valid_per_batch = 20;
    std::map<FieldId, FieldIndexMeta> field_map = {
        {vec, std::move(field_index_meta)}};
    IndexMetaPtr index_meta =
        std::make_shared<CollectionIndexMeta>(total_rows, std::move(field_map));

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(16);
    config.set_nlist(1);
    config.set_nprobe(1);
    config.set_build_ratio(0.1F);
    config.set_enable_interim_segment_index(true);
    config.set_dense_vector_intermin_index_type(
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC);
    auto segment = CreateGrowingSegment(schema, index_meta, 1, config);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto first = DataGen(schema,
                         rows_per_batch,
                         42,
                         0,
                         1,
                         10,
                         1,
                         false,
                         true,
                         false,
                         valid_per_batch);
    auto second = DataGen(schema,
                          rows_per_batch,
                          43,
                          rows_per_batch,
                          1,
                          10,
                          1,
                          false,
                          true,
                          false,
                          valid_per_batch);
    ASSERT_EQ(segment->PreInsert(total_rows), 0);

    // The querynode pipeline drives inserts for a vchannel from a single
    // goroutine, so batches reach segcore in reserved logical order. That is
    // the precondition GrowingOffsetMapping::Append asserts on, and what makes
    // physical offsets a monotonic prefix of the logical ones.
    segment->Insert(0,
                    rows_per_batch,
                    first.row_ids_.data(),
                    first.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*first.raw_));
    segment->Insert(rows_per_batch,
                    rows_per_batch,
                    second.row_ids_.data(),
                    second.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*second.raw_));
    EXPECT_EQ(segment->get_row_count(), total_rows);
    const auto index_pin = segment_impl->PinGrowingIndex(vec);
    ASSERT_TRUE(index_pin);
    EXPECT_EQ(index_pin.CoveredRowEnd(), total_rows);
    EXPECT_EQ(index_pin.Reader().Count(), valid_per_batch * 2);

    const auto& insert_record = segment_impl->get_insert_record();
    auto* vector_data = insert_record.get_data_base(vec);
    const auto& mapping = vector_data->get_offset_mapping();
    EXPECT_EQ(mapping.GetTotalCount(), total_rows);
    EXPECT_EQ(mapping.GetValidCount(), valid_per_batch * 2);
    EXPECT_EQ(mapping.ValidCountBelow(rows_per_batch), valid_per_batch);
    EXPECT_EQ(mapping.GetLogicalOffset(0), valid_per_batch);
    EXPECT_EQ(mapping.GetLogicalOffset(valid_per_batch - 1),
              rows_per_batch - 1);
    EXPECT_EQ(mapping.GetLogicalOffset(valid_per_batch),
              rows_per_batch + valid_per_batch);
    EXPECT_EQ(mapping.GetLogicalOffset(valid_per_batch * 2 - 1),
              total_rows - 1);

    auto valid_data = vector_data->get_valid_data();
    ASSERT_EQ(valid_data.size(), total_rows);
    for (int64_t i = 0; i < total_rows; ++i) {
        EXPECT_EQ(valid_data[i], i % rows_per_batch >= valid_per_batch);
    }
}

// clear() swaps the chunk container rather than clearing it in place, so
// interim-index reclamation runs immediately even while a brute-force
// iterator still reads the old storage through its share_chunk_storage()
// reference -- and that old storage stays alive and intact until the last
// reference dies. Iterator-level lifetime coverage (iterator usable after
// SearchOnGrowing returns, reference releasable from another thread) lives in
// SearchOnSealedIndexBitsetLifetimeTest.cpp.
TEST(Growing, ChunkReclamationKeepsSharedStorageAlive) {
    auto schema = std::make_shared<Schema>();
    constexpr int64_t dim = 8;
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_fid);

    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
        {"metric_type", knowhere::metric::L2},
        {"nlist", "1"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta field_index_meta(
        vec, std::move(index_params), std::move(type_params));

    // The interim index builds at build_ratio * max_row_count = 8 rows, so
    // the first batch stays below it and the second crosses it.
    constexpr int64_t max_rows = 80;
    constexpr int64_t first_batch = 4;
    constexpr int64_t second_batch = 36;
    constexpr int64_t total_rows = first_batch + second_batch;
    std::map<FieldId, FieldIndexMeta> field_map = {
        {vec, std::move(field_index_meta)}};
    IndexMetaPtr index_meta =
        std::make_shared<CollectionIndexMeta>(max_rows, std::move(field_map));

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(16);
    config.set_nlist(1);
    config.set_nprobe(1);
    config.set_build_ratio(0.1F);
    config.set_enable_interim_segment_index(true);
    config.set_dense_vector_intermin_index_type(
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC);
    auto segment = CreateGrowingSegment(schema, index_meta, 1, config);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto first = DataGen(schema, first_batch, 42);
    auto second = DataGen(schema, second_batch, 43, first_batch);
    ASSERT_EQ(segment->PreInsert(total_rows), 0);

    segment->Insert(0,
                    first_batch,
                    first.row_ids_.data(),
                    first.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*first.raw_));

    // Fixture guard: the first batch must stay below the build threshold so
    // the raw chunks still exist when the reference is taken.
    ASSERT_FALSE(segment_impl->PinGrowingIndex(vec));
    auto* vec_base = segment_impl->get_insert_record().get_data_base(vec);
    ASSERT_GT(vec_base->num_chunk(), 0);

    // What a brute-force iterator holds: a pinned chunk generation plus raw
    // pointers read through it. No lock is taken -- the pin alone is what
    // keeps the buffers alive once try_remove_chunks swaps the column out.
    const auto chunks = vec_base->acquire_chunks();
    const float* old_chunk =
        static_cast<const float*>(vec_base->get_chunk_data(chunks, 0));
    auto storage = chunks.storage;
    ASSERT_NE(storage, nullptr);
    ASSERT_GT(chunks.count, 0);
    ASSERT_NE(old_chunk, nullptr);
    std::vector<float> expected(old_chunk, old_chunk + dim);

    // The second batch crosses the build threshold: the index syncs and owns
    // raw data inside this Insert, whose trailing try_remove_chunks swaps the
    // container out immediately -- a live reference must not delay it.
    segment->Insert(first_batch,
                    second_batch,
                    second.row_ids_.data(),
                    second.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*second.raw_));
    const auto index_pin = segment_impl->PinGrowingIndex(vec);
    ASSERT_TRUE(index_pin);
    EXPECT_EQ(index_pin.CoveredRowEnd(), total_rows);
    EXPECT_EQ(index_pin.Reader().Count(), total_rows);
    ASSERT_TRUE(segment_impl->CanReadRawVectorFromIndex(vec));
    EXPECT_EQ(vec_base->num_chunk(), 0)
        << "reclamation must proceed even while storage references are live";

    // The old storage is still alive and intact through the reference; ASan
    // turns a violation into a hard failure.
    for (int64_t i = 0; i < dim; ++i) {
        EXPECT_FLOAT_EQ(old_chunk[i], expected[i])
            << "swapped-out chunk storage must stay readable at index " << i;
    }
    storage.reset();
}

// A failed Reopen must stay retryable. Reopen backfills new columns BEFORE
// building text indexes and publishes the schema only after every step
// succeeds, so a text-index failure leaves the new nullable-vector column --
// and its order-asserted offset mapping -- already backfilled while the
// segment still reports the old schema. The retry re-enters fill_empty_field
// for the same field and must converge on the missing suffix instead of
// re-appending logical rows the mapping already holds from offset 0.
TEST(Growing, ReopenRetryAfterTextIndexFailureBackfillsOnce) {
    constexpr int64_t dim = 8;
    constexpr int64_t N = 32;

    auto make_schema = [&](int64_t version,
                           bool with_new_fields,
                           std::map<std::string, std::string> analyzer_params) {
        auto sch = std::make_shared<Schema>();
        sch->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
        auto pk = sch->AddDebugField("pk", DataType::INT64);
        if (with_new_fields) {
            sch->AddDebugField("nvec",
                               DataType::VECTOR_FLOAT,
                               dim,
                               knowhere::metric::L2,
                               /*nullable=*/true);
            // Nullable: Reopen backfills the column BEFORE it builds the text
            // index, and bulk_subscript_not_exist_field asserts on a
            // non-nullable scalar with no default. A non-nullable field would
            // therefore throw from the backfill and never reach the text-index
            // build this test is about.
            sch->AddDebugVarcharField(FieldName("match_text"),
                                      DataType::VARCHAR,
                                      256,
                                      /*nullable=*/true,
                                      /*enable_match=*/true,
                                      /*enable_analyzer=*/true,
                                      analyzer_params,
                                      std::nullopt);
        }
        sch->set_primary_field_id(pk);
        sch->set_schema_version(version);
        return sch;
    };

    auto v1 = make_schema(1, false, {});
    auto segment = CreateGrowingSegment(v1, empty_index_meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto dataset = DataGen(v1, N);
    ASSERT_EQ(segment->PreInsert(N), 0);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    // Attempt 1: analyzer params are not valid JSON, so the text-index build
    // throws AFTER the nullable-vector column was backfilled. The EXPECT also
    // guards the fixture itself: if an invalid analyzer ever stops throwing,
    // this fails loudly instead of the test passing without exercising the
    // retry.
    std::map<std::string, std::string> bad_params{
        {"analyzer_params", "{not valid json"}};
    auto v2 = make_schema(2, true, bad_params);
    std::string first_failure;
    try {
        segment_impl->Reopen(v2);
        FAIL() << "Reopen with invalid analyzer params must throw";
    } catch (const std::exception& e) {
        first_failure = e.what();
    }
    // Pin down WHERE it failed. Reopen backfills the new columns before it
    // builds text indexes, so a backfill that throws would leave nothing for
    // the retry to converge on and this test would silently stop covering the
    // case it exists for. Only the text-index build may fail here.
    EXPECT_NE(first_failure.find("failed to create text writer"),
              std::string::npos)
        << "attempt 1 did not reach the text index build: " << first_failure;
    EXPECT_EQ(segment->get_schema_snapshot()->get_schema_version(), 1);

    auto nvec_fid = v2->get_field_id(FieldName("nvec"));
    {
        const auto& mapping = segment_impl->get_insert_record()
                                  .get_data_base(nvec_fid)
                                  ->get_offset_mapping();
        ASSERT_EQ(mapping.GetTotalCount(), N);
        ASSERT_EQ(mapping.GetValidCount(), 0);
    }

    // Attempt 2 with corrected params re-enters the backfill for the same
    // fields. Without the missing-suffix fill this trips GrowingOffsetMapping
    // Append's ordering assertion (start_logical 0 vs total_count N) and the
    // segment can never take a schema upgrade again.
    auto v3 = make_schema(3, true, {});
    segment_impl->Reopen(v3);
    EXPECT_EQ(segment->get_schema_snapshot()->get_schema_version(), 3);
    {
        const auto& mapping = segment_impl->get_insert_record()
                                  .get_data_base(nvec_fid)
                                  ->get_offset_mapping();
        EXPECT_EQ(mapping.GetTotalCount(), N);
        EXPECT_EQ(mapping.GetValidCount(), 0);
    }

    // The segment stays writable under the evolved schema: the next insert
    // appends right after the backfilled prefix.
    auto more = DataGen(v3, N, 43, N);
    ASSERT_EQ(segment->PreInsert(N), N);
    segment->Insert(N,
                    N,
                    more.row_ids_.data(),
                    more.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*more.raw_));
    EXPECT_EQ(segment->get_row_count(), 2 * N);
    EXPECT_EQ(segment_impl->get_insert_record()
                  .get_data_base(nvec_fid)
                  ->get_offset_mapping()
                  .GetTotalCount(),
              2 * N);
}

TEST(Growing, MultipleFieldsResourceEstimation) {
    // Create schema with multiple fields
    auto schema = std::make_shared<Schema>();
    auto dim = 64;
    auto metric_type = knowhere::metric::L2;
    schema->AddDebugField("vec", DataType::VECTOR_FLOAT, dim, metric_type);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField("age", DataType::FLOAT);
    schema->AddDebugField("score", DataType::DOUBLE);
    schema->set_primary_field_id(pk_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    // Insert data
    const int64_t N = 500;
    auto dataset = DataGen(schema, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*dataset.raw_));

    auto resource = segment_impl->EstimateSegmentResourceUsage();

    // Memory should include all fields:
    // - Vector: N * dim * sizeof(float) = 500 * 64 * 4 = 128000 bytes
    // - pk (int64): N * 8 = 4000 bytes
    // - age (float): N * 4 = 2000 bytes
    // - score (double): N * 8 = 4000 bytes
    // - Timestamps: N * 8 = 4000 bytes
    // Plus safety margin
    int64_t min_expected = N * dim * sizeof(float) + N * sizeof(int64_t) +
                           N * sizeof(float) + N * sizeof(double) +
                           N * sizeof(Timestamp);
    EXPECT_GE(resource.memory_bytes, min_expected);
}

// Regression for PR #50951 review (round Dd91dab7a02): fill_empty_field()
// backfills a newly added field's validity bits and can then throw (the
// geometry cache build raises a retriable MemAllocateFailed on OOM). A throw
// leaves the schema unpublished, so the next query re-enters
// LazyCheckSchema -> Reopen -> fill_empty_field for the same field. With the
// appending set_data_raw() that retry added a SECOND block of N bits: the
// validity vector reached 2N while the record still held N rows, so every
// later inserted row stored its geometry at N+i but its validity bit at
// 2N+i and read back the all-false backfill -- rows silently dropped by ST_*
// predicates, returned as null, and matched by IS NULL, permanently.
//
// The offset-addressed set_data_raw() overload writes the backfill at [0, N),
// so re-running it is a no-op. This pins that directly rather than through a
// fault-injected Reopen, which segcore has no hook for. (The appending
// overload has since been removed outright, so the shape above is no longer
// reachable at all.)
TEST(Growing, BackfillValidDataIsIdempotentAcrossRetries) {
    constexpr int64_t kRows = 100;
    constexpr int64_t kSizePerChunk = 32;

    auto schema = std::make_shared<Schema>();
    auto field_id =
        schema->AddDebugField("nullable_geo", DataType::GEOMETRY, true);

    // The backfill payload fill_empty_field() would produce: kRows entries,
    // every row marked valid (the shape a non-null default value yields).
    auto data = std::make_unique<milvus::DataArray>();
    data->set_field_id(field_id.get());
    data->set_type(
        static_cast<milvus::proto::schema::DataType>(DataType::GEOMETRY));
    auto* geo = data->mutable_scalars()->mutable_geometry_data();
    for (int64_t i = 0; i < kRows; ++i) {
        *(geo->mutable_data()->Add()) = std::string();
        MutableFieldDataRowValidData(data.get())->Add(true);
    }

    milvus::segcore::ThreadSafeValidData valid_data(kSizePerChunk);
    const auto& field_meta = schema->operator[](field_id);

    valid_data.set_data_raw(0, kRows, data.get(), field_meta);
    ASSERT_EQ(valid_data.get_data().size(), static_cast<size_t>(kRows));

    // The retry after a failed reopen re-runs the same backfill. Length must
    // not grow, or every subsequent row's validity bit lands past its data.
    valid_data.set_data_raw(0, kRows, data.get(), field_meta);
    EXPECT_EQ(valid_data.get_data().size(), static_cast<size_t>(kRows));
    for (int64_t i = 0; i < kRows; ++i) {
        EXPECT_TRUE(valid_data.is_valid(i)) << "row " << i;
    }

    // Rows inserted after the backfill still line up: their validity bits
    // continue from kRows, not from 2 * kRows.
    auto tail = std::make_unique<milvus::DataArray>();
    tail->set_field_id(field_id.get());
    tail->set_type(
        static_cast<milvus::proto::schema::DataType>(DataType::GEOMETRY));
    auto* tail_geo = tail->mutable_scalars()->mutable_geometry_data();
    *(tail_geo->mutable_data()->Add()) = std::string();
    MutableFieldDataRowValidData(tail.get())->Add(false);

    valid_data.set_data_raw(kRows, 1, tail.get(), field_meta);
    ASSERT_EQ(valid_data.get_data().size(), static_cast<size_t>(kRows + 1));
    EXPECT_FALSE(valid_data.is_valid(kRows));
}

// Regression for PR #50951 review (round Dc417b11277): the valid_data shape
// guard used to sit only on the appending set_data_raw() overload, which no
// production path called and which has since been removed. Every nullable
// field on the ingest path goes through the offset-addressed overload
// (SegmentGrowingImpl::Insert), so a producer
// payload with fewer validity entries than rows made write_from() read past
// the end of the protobuf RepeatedField and publish adjacent heap bytes as
// validity bits. GEOMETRY is separately covered by
// ValidateGeometryInsertDataShape; this pins the guard for every other
// nullable type.
TEST(Growing, InsertRejectsShortValidDataForNullableVarchar) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto text = schema->AddDebugField("text", DataType::VARCHAR, true);
    schema->set_primary_field_id(pk);

    constexpr int64_t row_count = 2;
    std::array<int64_t, row_count> row_ids = {10, 11};
    std::array<Timestamp, row_count> timestamps = {100, 101};
    std::array<int64_t, row_count> pks = {1, 2};
    std::array<bool, row_count> valid = {true, true};
    std::array<std::string, row_count> texts = {"a", "b"};

    auto record = std::make_shared<InsertRecordProto>();
    record->set_num_rows(row_count);
    record->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk])
            .release());
    record->mutable_fields_data()->AddAllocated(
        CreateDataArrayFrom(
            texts.data(), valid.data(), row_count, (*schema)[text])
            .release());
    // One validity entry short of the row count.
    MutableFieldDataRowValidData(record->mutable_fields_data(1))->RemoveLast();

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = segment->PreInsert(row_count);
    try {
        segment->Insert(
            offset, row_count, row_ids.data(), timestamps.data(), record);
        FAIL() << "expected short valid_data to be rejected";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}
