// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/ArrayValue.h"

#include <arrow/api.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <initializer_list>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/ArrayOffsets.h"
#include "common/ChunkWriter.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "folly/CancellationToken.h"
#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"
#include "mmap/ChunkedColumnGroup.h"
#include "plan/PlanNode.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/segment_c.h"
#include "storage/MmapManager.h"
#include "storage/Util.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {
namespace {

proto::schema::TypeSchema
LeafArrayType(proto::schema::DataType leaf_type) {
    proto::schema::TypeSchema type;
    type.set_data_type(proto::schema::DataType::Array);
    type.set_element_type(leaf_type);
    return type;
}

proto::schema::TypeSchema
NestedArrayType(proto::schema::TypeSchema child) {
    proto::schema::TypeSchema type;
    type.set_data_type(proto::schema::DataType::Array);
    *type.mutable_element_schema() = std::move(child);
    return type;
}

FieldMeta
NestedArrayFieldMeta(FieldId field_id,
                     const proto::schema::TypeSchema& type,
                     bool nullable = true) {
    return FieldMeta(FieldName("nested_array"),
                     field_id,
                     DataType::ARRAY,
                     DataType::NONE,
                     nullable,
                     std::nullopt,
                     std::string{},
                     LOCAL_FORMAT_RAW,
                     std::make_optional(type.element_schema()));
}

ScalarFieldProto
BoolArrayRow(std::initializer_list<bool> values) {
    ScalarFieldProto row;
    auto* data = row.mutable_bool_data();
    for (auto value : values) {
        data->add_data(value);
    }
    return row;
}

ScalarFieldProto
IntArrayRow(std::initializer_list<int32_t> values) {
    ScalarFieldProto row;
    auto* data = row.mutable_int_data();
    for (auto value : values) {
        data->add_data(value);
    }
    return row;
}

ScalarFieldProto
LongArrayRow(std::initializer_list<int64_t> values) {
    ScalarFieldProto row;
    auto* data = row.mutable_long_data();
    for (auto value : values) {
        data->add_data(value);
    }
    return row;
}

ScalarFieldProto
FloatArrayRow(std::initializer_list<float> values) {
    ScalarFieldProto row;
    auto* data = row.mutable_float_data();
    for (auto value : values) {
        data->add_data(value);
    }
    return row;
}

ScalarFieldProto
DoubleArrayRow(std::initializer_list<double> values) {
    ScalarFieldProto row;
    auto* data = row.mutable_double_data();
    for (auto value : values) {
        data->add_data(value);
    }
    return row;
}

ScalarFieldProto
StringArrayRow(std::initializer_list<std::string> values) {
    ScalarFieldProto row;
    auto* data = row.mutable_string_data();
    for (const auto& value : values) {
        data->add_data(value);
    }
    return row;
}

ScalarFieldProto
NestedArrayRow(proto::schema::DataType element_type,
               std::vector<ScalarFieldProto> rows) {
    ScalarFieldProto row;
    auto* data = row.mutable_array_data();
    data->set_element_type(element_type);
    for (auto& child : rows) {
        *data->add_data() = std::move(child);
    }
    return row;
}

void
AssertProtoEqual(const ScalarFieldProto& expected,
                 const ScalarFieldProto& actual) {
    ASSERT_TRUE(
        google::protobuf::util::MessageDifferencer::Equals(expected, actual));
}

void
AssertOffsets(std::span<const ArrayOffset> actual,
              std::initializer_list<ArrayOffset> expected) {
    ASSERT_EQ(actual.size(), expected.size());
    ASSERT_TRUE(std::equal(actual.begin(), actual.end(), expected.begin()));
}

bool
PointerInChunk(const ColumnarArrayChunk& root, const void* pointer) {
    const auto begin = reinterpret_cast<uintptr_t>(root.Data());
    const auto end = begin + root.Size();
    const auto value = reinterpret_cast<uintptr_t>(pointer);
    return value >= begin && value < end;
}

bool
PointerInArrayValue(const ArrayValue& array, const void* pointer) {
    const auto begin = reinterpret_cast<uintptr_t>(array.data());
    const auto end = begin + array.byte_size();
    const auto value = reinterpret_cast<uintptr_t>(pointer);
    return value >= begin && value < end;
}

class ScopedDirectoryCleanup {
 public:
    explicit ScopedDirectoryCleanup(std::string path) : path_(std::move(path)) {
        std::filesystem::remove_all(path_);
    }

    ~ScopedDirectoryCleanup() {
        std::filesystem::remove_all(path_);
    }

 private:
    std::string path_;
};

class ScopedFlushResult {
 public:
    ~ScopedFlushResult() {
        FreeFlushResult(&result);
    }

    CFlushResult result{};
};

SchemaPtr
StorageV3NestedArraySchema(bool enable_mmap) {
    proto::schema::CollectionSchema schema_proto;
    schema_proto.set_name("storage_v3_nested_array");

    auto add_int64_field =
        [&](int64_t field_id, const std::string& name, bool primary = false) {
            auto* field = schema_proto.add_fields();
            field->set_fieldid(field_id);
            field->set_name(name);
            field->set_data_type(proto::schema::DataType::Int64);
            field->set_is_primary_key(primary);
        };
    add_int64_field(RowFieldID.get(), "RowID");
    add_int64_field(TimestampFieldID.get(), "Timestamp");
    add_int64_field(100, "pk", true);

    auto* nested = schema_proto.add_fields();
    nested->set_fieldid(101);
    nested->set_name("nested_array");
    nested->set_data_type(proto::schema::DataType::Array);
    nested->set_nullable(false);
    auto type = NestedArrayType(LeafArrayType(proto::schema::DataType::String));
    *nested->mutable_element_schema() = type.element_schema();

    auto* mmap = nested->add_type_params();
    mmap->set_key(MMAP_ENABLED_KEY);
    mmap->set_value(enable_mmap ? "true" : "false");
    auto* warmup = nested->add_type_params();
    warmup->set_key(WARMUP_KEY);
    warmup->set_value("disable");

    return Schema::ParseFrom(schema_proto);
}

std::vector<ScalarFieldProto>
StorageV3NestedArrayRows() {
    return {
        NestedArrayRow(proto::schema::DataType::String,
                       {StringArrayRow({"a", "bb"}),
                        ScalarFieldProto{},
                        StringArrayRow({})}),
        NestedArrayRow(proto::schema::DataType::String, {}),
        NestedArrayRow(proto::schema::DataType::String,
                       {StringArrayRow({"c"})}),
        NestedArrayRow(proto::schema::DataType::String,
                       {StringArrayRow({}), StringArrayRow({"d", "ee"})}),
    };
}

void
AssertStorageV3NestedArrayResult(const proto::segcore::RetrieveResults& result,
                                 const std::vector<ScalarFieldProto>& rows,
                                 const std::vector<int64_t>& offsets) {
    ASSERT_EQ(result.fields_data_size(), 1);
    const auto& field = result.fields_data(0);
    ASSERT_EQ(field.field_id(), 101);
    ASSERT_EQ(field.type(), proto::schema::DataType::Array);
    ASSERT_EQ(field.valid_data_size(), 0);

    const auto& arrays = field.scalars().array_data();
    ASSERT_EQ(arrays.element_type(), proto::schema::DataType::Array);
    ASSERT_EQ(arrays.data_size(), offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        AssertProtoEqual(rows[offsets[i]], arrays.data(i));
    }
}

void
RunStorageV3SealedRetrieve(bool enable_mmap, bool use_take) {
    const auto unique =
        std::chrono::steady_clock::now().time_since_epoch().count();
    const auto segment_path =
        (std::filesystem::temp_directory_path() /
         ("milvus_nested_array_v3_" + std::to_string(unique) + "_" +
          (enable_mmap ? "mmap" : "memory") + "_" +
          (use_take ? "take" : "chunk")))
            .string();
    ScopedDirectoryCleanup cleanup(segment_path);

    auto schema = StorageV3NestedArraySchema(enable_mmap);
    const auto nested_field = FieldId(101);
    auto rows = StorageV3NestedArrayRows();
    const auto row_count = static_cast<int64_t>(rows.size());

    auto growing = segcore::CreateGrowingSegment(schema, empty_index_meta);
    ASSERT_NE(growing, nullptr);

    std::vector<int64_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count);
    std::vector<int64_t> pks(row_count);
    for (int64_t i = 0; i < row_count; ++i) {
        row_ids[i] = 1000 + i;
        timestamps[i] = 2000 + i;
        pks[i] = 3000 + i;
    }

    InsertRecordProto insert;
    insert.set_num_rows(row_count);
    insert.mutable_fields_data()->AddAllocated(
        segcore::CreateDataArrayFrom(
            pks.data(), nullptr, row_count, (*schema)[FieldId(100)])
            .release());
    auto* nested_data = insert.add_fields_data();
    nested_data->set_field_id(nested_field.get());
    nested_data->set_type(proto::schema::DataType::Array);
    auto* array_data = nested_data->mutable_scalars()->mutable_array_data();
    array_data->set_element_type(proto::schema::DataType::Array);
    for (const auto& row : rows) {
        *array_data->add_data() = row;
    }

    const auto insert_offset = growing->PreInsert(row_count);
    ASSERT_EQ(insert_offset, 0);
    ASSERT_NO_THROW(growing->Insert(
        insert_offset, row_count, row_ids.data(), timestamps.data(), &insert));

    auto schema_blob = schema->ToProto().SerializeAsString();
    std::string column_group_pattern = "0|1|100,101";
    CFlushConfig config{};
    config.segment_path = segment_path.c_str();
    config.read_version = -1;
    config.retry_limit = 3;
    config.schema_blob = schema_blob.data();
    config.schema_length = static_cast<int64_t>(schema_blob.size());
    config.schema_based_pattern = column_group_pattern.c_str();

    ScopedFlushResult flush;
    auto status = FlushGrowingSegmentData(
        growing.get(), 0, row_count, &config, &flush.result);
    ASSERT_EQ(status.error_code, Success) << status.error_msg;
    ASSERT_EQ(flush.result.num_rows, row_count);
    ASSERT_GT(flush.result.committed_version, 0);

    const auto manifest_path =
        "{\"base_path\":\"" + segment_path +
        "\",\"ver\":" + std::to_string(flush.result.committed_version) + "}";
    proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(1);
    load_info.set_partitionid(2);
    load_info.set_segmentid(3);
    load_info.set_storageversion(STORAGE_V3);
    load_info.set_num_of_rows(row_count);
    load_info.set_manifest_path(manifest_path);
    load_info.set_insert_channel("nested-array-v3-test");

    auto sealed = segcore::CreateSealedSegment(schema, empty_index_meta, 3);
    ASSERT_NE(sealed, nullptr);
    sealed->SetLoadInfo(load_info);
    milvus::tracer::TraceContext trace_ctx;
    ASSERT_NO_THROW(sealed->Load(trace_ctx, nullptr));

    auto* chunked =
        dynamic_cast<segcore::ChunkedSegmentSealedImpl*>(sealed.get());
    ASSERT_NE(chunked, nullptr);
    chunked->SetUseTakeForOutputForTesting(use_take);

    query::RetrievePlan plan(schema);
    plan.field_ids_ = {nested_field};
    std::vector<int64_t> offsets = {3, 0, 1, 2, 0};
    if (use_take) {
        auto result = std::make_unique<proto::segcore::RetrieveResults>();
        ASSERT_TRUE(chunked->TryTakeForRetrieve(
            &plan, result, offsets.data(), offsets.size(), false, false));
        AssertStorageV3NestedArrayResult(*result, rows, offsets);
        return;
    }

    auto result = chunked->Retrieve(nullptr,
                                    &plan,
                                    offsets.data(),
                                    offsets.size(),
                                    folly::CancellationToken());
    AssertStorageV3NestedArrayResult(*result, rows, offsets);
}

TEST(ArrayValue, ScalarLeavesRoundTrip) {
    {
        auto row = BoolArrayRow({true, false, true});
        auto array = ArrayValue::FromProto(
            row, LeafArrayType(proto::schema::DataType::Bool));
        ASSERT_TRUE(array.View().get_data<bool>(0));
        ASSERT_FALSE(array.View().get_data<bool>(1));
        AssertProtoEqual(row, array.output_data());
    }
    {
        auto row = IntArrayRow({-3, 7});
        auto array = ArrayValue::FromProto(
            row, LeafArrayType(proto::schema::DataType::Int16));
        ASSERT_EQ(array.size(), 2);
        ASSERT_EQ(array.child().RowNums(), 2);
        ASSERT_EQ(array.child().Data(), array.data());
        ASSERT_EQ(array.byte_size(), 2 * sizeof(int32_t) + MMAP_ARRAY_PADDING);
        ASSERT_EQ(array.View().get_data<int16_t>(0), -3);
        ASSERT_EQ(array.View().get_data<int16_t>(1), 7);
        AssertProtoEqual(row, array.output_data());
    }
    {
        auto row = LongArrayRow({-9, 11});
        auto array = ArrayValue::FromProto(
            row, LeafArrayType(proto::schema::DataType::Int64));
        ASSERT_EQ(array.View().get_data<int64_t>(1), 11);
        AssertProtoEqual(row, array.output_data());
    }
    {
        auto row = FloatArrayRow({1.5F, -2.0F});
        auto array = ArrayValue::FromProto(
            row, LeafArrayType(proto::schema::DataType::Float));
        ASSERT_FLOAT_EQ(array.View().get_data<float>(0), 1.5F);
        AssertProtoEqual(row, array.output_data());
    }
    {
        auto row = DoubleArrayRow({1.25, -4.5});
        auto array = ArrayValue::FromProto(
            row, LeafArrayType(proto::schema::DataType::Double));
        ASSERT_DOUBLE_EQ(array.View().get_data<double>(1), -4.5);
        AssertProtoEqual(row, array.output_data());
    }
    {
        auto row = StringArrayRow({"a", "bb", "c"});
        auto array = ArrayValue::FromProto(
            row, LeafArrayType(proto::schema::DataType::VarChar));
        ASSERT_NE(dynamic_cast<const StringChunk*>(&array.child()), nullptr);
        ASSERT_EQ(array.View().get_data<std::string_view>(1), "bb");
        AssertProtoEqual(row, array.output_data());
    }
}

TEST(ArrayValue, NestedStringArrayUsesRecursiveNodes) {
    auto type = NestedArrayType(LeafArrayType(proto::schema::DataType::String));
    auto row = NestedArrayRow(proto::schema::DataType::String,
                              {StringArrayRow({"a", "bb"}),
                               StringArrayRow({}),
                               StringArrayRow({"c"})});

    auto array = ArrayValue::FromProto(row, type);
    auto root = array.View();

    ASSERT_EQ(root.size(), 3);
    ASSERT_TRUE(root.is_nested_array());
    ASSERT_EQ(root.array_at(0).get_data<std::string_view>(1), "bb");
    ASSERT_TRUE(root.array_at(1).empty());
    ASSERT_EQ(root.array_at(2).get_data<std::string_view>(0), "c");

    const auto* inner = dynamic_cast<const ColumnarArrayChunk*>(&array.child());
    ASSERT_NE(inner, nullptr);
    ASSERT_EQ(&inner->type(), &array.type().element_schema());
    AssertOffsets(inner->offsets(), {0, 2, 2, 3});

    ASSERT_NE(array.data(), nullptr);
    ASSERT_EQ(inner->Data(), array.data());
    ASSERT_TRUE(PointerInArrayValue(array, inner->offsets().data()));
    ASSERT_TRUE(PointerInArrayValue(array, inner->child().Data()));
    AssertProtoEqual(row, array.output_data());
}

TEST(ArrayValue, RootNullRoundTripsWithoutMaterializingRootOffsets) {
    ScalarFieldProto row;
    auto array = ArrayValue::FromProto(
        row, LeafArrayType(proto::schema::DataType::Int32));

    ASSERT_TRUE(array.is_null());
    ASSERT_EQ(array.size(), 0);
    ASSERT_TRUE(array.View().is_null());
    ASSERT_TRUE(array.View().empty());
    ASSERT_EQ(array.output_data().data_case(), ScalarFieldProto::DATA_NOT_SET);
    AssertProtoEqual(row, array.output_data());
}

TEST(ArrayValue, NullAndEmptyArraysRemainDistinctAtEveryLevel) {
    auto type = NestedArrayType(
        NestedArrayType(LeafArrayType(proto::schema::DataType::Int32)));
    auto row = NestedArrayRow(
        proto::schema::DataType::Array,
        {NestedArrayRow(
             proto::schema::DataType::Int32,
             {IntArrayRow({1}), ScalarFieldProto{}, IntArrayRow({})}),
         ScalarFieldProto{},
         NestedArrayRow(proto::schema::DataType::Int32,
                        {IntArrayRow({4})})});

    auto array = ArrayValue::FromProto(row, type);
    auto root = array.View();

    ASSERT_FALSE(root.is_null());
    ASSERT_FALSE(root.array_at(0).is_null());
    ASSERT_TRUE(root.array_at(1).is_null());
    ASSERT_TRUE(root.array_at(1).empty());
    ASSERT_TRUE(root.array_at(0).array_at(1).is_null());
    ASSERT_FALSE(root.array_at(0).array_at(2).is_null());
    ASSERT_TRUE(root.array_at(0).array_at(2).empty());
    ASSERT_EQ(root.array_at(2).array_at(0).get_data<int32_t>(0), 4);

    const auto* level_one =
        dynamic_cast<const ColumnarArrayChunk*>(&array.child());
    ASSERT_NE(level_one, nullptr);
    AssertOffsets(level_one->offsets(), {0, 3, 3, 4});
    ASSERT_TRUE(level_one->is_valid(0));
    ASSERT_FALSE(level_one->is_valid(1));
    ASSERT_TRUE(level_one->is_valid(2));

    const auto* level_two =
        dynamic_cast<const ColumnarArrayChunk*>(&level_one->child());
    ASSERT_NE(level_two, nullptr);
    AssertOffsets(level_two->offsets(), {0, 1, 1, 1, 2});
    ASSERT_TRUE(level_two->is_valid(0));
    ASSERT_FALSE(level_two->is_valid(1));
    ASSERT_TRUE(level_two->is_valid(2));
    ASSERT_TRUE(level_two->is_valid(3));

    ASSERT_EQ(root.array_at(0).array_at(1).output_data().data_case(),
              ScalarFieldProto::DATA_NOT_SET);
    ASSERT_EQ(root.array_at(0).array_at(2).output_data().data_case(),
              ScalarFieldProto::kIntData);
    AssertProtoEqual(row, array.output_data());
}

TEST(ArrayValue, TripleNestedIntAccess) {
    auto type = NestedArrayType(
        NestedArrayType(LeafArrayType(proto::schema::DataType::Int32)));
    auto row = NestedArrayRow(
        proto::schema::DataType::Array,
        {NestedArrayRow(proto::schema::DataType::Int32,
                        {IntArrayRow({1}), IntArrayRow({2, 3})}),
         NestedArrayRow(proto::schema::DataType::Int32, {}),
         NestedArrayRow(proto::schema::DataType::Int32,
                        {IntArrayRow({4})})});

    auto array = ArrayValue::FromProto(row, type);
    auto root = array.View();

    ASSERT_EQ(root.array_at(2).array_at(0).get_data<int32_t>(0), 4);

    const auto* level_one =
        dynamic_cast<const ColumnarArrayChunk*>(&array.child());
    ASSERT_NE(level_one, nullptr);
    AssertOffsets(level_one->offsets(), {0, 2, 2, 3});
    const auto* level_two =
        dynamic_cast<const ColumnarArrayChunk*>(&level_one->child());
    ASSERT_NE(level_two, nullptr);
    AssertOffsets(level_two->offsets(), {0, 1, 3, 4});
    AssertProtoEqual(row, array.output_data());
}

TEST(ArrayValue, QuadrupleNestedIntAccess) {
    auto type = NestedArrayType(NestedArrayType(
        NestedArrayType(LeafArrayType(proto::schema::DataType::Int32))));
    auto row = NestedArrayRow(
        proto::schema::DataType::Array,
        {NestedArrayRow(
             proto::schema::DataType::Array,
             {NestedArrayRow(proto::schema::DataType::Int32,
                             {IntArrayRow({1, 2}), IntArrayRow({3})})}),
         NestedArrayRow(proto::schema::DataType::Array, {})});

    auto array = ArrayValue::FromProto(row, type);
    auto root = array.View();

    ASSERT_EQ(root.array_at(0).array_at(0).array_at(1).get_data<int32_t>(0),
              3);
    const auto output = array.output_data();
    ASSERT_EQ(output.array_data().element_type(),
              proto::schema::DataType::Array);
    ASSERT_EQ(output.array_data().data(0).array_data().element_type(),
              proto::schema::DataType::Array);
    ASSERT_EQ(output.array_data()
                  .data(0)
                  .array_data()
                  .data(0)
                  .array_data()
                  .element_type(),
              proto::schema::DataType::Int32);
    AssertProtoEqual(row, output);
}

TEST(ArrayValue, RecursiveSchemaSelectsStorageFieldData) {
    auto type = NestedArrayType(
        NestedArrayType(LeafArrayType(proto::schema::DataType::Int32)));
    proto::schema::FieldSchema schema_proto;
    schema_proto.set_name("nested_array");
    schema_proto.set_fieldid(100);
    schema_proto.set_data_type(proto::schema::DataType::Array);
    schema_proto.set_nullable(true);
    *schema_proto.mutable_element_schema() = type.element_schema();

    auto field_meta = FieldMeta::ParseFrom(schema_proto);
    ASSERT_TRUE(field_meta.has_element_schema());
    ASSERT_EQ(field_meta.get_array_element_type(), DataType::ARRAY);
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        type, field_meta.get_array_type_schema()));
    ASSERT_TRUE(google::protobuf::util::MessageDifferencer::Equals(
        schema_proto, field_meta.ToProto()));

    auto row0 = NestedArrayRow(
        proto::schema::DataType::Array,
        {NestedArrayRow(proto::schema::DataType::Int32,
                        {IntArrayRow({1}), IntArrayRow({2, 3})})});
    auto row2 = NestedArrayRow(proto::schema::DataType::Array, {});
    const auto bytes0 = row0.SerializeAsString();
    const auto bytes2 = row2.SerializeAsString();
    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append(bytes0).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append(bytes2).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder.Finish(&arrow_array).ok());

    auto field_data = storage::CreateFieldData(
        DataType::ARRAY, DataType::NONE, true, 1, 0, type);
    auto nested_field_data =
        std::dynamic_pointer_cast<FieldData<ArrayValue>>(field_data);
    ASSERT_NE(nested_field_data, nullptr);
    field_data->FillFieldData(arrow_array);

    ASSERT_EQ(field_data->get_num_rows(), 3);
    ASSERT_TRUE(field_data->is_valid(0));
    ASSERT_FALSE(field_data->is_valid(1));
    ASSERT_TRUE(field_data->is_valid(2));
    const auto* values = static_cast<const ArrayValue*>(field_data->Data());
    ASSERT_EQ(&values[0].type(), &values[1].type());
    ASSERT_EQ(&values[0].type(), &values[2].type());
    AssertProtoEqual(row0, values[0].output_data());
    ASSERT_TRUE(values[1].is_null());
    AssertProtoEqual(row2, values[2].output_data());
}

TEST(ArrayValue, GrowingSchemaUsesArrayValueAndDenseNullableRows) {
    auto type = NestedArrayType(LeafArrayType(proto::schema::DataType::Int32));
    const auto field_id = FieldId(100);
    Schema schema;
    schema.AddField(NestedArrayFieldMeta(field_id, type));
    const auto& field_meta = schema[field_id];

    segcore::InsertRecordGrowing record(schema, 4);
    auto* values = dynamic_cast<segcore::ConcurrentVector<ArrayValue>*>(
        record.get_data_base(field_id));
    ASSERT_NE(values, nullptr);

    auto row0 = NestedArrayRow(proto::schema::DataType::Int32,
                               {IntArrayRow({1}), IntArrayRow({2, 3})});
    auto row2 = NestedArrayRow(proto::schema::DataType::Int32, {});
    proto::schema::FieldData data;
    data.set_field_id(field_id.get());
    data.set_type(proto::schema::DataType::Array);
    data.add_valid_data(true);
    data.add_valid_data(false);
    data.add_valid_data(true);
    auto* array_data = data.mutable_scalars()->mutable_array_data();
    array_data->set_element_type(proto::schema::DataType::Array);
    *array_data->add_data() = row0;
    array_data->add_data();
    *array_data->add_data() = row2;

    record.get_valid_data(field_id)->set_data_raw(3, &data, field_meta);
    record.get_data_base(field_id)->set_data_raw(0, 3, &data, field_meta);

    ASSERT_TRUE(record.get_valid_data(field_id)->is_valid(0));
    ASSERT_FALSE(record.get_valid_data(field_id)->is_valid(1));
    ASSERT_TRUE(record.get_valid_data(field_id)->is_valid(2));
    ASSERT_EQ(&(*values)[0].type(), &(*values)[1].type());
    ASSERT_EQ(&(*values)[0].type(), &(*values)[2].type());
    AssertProtoEqual(row0, (*values)[0].output_data());
    ASSERT_TRUE((*values)[1].is_null());
    AssertProtoEqual(row2, (*values)[2].output_data());

    proto::schema::FieldData next_batch;
    next_batch.set_field_id(field_id.get());
    next_batch.set_type(proto::schema::DataType::Array);
    next_batch.add_valid_data(true);
    auto* next_array = next_batch.mutable_scalars()->mutable_array_data();
    next_array->set_element_type(proto::schema::DataType::Array);
    *next_array->add_data() = row2;

    record.get_valid_data(field_id)->set_data_raw(
        1, &next_batch, field_meta);
    record.get_data_base(field_id)->set_data_raw(
        3, 1, &next_batch, field_meta);
    ASSERT_EQ(&(*values)[0].type(), &(*values)[3].type());
}

TEST(ArrayValue, GrowingMmapBuildsColumnarBlocksDirectlyFromDataArray) {
    auto type = NestedArrayType(LeafArrayType(proto::schema::DataType::Int32));
    const auto field_id = FieldId(100);
    const auto field_meta = NestedArrayFieldMeta(field_id, type);

    auto mmap_manager =
        storage::MmapManager::GetInstance().GetMmapChunkManager();
    auto mmap_descriptor = mmap_manager->Register();
    segcore::ConcurrentVector<ArrayValue> values(2, mmap_descriptor);

    auto row0 = NestedArrayRow(proto::schema::DataType::Int32,
                               {IntArrayRow({1}), IntArrayRow({2, 3})});
    auto row2 = NestedArrayRow(proto::schema::DataType::Int32, {});
    proto::schema::FieldData first_batch;
    first_batch.set_field_id(field_id.get());
    first_batch.set_type(proto::schema::DataType::Array);
    first_batch.add_valid_data(true);
    auto* first_array = first_batch.mutable_scalars()->mutable_array_data();
    first_array->set_element_type(proto::schema::DataType::Array);
    *first_array->add_data() = row0;
    values.set_data_raw(0, 1, &first_batch, field_meta);

    proto::schema::FieldData second_batch;
    second_batch.set_field_id(field_id.get());
    second_batch.set_type(proto::schema::DataType::Array);
    second_batch.add_valid_data(false);
    second_batch.add_valid_data(true);
    auto* second_array = second_batch.mutable_scalars()->mutable_array_data();
    second_array->set_element_type(proto::schema::DataType::Array);
    second_array->add_data();
    *second_array->add_data() = row2;
    values.set_data_raw(1, 2, &second_batch, field_meta);

    ASSERT_TRUE(values.is_mmap());
    ASSERT_EQ(values.num_chunk(), 2);
    AssertProtoEqual(row0, values.view_element(0).output_data());
    ASSERT_TRUE(values.view_element(1).is_null());
    AssertProtoEqual(row2, values.view_element(2).output_data());
}

TEST(ArrayValue, GrowingSegmentInsertAndRetrieveNestedArray) {
    auto type = NestedArrayType(LeafArrayType(proto::schema::DataType::Int32));
    auto schema = std::make_shared<Schema>();
    const auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto array_field = FieldId(pk.get() + 1);
    schema->AddField(NestedArrayFieldMeta(array_field, type));

    auto segment = segcore::CreateGrowingSegment(schema, empty_index_meta);
    auto row0 = NestedArrayRow(proto::schema::DataType::Int32,
                               {IntArrayRow({1}), IntArrayRow({2, 3})});
    auto row2 = NestedArrayRow(proto::schema::DataType::Int32, {});

    InsertRecordProto insert;
    insert.set_num_rows(3);
    auto* pk_data = insert.add_fields_data();
    pk_data->set_field_id(pk.get());
    pk_data->set_type(proto::schema::DataType::Int64);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(10);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(11);
    pk_data->mutable_scalars()->mutable_long_data()->add_data(12);

    auto* nested_data = insert.add_fields_data();
    nested_data->set_field_id(array_field.get());
    nested_data->set_type(proto::schema::DataType::Array);
    nested_data->add_valid_data(true);
    nested_data->add_valid_data(false);
    nested_data->add_valid_data(true);
    auto* array_data = nested_data->mutable_scalars()->mutable_array_data();
    array_data->set_element_type(proto::schema::DataType::Array);
    *array_data->add_data() = row0;
    array_data->add_data();
    *array_data->add_data() = row2;

    std::vector<int64_t> row_ids{100, 101, 102};
    std::vector<Timestamp> timestamps{1, 2, 3};
    const auto offset = segment->PreInsert(3);
    ASSERT_NO_THROW(
        segment->Insert(offset, 3, row_ids.data(), timestamps.data(), &insert));

    std::vector<int64_t> result_offsets{0, 1, 2};
    auto result = segment->bulk_subscript(
        nullptr, array_field, result_offsets.data(), result_offsets.size());
    ASSERT_EQ(result->valid_data_size(), 3);
    ASSERT_TRUE(result->valid_data(0));
    ASSERT_FALSE(result->valid_data(1));
    ASSERT_TRUE(result->valid_data(2));
    const auto& result_arrays = result->scalars().array_data();
    ASSERT_EQ(result_arrays.element_type(), proto::schema::DataType::Array);
    ASSERT_EQ(result_arrays.data_size(), 3);
    AssertProtoEqual(row0, result_arrays.data(0));
    ASSERT_EQ(result_arrays.data(1).data_case(),
              ScalarFieldProto::DATA_NOT_SET);
    AssertProtoEqual(row2, result_arrays.data(2));
}

TEST(ColumnarArrayChunk, WriterAndChunkShareOneContiguousBuffer) {
    auto type = NestedArrayType(LeafArrayType(proto::schema::DataType::String));
    auto row0 = NestedArrayRow(proto::schema::DataType::String,
                               {StringArrayRow({"a", "bb"}),
                                ScalarFieldProto{},
                                StringArrayRow({}),
                                StringArrayRow({"c"})});
    auto row2 = NestedArrayRow(proto::schema::DataType::String,
                               {StringArrayRow({"d"})});

    arrow::BinaryBuilder builder;
    const auto bytes0 = row0.SerializeAsString();
    const auto bytes2 = row2.SerializeAsString();
    ASSERT_TRUE(builder.Append(bytes0).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append(bytes2).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder.Finish(&arrow_array).ok());

    ColumnarArrayChunkWriter writer(type, true);
    arrow::ArrayVector arrays{arrow_array};
    const auto [size, row_count] = writer.calculate_size(arrays);
    const auto aligned_size = (size + ChunkTarget::ALIGNED_SIZE - 1) &
                              ~(ChunkTarget::ALIGNED_SIZE - 1);
    auto target = std::make_shared<MemChunkTarget>(aligned_size, true);
    writer.write_to_target(arrays, target);
    auto* data = target->release();
    auto guard = std::make_shared<ChunkMmapGuard>(data, size, "");
    ColumnarArrayChunk array_chunk(
        row_count, data, size, type, true, std::move(guard));

    ASSERT_EQ(array_chunk.row_count(), 3);
    ASSERT_TRUE(array_chunk.is_valid(0));
    ASSERT_FALSE(array_chunk.is_valid(1));
    ASSERT_TRUE(array_chunk.is_valid(2));
    AssertOffsets(array_chunk.offsets(), {0, 4, 4, 5});
    ASSERT_EQ(array_chunk.View(0).array_at(0).get_data<std::string_view>(1),
              "bb");
    ASSERT_TRUE(array_chunk.View(0).array_at(1).is_null());
    ASSERT_FALSE(array_chunk.View(0).array_at(2).is_null());
    ASSERT_TRUE(array_chunk.View(0).array_at(2).empty());
    ASSERT_TRUE(array_chunk.View(1).is_null());
    ASSERT_TRUE(array_chunk.View(1).empty());
    ASSERT_EQ(array_chunk.View(2).array_at(0).get_data<std::string_view>(0),
              "d");

    const auto* inner =
        dynamic_cast<const ColumnarArrayChunk*>(&array_chunk.child());
    ASSERT_NE(inner, nullptr);
    AssertOffsets(inner->offsets(), {0, 2, 2, 2, 3, 4});
    ASSERT_TRUE(inner->is_valid(0));
    ASSERT_FALSE(inner->is_valid(1));
    ASSERT_TRUE(inner->is_valid(2));
    ASSERT_TRUE(inner->is_valid(3));
    ASSERT_TRUE(inner->is_valid(4));
    ASSERT_TRUE(PointerInChunk(array_chunk, array_chunk.offsets().data()));
    ASSERT_TRUE(PointerInChunk(array_chunk, inner->Data()));
    ASSERT_TRUE(PointerInChunk(array_chunk, inner->child().Data()));
    ASSERT_EQ(array_chunk.output_data(1).data_case(),
              ScalarFieldProto::DATA_NOT_SET);
    AssertProtoEqual(row0, array_chunk.output_data(0));
    AssertProtoEqual(row2, array_chunk.output_data(2));
}

TEST(ColumnarArrayChunk, SealedFactoriesUseRecursiveChunk) {
    auto type = NestedArrayType(LeafArrayType(proto::schema::DataType::String));
    const auto field_id = FieldId(100);
    auto field_meta = NestedArrayFieldMeta(field_id, type);
    auto row0 = NestedArrayRow(proto::schema::DataType::String,
                               {StringArrayRow({"a", "bb"}),
                                StringArrayRow({})});
    auto row2 = NestedArrayRow(proto::schema::DataType::String,
                               {StringArrayRow({"c"})});

    arrow::BinaryBuilder builder;
    const auto bytes0 = row0.SerializeAsString();
    const auto bytes2 = row2.SerializeAsString();
    ASSERT_TRUE(builder.Append(bytes0).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append(bytes2).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder.Finish(&arrow_array).ok());
    arrow::ArrayVector arrays{arrow_array};

    auto chunk = create_chunk(field_meta, arrays);
    auto* array_chunk = dynamic_cast<ColumnarArrayChunk*>(chunk.get());
    ASSERT_NE(array_chunk, nullptr);
    AssertProtoEqual(row0, array_chunk->output_data(0));
    ASSERT_TRUE(array_chunk->View(1).is_null());
    AssertProtoEqual(row2, array_chunk->output_data(2));

    std::vector<FieldId> field_ids{field_id};
    std::vector<FieldMeta> field_metas{field_meta};
    std::vector<arrow::ArrayVector> column_arrays{arrays};
    auto group_chunks =
        create_group_chunk(field_ids, field_metas, column_arrays);
    auto group_chunk = std::dynamic_pointer_cast<ColumnarArrayChunk>(
        group_chunks.at(field_id));
    ASSERT_NE(group_chunk, nullptr);
    AssertProtoEqual(row0, group_chunk->output_data(0));
    ASSERT_TRUE(group_chunk->View(1).is_null());
    AssertProtoEqual(row2, group_chunk->output_data(2));
}

TEST(ColumnarArrayChunk, SealedArrayOffsetsUseRecursiveRootOffsets) {
    auto type = NestedArrayType(LeafArrayType(proto::schema::DataType::Int32));
    const auto field_id = FieldId(100);
    auto field_meta = NestedArrayFieldMeta(field_id, type);

    auto row0 = NestedArrayRow(proto::schema::DataType::Int32,
                               {IntArrayRow({1}), IntArrayRow({2, 3})});
    auto row2 = NestedArrayRow(proto::schema::DataType::Int32,
                               {IntArrayRow({})});
    auto row3 = NestedArrayRow(proto::schema::DataType::Int32, {});
    auto row4 = NestedArrayRow(proto::schema::DataType::Int32,
                               {IntArrayRow({4})});

    arrow::BinaryBuilder builder;
    ASSERT_TRUE(builder.Append(row0.SerializeAsString()).ok());
    ASSERT_TRUE(builder.AppendNull().ok());
    ASSERT_TRUE(builder.Append(row2.SerializeAsString()).ok());
    ASSERT_TRUE(builder.Append(row3.SerializeAsString()).ok());
    ASSERT_TRUE(builder.Append(row4.SerializeAsString()).ok());
    std::shared_ptr<arrow::Array> arrow_array;
    ASSERT_TRUE(builder.Finish(&arrow_array).ok());

    auto chunk = create_chunk(field_meta, arrow::ArrayVector{arrow_array});
    ASSERT_NE(dynamic_cast<ColumnarArrayChunk*>(chunk.get()), nullptr);

    std::unordered_map<FieldId, std::shared_ptr<Chunk>> fields;
    fields.emplace(field_id, std::shared_ptr<Chunk>(std::move(chunk)));
    std::vector<std::unique_ptr<GroupChunk>> group_chunks;
    group_chunks.push_back(std::make_unique<GroupChunk>(std::move(fields)));
    auto translator = std::make_unique<TestGroupChunkTranslator>(
        1,
        std::vector<int64_t>{5},
        "recursive_array_offsets",
        std::move(group_chunks));
    auto group = std::make_shared<ChunkedColumnGroup>(std::move(translator));
    ProxyChunkColumn column(group, field_id, field_meta);

    auto offsets =
        ArrayOffsetsSealed::BuildFromColumn(column, field_meta, 5);
    EXPECT_EQ(offsets->ElementIDRangeOfRow(0), std::make_pair(0, 2));
    EXPECT_EQ(offsets->ElementIDRangeOfRow(1), std::make_pair(2, 2));
    EXPECT_EQ(offsets->ElementIDRangeOfRow(2), std::make_pair(2, 3));
    EXPECT_EQ(offsets->ElementIDRangeOfRow(3), std::make_pair(3, 3));
    EXPECT_EQ(offsets->ElementIDRangeOfRow(4), std::make_pair(3, 4));
    EXPECT_EQ(offsets->GetTotalElementCount(), 4);
}

TEST(ArrayValue, SealedStorageV3RetrieveNestedArrayFromMemoryChunk) {
    RunStorageV3SealedRetrieve(false, false);
}

TEST(ArrayValue, SealedStorageV3RetrieveNestedArrayFromMmapChunk) {
    RunStorageV3SealedRetrieve(true, false);
}

TEST(ArrayValue, SealedStorageV3TakeNestedArray) {
    RunStorageV3SealedRetrieve(false, true);
}

TEST(ColumnarArrayChunk, RejectsMalformedSchema) {
    proto::schema::TypeSchema missing_leaf;
    missing_leaf.set_data_type(proto::schema::DataType::Array);
    ASSERT_ANY_THROW(ArrayValue::FromProto(IntArrayRow({1}), missing_leaf));

    auto nested = LeafArrayType(proto::schema::DataType::Array);
    ASSERT_ANY_THROW(ArrayValue::FromProto(IntArrayRow({1}), nested));

    arrow::BinaryBuilder null_builder;
    ASSERT_TRUE(null_builder.AppendNull().ok());
    std::shared_ptr<arrow::Array> null_array;
    ASSERT_TRUE(null_builder.Finish(&null_array).ok());
    ColumnarArrayChunkWriter non_nullable_writer(
        LeafArrayType(proto::schema::DataType::Int32), false);
    ASSERT_ANY_THROW(
        non_nullable_writer.calculate_size(arrow::ArrayVector{null_array}));

    arrow::BinaryBuilder empty_payload_builder;
    ASSERT_TRUE(empty_payload_builder.Append("", 0).ok());
    std::shared_ptr<arrow::Array> empty_payload_array;
    ASSERT_TRUE(empty_payload_builder.Finish(&empty_payload_array).ok());
    ColumnarArrayChunkWriter nullable_writer(
        LeafArrayType(proto::schema::DataType::Int32), true);
    ASSERT_ANY_THROW(nullable_writer.calculate_size(
        arrow::ArrayVector{empty_payload_array}));
}

}  // namespace
}  // namespace milvus
