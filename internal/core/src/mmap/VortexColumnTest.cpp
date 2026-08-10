// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mmap/VortexColumn.h"

#include <array>
#include <cstdint>
#include <cmath>
#include <exception>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

#include "arrow/api.h"
#include "arrow/filesystem/localfs.h"
#include "common/Common.h"
#include "common/FieldMeta.h"
#include "common/Geometry.h"
#include "common/OpContext.h"
#include "exec/expression/Expr.h"
#include "gtest/gtest.h"
#include "mmap/ChunkedColumnFilter.h"
#include "milvus-storage/column_groups.h"
#include "milvus-storage/format/vortex/vortex_types.h"
#include "milvus-storage/format/vortex/vortex_writer.h"
#include "milvus-storage/properties.h"
#include "mmap/SparseVortexFileSystem.h"

namespace milvus {
namespace {

ChunkedColumnInterface::ScanBatch
MakeNullableRowIdBatch() {
    ChunkedColumnInterface::ScanBatch batch;
    batch.row_ids = {1, 2};
    batch.row_id_start = 1;
    batch.size = 2;
    auto validity = std::make_shared<std::array<uint8_t, 1>>();
    (*validity)[0] = 0b10;
    batch.validity = ValidityView::FromPacked(validity->data());
    batch.owner = std::move(validity);
    return batch;
}

constexpr int64_t kIntFieldId = 101;
constexpr int64_t kStringFieldId = 102;
constexpr int64_t kNullableFieldIdBase = 200;
constexpr int64_t kNormalizedFieldIdBase = 300;
constexpr int64_t kNullableRows = 16;

bool
IsScanRowValid(const ChunkedColumnInterface::ScanBatch& batch, int64_t offset) {
    return !batch.validity || batch.validity[offset];
}

std::shared_ptr<arrow::Schema>
MakeSchema() {
    return arrow::schema({
        arrow::field(std::to_string(kIntFieldId), arrow::int32(), false),
        arrow::field(std::to_string(kStringFieldId), arrow::utf8(), false),
    });
}

std::shared_ptr<arrow::RecordBatch>
MakeRecordBatch(int64_t begin, int64_t count) {
    arrow::Int32Builder int_builder;
    arrow::StringBuilder string_builder;
    EXPECT_TRUE(int_builder.Reserve(count).ok());
    EXPECT_TRUE(string_builder.Reserve(count).ok());
    for (int64_t i = begin; i < begin + count; ++i) {
        EXPECT_TRUE(int_builder.Append(static_cast<int32_t>(i * 10)).ok());
        auto value = "v" + std::to_string(i);
        EXPECT_TRUE(string_builder.Append(value).ok());
    }

    std::shared_ptr<arrow::Array> int_array;
    std::shared_ptr<arrow::Array> string_array;
    EXPECT_TRUE(int_builder.Finish(&int_array).ok());
    EXPECT_TRUE(string_builder.Finish(&string_array).ok());
    return arrow::RecordBatch::Make(
        MakeSchema(), count, {std::move(int_array), std::move(string_array)});
}

milvus_storage::api::Properties
MakeProperties() {
    milvus_storage::api::Properties properties;
    properties[PROPERTY_FS_STORAGE_TYPE] = std::string("local");
    properties[PROPERTY_FS_ROOT_PATH] = std::string("/");
    return properties;
}

void
CheckSparseVortexFileBacking(SparseVortexFileBacking backing) {
    const auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_sparse_fs_test_" + std::to_string(::getpid()) + "_" +
         std::to_string(static_cast<int>(backing)));
    std::filesystem::create_directories(dir);
    const auto file_path = dir / "sparse.vx";
    const std::string logical_path = "sparse-test.vx";

    SparseVortexFileSystemOptions options;
    options.backing = backing;
    options.file_path = file_path.string();
    options.mmap_populate = true;
    auto fs = MakeSparseVortexFileSystem(logical_path, std::move(options));
    auto range_result =
        milvus_storage::vortex::GetVortexRangeFile(fs, logical_path);
    ASSERT_TRUE(range_result.ok()) << range_result.status().ToString();
    auto range_file = std::move(range_result).ValueOrDie();

    range_file->Resize(4096);
    if (backing == SparseVortexFileBacking::Memory) {
        ASSERT_FALSE(std::filesystem::exists(file_path));
    } else {
        ASSERT_TRUE(std::filesystem::exists(file_path));
    }
    auto status = range_file->WriteAt(128, arrow::Buffer::FromString("abc"));
    ASSERT_TRUE(status.ok()) << status.ToString();

    std::array<char, 3> out{};
    auto read_result = range_file->ReadAt(128, out.size(), out.data());
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    ASSERT_EQ(read_result.ValueOrDie(), static_cast<int64_t>(out.size()));
    ASSERT_EQ(std::string(out.data(), out.size()), "abc");

    range_file->Punch(128, out.size());
    out.fill('\1');
    read_result = range_file->ReadAt(128, out.size(), out.data());
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    ASSERT_EQ(read_result.ValueOrDie(), static_cast<int64_t>(out.size()));
    ASSERT_EQ(std::string(out.data(), out.size()), std::string(out.size(), 0));

    range_file.reset();
    fs.reset();
    ASSERT_FALSE(std::filesystem::exists(file_path));
    std::filesystem::remove_all(dir);
}

VortexColumn::FileInfo
WriteVortexFile(const std::string& path,
                const std::shared_ptr<arrow::Schema>& schema,
                const milvus_storage::api::Properties& properties,
                int64_t begin = 0) {
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    auto writer_result = milvus_storage::vortex::VortexFileWriter::Open(
        fs, schema, path, properties);
    EXPECT_TRUE(writer_result.ok()) << writer_result.status().ToString();
    if (!writer_result.ok()) {
        return {};
    }
    auto writer = std::move(writer_result).ValueOrDie();
    EXPECT_TRUE(writer->Write(MakeRecordBatch(begin, 8)).ok());
    EXPECT_TRUE(writer->Write(MakeRecordBatch(begin + 8, 8)).ok());
    EXPECT_TRUE(writer->Flush().ok());
    auto close_result = writer->Close();
    EXPECT_TRUE(close_result.ok());
    auto cg_file = close_result.ValueOrDie();

    VortexColumn::FileInfo info;
    info.path = path;
    info.start_index = begin;
    info.end_index = begin + 16;
    info.file_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFileSize, 0);
    info.footer_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFooterSize, 0);
    return info;
}

VortexColumn::FileInfo
WriteVortexBatches(
    const std::string& path,
    const std::shared_ptr<arrow::Schema>& schema,
    const milvus_storage::api::Properties& properties,
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    auto writer_result = milvus_storage::vortex::VortexFileWriter::Open(
        fs, schema, path, properties);
    EXPECT_TRUE(writer_result.ok()) << writer_result.status().ToString();
    if (!writer_result.ok()) {
        return {};
    }
    auto writer = std::move(writer_result).ValueOrDie();
    int64_t rows = 0;
    for (const auto& batch : batches) {
        EXPECT_TRUE(writer->Write(batch).ok());
        rows += batch->num_rows();
    }
    EXPECT_TRUE(writer->Flush().ok());
    auto close_result = writer->Close();
    EXPECT_TRUE(close_result.ok());
    if (!close_result.ok()) {
        return {};
    }
    auto cg_file = close_result.ValueOrDie();

    VortexColumn::FileInfo info;
    info.path = path;
    info.start_index = 0;
    info.end_index = rows;
    info.file_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFileSize, 0);
    info.footer_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFooterSize, 0);
    return info;
}

std::vector<int32_t>
CollectIntScanValues(VortexColumn& column, int64_t start, int64_t length) {
    auto options = ChunkedColumnInterface::ScanOptions::ForData(
        start, ChunkedColumnInterface::TargetType::Int32);

    auto result = column.Scan(nullptr, options);
    EXPECT_NE(result, nullptr);
    std::vector<int32_t> values;
    if (result == nullptr) {
        return values;
    }

    int64_t processed = 0;
    while (processed < length) {
        ChunkedColumnInterface::ScanBatch batch;
        EXPECT_TRUE(
            result->Next(length - processed,
                         ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                         &batch));
        const auto* data = batch.values.data_as<int32_t>();
        values.insert(values.end(), data, data + batch.size);
        processed += batch.size;
    }
    return values;
}

std::vector<std::string>
CollectStringScanValues(VortexColumn& column, int64_t start, int64_t length) {
    auto options = ChunkedColumnInterface::ScanOptions::ForData(
        start, ChunkedColumnInterface::TargetType::StringView);

    auto result = column.Scan(nullptr, options);
    EXPECT_NE(result, nullptr);
    std::vector<std::string> values;
    if (result == nullptr) {
        return values;
    }

    int64_t processed = 0;
    while (processed < length) {
        ChunkedColumnInterface::ScanBatch batch;
        EXPECT_TRUE(
            result->Next(length - processed,
                         ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                         &batch));
        const auto* data = batch.values.data_as<std::string_view>();
        for (int64_t i = 0; i < batch.size; ++i) {
            values.emplace_back(data[i]);
        }
        processed += batch.size;
    }
    return values;
}

std::vector<int64_t>
CollectFilteredRowIdPayload(VortexColumn& column,
                            const ChunkedColumnInterface::ScanOptions& options,
                            int64_t length) {
    auto result = column.Scan(nullptr, options);
    EXPECT_NE(result, nullptr);
    std::vector<int64_t> row_ids;
    if (result == nullptr) {
        return row_ids;
    }

    ChunkedColumnInterface::ScanBatch batch;
    EXPECT_TRUE(result->Next(
        length, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_TRUE(batch.values.empty());
    EXPECT_EQ(batch.size, static_cast<int64_t>(batch.row_ids.size()));
    for (size_t i = 0; i < batch.row_ids.size(); ++i) {
        if (i > 0) {
            EXPECT_LE(batch.row_ids[i - 1], batch.row_ids[i]);
        }
    }
    row_ids.insert(row_ids.end(), batch.row_ids.begin(), batch.row_ids.end());
    return row_ids;
}

proto::plan::GenericValue
IntValue(int64_t value) {
    proto::plan::GenericValue generic_value;
    generic_value.set_int64_val(value);
    return generic_value;
}

proto::plan::GenericValue
StringValue(std::string_view value) {
    proto::plan::GenericValue generic_value;
    generic_value.set_string_val(std::string(value));
    return generic_value;
}

bool
ExpectedValid(int64_t row) {
    return row % 4 != 1;
}

std::string
ExpectedString(DataType type, int64_t row);

bool
IsVortexStringPushdownType(DataType type) {
    return type == DataType::STRING || type == DataType::VARCHAR;
}

class ScopedVortexScanPushdownEnable {
 public:
    explicit ScopedVortexScanPushdownEnable(bool enable)
        : old_(ENABLE_VORTEX_SCAN_PUSHDOWN.load()) {
        SetDefaultVortexScanPushdownEnable(enable);
    }

    ~ScopedVortexScanPushdownEnable() {
        SetDefaultVortexScanPushdownEnable(old_);
    }

 private:
    bool old_;
};

void
CheckNullableFilteredScanReturnsValidity(VortexColumn& column, DataType type) {
    const auto value = StringValue(ExpectedString(type, 8));
    auto options = ChunkedColumnInterface::ScanOptions::ForUnary(
        0, proto::plan::OpType::Equal, value);
    EXPECT_TRUE(column.SupportsScanPushdown(options));
    auto scan_result = column.Scan(nullptr, options);
    ASSERT_NE(scan_result, nullptr);

    std::vector<int64_t> row_ids;
    ChunkedColumnInterface::ScanBatch scan_batch;
    ASSERT_TRUE(
        scan_result->Next(kNullableRows,
                          ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                          &scan_batch));
    EXPECT_TRUE(scan_batch.values.empty());
    EXPECT_EQ(scan_batch.size, static_cast<int64_t>(scan_batch.row_ids.size()));
    row_ids.insert(
        row_ids.end(), scan_batch.row_ids.begin(), scan_batch.row_ids.end());
    for (size_t i = 0; i < scan_batch.row_ids.size(); ++i) {
        if (i > 0) {
            EXPECT_LE(scan_batch.row_ids[i - 1], scan_batch.row_ids[i]);
        }
        const auto row = scan_batch.row_ids[i];
        EXPECT_EQ(IsScanRowValid(scan_batch, static_cast<int64_t>(i)),
                  ExpectedValid(row))
            << row;
    }
    EXPECT_EQ(row_ids, (std::vector<int64_t>{1, 5, 8, 9, 13}));

    auto offset_options = ChunkedColumnInterface::ScanOptions::ForUnary(
        3, proto::plan::OpType::Equal, value);
    EXPECT_EQ(CollectFilteredRowIdPayload(column, offset_options, 10),
              (std::vector<int64_t>{5, 8, 9}));
}

std::string
ExpectedString(DataType type, int64_t row) {
    switch (type) {
        case DataType::STRING:
            return "string_" + std::to_string(row);
        case DataType::VARCHAR:
            return "varchar_" + std::to_string(row);
        case DataType::TEXT:
            return "text_" + std::to_string(row);
        case DataType::JSON:
            return "{\"row\":" + std::to_string(row) + "}";
        case DataType::GEOMETRY:
            return "geometry_wkb_" + std::to_string(row);
        default:
            return {};
    }
}

FieldMeta
MakeNullableFieldMeta(FieldId field_id, DataType type) {
    auto name = FieldName("nullable_" + std::to_string(field_id.get()));
    switch (type) {
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return FieldMeta(name, field_id, type, 256, true, std::nullopt);
        case DataType::ARRAY:
            return FieldMeta(
                name, field_id, type, DataType::INT64, true, std::nullopt);
        default:
            return FieldMeta(name, field_id, type, true, std::nullopt);
    }
}

std::shared_ptr<arrow::DataType>
ArrowTypeForNullableField(DataType type) {
    switch (type) {
        case DataType::BOOL:
            return arrow::boolean();
        case DataType::INT8:
            return arrow::int8();
        case DataType::INT16:
            return arrow::int16();
        case DataType::INT32:
            return arrow::int32();
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return arrow::int64();
        case DataType::FLOAT:
            return arrow::float32();
        case DataType::DOUBLE:
            return arrow::float64();
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return arrow::utf8();
        case DataType::JSON:
        case DataType::GEOMETRY:
            return arrow::binary();
        case DataType::ARRAY:
            return arrow::list(arrow::int64());
        default:
            return arrow::null();
    }
}

std::vector<DataType>
NullableLocalVortexTypes() {
    return {DataType::BOOL,
            DataType::INT8,
            DataType::INT16,
            DataType::INT32,
            DataType::INT64,
            DataType::FLOAT,
            DataType::DOUBLE,
            DataType::TIMESTAMPTZ,
            DataType::STRING,
            DataType::VARCHAR,
            DataType::TEXT,
            DataType::JSON,
            DataType::GEOMETRY,
            DataType::ARRAY};
}

std::shared_ptr<arrow::Schema>
MakeNullableSchema() {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    auto types = NullableLocalVortexTypes();
    fields.reserve(types.size());
    for (size_t i = 0; i < types.size(); ++i) {
        const auto field_id = kNullableFieldIdBase + static_cast<int64_t>(i);
        fields.emplace_back(arrow::field(std::to_string(field_id),
                                         ArrowTypeForNullableField(types[i]),
                                         true));
    }
    return arrow::schema(std::move(fields));
}

std::shared_ptr<arrow::Array>
BuildNullableArray(DataType type, int64_t begin, int64_t count) {
    switch (type) {
        case DataType::BOOL: {
            arrow::BooleanBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(row % 2 == 0).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::INT8: {
            arrow::Int8Builder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(
                        builder.Append(static_cast<int8_t>(row - 8)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::INT16: {
            arrow::Int16Builder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(
                        builder.Append(static_cast<int16_t>(row * 10)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::INT32: {
            arrow::Int32Builder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(
                        builder.Append(static_cast<int32_t>(row * 100)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::INT64:
        case DataType::TIMESTAMPTZ: {
            arrow::Int64Builder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    const auto value = type == DataType::TIMESTAMPTZ
                                           ? 1700000000000000LL + row
                                           : row * 1000;
                    EXPECT_TRUE(builder.Append(value).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::FLOAT: {
            arrow::FloatBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(
                        builder.Append(static_cast<float>(row) * 1.5f).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::DOUBLE: {
            arrow::DoubleBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(
                        builder.Append(static_cast<double>(row) * 2.25).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT: {
            arrow::StringBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(ExpectedString(type, row)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::JSON:
        case DataType::GEOMETRY: {
            arrow::BinaryBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append(ExpectedString(type, row)).ok());
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        case DataType::ARRAY: {
            auto value_builder = std::make_shared<arrow::Int64Builder>();
            arrow::ListBuilder builder(arrow::default_memory_pool(),
                                       value_builder);
            auto* values =
                static_cast<arrow::Int64Builder*>(builder.value_builder());
            for (int64_t row = begin; row < begin + count; ++row) {
                if (!ExpectedValid(row)) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                } else {
                    EXPECT_TRUE(builder.Append().ok());
                    if (row != 0) {
                        EXPECT_TRUE(values->Append(row).ok());
                        EXPECT_TRUE(values->Append(row + 1).ok());
                        EXPECT_TRUE(values->Append(row + 2).ok());
                    }
                }
            }
            std::shared_ptr<arrow::Array> array;
            EXPECT_TRUE(builder.Finish(&array).ok());
            return array;
        }
        default:
            return nullptr;
    }
}

std::shared_ptr<arrow::RecordBatch>
MakeNullableRecordBatch(int64_t begin, int64_t count) {
    auto types = NullableLocalVortexTypes();
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(types.size());
    for (auto type : types) {
        arrays.emplace_back(BuildNullableArray(type, begin, count));
    }
    return arrow::RecordBatch::Make(
        MakeNullableSchema(), count, std::move(arrays));
}

VortexColumn::FileInfo
WriteNullableVortexFile(const std::string& path,
                        const std::shared_ptr<arrow::Schema>& schema,
                        const milvus_storage::api::Properties& properties,
                        int64_t begin = 0) {
    auto fs = std::make_shared<arrow::fs::LocalFileSystem>();
    auto writer_result = milvus_storage::vortex::VortexFileWriter::Open(
        fs, schema, path, properties);
    EXPECT_TRUE(writer_result.ok()) << writer_result.status().ToString();
    if (!writer_result.ok()) {
        return {};
    }
    auto writer = std::move(writer_result).ValueOrDie();
    EXPECT_TRUE(writer->Write(MakeNullableRecordBatch(begin, 8)).ok());
    EXPECT_TRUE(writer->Write(MakeNullableRecordBatch(begin + 8, 8)).ok());
    EXPECT_TRUE(writer->Flush().ok());
    auto close_result = writer->Close();
    EXPECT_TRUE(close_result.ok());
    auto cg_file = close_result.ValueOrDie();

    VortexColumn::FileInfo info;
    info.path = path;
    info.start_index = begin;
    info.end_index = begin + kNullableRows;
    info.file_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFileSize, 0);
    info.footer_size =
        cg_file.Get<uint64_t>(milvus_storage::api::kPropertyFooterSize, 0);
    return info;
}

ChunkedColumnInterface::TargetType
TargetTypeForDataType(DataType type) {
    switch (type) {
        case DataType::BOOL:
            return ChunkedColumnInterface::TargetType::Bool;
        case DataType::INT8:
            return ChunkedColumnInterface::TargetType::Int8;
        case DataType::INT16:
            return ChunkedColumnInterface::TargetType::Int16;
        case DataType::INT32:
            return ChunkedColumnInterface::TargetType::Int32;
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return ChunkedColumnInterface::TargetType::Int64;
        case DataType::FLOAT:
            return ChunkedColumnInterface::TargetType::Float;
        case DataType::DOUBLE:
            return ChunkedColumnInterface::TargetType::Double;
        case DataType::JSON:
            return ChunkedColumnInterface::TargetType::Json;
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
        case DataType::GEOMETRY:
            return ChunkedColumnInterface::TargetType::StringView;
        case DataType::ARRAY:
            return ChunkedColumnInterface::TargetType::ArrayView;
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported test data type {}",
                      static_cast<int>(type));
    }
}

std::shared_ptr<VortexColumnGroup>
MakeColumnGroup(
    std::vector<VortexColumn::FileInfo> files,
    const std::shared_ptr<milvus_storage::api::Properties>& properties,
    std::vector<std::string> field_names) {
    return std::make_shared<VortexColumnGroup>(
        files,
        properties,
        field_names,
        CacheWarmupPolicy::CacheWarmupPolicy_Disable,
        nullptr);
}

VortexColumn
MakeNullableColumn(
    DataType type,
    FieldId field_id,
    const VortexColumn::FileInfo& file_info,
    const std::shared_ptr<milvus_storage::api::Properties>& properties) {
    auto column_group = MakeColumnGroup(
        {file_info}, properties, {std::to_string(field_id.get())});
    return VortexColumn(field_id,
                        MakeNullableFieldMeta(field_id, type),
                        properties,
                        column_group);
}

void
CheckNoDataScan(VortexColumn& column) {
    auto options = ChunkedColumnInterface::ScanOptions::ForData(
        0, ChunkedColumnInterface::TargetType::None);

    auto result = column.Scan(nullptr, options);
    ASSERT_NE(result, nullptr);
    int64_t seen = 0;
    while (seen < kNullableRows) {
        ChunkedColumnInterface::ScanBatch batch;
        ASSERT_TRUE(
            result->Next(kNullableRows - seen,
                         ChunkedColumnInterface::ScanReadMode::ValidityOnly,
                         &batch));
        EXPECT_TRUE(batch.values.empty());
        for (int64_t i = 0; i < batch.size; ++i) {
            const auto row = batch.row_id_start + i;
            EXPECT_EQ(IsScanRowValid(batch, i), ExpectedValid(row)) << row;
        }
        seen += batch.size;
    }
    EXPECT_EQ(seen, kNullableRows);

    auto all_valid_cursor =
        column.Scan(nullptr,
                    ChunkedColumnInterface::ScanOptions::ForData(
                        2, ChunkedColumnInterface::TargetType::None));
    ASSERT_NE(all_valid_cursor, nullptr);
    ChunkedColumnInterface::ScanBatch all_valid_batch;
    ASSERT_TRUE(all_valid_cursor->Next(
        1,
        ChunkedColumnInterface::ScanReadMode::ValidityOnly,
        &all_valid_batch));
    EXPECT_TRUE(all_valid_batch.values.empty());
    EXPECT_FALSE(all_valid_batch.validity);
    EXPECT_TRUE(IsScanRowValid(all_valid_batch, 0));
}

void
CheckApplyValidDataInChunk(VortexColumn& column) {
    constexpr int64_t offset = 3;
    constexpr int64_t size = 9;
    TargetBitmap valid(size, true);
    TargetBitmapView valid_view(valid);

    column.ApplyValidDataInChunk(nullptr, 0, offset, size, valid_view);

    for (int64_t i = 0; i < size; ++i) {
        EXPECT_EQ(valid[i], ExpectedValid(offset + i)) << offset + i;
    }
}

template <typename T>
void
CheckFixedWidthBatch(DataType type,
                     const ChunkedColumnInterface::ScanBatch& batch) {
    const auto* values = batch.values.data_as<T>();
    for (int64_t i = 0; i < batch.size; ++i) {
        const auto row = batch.row_id_start + i;
        EXPECT_EQ(IsScanRowValid(batch, i), ExpectedValid(row)) << row;
        if (!ExpectedValid(row)) {
            continue;
        }
        if constexpr (std::is_same_v<T, bool>) {
            EXPECT_EQ(values[i], row % 2 == 0) << row;
        } else if constexpr (std::is_same_v<T, int8_t>) {
            EXPECT_EQ(values[i], static_cast<int8_t>(row - 8)) << row;
        } else if constexpr (std::is_same_v<T, int16_t>) {
            EXPECT_EQ(values[i], static_cast<int16_t>(row * 10)) << row;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            EXPECT_EQ(values[i], static_cast<int32_t>(row * 100)) << row;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            const auto expected = type == DataType::TIMESTAMPTZ
                                      ? 1700000000000000LL + row
                                      : row * 1000;
            EXPECT_EQ(values[i], expected) << row;
        } else if constexpr (std::is_same_v<T, float>) {
            EXPECT_NEAR(values[i], static_cast<float>(row) * 1.5f, 1e-5) << row;
        } else if constexpr (std::is_same_v<T, double>) {
            EXPECT_NEAR(values[i], static_cast<double>(row) * 2.25, 1e-9)
                << row;
        }
    }
}

void
CheckStringLikeBatch(DataType type,
                     const ChunkedColumnInterface::ScanBatch& batch) {
    if (type == DataType::JSON) {
        const auto* values = batch.values.data_as<Json>();
        for (int64_t i = 0; i < batch.size; ++i) {
            const auto row = batch.row_id_start + i;
            EXPECT_EQ(IsScanRowValid(batch, i), ExpectedValid(row)) << row;
            if (ExpectedValid(row)) {
                std::string_view view = values[i];
                EXPECT_EQ(view, ExpectedString(type, row)) << row;
            }
        }
        return;
    }

    const auto* values = batch.values.data_as<std::string_view>();
    for (int64_t i = 0; i < batch.size; ++i) {
        const auto row = batch.row_id_start + i;
        EXPECT_EQ(IsScanRowValid(batch, i), ExpectedValid(row)) << row;
        if (ExpectedValid(row)) {
            EXPECT_EQ(values[i], ExpectedString(type, row)) << row;
        }
    }
}

void
CheckArrayBatch(const ChunkedColumnInterface::ScanBatch& batch) {
    const auto* values = batch.values.data_as<ArrayView>();
    for (int64_t i = 0; i < batch.size; ++i) {
        const auto row = batch.row_id_start + i;
        EXPECT_EQ(IsScanRowValid(batch, i), ExpectedValid(row)) << row;
        if (!ExpectedValid(row)) {
            continue;
        }
        if (row == 0) {
            EXPECT_EQ(values[i].length(), 0);
            EXPECT_EQ(values[i].get_element_type(), DataType::INT64);
            EXPECT_NE(values[i].data(), nullptr);
            continue;
        }
        EXPECT_EQ(values[i].length(), 3) << row;
        EXPECT_EQ(values[i].get_data<int64_t>(0), row) << row;
        EXPECT_EQ(values[i].get_data<int64_t>(1), row + 1) << row;
        EXPECT_EQ(values[i].get_data<int64_t>(2), row + 2) << row;
    }
}

void
CheckDataScan(VortexColumn& column, DataType type) {
    auto options = ChunkedColumnInterface::ScanOptions::ForData(
        0, TargetTypeForDataType(type));

    auto result = column.Scan(nullptr, options);
    ASSERT_NE(result, nullptr);

    int64_t seen = 0;
    while (seen < kNullableRows) {
        ChunkedColumnInterface::ScanBatch batch;
        ASSERT_TRUE(
            result->Next(kNullableRows - seen,
                         ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                         &batch));
        ASSERT_GT(batch.size, 0);
        EXPECT_TRUE(batch.row_ids.empty());
        switch (type) {
            case DataType::BOOL:
                CheckFixedWidthBatch<bool>(type, batch);
                break;
            case DataType::INT8:
                CheckFixedWidthBatch<int8_t>(type, batch);
                break;
            case DataType::INT16:
                CheckFixedWidthBatch<int16_t>(type, batch);
                break;
            case DataType::INT32:
                CheckFixedWidthBatch<int32_t>(type, batch);
                break;
            case DataType::INT64:
            case DataType::TIMESTAMPTZ:
                CheckFixedWidthBatch<int64_t>(type, batch);
                break;
            case DataType::FLOAT:
                CheckFixedWidthBatch<float>(type, batch);
                break;
            case DataType::DOUBLE:
                CheckFixedWidthBatch<double>(type, batch);
                break;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
            case DataType::JSON:
            case DataType::GEOMETRY:
                CheckStringLikeBatch(type, batch);
                break;
            case DataType::ARRAY:
                CheckArrayBatch(batch);
                break;
            default:
                FAIL() << "unexpected data type";
        }
        seen += batch.size;
    }
    EXPECT_EQ(seen, kNullableRows);

    if (type == DataType::INT32) {
        auto skip_file = std::make_shared<const detail::ColumnFilter>(
            detail::ColumnFilter::MetricsSource::PreloadedStatistics,
            [](int64_t cell_id) { return cell_id == 0; });
        auto filtered_options = ChunkedColumnInterface::ScanOptions::ForData(
            0, TargetTypeForDataType(type));
        filtered_options.filter = std::move(skip_file);
        auto filtered = column.Scan(nullptr, filtered_options);
        ASSERT_NE(filtered, nullptr);
        int64_t filtered_seen = 0;
        while (filtered_seen < kNullableRows) {
            ChunkedColumnInterface::ScanBatch batch;
            ASSERT_TRUE(filtered->Next(
                kNullableRows - filtered_seen,
                ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                &batch));
            EXPECT_TRUE(batch.data_skipped);
            EXPECT_TRUE(batch.values.empty());
            for (int64_t i = 0; i < batch.size; ++i) {
                const auto row = batch.row_id_start + i;
                EXPECT_EQ(IsScanRowValid(batch, i), ExpectedValid(row)) << row;
            }
            filtered_seen += batch.size;
        }
    }
}

void
CheckOrderedTake(VortexColumn& column, DataType type) {
    const std::vector<int64_t> offsets{15, 1, 8, 1, 0};
    auto take = column.Take(nullptr,
                            ChunkedColumnInterface::TakeOptions{
                                ChunkedColumnInterface::OffsetView::From(
                                    offsets.data(), offsets.size()),
                                TargetTypeForDataType(type)});
    ASSERT_NE(take, nullptr);
    ASSERT_EQ(take->Size(), static_cast<int64_t>(offsets.size()));
    EXPECT_TRUE(take->IsOwned());
    auto owned = take->GetOwn();
    EXPECT_NE(owned.owner, nullptr);
    EXPECT_EQ(owned.size, static_cast<int64_t>(offsets.size()));

    for (int64_t i = 0; i < take->Size(); ++i) {
        const auto row = offsets[i];
        const auto valid = take->IsValid(i);
        EXPECT_EQ(valid, ExpectedValid(row)) << row;
        if (!valid) {
            continue;
        }
        switch (type) {
            case DataType::BOOL:
                EXPECT_EQ(*take->Get<bool>(i).value, row % 2 == 0) << row;
                break;
            case DataType::INT8:
                EXPECT_EQ(*take->Get<int8_t>(i).value,
                          static_cast<int8_t>(row - 8))
                    << row;
                break;
            case DataType::INT16:
                EXPECT_EQ(*take->Get<int16_t>(i).value,
                          static_cast<int16_t>(row * 10))
                    << row;
                break;
            case DataType::INT32:
                EXPECT_EQ(*take->Get<int32_t>(i).value,
                          static_cast<int32_t>(row * 100))
                    << row;
                break;
            case DataType::INT64:
                EXPECT_EQ(*take->Get<int64_t>(i).value, row * 1000) << row;
                break;
            case DataType::TIMESTAMPTZ:
                EXPECT_EQ(*take->Get<int64_t>(i).value,
                          1700000000000000LL + row)
                    << row;
                break;
            case DataType::FLOAT:
                EXPECT_NEAR(*take->Get<float>(i).value,
                            static_cast<float>(row) * 1.5f,
                            1e-5)
                    << row;
                break;
            case DataType::DOUBLE:
                EXPECT_NEAR(*take->Get<double>(i).value,
                            static_cast<double>(row) * 2.25,
                            1e-9)
                    << row;
                break;
            case DataType::STRING:
            case DataType::VARCHAR:
            case DataType::TEXT:
            case DataType::GEOMETRY:
                EXPECT_EQ(*take->Get<std::string_view>(i).value,
                          ExpectedString(type, row))
                    << row;
                break;
            case DataType::JSON: {
                std::string_view value = *take->Get<Json>(i).value;
                EXPECT_EQ(value, ExpectedString(type, row)) << row;
                break;
            }
            case DataType::ARRAY: {
                const auto value = *take->Get<ArrayView>(i).value;
                if (row == 0) {
                    EXPECT_EQ(value.length(), 0);
                    EXPECT_EQ(value.get_element_type(), DataType::INT64);
                    EXPECT_NE(value.data(), nullptr);
                    break;
                }
                EXPECT_EQ(value.length(), 3) << row;
                EXPECT_EQ(value.get_data<int64_t>(0), row) << row;
                EXPECT_EQ(value.get_data<int64_t>(1), row + 1) << row;
                EXPECT_EQ(value.get_data<int64_t>(2), row + 2) << row;
                break;
            }
            default:
                FAIL() << "unexpected take data type";
        }
    }

    if (type == DataType::INT32) {
        auto skip_file = std::make_shared<const detail::ColumnFilter>(
            detail::ColumnFilter::MetricsSource::PreloadedStatistics,
            [](int64_t cell_id) { return cell_id == 0; });
        auto filtered =
            column.Take(nullptr,
                        ChunkedColumnInterface::TakeOptions{
                            ChunkedColumnInterface::OffsetView::From(
                                offsets.data(), offsets.size()),
                            TargetTypeForDataType(type),
                            std::move(skip_file)});
        ASSERT_NE(filtered, nullptr);
        const auto filtered_owned = filtered->GetOwn();
        ASSERT_TRUE(filtered_owned.data_skipped);
        for (int64_t i = 0; i < filtered->Size(); ++i) {
            EXPECT_EQ(filtered->IsValid(i), ExpectedValid(offsets[i]));
            EXPECT_TRUE(filtered_owned.data_skipped[i]);
            const auto item = filtered->Get<int32_t>(i);
            EXPECT_EQ(item.is_valid, ExpectedValid(offsets[i]));
            EXPECT_TRUE(item.data_skipped);
            EXPECT_FALSE(item.value.has_value());
        }
    }
}

}  // namespace

TEST(VortexColumnTest, RowIdScanPreservesExpressionValiditySemantics) {
    TargetBitmap bitmap_input(4, true);
    bitmap_input[1] = false;

    auto evaluate = [&](bool mask_validity_by_bitmap_input) {
        return exec::RowIdScanBatchToBitmaps(MakeNullableRowIdBatch(),
                                             0,
                                             4,
                                             bitmap_input,
                                             mask_validity_by_bitmap_input);
    };

    auto unary = evaluate(true);
    EXPECT_TRUE(unary.validity[1]);
    EXPECT_TRUE(unary.result[2]);

    auto binary = evaluate(false);
    EXPECT_FALSE(binary.validity[1]);
    EXPECT_TRUE(binary.result[2]);
}

TEST(VortexColumnTest, SparseFileSystemBackingModes) {
    CheckSparseVortexFileBacking(SparseVortexFileBacking::Memory);
    CheckSparseVortexFileBacking(SparseVortexFileBacking::Mmap);
    CheckSparseVortexFileBacking(SparseVortexFileBacking::Disk);
}

TEST(VortexColumnTest, ScanAndTake) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());

    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_test_" + std::to_string(::getpid()) + "_" +
         std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);
    auto path = (dir / "cg0.vx").string();

    auto file_info = WriteVortexFile(path, schema, *properties);
    auto column_group = MakeColumnGroup(
        {file_info},
        properties,
        {std::to_string(kIntFieldId), std::to_string(kStringFieldId)});

    FieldMeta int_meta(FieldName("int_field"),
                       FieldId(kIntFieldId),
                       DataType::INT32,
                       false,
                       std::nullopt);
    VortexColumn int_column(
        FieldId(kIntFieldId), int_meta, properties, column_group);
    EXPECT_EQ(int_column.NumRows(), 16);
    EXPECT_EQ(int_column.num_chunks(), 1);

    std::vector<int64_t> offsets{7, 1, 7, 15};
    std::vector<int32_t> values(offsets.size());
    int_column.BulkPrimitiveValueAt(
        nullptr, values.data(), offsets.data(), offsets.size(), false);
    EXPECT_EQ(values, (std::vector<int32_t>{70, 10, 70, 150}));

    column_group->ManualEvictCache();
    EXPECT_FALSE(int_column.CellsLoaded(offsets.data(), offsets.size()));
    auto take_result =
        int_column.Take(nullptr,
                        ChunkedColumnInterface::TakeOptions{
                            ChunkedColumnInterface::OffsetView::From(
                                offsets.data(), offsets.size()),
                            ChunkedColumnInterface::TargetType::Int32});
    ASSERT_NE(take_result, nullptr);
    ASSERT_EQ(take_result->Size(), static_cast<int64_t>(offsets.size()));
    EXPECT_TRUE(int_column.CellsLoaded(offsets.data(), offsets.size()));
    column_group->ManualEvictCache();
    EXPECT_FALSE(int_column.CellsLoaded(offsets.data(), offsets.size()));
    EXPECT_EQ(*take_result->Get<int32_t>(0).value, 70);
    EXPECT_EQ(*take_result->Get<int32_t>(1).value, 10);
    EXPECT_EQ(*take_result->Get<int32_t>(2).value, 70);
    EXPECT_EQ(*take_result->Get<int32_t>(3).value, 150);
    take_result.reset();
    column_group->ManualEvictCache();
    EXPECT_FALSE(int_column.CellsLoaded(offsets.data(), offsets.size()));

    auto scan_values = CollectIntScanValues(int_column, 3, 5);
    EXPECT_EQ(scan_values, (std::vector<int32_t>{30, 40, 50, 60, 70}));

    FieldMeta string_meta(FieldName("string_field"),
                          FieldId(kStringFieldId),
                          DataType::VARCHAR,
                          128,
                          false,
                          std::nullopt);
    VortexColumn string_column(
        FieldId(kStringFieldId), string_meta, properties, column_group);

    std::vector<std::string> strings(offsets.size());
    string_column.BulkRawStringAt(
        nullptr,
        [&](std::string_view value, size_t index, bool valid) {
            EXPECT_TRUE(valid);
            strings[index] = std::string(value);
        },
        offsets.data(),
        offsets.size());
    EXPECT_EQ(strings, (std::vector<std::string>{"v7", "v1", "v7", "v15"}));

    take_result =
        string_column.Take(nullptr,
                           ChunkedColumnInterface::TakeOptions{
                               ChunkedColumnInterface::OffsetView::From(
                                   offsets.data(), offsets.size()),
                               ChunkedColumnInterface::TargetType::StringView});
    ASSERT_NE(take_result, nullptr);
    EXPECT_EQ(*take_result->Get<std::string_view>(0).value, "v7");
    EXPECT_EQ(*take_result->Get<std::string_view>(1).value, "v1");
    EXPECT_EQ(*take_result->Get<std::string_view>(2).value, "v7");
    EXPECT_EQ(*take_result->Get<std::string_view>(3).value, "v15");

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, UnsupportedInt32FilteredScanThrows) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());

    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_filter_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file_info =
        WriteVortexFile((dir / "cg0.vx").string(), schema, *properties);
    auto column_group =
        MakeColumnGroup({file_info}, properties, {std::to_string(kIntFieldId)});

    FieldMeta int_meta(FieldName("int_field"),
                       FieldId(kIntFieldId),
                       DataType::INT32,
                       false,
                       std::nullopt);
    VortexColumn int_column(
        FieldId(kIntFieldId), int_meta, properties, column_group);

    auto unary_options = ChunkedColumnInterface::ScanOptions::ForUnary(
        3, proto::plan::OpType::GreaterThan, IntValue(80));
    EXPECT_FALSE(int_column.SupportsScanPushdown(unary_options));
    EXPECT_THROW(CollectFilteredRowIdPayload(int_column, unary_options, 10),
                 std::exception);

    auto range_options = ChunkedColumnInterface::ScanOptions::ForBinaryRange(
        2, IntValue(40), false, IntValue(90), true);
    EXPECT_FALSE(int_column.SupportsScanPushdown(range_options));
    EXPECT_THROW(CollectFilteredRowIdPayload(int_column, range_options, 10),
                 std::exception);

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, MultiFileTakeAndScan) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());

    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_multifile_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file0 =
        WriteVortexFile((dir / "cg0.vx").string(), schema, *properties, 0);
    auto file1 =
        WriteVortexFile((dir / "cg1.vx").string(), schema, *properties, 16);
    auto column_group = MakeColumnGroup(
        {file0, file1}, properties, {std::to_string(kIntFieldId)});

    FieldMeta int_meta(FieldName("int_field"),
                       FieldId(kIntFieldId),
                       DataType::INT32,
                       false,
                       std::nullopt);
    VortexColumn int_column(
        FieldId(kIntFieldId), int_meta, properties, column_group);
    EXPECT_EQ(int_column.NumRows(), 32);
    EXPECT_EQ(int_column.num_chunks(), 2);
    EXPECT_EQ(int_column.chunk_row_nums(0), 16);
    EXPECT_EQ(int_column.chunk_row_nums(1), 16);

    const int64_t first_file_offset[] = {0};
    const int64_t second_file_offset[] = {16};
    const int64_t cross_file_offsets[] = {0, 16};
    EXPECT_TRUE(int_column.CellsLoaded(nullptr, 0));
    EXPECT_FALSE(int_column.CellsLoaded(first_file_offset, 1));
    EXPECT_FALSE(int_column.CellsLoaded(second_file_offset, 1));

    int32_t first_value = 0;
    int_column.BulkPrimitiveValueAt(
        nullptr, &first_value, first_file_offset, 1, false);
    EXPECT_EQ(first_value, 0);
    EXPECT_TRUE(int_column.CellsLoaded(first_file_offset, 1));
    EXPECT_FALSE(int_column.CellsLoaded(second_file_offset, 1));
    EXPECT_FALSE(int_column.CellsLoaded(cross_file_offsets, 2));

    int_column.ManualEvictCache();
    EXPECT_FALSE(int_column.CellsLoaded(first_file_offset, 1));

    std::vector<int64_t> offsets{0, 15, 16, 17, 31, 16};
    std::vector<int32_t> values(offsets.size());
    int_column.BulkPrimitiveValueAt(
        nullptr, values.data(), offsets.data(), offsets.size(), false);
    EXPECT_EQ(values, (std::vector<int32_t>{0, 150, 160, 170, 310, 160}));

    auto take_result =
        int_column.Take(nullptr,
                        ChunkedColumnInterface::TakeOptions{
                            ChunkedColumnInterface::OffsetView::From(
                                offsets.data(), offsets.size()),
                            ChunkedColumnInterface::TargetType::Int32});
    ASSERT_NE(take_result, nullptr);
    ASSERT_EQ(take_result->Size(), static_cast<int64_t>(offsets.size()));
    std::vector<int32_t> taken_values;
    for (int64_t i = 0; i < take_result->Size(); ++i) {
        taken_values.emplace_back(*take_result->Get<int32_t>(i).value);
    }
    EXPECT_EQ(taken_values, (std::vector<int32_t>{0, 150, 160, 170, 310, 160}));

    auto skip_second_file = std::make_shared<const detail::ColumnFilter>(
        detail::ColumnFilter::MetricsSource::PreloadedStatistics,
        [](int64_t cell_id) { return cell_id == 1; });
    auto filtered_take =
        int_column.Take(nullptr,
                        ChunkedColumnInterface::TakeOptions{
                            ChunkedColumnInterface::OffsetView::From(
                                offsets.data(), offsets.size()),
                            ChunkedColumnInterface::TargetType::Int32,
                            skip_second_file});
    ASSERT_NE(filtered_take, nullptr);
    for (int64_t i = 0; i < filtered_take->Size(); ++i) {
        const auto item = filtered_take->Get<int32_t>(i);
        EXPECT_TRUE(item.is_valid);
        const auto expected_skip = offsets[i] >= 16;
        EXPECT_EQ(item.data_skipped, expected_skip);
        EXPECT_EQ(item.value.has_value(), !expected_skip);
    }

    auto scan_values = CollectIntScanValues(int_column, 18, 4);
    EXPECT_EQ(scan_values, (std::vector<int32_t>{180, 190, 200, 210}));

    int_column.ManualEvictCache();
    EXPECT_FALSE(int_column.CellsLoaded(first_file_offset, 1));
    EXPECT_FALSE(int_column.CellsLoaded(second_file_offset, 1));
    auto all_valid_cursor =
        int_column.Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            14, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(all_valid_cursor, nullptr);
    EXPECT_FALSE(int_column.CellsLoaded(first_file_offset, 1));
    EXPECT_FALSE(int_column.CellsLoaded(second_file_offset, 1));
    auto cross_chunk_options = ChunkedColumnInterface::ScanOptions::ForData(
        14, ChunkedColumnInterface::TargetType::Int32);
    auto scan_result = int_column.Scan(nullptr, cross_chunk_options);
    ASSERT_NE(scan_result, nullptr);
    EXPECT_FALSE(int_column.CellsLoaded(first_file_offset, 1));
    EXPECT_FALSE(int_column.CellsLoaded(second_file_offset, 1));

    std::vector<ChunkedColumnInterface::ScanBatch> scan_batches;
    int64_t processed = 0;
    while (processed < 5) {
        ChunkedColumnInterface::ScanBatch batch;
        ASSERT_TRUE(scan_result->Next(
            5 - processed,
            ChunkedColumnInterface::ScanReadMode::DataAndValidity,
            &batch));
        processed += batch.size;
        scan_batches.emplace_back(std::move(batch));
    }
    EXPECT_TRUE(int_column.CellsLoaded(cross_file_offsets, 2));

    std::vector<int64_t> batch_starts;
    std::vector<int64_t> batch_sizes;
    std::vector<int32_t> cross_chunk_values;
    for (const auto& batch : scan_batches) {
        batch_starts.emplace_back(batch.row_id_start);
        batch_sizes.emplace_back(batch.size);
        const auto* data = batch.values.data_as<int32_t>();
        cross_chunk_values.insert(
            cross_chunk_values.end(), data, data + batch.size);
    }
    EXPECT_EQ(batch_starts, (std::vector<int64_t>{14, 16}));
    EXPECT_EQ(batch_sizes, (std::vector<int64_t>{2, 3}));
    EXPECT_EQ(cross_chunk_values,
              (std::vector<int32_t>{140, 150, 160, 170, 180}));

    auto filtered_scan_options = ChunkedColumnInterface::ScanOptions::ForData(
        14, ChunkedColumnInterface::TargetType::Int32);
    filtered_scan_options.filter = skip_second_file;
    auto filtered_scan = int_column.Scan(nullptr, filtered_scan_options);
    ASSERT_NE(filtered_scan, nullptr);
    ChunkedColumnInterface::ScanBatch filtered_batch;
    ASSERT_TRUE(filtered_scan->Next(
        5,
        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
        &filtered_batch));
    EXPECT_EQ(filtered_batch.row_id_start, 14);
    EXPECT_EQ(filtered_batch.size, 2);
    EXPECT_FALSE(filtered_batch.data_skipped);
    ASSERT_TRUE(filtered_scan->Next(
        3,
        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
        &filtered_batch));
    EXPECT_EQ(filtered_batch.row_id_start, 16);
    EXPECT_EQ(filtered_batch.size, 3);
    EXPECT_TRUE(filtered_batch.data_skipped);
    EXPECT_TRUE(filtered_batch.values.empty());

    auto filter_options = ChunkedColumnInterface::ScanOptions::ForUnary(
        14, proto::plan::OpType::GreaterThan, IntValue(160));
    EXPECT_THROW(CollectFilteredRowIdPayload(int_column, filter_options, 6),
                 std::exception);

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, RejectsNonContiguousFileRanges) {
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    std::vector<VortexColumn::FileInfo> files{
        VortexColumn::FileInfo{"unused0.vx", 0, 16, 0, 0},
        VortexColumn::FileInfo{"unused1.vx", 17, 33, 0, 0},
    };

    EXPECT_THROW(
        MakeColumnGroup(
            std::move(files), properties, {std::to_string(kIntFieldId)}),
        std::exception);
}

TEST(VortexColumnTest, CursorOwnedScanReleasesOldFileBeforePinningNextFile) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    auto dir = std::filesystem::temp_directory_path() /
               ("milvus_vortex_cursor_pin_transition_test_" +
                std::to_string(::getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file0 =
        WriteVortexFile((dir / "cg0.vx").string(), schema, *properties, 0);
    auto file1 =
        WriteVortexFile((dir / "cg1.vx").string(), schema, *properties, 16);
    auto column_group = MakeColumnGroup(
        {file0, file1}, properties, {std::to_string(kIntFieldId)});
    FieldMeta field_meta(FieldName("int_field"),
                         FieldId(kIntFieldId),
                         DataType::INT32,
                         false,
                         std::nullopt);
    VortexColumn column(
        FieldId(kIntFieldId), field_meta, properties, column_group);

    folly::CancellationSource cancellation_source;
    OpContext op_ctx(cancellation_source.getToken());
    auto cursor =
        column.Scan(&op_ctx,
                    ChunkedColumnInterface::ScanOptions::ForData(
                        14,
                        ChunkedColumnInterface::TargetType::Int32,
                        ChunkedColumnInterface::ScanPinPolicy::CursorOwned));
    ASSERT_NE(cursor, nullptr);

    const int64_t first_file_offset[]{14};
    const int64_t second_file_offset[]{16};
    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_TRUE(column.CellsLoaded(first_file_offset, 1));
    EXPECT_FALSE(column.CellsLoaded(second_file_offset, 1));
    batch = {};

    cancellation_source.requestCancellation();
    EXPECT_THROW(
        cursor->Next(
            1, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch),
        std::exception);

    column.ManualEvictCache();
    EXPECT_FALSE(column.CellsLoaded(first_file_offset, 1));
    EXPECT_FALSE(column.CellsLoaded(second_file_offset, 1));

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, MultiFieldColumnsShareColumnGroup) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());

    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_group_test_" + std::to_string(::getpid()) + "_" +
         std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file_info =
        WriteVortexFile((dir / "cg0.vx").string(), schema, *properties);
    auto column_group = std::make_shared<VortexColumnGroup>(
        std::vector<VortexColumn::FileInfo>{file_info},
        properties,
        std::vector<std::string>{std::to_string(kIntFieldId),
                                 std::to_string(kStringFieldId)},
        CacheWarmupPolicy::CacheWarmupPolicy_Disable,
        nullptr);
    ASSERT_EQ(column_group->files().size(), 1);
    EXPECT_EQ(column_group->files()[0].field_planners.size(), 2);
    EXPECT_EQ(
        column_group->FieldPlanner(0, std::to_string(kIntFieldId))->rows(), 16);
    EXPECT_EQ(
        column_group->FieldPlanner(0, std::to_string(kStringFieldId))->rows(),
        16);

    FieldMeta int_meta(FieldName("int_field"),
                       FieldId(kIntFieldId),
                       DataType::INT32,
                       false,
                       std::nullopt);
    VortexColumn int_column(
        FieldId(kIntFieldId), int_meta, properties, column_group);

    FieldMeta string_meta(FieldName("string_field"),
                          FieldId(kStringFieldId),
                          DataType::VARCHAR,
                          128,
                          false,
                          std::nullopt);
    VortexColumn string_column(
        FieldId(kStringFieldId), string_meta, properties, column_group);

    EXPECT_TRUE(int_column.IsInMultiFieldColumnGroup());
    EXPECT_TRUE(string_column.IsInMultiFieldColumnGroup());
    EXPECT_EQ(int_column.NumRows(), 16);
    EXPECT_EQ(string_column.NumRows(), 16);
    EXPECT_EQ(CollectIntScanValues(int_column, 4, 4),
              (std::vector<int32_t>{40, 50, 60, 70}));
    EXPECT_EQ(CollectStringScanValues(string_column, 4, 4),
              (std::vector<std::string>{"v4", "v5", "v6", "v7"}));

    auto string_filter_options = ChunkedColumnInterface::ScanOptions::ForUnary(
        0, proto::plan::OpType::Equal, StringValue("v4"));
    EXPECT_TRUE(string_column.SupportsScanPushdown(string_filter_options));
    {
        ScopedVortexScanPushdownEnable disable_pushdown(false);
        EXPECT_FALSE(string_column.SupportsScanPushdown(string_filter_options));
        EXPECT_EQ(CollectFilteredRowIdPayload(
                      string_column, string_filter_options, 16),
                  (std::vector<int64_t>{4}));
    }
    EXPECT_TRUE(string_column.SupportsScanPushdown(string_filter_options));

    auto filter_options = ChunkedColumnInterface::ScanOptions::ForBinaryRange(
        0, IntValue(30), true, IntValue(60), true);
    EXPECT_FALSE(int_column.SupportsScanPushdown(filter_options));
    EXPECT_THROW(CollectFilteredRowIdPayload(int_column, filter_options, 16),
                 std::exception);

    const int64_t loaded_offset = 4;
    EXPECT_TRUE(int_column.CellsLoaded(&loaded_offset, 1));
    EXPECT_TRUE(string_column.CellsLoaded(&loaded_offset, 1));
    int_column.ManualEvictCache();
    EXPECT_TRUE(int_column.CellsLoaded(&loaded_offset, 1));
    EXPECT_TRUE(string_column.CellsLoaded(&loaded_offset, 1));
    column_group->ManualEvictCache();
    EXPECT_FALSE(int_column.CellsLoaded(&loaded_offset, 1));
    EXPECT_FALSE(string_column.CellsLoaded(&loaded_offset, 1));

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, RejectsIncompatibleArrowTypeAtConstruction) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    auto dir = std::filesystem::temp_directory_path() /
               ("milvus_vortex_column_type_mismatch_test_" +
                std::to_string(::getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file_info = WriteVortexFile(
        (dir / "type_mismatch.vx").string(), schema, *properties);
    auto column_group =
        MakeColumnGroup({file_info}, properties, {std::to_string(kIntFieldId)});
    FieldMeta mismatched_meta(FieldName("int_field"),
                              FieldId(kIntFieldId),
                              DataType::INT64,
                              false,
                              std::nullopt);

    try {
        VortexColumn column(
            FieldId(kIntFieldId), mismatched_meta, properties, column_group);
        FAIL() << "expected incompatible Arrow type to be rejected";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
    }

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, RejectsNullRowsForNonNullableField) {
    auto schema = MakeNullableSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    auto dir = std::filesystem::temp_directory_path() /
               ("milvus_vortex_column_nullability_mismatch_test_" +
                std::to_string(::getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file_info = WriteNullableVortexFile(
        (dir / "nullability_mismatch.vx").string(), schema, *properties);
    const FieldId field_id(kNullableFieldIdBase + 4);
    auto column_group = MakeColumnGroup(
        {file_info}, properties, {std::to_string(field_id.get())});
    FieldMeta non_nullable_meta(FieldName("non_nullable_int64"),
                                field_id,
                                DataType::INT64,
                                false,
                                std::nullopt);
    VortexColumn column(field_id, non_nullable_meta, properties, column_group);

    std::vector<int64_t> offsets{0, 1, 2, 3};
    std::vector<int64_t> values(offsets.size());
    try {
        column.BulkPrimitiveValueAt(
            nullptr, values.data(), offsets.data(), offsets.size(), true);
        FAIL() << "expected take to reject physical nulls";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
    }

    auto cursor =
        column.Scan(nullptr,
                    ChunkedColumnInterface::ScanOptions::ForData(
                        0, ChunkedColumnInterface::TargetType::Int64));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch scan_batch;
    try {
        cursor->Next(kNullableRows,
                     ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                     &scan_batch);
        FAIL() << "expected physical nulls to be rejected";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
    }

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, PreservesNormalizationForDataScan) {
    constexpr int64_t timestamp_field_id = kNormalizedFieldIdBase;
    constexpr int64_t geometry_field_id = kNormalizedFieldIdBase + 1;
    const auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MILLI);
    auto schema = arrow::schema(
        {arrow::field(
             std::to_string(timestamp_field_id), timestamp_type, false),
         arrow::field(
             std::to_string(geometry_field_id), arrow::utf8(), false)});

    arrow::TimestampBuilder timestamp_builder(timestamp_type,
                                              arrow::default_memory_pool());
    arrow::StringBuilder geometry_builder;
    const std::vector<int64_t> timestamp_millis{1700000000000LL,
                                                1700000000123LL};
    const std::vector<std::string> geometry_wkt{"POINT (1 2)", "POINT (3 4)"};
    for (size_t i = 0; i < timestamp_millis.size(); ++i) {
        ASSERT_TRUE(timestamp_builder.Append(timestamp_millis[i]).ok());
        ASSERT_TRUE(geometry_builder.Append(geometry_wkt[i]).ok());
    }
    std::shared_ptr<arrow::Array> timestamps;
    std::shared_ptr<arrow::Array> geometries;
    ASSERT_TRUE(timestamp_builder.Finish(&timestamps).ok());
    ASSERT_TRUE(geometry_builder.Finish(&geometries).ok());
    auto batch = arrow::RecordBatch::Make(
        schema, timestamp_millis.size(), {timestamps, geometries});

    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    auto dir = std::filesystem::temp_directory_path() /
               ("milvus_vortex_column_normalization_test_" +
                std::to_string(::getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);
    auto file_info = WriteVortexBatches(
        (dir / "normalization.vx").string(), schema, *properties, {batch});
    auto column_group = MakeColumnGroup({file_info},
                                        properties,
                                        {std::to_string(timestamp_field_id),
                                         std::to_string(geometry_field_id)});

    FieldMeta timestamp_meta(FieldName("timestamp_field"),
                             FieldId(timestamp_field_id),
                             DataType::TIMESTAMPTZ,
                             false,
                             std::nullopt);
    VortexColumn timestamp_column(
        FieldId(timestamp_field_id), timestamp_meta, properties, column_group);
    auto timestamp_scan = timestamp_column.Scan(
        nullptr,
        ChunkedColumnInterface::ScanOptions::ForData(
            0, ChunkedColumnInterface::TargetType::Int64));
    ASSERT_NE(timestamp_scan, nullptr);
    ChunkedColumnInterface::ScanBatch timestamp_batch;
    ASSERT_TRUE(timestamp_scan->Next(
        1024,
        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
        &timestamp_batch));
    EXPECT_EQ(timestamp_batch.size, 2);
    const auto* timestamp_values = timestamp_batch.values.data_as<int64_t>();
    EXPECT_EQ(timestamp_values[0], timestamp_millis[0] * 1000);
    EXPECT_EQ(timestamp_values[1], timestamp_millis[1] * 1000);
    EXPECT_FALSE(timestamp_scan->Next(
        1024,
        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
        &timestamp_batch));
    const std::vector<int64_t> timestamp_offsets{1, 0};
    std::vector<int64_t> taken_timestamps(timestamp_offsets.size());
    timestamp_column.BulkPrimitiveValueAt(nullptr,
                                          taken_timestamps.data(),
                                          timestamp_offsets.data(),
                                          timestamp_offsets.size(),
                                          true);
    EXPECT_EQ(taken_timestamps,
              (std::vector<int64_t>{timestamp_millis[1] * 1000,
                                    timestamp_millis[0] * 1000}));
    auto timestamp_take = timestamp_column.Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(timestamp_offsets.data(),
                                                     timestamp_offsets.size()),
            ChunkedColumnInterface::TargetType::Int64});
    ASSERT_NE(timestamp_take, nullptr);
    ASSERT_EQ(timestamp_take->Size(),
              static_cast<int64_t>(timestamp_offsets.size()));
    EXPECT_EQ(*timestamp_take->Get<int64_t>(0).value,
              timestamp_millis[1] * 1000);
    EXPECT_EQ(*timestamp_take->Get<int64_t>(1).value,
              timestamp_millis[0] * 1000);

    FieldMeta geometry_meta(FieldName("geometry_field"),
                            FieldId(geometry_field_id),
                            DataType::GEOMETRY,
                            false,
                            std::nullopt);
    VortexColumn geometry_column(
        FieldId(geometry_field_id), geometry_meta, properties, column_group);
    const auto geometry_values = CollectStringScanValues(geometry_column, 0, 2);
    ASSERT_EQ(geometry_values.size(), geometry_wkt.size());
    auto ctx = GetThreadLocalGEOSContext();
    for (size_t i = 0; i < geometry_values.size(); ++i) {
        EXPECT_EQ(geometry_values[i],
                  Geometry(ctx, geometry_wkt[i].c_str()).to_wkb_string());
    }

    FixedVector<int32_t> geometry_offsets{1, 0};
    auto offset_views =
        geometry_column.StringViewsByOffsets(nullptr, 0, geometry_offsets);
    ASSERT_EQ(offset_views.get().first.size(), geometry_offsets.size());
    EXPECT_EQ(offset_views.get().first[0],
              Geometry(ctx, geometry_wkt[1].c_str()).to_wkb_string());
    EXPECT_EQ(offset_views.get().first[1],
              Geometry(ctx, geometry_wkt[0].c_str()).to_wkb_string());

    std::vector<std::string> bulk_geometry_values;
    geometry_column.BulkRawStringAt(
        nullptr,
        [&](std::string_view value, size_t index, bool valid) {
            ASSERT_EQ(index, bulk_geometry_values.size());
            ASSERT_TRUE(valid);
            bulk_geometry_values.emplace_back(value);
        },
        nullptr,
        geometry_wkt.size());
    ASSERT_EQ(bulk_geometry_values.size(), geometry_wkt.size());
    for (size_t i = 0; i < bulk_geometry_values.size(); ++i) {
        EXPECT_EQ(bulk_geometry_values[i],
                  Geometry(ctx, geometry_wkt[i].c_str()).to_wkb_string());
    }

    const std::vector<int64_t> taken_geometry_offsets{1, 0};
    bulk_geometry_values.clear();
    geometry_column.BulkRawStringAt(
        nullptr,
        [&](std::string_view value, size_t index, bool valid) {
            ASSERT_EQ(index, bulk_geometry_values.size());
            ASSERT_TRUE(valid);
            bulk_geometry_values.emplace_back(value);
        },
        taken_geometry_offsets.data(),
        taken_geometry_offsets.size());
    EXPECT_EQ(bulk_geometry_values,
              (std::vector<std::string>{
                  Geometry(ctx, geometry_wkt[1].c_str()).to_wkb_string(),
                  Geometry(ctx, geometry_wkt[0].c_str()).to_wkb_string()}));

    auto geometry_take = geometry_column.Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                taken_geometry_offsets.data(), taken_geometry_offsets.size()),
            ChunkedColumnInterface::TargetType::StringView});
    ASSERT_NE(geometry_take, nullptr);
    ASSERT_EQ(geometry_take->Size(),
              static_cast<int64_t>(taken_geometry_offsets.size()));
    EXPECT_EQ(*geometry_take->Get<std::string_view>(0).value,
              Geometry(ctx, geometry_wkt[1].c_str()).to_wkb_string());
    EXPECT_EQ(*geometry_take->Get<std::string_view>(1).value,
              Geometry(ctx, geometry_wkt[0].c_str()).to_wkb_string());

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, RecursiveArrayUsesExactTargetType) {
    constexpr int64_t field_id = kNormalizedFieldIdBase + 10;
    auto value_builder = std::make_shared<arrow::Int64Builder>();
    auto inner_builder = std::make_shared<arrow::ListBuilder>(
        arrow::default_memory_pool(), value_builder);
    arrow::ListBuilder outer_builder(arrow::default_memory_pool(),
                                     inner_builder);
    auto& inner =
        dynamic_cast<arrow::ListBuilder&>(*outer_builder.value_builder());
    auto& values = dynamic_cast<arrow::Int64Builder&>(*inner.value_builder());

    ASSERT_TRUE(outer_builder.Append().ok());
    ASSERT_TRUE(inner.Append().ok());
    ASSERT_TRUE(values.AppendValues({1, 2}).ok());
    ASSERT_TRUE(inner.Append().ok());
    ASSERT_TRUE(values.Append(3).ok());

    ASSERT_TRUE(outer_builder.Append().ok());
    ASSERT_TRUE(inner.Append().ok());
    ASSERT_TRUE(inner.Append().ok());
    ASSERT_TRUE(values.AppendValues({4, 5}).ok());

    ASSERT_TRUE(outer_builder.Append().ok());
    ASSERT_TRUE(inner.Append().ok());
    ASSERT_TRUE(values.Append(6).ok());

    ASSERT_TRUE(outer_builder.AppendNull().ok());

    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(outer_builder.Finish(&array).ok());
    auto schema =
        arrow::schema({arrow::field(std::to_string(field_id),
                                    arrow::list(arrow::list(arrow::int64())),
                                    true)});
    auto batch = arrow::RecordBatch::Make(schema, 4, {array});

    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_recursive_array_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);
    auto file_info = WriteVortexBatches(
        (dir / "recursive_array.vx").string(), schema, *properties, {batch});
    auto column_group =
        MakeColumnGroup({file_info}, properties, {std::to_string(field_id)});

    proto::schema::FieldSchema field_schema;
    field_schema.set_name("recursive_array");
    field_schema.set_fieldid(field_id);
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(proto::schema::DataType::Array);
    field_schema.set_nullable(true);
    field_schema.mutable_type_schema()
        ->mutable_array_element()
        ->mutable_array_element()
        ->set_leaf_type(proto::schema::DataType::Int64);
    auto field_meta = FieldMeta::ParseFrom(field_schema);
    ASSERT_TRUE(field_meta.is_nested_array());
    VortexColumn column(
        FieldId(field_id), field_meta, properties, column_group);

    auto check_row = [](ArrayValueView value, int64_t row) {
        ASSERT_TRUE(value.is_nested_array());
        if (row == 0) {
            ASSERT_EQ(value.size(), 2);
            EXPECT_EQ(value.array_at(0).get_data<int64_t>(0), 1);
            EXPECT_EQ(value.array_at(0).get_data<int64_t>(1), 2);
            EXPECT_EQ(value.array_at(1).get_data<int64_t>(0), 3);
        } else if (row == 1) {
            ASSERT_EQ(value.size(), 2);
            EXPECT_TRUE(value.array_at(0).empty());
            EXPECT_EQ(value.array_at(1).get_data<int64_t>(0), 4);
            EXPECT_EQ(value.array_at(1).get_data<int64_t>(1), 5);
        } else {
            ASSERT_EQ(row, 2);
            ASSERT_EQ(value.size(), 1);
            EXPECT_EQ(value.array_at(0).get_data<int64_t>(0), 6);
        }
    };

    auto cursor =
        column.Scan(nullptr,
                    ChunkedColumnInterface::ScanOptions::ForData(
                        0, ChunkedColumnInterface::TargetType::ArrayValueView));
    ASSERT_NE(cursor, nullptr);
    int64_t scanned = 0;
    while (scanned < 4) {
        ChunkedColumnInterface::ScanBatch scan_batch;
        ASSERT_TRUE(
            cursor->Next(4 - scanned,
                         ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                         &scan_batch));
        const auto* scanned_values =
            scan_batch.values.data_as<ArrayValueView>();
        for (int64_t i = 0; i < scan_batch.size; ++i) {
            const auto row = scan_batch.row_id_start + i;
            if (row == 3) {
                ASSERT_TRUE(scan_batch.validity);
                EXPECT_FALSE(scan_batch.validity[i]);
            } else {
                check_row(scanned_values[i], row);
            }
        }
        scanned += scan_batch.size;
    }

    const std::vector<int64_t> offsets{3, 2, 0, 1, 3};
    auto take =
        column.Take(nullptr,
                    ChunkedColumnInterface::TakeOptions{
                        ChunkedColumnInterface::OffsetView::From(
                            offsets.data(), offsets.size()),
                        ChunkedColumnInterface::TargetType::ArrayValueView});
    ASSERT_NE(take, nullptr);
    for (int64_t i = 0; i < take->Size(); ++i) {
        if (offsets[i] == 3) {
            EXPECT_FALSE(take->IsValid(i));
        } else {
            EXPECT_TRUE(take->IsValid(i));
            check_row(*take->Get<ArrayValueView>(i).value, offsets[i]);
        }
    }

    auto chunk_views = column.ArrayValueViews(nullptr, 0, std::nullopt);
    ASSERT_EQ(chunk_views.get().first.size(), 4);
    for (int64_t i = 0; i < 3; ++i) {
        check_row(chunk_views.get().first[i], i);
    }
    ASSERT_TRUE(chunk_views.get().second);
    EXPECT_FALSE(chunk_views.get().second[3]);
    EXPECT_TRUE(chunk_views.get().first[3].is_null());

    const FixedVector<int32_t> chunk_offsets{3, 2, 0, 3};
    auto offset_views =
        column.ArrayValueViewsByOffsets(nullptr, 0, chunk_offsets);
    ASSERT_EQ(offset_views.get().first.size(), chunk_offsets.size());
    for (size_t i = 0; i < chunk_offsets.size(); ++i) {
        if (chunk_offsets[i] == 3) {
            EXPECT_FALSE(offset_views.get().second[i]);
        } else {
            EXPECT_TRUE(offset_views.get().second[i]);
            check_row(offset_views.get().first[i], chunk_offsets[i]);
        }
    }

    std::vector<ArrayValue> bulk_values;
    column.BulkArrayValueAt(
        nullptr,
        [&](ScalarFieldProto&& value, size_t) {
            bulk_values.emplace_back(value, field_meta.get_array_type_schema());
        },
        offsets.data(),
        offsets.size());
    ASSERT_EQ(bulk_values.size(), offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        if (offsets[i] == 3) {
            EXPECT_TRUE(bulk_values[i].is_null());
        } else {
            check_row(bulk_values[i].View(), offsets[i]);
        }
    }

    EXPECT_THROW(
        column.Scan(nullptr,
                    ChunkedColumnInterface::ScanOptions::ForData(
                        0, ChunkedColumnInterface::TargetType::ArrayView)),
        std::exception);
    EXPECT_THROW(
        column.Take(nullptr,
                    ChunkedColumnInterface::TakeOptions{
                        ChunkedColumnInterface::OffsetView::From(
                            offsets.data(), offsets.size()),
                        ChunkedColumnInterface::TargetType::ArrayView}),
        std::exception);
    EXPECT_THROW(column.ArrayViews(nullptr, 0, std::nullopt), std::exception);
    EXPECT_THROW(column.ArrayViewsByOffsets(nullptr, 0, chunk_offsets),
                 std::exception);

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, MixedFileRepresentationsNormalizePerFile) {
    constexpr int64_t timestamp_field_id = kNormalizedFieldIdBase + 2;
    constexpr int64_t geometry_field_id = kNormalizedFieldIdBase + 3;
    const auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MILLI);
    auto direct_schema = arrow::schema(
        {arrow::field(
             std::to_string(timestamp_field_id), arrow::int64(), false),
         arrow::field(
             std::to_string(geometry_field_id), arrow::binary(), false)});
    auto normalized_schema = arrow::schema(
        {arrow::field(
             std::to_string(timestamp_field_id), timestamp_type, false),
         arrow::field(
             std::to_string(geometry_field_id), arrow::utf8(), false)});

    const std::vector<int64_t> timestamp_millis{
        1700000000000LL, 1700000000123LL, 1700000000456LL, 1700000000789LL};
    std::vector<int64_t> timestamp_micros;
    timestamp_micros.reserve(timestamp_millis.size());
    for (auto value : timestamp_millis) {
        timestamp_micros.emplace_back(value * 1000);
    }
    const std::vector<std::string> geometry_wkt{
        "POINT (1 2)", "POINT (3 4)", "POINT (5 6)", "POINT (7 8)"};
    auto ctx = GetThreadLocalGEOSContext();
    std::vector<std::string> geometry_wkb;
    geometry_wkb.reserve(geometry_wkt.size());
    for (const auto& value : geometry_wkt) {
        geometry_wkb.emplace_back(Geometry(ctx, value.c_str()).to_wkb_string());
    }

    arrow::Int64Builder direct_timestamp_builder;
    arrow::BinaryBuilder direct_geometry_builder;
    for (size_t i = 0; i < 2; ++i) {
        ASSERT_TRUE(direct_timestamp_builder.Append(timestamp_micros[i]).ok());
        ASSERT_TRUE(direct_geometry_builder.Append(geometry_wkb[i]).ok());
    }
    std::shared_ptr<arrow::Array> direct_timestamps;
    std::shared_ptr<arrow::Array> direct_geometries;
    ASSERT_TRUE(direct_timestamp_builder.Finish(&direct_timestamps).ok());
    ASSERT_TRUE(direct_geometry_builder.Finish(&direct_geometries).ok());
    auto direct_batch = arrow::RecordBatch::Make(
        direct_schema, 2, {direct_timestamps, direct_geometries});

    arrow::TimestampBuilder normalized_timestamp_builder(
        timestamp_type, arrow::default_memory_pool());
    arrow::StringBuilder normalized_geometry_builder;
    for (size_t i = 2; i < timestamp_millis.size(); ++i) {
        ASSERT_TRUE(
            normalized_timestamp_builder.Append(timestamp_millis[i]).ok());
        ASSERT_TRUE(normalized_geometry_builder.Append(geometry_wkt[i]).ok());
    }
    std::shared_ptr<arrow::Array> normalized_timestamps;
    std::shared_ptr<arrow::Array> normalized_geometries;
    ASSERT_TRUE(
        normalized_timestamp_builder.Finish(&normalized_timestamps).ok());
    ASSERT_TRUE(
        normalized_geometry_builder.Finish(&normalized_geometries).ok());
    auto normalized_batch = arrow::RecordBatch::Make(
        normalized_schema, 2, {normalized_timestamps, normalized_geometries});

    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    auto dir = std::filesystem::temp_directory_path() /
               ("milvus_vortex_mixed_file_representation_test_" +
                std::to_string(::getpid()) + "_" +
                std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);
    auto direct_file = WriteVortexBatches((dir / "direct.vx").string(),
                                          direct_schema,
                                          *properties,
                                          {direct_batch});
    auto normalized_file = WriteVortexBatches((dir / "normalized.vx").string(),
                                              normalized_schema,
                                              *properties,
                                              {normalized_batch});
    normalized_file.start_index = direct_file.end_index;
    normalized_file.end_index =
        normalized_file.start_index + normalized_batch->num_rows();
    auto column_group = MakeColumnGroup({direct_file, normalized_file},
                                        properties,
                                        {std::to_string(timestamp_field_id),
                                         std::to_string(geometry_field_id)});

    FieldMeta timestamp_meta(FieldName("timestamp_field"),
                             FieldId(timestamp_field_id),
                             DataType::TIMESTAMPTZ,
                             false,
                             std::nullopt);
    VortexColumn timestamp_column(
        FieldId(timestamp_field_id), timestamp_meta, properties, column_group);
    auto timestamp_scan = timestamp_column.Scan(
        nullptr,
        ChunkedColumnInterface::ScanOptions::ForData(
            0, ChunkedColumnInterface::TargetType::Int64));
    ASSERT_NE(timestamp_scan, nullptr);
    std::vector<int64_t> scanned_timestamps;
    const auto row_count = static_cast<int64_t>(timestamp_micros.size());
    int64_t processed = 0;
    while (processed < row_count) {
        ChunkedColumnInterface::ScanBatch batch;
        ASSERT_TRUE(timestamp_scan->Next(
            row_count - processed,
            ChunkedColumnInterface::ScanReadMode::DataAndValidity,
            &batch));
        EXPECT_EQ(batch.row_id_start, processed);
        const auto* values = batch.values.data_as<int64_t>();
        scanned_timestamps.insert(
            scanned_timestamps.end(), values, values + batch.size);
        processed += batch.size;
    }
    EXPECT_EQ(scanned_timestamps, timestamp_micros);

    const std::vector<int64_t> offsets{3, 0, 2, 1, 3};
    auto timestamp_take =
        timestamp_column.Take(nullptr,
                              ChunkedColumnInterface::TakeOptions{
                                  ChunkedColumnInterface::OffsetView::From(
                                      offsets.data(), offsets.size()),
                                  ChunkedColumnInterface::TargetType::Int64});
    ASSERT_NE(timestamp_take, nullptr);
    ASSERT_EQ(timestamp_take->Size(), static_cast<int64_t>(offsets.size()));
    std::vector<int64_t> taken_timestamps;
    for (int64_t i = 0; i < timestamp_take->Size(); ++i) {
        taken_timestamps.emplace_back(*timestamp_take->Get<int64_t>(i).value);
    }
    EXPECT_EQ(taken_timestamps,
              (std::vector<int64_t>{timestamp_micros[3],
                                    timestamp_micros[0],
                                    timestamp_micros[2],
                                    timestamp_micros[1],
                                    timestamp_micros[3]}));

    FieldMeta geometry_meta(FieldName("geometry_field"),
                            FieldId(geometry_field_id),
                            DataType::GEOMETRY,
                            false,
                            std::nullopt);
    VortexColumn geometry_column(
        FieldId(geometry_field_id), geometry_meta, properties, column_group);
    column_group->ManualEvictCache();
    const int64_t direct_file_offset[]{0};
    const int64_t normalized_file_offset[]{2};
    {
        auto direct_views = geometry_column.StringViews(nullptr, 0);
        auto normalized_views = geometry_column.StringViews(nullptr, 1);
        EXPECT_EQ(
            direct_views.get().first,
            (std::vector<std::string_view>{geometry_wkb[0], geometry_wkb[1]}));
        EXPECT_EQ(
            normalized_views.get().first,
            (std::vector<std::string_view>{geometry_wkb[2], geometry_wkb[3]}));

        // Direct views retain the reader Cell pin. The normalization path
        // returns an independent materialized Chunk, so its source Cell is
        // evictable while the returned views remain alive.
        column_group->ManualEvictCache();
        EXPECT_TRUE(geometry_column.CellsLoaded(direct_file_offset, 1));
        EXPECT_FALSE(geometry_column.CellsLoaded(normalized_file_offset, 1));
    }
    column_group->ManualEvictCache();
    EXPECT_EQ(CollectStringScanValues(geometry_column, 0, row_count),
              geometry_wkb);
    auto geometry_take = geometry_column.Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(offsets.data(),
                                                     offsets.size()),
            ChunkedColumnInterface::TargetType::StringView});
    ASSERT_NE(geometry_take, nullptr);
    ASSERT_EQ(geometry_take->Size(), static_cast<int64_t>(offsets.size()));
    std::vector<std::string> taken_geometries;
    for (int64_t i = 0; i < geometry_take->Size(); ++i) {
        taken_geometries.emplace_back(
            *geometry_take->Get<std::string_view>(i).value);
    }
    EXPECT_EQ(taken_geometries,
              (std::vector<std::string>{geometry_wkb[3],
                                        geometry_wkb[0],
                                        geometry_wkb[2],
                                        geometry_wkb[1],
                                        geometry_wkb[3]}));

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, RejectsMismatchedTargetType) {
    auto schema = MakeSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_scan_kind_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file_info =
        WriteVortexFile((dir / "scan_kind.vx").string(), schema, *properties);
    auto column_group =
        MakeColumnGroup({file_info}, properties, {std::to_string(kIntFieldId)});
    FieldMeta field_meta(FieldName("int_field"),
                         FieldId(kIntFieldId),
                         DataType::INT32,
                         false,
                         std::nullopt);
    VortexColumn column(
        FieldId(kIntFieldId), field_meta, properties, column_group);

    auto options = ChunkedColumnInterface::ScanOptions::ForData(
        0, ChunkedColumnInterface::TargetType::StringView);
    EXPECT_THROW(column.Scan(nullptr, options), std::exception);

    const std::vector<int64_t> offsets{0};
    EXPECT_THROW(
        column.Take(nullptr,
                    ChunkedColumnInterface::TakeOptions{
                        ChunkedColumnInterface::OffsetView::From(
                            offsets.data(), offsets.size()),
                        ChunkedColumnInterface::TargetType::StringView}),
        std::exception);

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, ScanPlansAndPinsOnlyForTheRequestedRange) {
    auto schema = MakeNullableSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    milvus_storage::api::SetValue(
        *properties, PROPERTY_WRITER_VORTEX_FORMAT_VERSION, "2");
    milvus_storage::api::SetValue(
        *properties, PROPERTY_WRITER_VORTEX_ENABLE_STATISTICS, "true");
    milvus_storage::api::SetValue(*properties,
                                  PROPERTY_WRITER_VORTEX_V2_ROW_GROUP_MAX_SIZE,
                                  std::to_string(128 * 1024).c_str());
    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_pin_plan_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);
    auto file_info = WriteNullableVortexFile(
        (dir / "pin_plan.vx").string(), schema, *properties);

    const auto types = NullableLocalVortexTypes();
    const auto string_it =
        std::find(types.begin(), types.end(), DataType::VARCHAR);
    ASSERT_NE(string_it, types.end());
    const FieldId field_id(kNullableFieldIdBase +
                           std::distance(types.begin(), string_it));
    auto column_group = MakeColumnGroup(
        {file_info}, properties, {std::to_string(field_id.get())});
    VortexColumn column(field_id,
                        MakeNullableFieldMeta(field_id, DataType::VARCHAR),
                        properties,
                        column_group);

    const int64_t first_offset[]{0};
    const int64_t planned_offsets[]{0, kNullableRows - 1};
    column.ManualEvictCache();
    EXPECT_FALSE(column.CellsLoaded(planned_offsets, 2));
    auto validity_cursor =
        column.Scan(nullptr,
                    ChunkedColumnInterface::ScanOptions::ForData(
                        0, ChunkedColumnInterface::TargetType::None));
    ASSERT_NE(validity_cursor, nullptr);
    EXPECT_FALSE(column.CellsLoaded(planned_offsets, 2));

    ChunkedColumnInterface::ScanBatch validity_batch;
    ASSERT_TRUE(validity_cursor->Next(
        kNullableRows,
        ChunkedColumnInterface::ScanReadMode::ValidityOnly,
        &validity_batch));
    EXPECT_TRUE(column.CellsLoaded(first_offset, 1));
    validity_cursor.reset();
    column.ManualEvictCache();
    EXPECT_TRUE(column.CellsLoaded(first_offset, 1));
    validity_batch = {};
    column.ManualEvictCache();
    EXPECT_FALSE(column.CellsLoaded(first_offset, 1));

    auto skipped_cursor =
        column.Scan(nullptr,
                    ChunkedColumnInterface::ScanOptions::ForData(
                        0, ChunkedColumnInterface::TargetType::None));
    ASSERT_NE(skipped_cursor, nullptr);
    skipped_cursor->Seek(kNullableRows);
    EXPECT_FALSE(
        skipped_cursor->Next(0,
                             ChunkedColumnInterface::ScanReadMode::ValidityOnly,
                             &validity_batch));
    EXPECT_FALSE(column.CellsLoaded(planned_offsets, 2));

    // Use a value outside every row-group zonemap so the predicate plan is
    // deterministically empty. The nullable validity plan must still pin and
    // read the complete range because data and validity share the same Cells.
    const auto predicate = StringValue("zzzzzzzz");
    const auto row_id_options = ChunkedColumnInterface::ScanOptions::ForUnary(
        0, proto::plan::OpType::Equal, predicate);
    auto row_id_cursor = column.Scan(nullptr, row_id_options);
    ASSERT_NE(row_id_cursor, nullptr);
    EXPECT_FALSE(column.CellsLoaded(planned_offsets, 2));
    ChunkedColumnInterface::ScanBatch row_id_batch;
    ASSERT_TRUE(row_id_cursor->Next(
        kNullableRows,
        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
        &row_id_batch));
    // Predicate and validity readers use different plans, but their cell ids
    // are unioned into one pin for this file range.
    EXPECT_TRUE(column.CellsLoaded(planned_offsets, 2));
    std::vector<int64_t> unknown_rows;
    for (size_t i = 0; i < row_id_batch.row_ids.size(); ++i) {
        EXPECT_FALSE(IsScanRowValid(row_id_batch, static_cast<int64_t>(i)));
        unknown_rows.emplace_back(row_id_batch.row_ids[i]);
    }
    EXPECT_EQ(unknown_rows, (std::vector<int64_t>{1, 5, 9, 13}));

    row_id_batch = {};
    row_id_cursor.reset();
    column.ManualEvictCache();
    EXPECT_FALSE(column.CellsLoaded(planned_offsets, 2));

    auto advancing_row_id_cursor =
        column.Scan(nullptr,
                    ChunkedColumnInterface::ScanOptions::ForUnary(
                        0,
                        proto::plan::OpType::Equal,
                        StringValue(ExpectedString(DataType::VARCHAR, 8))));
    ASSERT_NE(advancing_row_id_cursor, nullptr);
    ASSERT_TRUE(advancing_row_id_cursor->Next(
        4,
        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
        &row_id_batch));
    EXPECT_EQ(row_id_batch.row_ids, (std::vector<int64_t>{1}));
    EXPECT_FALSE(IsScanRowValid(row_id_batch, 0));
    row_id_batch = {};
    column.ManualEvictCache();
    const int64_t skipped_offset[]{5};
    EXPECT_FALSE(column.CellsLoaded(skipped_offset, 1));

    advancing_row_id_cursor->Seek(8);
    ASSERT_TRUE(advancing_row_id_cursor->Next(
        4,
        ChunkedColumnInterface::ScanReadMode::DataAndValidity,
        &row_id_batch));
    EXPECT_EQ(row_id_batch.row_ids, (std::vector<int64_t>{8, 9}));
    EXPECT_TRUE(IsScanRowValid(row_id_batch, 0));
    EXPECT_FALSE(IsScanRowValid(row_id_batch, 1));
    // The skipped logical rows share one physical Cell with the requested
    // range. Pinning rows [8, 12) therefore makes offset 5 resident too; skip
    // planning avoids decoding rows, not loading a fraction of one Cell.
    EXPECT_TRUE(column.CellsLoaded(skipped_offset, 1));
    row_id_batch = {};

    auto result_owned_cursor =
        column.Scan(nullptr,
                    ChunkedColumnInterface::ScanOptions::ForData(
                        0, ChunkedColumnInterface::TargetType::None));
    ASSERT_NE(result_owned_cursor, nullptr);
    ChunkedColumnInterface::ScanBatch pin_batch;
    ASSERT_TRUE(result_owned_cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::ValidityOnly, &pin_batch));
    pin_batch = {};
    column.ManualEvictCache();
    EXPECT_FALSE(column.CellsLoaded(first_offset, 1));

    auto cursor_owned_options = ChunkedColumnInterface::ScanOptions::ForData(
        0,
        ChunkedColumnInterface::TargetType::None,
        ChunkedColumnInterface::ScanPinPolicy::CursorOwned);
    auto cursor_owned = column.Scan(nullptr, cursor_owned_options);
    ASSERT_NE(cursor_owned, nullptr);
    ASSERT_TRUE(cursor_owned->Next(
        2, ChunkedColumnInterface::ScanReadMode::ValidityOnly, &pin_batch));
    pin_batch = {};
    column.ManualEvictCache();
    EXPECT_TRUE(column.CellsLoaded(first_offset, 1));

    ASSERT_TRUE(cursor_owned->Next(
        2, ChunkedColumnInterface::ScanReadMode::ValidityOnly, &pin_batch));
    pin_batch = {};
    column.ManualEvictCache();
    EXPECT_TRUE(column.CellsLoaded(first_offset, 1));

    // A forward seek releases the retained Cell before handling the next
    // request. A zero-length request performs no planning or pinning.
    cursor_owned->Seek(kNullableRows);
    EXPECT_FALSE(cursor_owned->Next(
        0, ChunkedColumnInterface::ScanReadMode::ValidityOnly, &pin_batch));
    column.ManualEvictCache();
    EXPECT_FALSE(column.CellsLoaded(first_offset, 1));

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, PlannerSkipCellIdsRemainFileScopedAcrossFiles) {
    auto schema = MakeNullableSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());
    milvus_storage::api::SetValue(
        *properties, PROPERTY_WRITER_VORTEX_FORMAT_VERSION, "2");
    milvus_storage::api::SetValue(
        *properties, PROPERTY_WRITER_VORTEX_ENABLE_STATISTICS, "true");
    milvus_storage::api::SetValue(*properties,
                                  PROPERTY_WRITER_VORTEX_V2_ROW_GROUP_MAX_SIZE,
                                  std::to_string(128 * 1024).c_str());
    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_scoped_skip_cells_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);
    auto file0 = WriteNullableVortexFile(
        (dir / "cg0.vx").string(), schema, *properties, 0);
    auto file1 = WriteNullableVortexFile(
        (dir / "cg1.vx").string(), schema, *properties, kNullableRows);

    const auto types = NullableLocalVortexTypes();
    const auto string_it =
        std::find(types.begin(), types.end(), DataType::VARCHAR);
    ASSERT_NE(string_it, types.end());
    const FieldId field_id(kNullableFieldIdBase +
                           std::distance(types.begin(), string_it));
    auto column_group = MakeColumnGroup(
        {file0, file1}, properties, {std::to_string(field_id.get())});
    VortexColumn column(field_id,
                        MakeNullableFieldMeta(field_id, DataType::VARCHAR),
                        properties,
                        column_group);

    const auto predicate = StringValue(ExpectedString(DataType::VARCHAR, 20));
    auto cursor = column.Scan(nullptr,
                              ChunkedColumnInterface::ScanOptions::ForUnary(
                                  0, proto::plan::OpType::Equal, predicate));
    ASSERT_NE(cursor, nullptr);
    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(
        cursor->Next(2 * kNullableRows,
                     ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                     &batch));
    const auto& rows = batch.row_ids;
    EXPECT_EQ(rows, (std::vector<int64_t>{1, 5, 9, 13, 17, 20, 21, 25, 29}));

    std::filesystem::remove_all(dir);
}

TEST(VortexColumnTest, NullableAllScalarTypesScanCorrectness) {
    auto schema = MakeNullableSchema();
    auto properties =
        std::make_shared<milvus_storage::api::Properties>(MakeProperties());

    auto dir =
        std::filesystem::temp_directory_path() /
        ("milvus_vortex_column_nullable_test_" + std::to_string(::getpid()) +
         "_" + std::to_string(reinterpret_cast<uintptr_t>(properties.get())));
    std::filesystem::create_directories(dir);

    auto file_info = WriteNullableVortexFile(
        (dir / "nullable.vx").string(), schema, *properties);

    auto types = NullableLocalVortexTypes();
    for (size_t i = 0; i < types.size(); ++i) {
        const auto type = types[i];
        FieldId field_id(kNullableFieldIdBase + static_cast<int64_t>(i));
        auto column = MakeNullableColumn(type, field_id, file_info, properties);

        ASSERT_EQ(column.NumRows(), kNullableRows);
        ASSERT_TRUE(column.IsNullable());
        CheckNoDataScan(column);
        CheckApplyValidDataInChunk(column);
        CheckDataScan(column, type);
        CheckOrderedTake(column, type);
        if (type == DataType::STRING) {
            EXPECT_NO_THROW(column.BulkRawBsonAt(
                nullptr,
                [](BsonView, uint32_t, uint32_t) {},
                nullptr,
                nullptr,
                0));
        }
        if (IsVortexStringPushdownType(type)) {
            CheckNullableFilteredScanReturnsValidity(column, type);
        } else if (type == DataType::TEXT) {
            auto options = ChunkedColumnInterface::ScanOptions::ForUnary(
                0,
                proto::plan::OpType::Equal,
                StringValue(ExpectedString(type, 8)));
            EXPECT_FALSE(column.SupportsScanPushdown(options));
        }
    }

    std::filesystem::remove_all(dir);
}

}  // namespace milvus
