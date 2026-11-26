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

#include <arrow/array/array_base.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <folly/ScopeGuard.h>
#include <gmock/gmock.h>
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/common_type_c.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/reader.h"
#include "segcore/column_groups_c.h"
#include "segcore/packed_reader_c.h"
#include "segcore/packed_writer_c.h"
#include "storage/PluginLoader.h"
#include "storage/KeyRetriever.h"
#include "storage/loon_ffi/ffi_reader_c.h"
#include "storage/loon_ffi/ffi_writer_c.h"
#include "storage/loon_ffi/property_singleton.h"
#include "storage/storage_c.h"
#include "test_utils/Constants.h"
#include "test_utils/PlannerCipherPlugin.h"

TEST(CPackedTest, PackedWriterAndReader) {
    std::vector<int64_t> test_data(5);
    std::iota(test_data.begin(), test_data.end(), 0);

    auto builder = std::make_shared<arrow::Int64Builder>();
    auto status = builder->AppendValues(test_data.begin(), test_data.end());
    ASSERT_TRUE(status.ok());
    auto res = builder->Finish();
    ASSERT_TRUE(res.ok());
    std::shared_ptr<arrow::Array> array = res.ValueOrDie();

    auto schema = arrow::schema(
        {arrow::field("int64",
                      arrow::int64(),
                      false,
                      arrow::key_value_metadata(
                          {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"}))});
    auto origin_schema = arrow::schema({schema->fields()[0]->Copy()});
    auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});

    struct ArrowSchema c_write_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_write_schema).ok());

    struct ArrowSchema c_origin_schema;
    ASSERT_TRUE(arrow::ExportSchema(*origin_schema, &c_origin_schema).ok());

    const int64_t buffer_size = 10 * 1024 * 1024;
    std::string root_path = TestLocalPath;
    std::string file_path = TestLocalPath + "0";
    char* path = const_cast<char*>(root_path.c_str());
    char* paths[] = {const_cast<char*>(file_path.c_str())};
    int64_t part_upload_size = 0;

    CColumnSplits cgs = NewCColumnSplits();
    int group[] = {0};
    AddCColumnSplit(cgs, group, 1);

    CPackedWriter c_packed_writer = nullptr;
    auto c_status = NewPackedWriter(&c_write_schema,
                                    buffer_size,
                                    paths,
                                    1,
                                    part_upload_size,
                                    cgs,
                                    &c_packed_writer,
                                    nullptr);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_NE(c_packed_writer, nullptr);

    struct ArrowArray carray;
    struct ArrowSchema cschema;
    ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &carray, &cschema).ok());

    struct ArrowArray arrays[] = {carray};
    struct ArrowSchema array_schemas[] = {cschema};

    c_status = WriteRecordBatch(
        c_packed_writer, arrays, array_schemas, &c_origin_schema);
    EXPECT_EQ(c_status.error_code, 0);

    c_status = CloseWriter(c_packed_writer);
    EXPECT_EQ(c_status.error_code, 0);

    struct ArrowSchema c_read_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_read_schema).ok());
    CPackedReader c_packed_reader = nullptr;
    c_status = NewPackedReader(
        paths, 1, &c_read_schema, buffer_size, 0, &c_packed_reader, nullptr);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_NE(c_packed_reader, nullptr);

    struct ArrowArray read_array {};
    struct ArrowSchema read_schema {};
    c_status = ReadNext(c_packed_reader, &read_array, &read_schema);
    ASSERT_EQ(c_status.error_code, 0);
    ASSERT_NE(read_array.release, nullptr);
    ASSERT_NE(read_schema.release, nullptr);
    auto imported = arrow::ImportRecordBatch(&read_array, &read_schema);
    ASSERT_TRUE(imported.ok());
    auto read_batch = imported.ValueOrDie();
    EXPECT_TRUE(batch->Equals(*read_batch));
    EXPECT_EQ(read_array.release, nullptr);
    EXPECT_EQ(read_schema.release, nullptr);

    // The same caller-owned structs can be reused after import; EOF leaves
    // both outputs released without affecting the previously imported batch.
    c_status = ReadNext(c_packed_reader, &read_array, &read_schema);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_EQ(read_array.release, nullptr);
    EXPECT_EQ(read_schema.release, nullptr);

    c_status = CloseReader(c_packed_reader);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_TRUE(batch->Equals(*read_batch));
    FreeCColumnSplits(cgs);
}

namespace {

// Writes the int64 values 0..num_rows-1 into one packed file at `file_path`.
void
WriteInt64File(const std::string& file_path,
               int64_t num_rows,
               std::shared_ptr<arrow::Schema>* out_schema,
               CPluginContext* target = nullptr) {
    std::vector<int64_t> values(num_rows);
    std::iota(values.begin(), values.end(), 0);
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.AppendValues(values).ok());
    auto array = builder.Finish().ValueOrDie();

    auto schema = arrow::schema(
        {arrow::field("int64",
                      arrow::int64(),
                      false,
                      arrow::key_value_metadata(
                          {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"}))});
    *out_schema = schema;

    struct ArrowSchema c_write_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_write_schema).ok());
    struct ArrowSchema c_origin_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_origin_schema).ok());

    char* paths[] = {const_cast<char*>(file_path.c_str())};
    CColumnSplits cgs = NewCColumnSplits();
    int group[] = {0};
    AddCColumnSplit(cgs, group, 1);

    CPackedWriter writer = nullptr;
    auto status = NewPackedWriter(
        &c_write_schema, 10 * 1024 * 1024, paths, 1, 0, cgs, &writer, target);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;

    struct ArrowArray carray;
    struct ArrowSchema cschema;
    ASSERT_TRUE(arrow::ExportArray(*array, &carray, &cschema).ok());
    struct ArrowArray arrays[] = {carray};
    struct ArrowSchema array_schemas[] = {cschema};
    status = WriteRecordBatch(writer, arrays, array_schemas, &c_origin_schema);
    ASSERT_EQ(status.error_code, 0);
    status = CloseWriter(writer);
    ASSERT_EQ(status.error_code, 0);
    FreeCColumnSplits(cgs);
}

class TestParquetKey : public milvus::test::PlannerIdentityEncryptor,
                       public milvus::test::PlannerIdentityDecryptor {
 public:
    std::string
    GetKey() const override {
        return std::string(32, '\0');
    }
};

class PackedImportCipher : public milvus::test::PlannerCipherPlugin {
 public:
    using EncryptorResult =
        std::pair<std::shared_ptr<milvus::storage::plugin::IEncryptor>,
                  std::string>;
    MOCK_METHOD(void,
                Update,
                (int64_t, int64_t, const std::string&),
                (override));
    MOCK_METHOD(void,
                RegisterReadKeyset,
                (int64_t, int64_t, const std::string&),
                (override));
    MOCK_METHOD(EncryptorResult,
                GetEncryptorForKeyset,
                (int64_t, int64_t, const std::string&),
                (override));
    MOCK_METHOD(std::shared_ptr<milvus::storage::plugin::IDecryptor>,
                GetDecryptor,
                (int64_t, int64_t, const std::string&),
                (const, override));
};

arrow::Result<int64_t>
ReadImportFFIFile(const std::string& path,
                  std::shared_ptr<arrow::Schema> schema,
                  CPluginContext* source) {
    const char* columns[] = {"int64"};
    char format[] = "parquet";
    char* paths[] = {const_cast<char*>(path.c_str())};
    int64_t start = 0;
    int64_t end = 3;
    LoonColumnGroups* groups = nullptr;
    auto result = loon_column_groups_create(
        columns, 1, format, paths, &start, &end, nullptr, 1, &groups);
    auto release_result =
        folly::makeGuard([&] { loon_ffi_free_result(&result); });
    if (!loon_ffi_is_success(&result)) {
        return arrow::Status::Invalid("could not create column group");
    }
    auto release_groups =
        folly::makeGuard([&] { loon_column_groups_destroy(groups); });
    CStorageConfig config{};
    config.storage_type = "local";
    config.root_path = "/";
    ArrowSchema exported{};
    ARROW_RETURN_NOT_OK(arrow::ExportSchema(*schema, &exported));
    CFFIPackedReader reader = nullptr;
    char column[] = "int64";
    char* needed[] = {column};
    auto status = NewPackedFFIReaderWithColumnGroups(groups,
                                                     &exported,
                                                     needed,
                                                     1,
                                                     &reader,
                                                     config,
                                                     source,
                                                     0,
                                                     nullptr,
                                                     nullptr);
    auto close = folly::makeGuard([&] {
        if (reader)
            CloseFFIReader(reader);
    });
    ArrowArrayStream stream{};
    if (status.error_code == 0) {
        status = GetFFIReaderStream(reader, 1024, &stream);
    }
    if (status.error_code != 0) {
        auto error = arrow::Status::Invalid(status.error_msg);
        free(const_cast<char*>(status.error_msg));
        return error;
    }
    ARROW_ASSIGN_OR_RAISE(auto batch_reader,
                          arrow::ImportRecordBatchReader(&stream));
    ARROW_ASSIGN_OR_RAISE(auto batch, batch_reader->Next());
    return batch ? batch->num_rows() : 0;
}

}  // namespace

TEST(CPackedTest, ImportWriterAndReadersUseTaskKeysets) {
    auto& loader = milvus::storage::PluginLoader::GetInstance();
    auto previous = loader.getCipherPlugin();
    auto restore = folly::makeGuard([&] {
        loader.unregisterPluginForTest("CipherPlugin");
        if (previous) {
            loader.registerPluginForTest(previous);
        }
    });
    auto cipher = std::make_shared<testing::StrictMock<PackedImportCipher>>();
    loader.registerPluginForTest(cipher);
    auto dek = std::make_shared<TestParquetKey>();
    CPluginContext target{17, 23, "dGFyZ2V0"};
    CPluginContext source{17, 0, "c291cmNl"};
    EXPECT_CALL(*cipher, RegisterReadKeyset(17, 0, "c291cmNl")).Times(3);
    EXPECT_CALL(*cipher, GetEncryptorForKeyset(17, 23, "dGFyZ2V0"))
        .WillOnce(testing::Return(
            PackedImportCipher::EncryptorResult{dek, "target-edek"}));
    EXPECT_CALL(*cipher, GetDecryptor(17, 23, "target-edek"))
        .Times(testing::AtLeast(2))
        .WillRepeatedly(testing::Return(dek));

    auto path = TestLocalPath + "import_task_keysets";
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_NO_FATAL_FAILURE(WriteInt64File(path, 3, &schema, &target));
    char* paths[] = {path.data()};
    // Each open can register an older source for the same EZ independently.
    int64_t* ids = nullptr;
    int64_t* counts = nullptr;
    auto status =
        GetPackedFileFieldIDs(paths, 1, nullptr, &source, &ids, &counts);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto free_footer = folly::makeGuard([&] {
        free(ids);
        free(counts);
    });
    ASSERT_EQ(counts[0], 1);
    EXPECT_EQ(ids[0], 100);

    ArrowSchema read_schema{};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &read_schema).ok());
    CPackedReader reader = nullptr;
    status = NewPackedReader(
        paths, 1, &read_schema, 1 << 20, false, &reader, &source);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto close_reader = folly::makeGuard([&] { CloseReader(reader); });
    ArrowArray batch{};
    status = ReadNext(reader, &batch, &read_schema);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto read = arrow::ImportRecordBatch(&batch, &read_schema);
    ASSERT_TRUE(read.ok()) << read.status().ToString();
    EXPECT_EQ((*read)->num_rows(), 3);
    auto ffi_rows = ReadImportFFIFile(path, schema, &source);
    ASSERT_TRUE(ffi_rows.ok()) << ffi_rows.status();
    EXPECT_EQ(*ffi_rows, 3);

    CPluginContext other{29, 0, "b3RoZXI="};
    EXPECT_CALL(*cipher, RegisterReadKeyset(29, 0, "b3RoZXI=")).Times(1);
    for (auto* context : {&other, static_cast<CPluginContext*>(nullptr)}) {
        auto rows = ReadImportFFIFile(path, schema, context);
        ASSERT_TRUE(rows.ok()) << rows.status();
        EXPECT_EQ(*rows, 3);
        int64_t* file_ids = nullptr;
        int64_t* file_counts = nullptr;
        status = GetPackedFileFieldIDs(
            paths, 1, nullptr, nullptr, &file_ids, &file_counts);
        ASSERT_EQ(status.error_code, 0) << status.error_msg;
        EXPECT_EQ(file_counts[0], 1);
        EXPECT_EQ(file_ids[0], 100);
        free(file_ids);
        free(file_counts);
    }
}

TEST(CPackedTest, PlaintextImportOpensPackedDataAndFooterWithoutCipher) {
    auto path = TestLocalPath + "plaintext_import_source";
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_NO_FATAL_FAILURE(WriteInt64File(path, 3, &schema));
    char* paths[] = {path.data()};
    int64_t* ids = nullptr;
    int64_t* counts = nullptr;
    auto status =
        GetPackedFileFieldIDs(paths, 1, nullptr, nullptr, &ids, &counts);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto free_footer = folly::makeGuard([&] {
        free(ids);
        free(counts);
    });
    ASSERT_EQ(counts[0], 1);
    EXPECT_EQ(ids[0], 100);
    ArrowSchema read_schema{};
    ASSERT_TRUE(arrow::ExportSchema(*schema, &read_schema).ok());
    CPackedReader reader = nullptr;
    status = NewPackedReader(
        paths, 1, &read_schema, 1 << 20, false, &reader, nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto close_reader = folly::makeGuard([&] { CloseReader(reader); });
    ArrowArray batch{};
    status = ReadNext(reader, &batch, &read_schema);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    auto rows = arrow::ImportRecordBatch(&batch, &read_schema);
    ASSERT_TRUE(rows.ok()) << rows.status();
    EXPECT_EQ((*rows)->num_rows(), 3);
    auto ffi_rows = ReadImportFFIFile(path, schema, nullptr);
    ASSERT_TRUE(ffi_rows.ok()) << ffi_rows.status();
    EXPECT_EQ(*ffi_rows, 3);
}

// The eager mode must return the same rows, in the same order, as the lazy
// mode. The last mode uses a read buffer smaller than the data, so the reader
// takes the file in more than one round.
TEST(CPackedTest, PackedReaderEagerPrebuffer) {
    // 8 bytes per row: about 3MB of values.
    const int64_t num_rows = 400 * 1000;
    const std::string file_path = TestLocalPath + "eager_ranges_0";
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_NO_FATAL_FAILURE(WriteInt64File(file_path, num_rows, &schema));

    struct ReadMode {
        int64_t buffer_size;
        bool eager_prebuffer;
    };
    const ReadMode modes[] = {
        {10 * 1024 * 1024, false},         // lazy, one round
        {10 * 1024 * 1024, true},          // eager, one round
        {1024 * 1024 + 512 * 1024, true},  // eager, several rounds
    };
    for (const auto& mode : modes) {
        SCOPED_TRACE(
            "buffer_size=" + std::to_string(mode.buffer_size) +
            " eager_prebuffer=" + std::to_string(mode.eager_prebuffer));
        struct ArrowSchema c_read_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_read_schema).ok());
        char* paths[] = {const_cast<char*>(file_path.c_str())};
        CPackedReader reader = nullptr;
        auto status = NewPackedReader(paths,
                                      1,
                                      &c_read_schema,
                                      mode.buffer_size,
                                      mode.eager_prebuffer,
                                      &reader,
                                      nullptr);
        ASSERT_EQ(status.error_code, 0);
        ASSERT_NE(reader, nullptr);

        int64_t next = 0;
        while (true) {
            struct ArrowArray read_array {};
            struct ArrowSchema read_schema {};
            status = ReadNext(reader, &read_array, &read_schema);
            ASSERT_EQ(status.error_code, 0);
            if (read_array.release == nullptr) {
                break;
            }
            auto imported = arrow::ImportRecordBatch(&read_array, &read_schema);
            ASSERT_TRUE(imported.ok());
            auto column = std::static_pointer_cast<arrow::Int64Array>(
                imported.ValueOrDie()->column(0));
            for (int64_t i = 0; i < column->length(); ++i) {
                ASSERT_EQ(column->Value(i), next);
                ++next;
            }
        }
        EXPECT_EQ(next, num_rows);

        status = CloseReader(reader);
        EXPECT_EQ(status.error_code, 0);
    }
}

TEST(CPackedTest, ReportsWriterAndReaderKeysetErrors) {
    auto& loader = milvus::storage::PluginLoader::GetInstance();
    auto previous = loader.getCipherPlugin();
    auto restore = folly::makeGuard([&] {
        loader.unregisterPluginForTest("CipherPlugin");
        if (previous) {
            loader.registerPluginForTest(previous);
        }
    });

    auto cipher = std::make_shared<testing::StrictMock<PackedImportCipher>>();
    loader.registerPluginForTest(cipher);

    auto schema = arrow::schema(
        {arrow::field("int64",
                      arrow::int64(),
                      false,
                      arrow::key_value_metadata(
                          {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"}))});

    CPluginContext empty_key{17, 23, ""};
    CPluginContext bad_reader_key{17, 0, "c291cmNl"};
    CPluginContext valid_target{17, 23, "dGFyZ2V0"};

    // Writer keyset errors: empty key -> InvalidParameter; null encryptor -> InvalidParameter.
    char* enc_key = nullptr;
    char* enc_meta = nullptr;
    auto empty_status = GetEncParams(&empty_key, &enc_key, &enc_meta);
    EXPECT_EQ(empty_status.error_code, milvus::ErrorCode::InvalidParameter);
    free(const_cast<char*>(empty_status.error_msg));
    EXPECT_EQ(enc_key, nullptr);
    EXPECT_EQ(enc_meta, nullptr);

    EXPECT_CALL(*cipher, GetEncryptorForKeyset(17, 23, "dGFyZ2V0"))
        .WillOnce(testing::Return(
            PackedImportCipher::EncryptorResult{nullptr, "edek"}));
    auto no_enc_status = GetEncParams(&valid_target, &enc_key, &enc_meta);
    EXPECT_EQ(no_enc_status.error_code, milvus::ErrorCode::InvalidParameter);
    free(const_cast<char*>(no_enc_status.error_msg));
    EXPECT_EQ(enc_key, nullptr);
    EXPECT_EQ(enc_meta, nullptr);

    // Reader keyset errors: empty key -> InvalidParameter; runtime_error -> UnexpectedError.
    std::string file = TestLocalPath + "rejected-reader.parquet";
    char* path = file.data();
    EXPECT_CALL(*cipher, RegisterReadKeyset(17, 0, "c291cmNl"))
        .WillOnce(testing::Throw(std::runtime_error("conflicting key ID")));
    for (auto [ctx, expected_code] : {
             std::pair{&empty_key, milvus::ErrorCode::InvalidParameter},
             std::pair{&bad_reader_key, milvus::ErrorCode::UnexpectedError},
         }) {
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        CPackedReader reader = nullptr;
        auto status =
            NewPackedReader(&path, 1, &c_schema, 1024, 0, &reader, ctx);
        if (c_schema.release != nullptr) {
            c_schema.release(&c_schema);
        }
        EXPECT_EQ(status.error_code, expected_code);
        free(const_cast<char*>(status.error_msg));
        EXPECT_EQ(reader, nullptr);
    }

    // Missing cipher plugin -> Unsupported for both writer and reader.
    loader.unregisterPluginForTest("CipherPlugin");
    auto no_plugin_writer = GetEncParams(&valid_target, &enc_key, &enc_meta);
    EXPECT_EQ(no_plugin_writer.error_code, milvus::ErrorCode::Unsupported);
    free(const_cast<char*>(no_plugin_writer.error_msg));
}

TEST(CPackedTest, PluginContextUpdateExceptionsReturnStatus) {
    auto& loader = milvus::storage::PluginLoader::GetInstance();
    auto previous = loader.getCipherPlugin();
    auto restore = folly::makeGuard([&] {
        loader.unregisterPluginForTest("CipherPlugin");
        if (previous) {
            loader.registerPluginForTest(previous);
        }
    });

    for (CPluginContext invalid :
         {CPluginContext{17, 23, nullptr}, CPluginContext{17, 23, ""}}) {
        auto status = PutOrRefPluginContext(invalid);
        EXPECT_EQ(status.error_code, milvus::ErrorCode::InvalidParameter);
        free(const_cast<char*>(status.error_msg));
    }

    auto cipher = std::make_shared<testing::StrictMock<PackedImportCipher>>();
    loader.registerPluginForTest(cipher);
    CPluginContext context{17, 23, "dGFyZ2V0"};

    EXPECT_CALL(*cipher, Update(17, 23, context.key))
        .WillOnce(testing::Throw(milvus::SegcoreError(
            milvus::ErrorCode::InvalidParameter, "invalid plugin context")));
    auto status = PutOrRefPluginContext(context);
    EXPECT_EQ(status.error_code, milvus::ErrorCode::InvalidParameter);
    free(const_cast<char*>(status.error_msg));

    EXPECT_CALL(*cipher, Update(17, 23, ""))
        .WillOnce(testing::Throw(std::runtime_error("unref failed")));
    status = UnRefPluginContext(context);
    EXPECT_EQ(status.error_code, milvus::ErrorCode::UnexpectedError);
    free(const_cast<char*>(status.error_msg));
}
