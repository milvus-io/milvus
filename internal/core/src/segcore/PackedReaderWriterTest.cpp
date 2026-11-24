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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "storage/KeyRetriever.h"
#include "storage/loon_ffi/ffi_writer_c.h"
#include "storage/PluginLoader.h"
#include "storage/storage_c.h"
#include "milvus-storage/common/constants.h"
#include "segcore/packed_writer_c.h"
#include "segcore/packed_reader_c.h"
#include "segcore/arrow_fs_c.h"
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/array/builder_primitive.h>
#include "arrow/table_builder.h"
#include "arrow/type_fwd.h"
#include <arrow/util/key_value_metadata.h>
#include <new>
#include <numeric>
#include "test_utils/Constants.h"

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

    CColumnGroups cgs = NewCColumnGroups();
    int group[] = {0};
    AddCColumnGroup(cgs, group, 1);

    auto c_status = InitLocalArrowFileSystemSingleton(path);
    EXPECT_EQ(c_status.error_code, 0);
    CPackedWriter c_packed_writer = nullptr;
    c_status = NewPackedWriter(&c_write_schema,
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
        paths, 1, &c_read_schema, buffer_size, &c_packed_reader, nullptr);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_NE(c_packed_reader, nullptr);

    c_status = CloseReader(c_packed_reader);
    EXPECT_EQ(c_status.error_code, 0);
    FreeCColumnGroups(cgs);
}

namespace {
namespace plugin = milvus::storage::plugin;
using EncryptorResult =
    std::pair<std::shared_ptr<plugin::IEncryptor>, std::string>;
using ::testing::AtLeast;
using ::testing::Return;
using ::testing::StrictMock;

class OrdinaryCipher : public plugin::ICipherPlugin {
 public:
    std::string
    getPluginName() const override {
        return "CipherPlugin";
    }
    MOCK_METHOD(void,
                Update,
                (int64_t, int64_t, const std::string&),
                (override));
    MOCK_METHOD(EncryptorResult,
                GetEncryptor,
                (int64_t, int64_t),
                (const, override));
    MOCK_METHOD(std::shared_ptr<plugin::IDecryptor>,
                GetDecryptor,
                (int64_t, int64_t, const std::string&),
                (const, override));
};

class ImportCipher : public OrdinaryCipher {
 public:
    MOCK_METHOD(void,
                RegisterReadKeyset,
                (int64_t, int64_t, const std::string&),
                (override));
    MOCK_METHOD(EncryptorResult,
                GetEncryptorForKeyset,
                (int64_t, int64_t, const std::string&),
                (override));
};

class ParquetFileKey : public plugin::IEncryptor, public plugin::IDecryptor {
 public:
    MOCK_METHOD(std::string, GetKey, (), (const, override));
    MOCK_METHOD(std::string, Encrypt, (const std::string&), (const, override));
    MOCK_METHOD(std::string, Encrypt, (std::string_view), (const, override));
    MOCK_METHOD(std::string, Encrypt, (const void*, size_t), (const, override));
    MOCK_METHOD(std::string, Decrypt, (const std::string&), (const, override));
    MOCK_METHOD(std::string, Decrypt, (std::string_view), (const, override));
    MOCK_METHOD(std::string, Decrypt, (const void*, size_t), (const, override));
};

::testing::AssertionResult
CheckStatus(CStatus status) {
    if (status.error_code == 0) {
        return ::testing::AssertionSuccess();
    }
    auto failure = ::testing::AssertionFailure() << status.error_msg;
    free(const_cast<char*>(status.error_msg));
    return failure;
}

class CPackedImportTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        ASSERT_TRUE(CheckStatus(
            InitLocalArrowFileSystemSingleton(TestLocalPath.data())));
        int column = 0;
        AddCColumnGroup(groups, &column, 1);
    }
    void
    TearDown() override {
        FreeCColumnGroups(groups);
        milvus::storage::PluginLoader::GetInstance().unregisterPluginForTest(
            "CipherPlugin");
    }

    void
    WriteAndClose(CPackedWriter writer, int64_t value) {
        arrow::Int64Builder builder;
        ASSERT_TRUE(builder.Append(value).ok());
        auto result = builder.Finish();
        ASSERT_TRUE(result.ok());
        ArrowSchema c_schema, c_field;
        ArrowArray c_array;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        ASSERT_TRUE(
            arrow::ExportArray(*result.ValueOrDie(), &c_array, &c_field).ok());
        ASSERT_TRUE(CheckStatus(
            WriteRecordBatch(writer, &c_array, &c_field, &c_schema)));
        ASSERT_TRUE(CheckStatus(CloseWriter(writer)));
    }

    void
    ReadImport(char* path, CPluginContext* source, int64_t expected) {
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        CPackedReader reader = nullptr;
        ASSERT_TRUE(CheckStatus(NewPackedReader(
            &path, 1, &c_schema, buffer_size, &reader, source)));
        ASSERT_NO_FATAL_FAILURE(ReadAndClose(reader, expected));
    }

    void
    ReadAndClose(CPackedReader reader, int64_t expected) {
        CArrowArray array = nullptr;
        CArrowSchema schema_out = nullptr;
        ASSERT_TRUE(CheckStatus(ReadNext(reader, &array, &schema_out)));
        std::unique_ptr<ArrowArray> c_array(static_cast<ArrowArray*>(array));
        std::unique_ptr<ArrowSchema> c_batch_schema(
            static_cast<ArrowSchema*>(schema_out));
        ASSERT_NE(c_array, nullptr);
        auto batch =
            arrow::ImportRecordBatch(c_array.get(), c_batch_schema.get());
        ASSERT_TRUE(batch.ok());
        ASSERT_EQ((*batch)->num_rows(), 1);
        EXPECT_EQ(
            std::static_pointer_cast<arrow::Int64Array>((*batch)->column(0))
                ->Value(0),
            expected);
        EXPECT_TRUE(CheckStatus(CloseReader(reader)));
    }

    static constexpr int64_t buffer_size = 1 << 20;
    CColumnGroups groups = NewCColumnGroups();
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {arrow::field("int64",
                      arrow::int64(),
                      false,
                      arrow::key_value_metadata(
                          {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"}))});
};

TEST_F(CPackedImportTest, EncryptedReaderRequiresImportCipher) {
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    std::string file_path = TestLocalPath + "missing-import-input";
    char* path = file_path.data();
    CPluginContext source{17, 23, "c291cmNl"};
    CPackedReader reader = nullptr;
    auto status = NewPackedReader(&path, 1, &c_schema, 1024, &reader, &source);
    ArrowSchemaRelease(&c_schema);
    EXPECT_EQ(status.error_code, milvus::ErrorCode::Unsupported);
    ASSERT_NE(status.error_msg, nullptr);
    EXPECT_NE(std::string(status.error_msg).find("cipher plugin"),
              std::string::npos);
    free(const_cast<char*>(status.error_msg));
    EXPECT_EQ(reader, nullptr);
}

TEST_F(CPackedImportTest, DecryptsUsingFileMetadataAfterRegistration) {
    auto cipher = std::make_shared<StrictMock<ImportCipher>>();
    milvus::storage::PluginLoader::GetInstance().registerPluginForTest(cipher);
    CPluginContext source{17, 0, "c291cmNl"};
    EXPECT_CALL(*cipher, RegisterReadKeyset(17, 0, source.key));
    milvus::storage::RegisterReaderKeys(&source);
    auto key = std::make_shared<StrictMock<ParquetFileKey>>();
    EXPECT_CALL(*cipher, GetDecryptor(18, 23, "wrapped")).WillOnce(Return(key));
    EXPECT_CALL(*key, GetKey()).WillOnce(Return("file-key"));
    auto properties = milvus::storage::GetReaderProperties();
    EXPECT_EQ(properties.file_decryption_properties()->key_retriever()->GetKey(
                  milvus::storage::EncodeKeyMetadata(18, 23, "wrapped")),
              "file-key");
}

TEST_F(CPackedImportTest, ReportsWriterKeysetErrors) {
    CPluginContext target{17, 23, "dGFyZ2V0"};
    char* enc_key = nullptr;
    char* enc_meta = nullptr;
    auto missing_enc = GetEncParams(&target, &enc_key, &enc_meta);
    EXPECT_EQ(missing_enc.error_code, milvus::ErrorCode::Unsupported);
    free(const_cast<char*>(missing_enc.error_msg));
    EXPECT_EQ(enc_key, nullptr);
    EXPECT_EQ(enc_meta, nullptr);

    CPluginContext null_key{17, 23, nullptr};
    CPluginContext empty_key{17, 23, ""};
    for (auto* context :
         {static_cast<CPluginContext*>(nullptr), &null_key, &empty_key}) {
        auto invalid = GetEncParams(context, &enc_key, &enc_meta);
        EXPECT_EQ(invalid.error_code, milvus::ErrorCode::InvalidParameter);
        EXPECT_FALSE(CheckStatus(invalid));
        EXPECT_EQ(enc_key, nullptr);
        EXPECT_EQ(enc_meta, nullptr);
    }

    auto cipher = std::make_shared<StrictMock<ImportCipher>>();
    milvus::storage::PluginLoader::GetInstance().registerPluginForTest(cipher);
    auto file_key = std::make_shared<StrictMock<ParquetFileKey>>();
    EXPECT_CALL(*file_key, GetKey())
        .WillOnce(Return(std::string(32, 'a')))
        .WillOnce(Return(std::string(32, 'a')))
        .WillOnce(Return("short"));
    EXPECT_CALL(*cipher, GetEncryptorForKeyset(17, 23, target.key))
        .WillOnce(Return(EncryptorResult{}))
        .WillOnce(Return(EncryptorResult{file_key, ""}))
        .WillOnce(
            Return(EncryptorResult{file_key, std::string("wrapped\0key", 11)}))
        .WillOnce(Return(EncryptorResult{file_key, "wrapped"}))
        .WillOnce(Return(EncryptorResult{}))
        .WillOnce(::testing::Throw(milvus::SegcoreError(
            milvus::ErrorCode::Unsupported, "writer keysets are unsupported")));
    std::string file = TestLocalPath + "rejected-target.parquet";
    char* path = file.data();
    for (const auto* reason : {"cipher plugin returned no encryptor",
                               "unusable encryption parameters",
                               "unusable encryption parameters",
                               "unusable encryption parameters"}) {
        SCOPED_TRACE(reason);
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        CPackedWriter writer = nullptr;
        auto status = NewPackedWriter(
            &c_schema, buffer_size, &path, 1, 0, groups, &writer, &target);
        EXPECT_EQ(status.error_code, milvus::ErrorCode::InvalidParameter);
        if (status.error_msg != nullptr) {
            EXPECT_THAT(status.error_msg, ::testing::HasSubstr(reason));
        }
        EXPECT_FALSE(CheckStatus(status));
        EXPECT_EQ(writer, nullptr);
        if (writer != nullptr) {
            EXPECT_TRUE(CheckStatus(CloseWriter(writer)));
        }
    }

    auto bad_enc = GetEncParams(&target, &enc_key, &enc_meta);
    EXPECT_EQ(bad_enc.error_code, milvus::ErrorCode::InvalidParameter);
    if (bad_enc.error_msg != nullptr) {
        EXPECT_THAT(bad_enc.error_msg,
                    ::testing::HasSubstr("cipher plugin returned no encryptor"));
    }
    EXPECT_FALSE(CheckStatus(bad_enc));
    EXPECT_EQ(enc_key, nullptr);
    EXPECT_EQ(enc_meta, nullptr);

    auto unsupported_enc = GetEncParams(&target, &enc_key, &enc_meta);
    EXPECT_EQ(unsupported_enc.error_code, milvus::ErrorCode::Unsupported);
    EXPECT_FALSE(CheckStatus(unsupported_enc));
    EXPECT_EQ(enc_key, nullptr);
    EXPECT_EQ(enc_meta, nullptr);
}

TEST_F(CPackedImportTest, ReportsReaderKeysetErrors) {
    auto cipher = std::make_shared<StrictMock<ImportCipher>>();
    milvus::storage::PluginLoader::GetInstance().registerPluginForTest(cipher);
    std::string file = TestLocalPath + "rejected-reader.parquet";
    char* path = file.data();

    CPluginContext empty_key{17, 0, ""};
    CPluginContext bad_key{17, 0, "c291cmNl"};
    EXPECT_CALL(*cipher, RegisterReadKeyset(17, 0, bad_key.key))
        .WillOnce(::testing::Throw(std::runtime_error("conflicting key ID")));

    for (auto [ctx, expected_code] : {
             std::pair{&empty_key, milvus::ErrorCode::InvalidParameter},
             std::pair{&bad_key, milvus::ErrorCode::UnexpectedError},
         }) {
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        CPackedReader reader = nullptr;
        auto status = NewPackedReader(&path, 1, &c_schema, 1024, &reader, ctx);
        ArrowSchemaRelease(&c_schema);
        EXPECT_EQ(status.error_code, expected_code);
        EXPECT_FALSE(CheckStatus(status));
        EXPECT_EQ(reader, nullptr);
    }
}

TEST_F(CPackedImportTest, PluginContextUpdateExceptionsReturnStatus) {
    for (CPluginContext invalid :
         {CPluginContext{17, 23, nullptr}, CPluginContext{17, 23, ""}}) {
        auto status = PutOrRefPluginContext(invalid);
        EXPECT_EQ(status.error_code, milvus::ErrorCode::InvalidParameter);
        EXPECT_FALSE(CheckStatus(status));
    }
    auto cipher = std::make_shared<StrictMock<OrdinaryCipher>>();
    milvus::storage::PluginLoader::GetInstance().registerPluginForTest(cipher);
    CPluginContext context{17, 23, "dGFyZ2V0"};

    EXPECT_CALL(*cipher, Update(17, 23, context.key))
        .WillOnce(::testing::Throw(milvus::SegcoreError(
            milvus::ErrorCode::InvalidParameter, "invalid plugin context")));
    auto status = PutOrRefPluginContext(context);
    EXPECT_EQ(status.error_code, milvus::ErrorCode::InvalidParameter);
    EXPECT_FALSE(CheckStatus(status));

    EXPECT_CALL(*cipher, Update(17, 23, ""))
        .WillOnce(::testing::Throw(std::runtime_error("unref failed")));
    status = UnRefPluginContext(context);
    EXPECT_EQ(status.error_code, milvus::ErrorCode::UnexpectedError);
    EXPECT_FALSE(CheckStatus(status));
}

TEST_F(CPackedImportTest, ReadsSourceFooterAndWritesWithTargetKeyset) {
    auto cipher = std::make_shared<StrictMock<ImportCipher>>();
    milvus::storage::PluginLoader::GetInstance().registerPluginForTest(cipher);
    auto source_key = std::make_shared<StrictMock<ParquetFileKey>>();
    auto target_key = std::make_shared<StrictMock<ParquetFileKey>>();
    EXPECT_CALL(*source_key, GetKey())
        .WillRepeatedly(Return(std::string(32, 'a')));
    EXPECT_CALL(*target_key, GetKey())
        .WillRepeatedly(Return(std::string(32, 'b')));
    const std::string source_edek = "wrapped-source-data-encryption-key";
    const std::string target_edek = "wrapped-target-data-encryption-key";
    CPluginContext source{17, 23, "c291cmNl"};
    CPluginContext target{17, 45, "dGFyZ2V0"};
    std::string source_file = TestLocalPath + "import-source.parquet";
    std::string target_file = TestLocalPath + "import-target.parquet";
    char* source_path = source_file.data();
    char* target_path = target_file.data();

    EXPECT_CALL(*cipher, GetEncryptorForKeyset(17, 23, "c291cmNl"))
        .WillOnce(Return(EncryptorResult{source_key, source_edek}));
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    CPackedWriter backup = nullptr;
    ASSERT_TRUE(CheckStatus(NewPackedWriter(
        &c_schema, buffer_size, &source_path, 1, 0, groups, &backup, &source)));
    ASSERT_NO_FATAL_FAILURE(WriteAndClose(backup, 11));

    EXPECT_CALL(*cipher, GetEncryptorForKeyset(17, 45, "dGFyZ2V0"))
        .Times(2)
        .WillRepeatedly(Return(EncryptorResult{target_key, target_edek}));
    char* key = nullptr;
    char* metadata = nullptr;
    ASSERT_TRUE(CheckStatus(GetEncParams(&target, &key, &metadata)));
    EXPECT_EQ(std::string(key), std::string(32, 'b'));
    EXPECT_EQ(std::string(metadata), "17_45_" + target_edek);
    free(key);
    free(metadata);
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    CPackedWriter writer = nullptr;
    ASSERT_TRUE(CheckStatus(NewPackedWriter(
        &c_schema, buffer_size, &target_path, 1, 0, groups, &writer, &target)));

    CPluginContext import_source{17, 0, "c291cmNl"};
    EXPECT_CALL(*cipher, RegisterReadKeyset(17, 0, "c291cmNl"))
        .Times(AtLeast(1));
    EXPECT_CALL(*cipher, GetDecryptor(17, 23, source_edek))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(source_key));
    ASSERT_NO_FATAL_FAILURE(ReadImport(source_path, &import_source, 11));

    ASSERT_NO_FATAL_FAILURE(ReadImport(source_path, nullptr, 11));

    ASSERT_NO_FATAL_FAILURE(WriteAndClose(writer, 22));
    EXPECT_CALL(*cipher, RegisterReadKeyset(17, 45, "dGFyZ2V0"))
        .Times(AtLeast(1));
    EXPECT_CALL(*cipher, GetDecryptor(17, 45, target_edek))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(target_key));
    ASSERT_NO_FATAL_FAILURE(ReadImport(target_path, &target, 22));
}

TEST_F(CPackedImportTest, FileImportUsesEncryptedLegacyTarget) {
    std::string file = TestLocalPath + "legacy-file-import.parquet";
    char* path = file.data();
    CPluginContext target{17, 45, "dGFyZ2V0"};
    CPackedWriter writer = nullptr;
    auto open = [&](CPluginContext* context) {
        ArrowSchema c_schema;
        EXPECT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        return NewPackedWriter(
            &c_schema, buffer_size, &path, 1, 0, groups, &writer, context);
    };
    auto expect_rejected = [&](CStatus status, milvus::ErrorCode code) {
        EXPECT_EQ(status.error_code, code);
        EXPECT_FALSE(CheckStatus(status));
        EXPECT_EQ(writer, nullptr);
        if (writer) {
            EXPECT_TRUE(CheckStatus(CloseWriter(writer)));
            writer = nullptr;
        }
    };
    expect_rejected(open(&target), milvus::ErrorCode::Unsupported);

    auto cipher = std::make_shared<StrictMock<OrdinaryCipher>>();
    milvus::storage::PluginLoader::GetInstance().registerPluginForTest(cipher);
    CPluginContext missing_key{17, 45, ""};
    expect_rejected(open(&missing_key), milvus::ErrorCode::InvalidParameter);

    auto key = std::make_shared<StrictMock<ParquetFileKey>>();
    EXPECT_CALL(*key, GetKey()).WillRepeatedly(Return(std::string(32, 'k')));
    {
        ::testing::InSequence ordered;
        EXPECT_CALL(*cipher, Update(17, 45, "dGFyZ2V0"));
        EXPECT_CALL(*cipher, GetEncryptor(17, 45))
            .WillOnce(Return(EncryptorResult{key, "wrapped-target"}));
    }
    ASSERT_TRUE(CheckStatus(open(&target)));
    ASSERT_NO_FATAL_FAILURE(WriteAndClose(writer, 31));

    EXPECT_CALL(*cipher, GetDecryptor(17, 45, "wrapped-target"))
        .Times(AtLeast(1))
        .WillRepeatedly(Return(key));
    ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
    CPackedReader reader = nullptr;
    ASSERT_TRUE(CheckStatus(
        NewPackedReader(&path, 1, &c_schema, buffer_size, &reader, nullptr)));
    ASSERT_NO_FATAL_FAILURE(ReadAndClose(reader, 31));
}
}  // namespace
