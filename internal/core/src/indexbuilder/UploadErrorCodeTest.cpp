// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/Json.h"
#include "common/protobuf_utils_c.h"
#include "index/Meta.h"
#include "indexbuilder/BuildSession.h"
#include "indexbuilder/index_c.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"
#include "segcore/json_stats/JsonKeyStats.h"
#include "segcore/json_stats/bson_inverted.h"
#include "simdjson/padded_string.h"
#include "storage/ChunkManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/LocalChunkManagerSingleton.h"

namespace milvus::indexbuilder::test {
namespace {

constexpr ErrorCode kUploadErrors[] = {
    ErrorCode::S3Error, ErrorCode::BucketInvalid, ErrorCode::ObjectNotExist};

// Read the real input binlog and fail only writes to this upload namespace.
class FailingUploadChunkManager final : public storage::ChunkManager {
 public:
    FailingUploadChunkManager(storage::ChunkManagerPtr inner,
                              std::string prefix,
                              ErrorCode code,
                              bool coded = true)
        : inner_(std::move(inner)),
          prefix_(std::move(prefix)),
          code_(code),
          coded_(coded) {
    }

    bool
    Exist(const std::string& path) override {
        return inner_->Exist(path);
    }
    uint64_t
    Size(const std::string& path) override {
        return inner_->Size(path);
    }
    uint64_t
    Read(const std::string& path, void* data, uint64_t size) override {
        return inner_->Read(path, data, size);
    }
    uint64_t
    Read(const std::string& path,
         uint64_t offset,
         void* data,
         uint64_t size) override {
        return inner_->Read(path, offset, data, size);
    }
    void
    Write(const std::string& path, void* data, uint64_t size) override {
        FailUpload(path);
        inner_->Write(path, data, size);
    }
    void
    Write(const std::string& path,
          uint64_t offset,
          void* data,
          uint64_t size) override {
        FailUpload(path);
        inner_->Write(path, offset, data, size);
    }
    std::vector<std::string>
    ListWithPrefix(const std::string& path) override {
        return inner_->ListWithPrefix(path);
    }
    void
    Remove(const std::string& path) override {
        inner_->Remove(path);
    }
    std::string
    GetName() const override {
        return inner_->GetName();
    }
    std::string
    GetRootPath() const override {
        return inner_->GetRootPath();
    }
    std::string
    GetBucketName() const override {
        return inner_->GetBucketName();
    }
    size_t
    FailedWrites() const {
        return failed_writes_.load();
    }

 private:
    void
    FailUpload(const std::string& path) {
        if (path.starts_with(prefix_)) {
            ++failed_writes_;
            if (!coded_) {
                throw std::runtime_error("uncoded artifact upload failure");
            }
            ThrowInfo(code_,
                      "injected artifact upload failure code {} at {}",
                      static_cast<int>(code_),
                      path);
        }
    }

    storage::ChunkManagerPtr inner_;
    std::string prefix_;
    ErrorCode code_;
    bool coded_;
    std::atomic<size_t> failed_writes_{0};
};

void
ExpectUploadError(ErrorCode code, const std::function<void()>& upload) {
    try {
        upload();
        FAIL() << "expected an upload failure";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), code) << error.what();
    }
}

void
ExpectUploadCStatus(BuildSession& session, ErrorCode code) {
    std::unique_ptr<::ProtoLayout, decltype(&ReleaseProtoLayout)> output(
        CreateProtoLayout(), ReleaseProtoLayout);
    ASSERT_NE(output, nullptr);
    const auto status = SerializeIndexAndUpLoad(
        reinterpret_cast<CIndex>(&session), output.get());
    EXPECT_EQ(status.error_code, code) << status.error_msg;
    EXPECT_EQ(output->blob, nullptr);
    EXPECT_EQ(output->size, 0);
    if (status.error_code != Success) {
        free(const_cast<char*>(status.error_msg));
    }
}

class UploadErrorCodeTest : public SourceBuildTest {
 protected:
    storage::FileManagerContext
    Context(DataType type, int64_t build_id) const {
        storage::StorageConfig config;
        config.storage_type = "local";
        config.root_path = directory_->Path();
        storage::FieldDataMeta field{1, 2, build_id, 100};
        field.field_schema.set_fieldid(100);
        field.field_schema.set_name("value");
        field.field_schema.set_data_type(
            static_cast<proto::schema::DataType>(type));
        return storage::FileManagerContext(
            field,
            storage::IndexMeta{build_id, 100, build_id, 1},
            storage::CreateChunkManager(config),
            storage::InitArrowFileSystem(config));
    }

    std::shared_ptr<FailingUploadChunkManager>
    Inject(storage::FileManagerContext& context,
           const std::string& prefix,
           ErrorCode code,
           bool coded = true) const {
        auto failing = std::make_shared<FailingUploadChunkManager>(
            context.chunkManagerPtr, prefix, code, coded);
        context.chunkManagerPtr = failing;
        return failing;
    }

    PreparedBuild
    TextInput(int32_t scalar_version) const {
        proto::indexcgo::BuildIndexInfo info;
        info.set_collectionid(1);
        info.set_partitionid(2);
        info.set_segmentid(3);
        info.set_buildid(1000);
        info.set_index_version(1);
        info.set_num_rows(2);
        info.set_current_scalar_index_version(scalar_version);
        auto* schema = info.mutable_field_schema();
        schema->set_fieldid(100);
        schema->set_name("value");
        schema->set_data_type(proto::schema::DataType::VarChar);
        auto* max_length = schema->add_type_params();
        max_length->set_key("max_length");
        max_length->set_value("256");
        auto* analyzer = schema->add_type_params();
        analyzer->set_key("enable_analyzer");
        analyzer->set_value("true");
        auto* param = info.add_index_params();
        param->set_key(index::INDEX_TYPE);
        param->set_value(index::INVERTED_INDEX_TYPE);
        info.add_insert_files(directory_->Path() + "/insert/1");
        auto* config = info.mutable_storage_config();
        config->set_storage_type("local");
        config->set_root_path(directory_->Path());
        auto prepared = AdaptBuildIndexInfo(info, BuildPurpose::TextIndex);
        prepared.request.staging_parent = directory_->Path();
        return prepared;
    }
};

// Scalar engine versions 1 and 2 both publish through the production V1/V2
// sink. Caller and C-ABI cases each build a fresh source-backed artifact.
class LegacyArtifactUploadErrorTest
    : public UploadErrorCodeTest,
      public ::testing::WithParamInterface<std::tuple<int32_t, bool>> {
 protected:
    void
    CheckUpload(PreparedBuild prepared,
                ErrorCode code,
                storage::ArtifactStorageNamespace storage_namespace) {
        ASSERT_EQ(prepared.request.output.generation,
                  storage::Generation::V1V2);
        ASSERT_EQ(prepared.request.output.storage_namespace, storage_namespace);
        storage::DiskFileManagerImpl paths(prepared.file_manager_context);
        const auto prefix =
            storage_namespace == storage::ArtifactStorageNamespace::TextLog
                ? paths.GetRemoteTextLogPrefix()
                : paths.GetRemoteIndexObjectPrefix();
        auto failing =
            Inject(prepared.file_manager_context, prefix + "/", code);
        BuildSession session(prepared.request, prepared.file_manager_context);
        ASSERT_NO_THROW(session.BuildFromSource());
        ASSERT_EQ(failing->FailedWrites(), 0);
        if (std::get<1>(GetParam())) {
            ExpectUploadCStatus(session, code);
        } else {
            ExpectUploadError(code,
                              [&] { static_cast<void>(session.Publish()); });
        }
        EXPECT_GT(failing->FailedWrites(), 0);
    }
};

TEST_P(LegacyArtifactUploadErrorTest, TantivyKeepsStorageErrorCode) {
    for (const auto code : kUploadErrors) {
        SCOPED_TRACE(static_cast<int>(code));
        auto prepared = Prepare(DataType::INT64,
                                DataType::NONE,
                                false,
                                index::INVERTED_INDEX_TYPE,
                                2,
                                0,
                                Config::object(),
                                std::get<0>(GetParam()));
        ASSERT_EQ(prepared.request.family, index::families::kInverted);
        const std::vector<int64_t> values{1, 2};
        auto data =
            storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
        data->FillFieldData(values.data(), values.size());
        WriteInsert(prepared, data);
        CheckUpload(std::move(prepared),
                    code,
                    storage::ArtifactStorageNamespace::Index);
    }
}

TEST_P(LegacyArtifactUploadErrorTest, RTreeKeepsStorageErrorCode) {
    for (const auto code : kUploadErrors) {
        SCOPED_TRACE(static_cast<int>(code));
        auto prepared = Prepare(DataType::GEOMETRY,
                                DataType::NONE,
                                false,
                                index::RTREE_INDEX_TYPE,
                                2,
                                0,
                                Config::object(),
                                std::get<0>(GetParam()));
        ASSERT_EQ(prepared.request.family, index::families::kRTree);
        const std::vector<std::string> values{
            Geometry(GetThreadLocalGEOSContext(), "POINT(0 0)").to_wkb_string(),
            Geometry(GetThreadLocalGEOSContext(), "POINT(2 2)")
                .to_wkb_string()};
        auto data =
            storage::CreateFieldData(DataType::GEOMETRY, DataType::NONE, false);
        data->FillFieldData(values.data(), values.size());
        WriteInsert(prepared, data);
        CheckUpload(std::move(prepared),
                    code,
                    storage::ArtifactStorageNamespace::Index);
    }
}

TEST_P(LegacyArtifactUploadErrorTest, TextLogKeepsStorageErrorCode) {
    for (const auto code : kUploadErrors) {
        SCOPED_TRACE(static_cast<int>(code));
        auto prepared = TextInput(std::get<0>(GetParam()));
        ASSERT_EQ(prepared.request.family, index::families::kText);
        const std::vector<std::string> values{"alpha beta", "beta gamma"};
        auto data =
            storage::CreateFieldData(DataType::VARCHAR, DataType::NONE, false);
        data->FillFieldData(values.data(), values.size());
        WriteInsert(prepared, data);
        CheckUpload(std::move(prepared),
                    code,
                    storage::ArtifactStorageNamespace::TextLog);
    }
}

INSTANTIATE_TEST_SUITE_P(
    V1V2,
    LegacyArtifactUploadErrorTest,
    ::testing::Combine(::testing::Values(1, 2), ::testing::Bool()),
    [](const ::testing::TestParamInfo<std::tuple<int32_t, bool>>& info) {
        return "V" + std::to_string(std::get<0>(info.param)) +
               (std::get<1>(info.param) ? "CStatus" : "Caller");
    });

TEST_F(UploadErrorCodeTest, JsonMetadataKeepsStorageErrorCode) {
    for (const auto code : kUploadErrors) {
        SCOPED_TRACE(static_cast<int>(code));
        auto context =
            Context(DataType::JSON, 10000 + static_cast<int64_t>(code));
        storage::DiskFileManagerImpl paths(context);
        const auto meta =
            paths.GetRemoteJsonStatsMetaPath(JSON_STATS_META_FILE_NAME);
        auto failing = Inject(context, meta, code);
        milvus::index::JsonKeyStats stats(context, false);
        std::vector<milvus::Json> values;
        values.emplace_back(
            simdjson::padded_string(std::string(R"({"key":1})")));
        auto data =
            storage::CreateFieldData(DataType::JSON, DataType::NONE, false);
        data->FillFieldData(values.data(), values.size());
        ASSERT_NO_THROW(stats.BuildWithFieldData({data}, false));
        ASSERT_EQ(failing->FailedWrites(), 0);
        ExpectUploadError(code, [&] { static_cast<void>(stats.Upload()); });
        EXPECT_GT(failing->FailedWrites(), 0);
    }
}

TEST_F(UploadErrorCodeTest, BsonSharedIndexKeepsStorageErrorCode) {
    for (const auto code : kUploadErrors) {
        SCOPED_TRACE(static_cast<int>(code));
        auto context =
            Context(DataType::JSON, 11000 + static_cast<int64_t>(code));
        storage::DiskFileManagerImpl paths(context);
        const auto remote = paths.GetRemoteJsonStatsSharedIndexPath("", 0);
        const auto prefix = remote.substr(0, remote.rfind('/')) + "/";
        auto failing = Inject(context, prefix, code);
        const auto local =
            paths.GetLocalTempJsonStatsPrefix() + "/shared_key_index";
        std::filesystem::create_directories(local);
        milvus::index::BsonInvertedIndex stats(
            local, 100, context, milvus::index::TANTIVY_INDEX_LATEST_VERSION);
        stats.AddRecord("/key", 0, 0);
        ASSERT_NO_THROW(stats.BuildIndex());
        ASSERT_EQ(failing->FailedWrites(), 0);
        ExpectUploadError(code,
                          [&] { static_cast<void>(stats.UploadIndex()); });
        EXPECT_GT(failing->FailedWrites(), 0);
    }
}

TEST_F(UploadErrorCodeTest, LogUploadsKeepStorageErrorCodes) {
    using Upload = void (storage::DiskFileManagerImpl::*)(const std::string&);
    const std::pair<const char*, Upload> operations[] = {
        {"text", &storage::DiskFileManagerImpl::AddTextLog},
        {"metadata", &storage::DiskFileManagerImpl::AddJsonStatsMetaLog},
        {"shared", &storage::DiskFileManagerImpl::AddJsonSharedIndexLog}};
    auto local =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    const auto file = directory_->Path() + "/local-log";
    std::vector<uint8_t> bytes(1024, 1);
    local->Write(file, bytes.data(), bytes.size());
    for (const auto& [name, operation] : operations) {
        SCOPED_TRACE(name);
        for (const auto code : kUploadErrors) {
            SCOPED_TRACE(static_cast<int>(code));
            auto context =
                Context(DataType::JSON, 12000 + static_cast<int64_t>(code));
            auto failing = Inject(context, directory_->Path() + "/", code);
            storage::DiskFileManagerImpl manager(context);
            ExpectUploadError(code, [&] { (manager.*operation)(file); });
            EXPECT_GT(failing->FailedWrites(), 0);
            EXPECT_TRUE(manager.GetRemotePathsToFileSize().empty());
        }
    }
    local->Remove(file);
}

TEST_F(UploadErrorCodeTest, MissingLocalUploadsReportPathNotExist) {
    using Upload = void (storage::DiskFileManagerImpl::*)(const std::string&);
    const Upload operations[] = {
        &storage::DiskFileManagerImpl::AddFileOrThrow,
        &storage::DiskFileManagerImpl::AddTextLog,
        &storage::DiskFileManagerImpl::AddJsonStatsMetaLog,
        &storage::DiskFileManagerImpl::AddJsonSharedIndexLog};
    auto context = Context(DataType::JSON, 13000);
    auto failing =
        Inject(context, directory_->Path() + "/", ErrorCode::S3Error);
    storage::DiskFileManagerImpl manager(context);
    const auto missing = directory_->Path() + "/absent";
    for (const auto operation : operations) {
        ExpectUploadError(ErrorCode::PathNotExist,
                          [&] { (manager.*operation)(missing); });
    }
    EXPECT_EQ(failing->FailedWrites(), 0);
    EXPECT_TRUE(manager.GetRemotePathsToFileSize().empty());
    EXPECT_FALSE(manager.AddFile(missing));
}

TEST_F(UploadErrorCodeTest, KnowhereBoolAddFileStillReturnsFalse) {
    auto local =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    const auto file = directory_->Path() + "/local-index";
    std::vector<uint8_t> bytes(1024, 1);
    local->Write(file, bytes.data(), bytes.size());
    for (const auto code : kUploadErrors) {
        SCOPED_TRACE(static_cast<int>(code));
        auto context =
            Context(DataType::INT64, 14000 + static_cast<int64_t>(code));
        auto failing = Inject(context, directory_->Path() + "/", code);
        storage::DiskFileManagerImpl manager(context);
        EXPECT_FALSE(manager.AddFile(file));
        EXPECT_GT(failing->FailedWrites(), 0);
        EXPECT_TRUE(manager.GetRemotePathsToFileSize().empty());
    }
    local->Remove(file);
}

TEST_F(UploadErrorCodeTest, IndexCStatusKeepsUncodedUnexpectedErrorFallback) {
    auto prepared = Prepare(DataType::INT64,
                            DataType::NONE,
                            false,
                            index::INVERTED_INDEX_TYPE,
                            2,
                            0,
                            Config::object(),
                            2);
    const std::vector<int64_t> values{1, 2};
    auto data =
        storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
    data->FillFieldData(values.data(), values.size());
    WriteInsert(prepared, data);
    storage::DiskFileManagerImpl paths(prepared.file_manager_context);
    auto failing = Inject(prepared.file_manager_context,
                          paths.GetRemoteIndexObjectPrefix() + "/",
                          ErrorCode::S3Error,
                          false);
    BuildSession session(prepared.request, prepared.file_manager_context);
    ASSERT_NO_THROW(session.BuildFromSource());
    ASSERT_EQ(failing->FailedWrites(), 0);
    ExpectUploadCStatus(session, ErrorCode::UnexpectedError);
    EXPECT_GT(failing->FailedWrites(), 0);
}

}  // namespace
}  // namespace milvus::indexbuilder::test
