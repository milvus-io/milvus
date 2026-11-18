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

#include <gtest/gtest.h>

#include <cerrno>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "arrow/filesystem/localfs.h"
#include "arrow/io/memory.h"
#include "arrow/util/io_util.h"
#include "common/CGoCatch.h"
#include "milvus-storage/common/extend_status.h"
#include "storage/ArrowFileSystemChunkManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/loon_ffi/util.h"

using namespace milvus;
using namespace milvus::storage;

namespace {

class FailingInput : public arrow::io::RandomAccessFile {
 public:
    FailingInput(arrow::Status size_status, arrow::Status read_status)
        : data_(arrow::Buffer::FromString("data")),
          size_status_(std::move(size_status)),
          read_status_(std::move(read_status)) {
    }

    arrow::Result<int64_t>
    GetSize() override {
        ARROW_RETURN_NOT_OK(size_status_);
        return data_.GetSize();
    }

    arrow::Result<int64_t>
    Read(int64_t size, void* out) override {
        ARROW_RETURN_NOT_OK(read_status_);
        return data_.Read(size, out);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t size) override {
        ARROW_RETURN_NOT_OK(read_status_);
        return data_.Read(size);
    }

    arrow::Status
    Close() override {
        return data_.Close();
    }

    bool
    closed() const override {
        return data_.closed();
    }

    arrow::Result<int64_t>
    Tell() const override {
        return data_.Tell();
    }

    arrow::Status
    Seek(int64_t position) override {
        return data_.Seek(position);
    }

 private:
    arrow::io::BufferReader data_;
    const arrow::Status size_status_;
    const arrow::Status read_status_;
};

class ProbeFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    ProbeFileSystem()
        : SubTreeFileSystem("",
                            std::make_shared<arrow::fs::LocalFileSystem>()) {
    }

    arrow::Status list_status = arrow::Status::OK();
    arrow::Status size_status = arrow::Status::OK();
    arrow::Status read_status = arrow::Status::OK();
    arrow::Status remove_status = arrow::Status::OK();
    std::vector<arrow::fs::FileInfo> entries;
    std::vector<arrow::fs::FileSelector> selectors;
    std::vector<std::string> opened;
    std::vector<std::string> removed;

    arrow::Result<std::vector<arrow::fs::FileInfo>>
    GetFileInfo(const arrow::fs::FileSelector& selector) override {
        selectors.push_back(selector);
        ARROW_RETURN_NOT_OK(list_status);
        return entries;
    }

    arrow::Result<arrow::fs::FileInfo>
    GetFileInfo(const std::string&) override {
        ADD_FAILURE()
            << "Exact object stat must not use directory-aware GetFileInfo";
        return arrow::Status::IOError("unexpected directory stat");
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        opened.push_back(path);
        return std::make_shared<FailingInput>(size_status, read_status);
    }

    arrow::Status
    DeleteFile(const std::string& path) override {
        removed.push_back(path);
        return remove_status;
    }
};

// Verify the same native exception -> CStatus boundary used by node init and
// index tasks, including the original failure message.
static void
ExpectFailure(const std::function<void()>& operation,
              ErrorCode code,
              const std::string& message) {
    const auto invoke = [&]() -> CStatus {
        try {
            operation();
            return SuccessCStatus();
        }
        CGO_CATCH_AND_RETURN_CSTATUS
    };
    const auto status = invoke();
    EXPECT_EQ(status.error_code, static_cast<int>(code));
    EXPECT_NE(
        std::string(status.error_msg ? status.error_msg : "").find(message),
        std::string::npos);
    if (status.error_code != 0) {
        std::free(const_cast<char*>(status.error_msg));
    }
}

}  // namespace

TEST(ArrowFileSystemChunkManager, LocalKeepsItsManager) {
    auto config = std::make_shared<milvus_storage::api::Properties>();
    (*config)[PROPERTY_FS_STORAGE_TYPE] = std::string("local");
    (*config)[PROPERTY_FS_ROOT_PATH] = std::string("/tmp");
    // A local filesystem does not use the remote provider setting.
    (*config)[PROPERTY_FS_CLOUD_PROVIDER] = std::string("gcpnative");
    EXPECT_EQ(CreateChunkManager(config)->GetName(), "LocalChunkManager");
    EXPECT_NO_THROW(MakeInternalProperties(config));
}

TEST(ArrowFileSystemChunkManager, InvalidProviderDoesNotFallBack) {
    auto config = std::make_shared<milvus_storage::api::Properties>();
    (*config)[PROPERTY_FS_BUCKET_NAME] = std::string("test-bucket");
    for (const auto* storage_type : {"remote", "minio"}) {
        (*config)[PROPERTY_FS_STORAGE_TYPE] = std::string(storage_type);
        for (const auto* provider : {"invalid-provider", "gcpnative"}) {
            SCOPED_TRACE(std::string(storage_type) + ":" + provider);
            (*config)[PROPERTY_FS_CLOUD_PROVIDER] = std::string(provider);
            ExpectFailure([&] { CreateChunkManager(config); },
                          ConfigInvalid,
                          "Unsupported cloud provider");
        }
        CStorageConfig c_config{};
        c_config.storage_type = storage_type;
        c_config.cloud_provider = "gcpnative";
        // Global filesystem init and packed IO share this C config boundary.
        ExpectFailure(
            [&] { MakeInternalPropertiesFromStorageConfig(c_config); },
            ConfigInvalid,
            "Unsupported cloud provider: gcpnative");
    }
}

TEST(ArrowFileSystemChunkManager, InvalidPropertyTypesReturnErrors) {
    auto properties = std::make_shared<milvus_storage::api::Properties>();
    (*properties)[PROPERTY_FS_STORAGE_TYPE] = false;
    ExpectFailure([&] { CreateChunkManager(properties); },
                  ConfigInvalid,
                  "fs.storage_type");
    ExpectFailure([&] { CreateChunkManager(nullptr); },
                  ConfigInvalid,
                  "Properties must not be null");
}

TEST(ArrowFileSystemChunkManager, EmptyProbePrefixIsAccepted) {
    auto fs = std::make_shared<ProbeFileSystem>();
    fs->list_status = arrow::internal::IOErrorFromErrno(ENOENT, "empty probe");
    ArrowFileSystemChunkManager cm(fs, "bucket", "files");
    ASSERT_EQ(fs->selectors.size(), 1);
    EXPECT_EQ(fs->selectors.front().base_dir, "files/justforconnectioncheck");
    EXPECT_FALSE(fs->selectors.front().allow_not_found);
    EXPECT_FALSE(fs->selectors.front().recursive);
    EXPECT_EQ(cm.GetRootPath(), "files");
    EXPECT_EQ(cm.GetBucketName(), "bucket");

    fs->selectors.clear();
    ArrowFileSystemChunkManager without_probe(
        fs, "bucket", "files", /*check_connection=*/false);
    EXPECT_TRUE(fs->selectors.empty());
}

TEST(ArrowFileSystemChunkManager, ProbeFailuresKeepTheirCodes) {
    using milvus_storage::ExtendStatusCode;
    for (const auto& [detail, code] :
         std::vector<std::pair<ExtendStatusCode, ErrorCode>>{
             {ExtendStatusCode::AwsErrorNotFound, ObjectNotExist},
             {ExtendStatusCode::AwsErrorAccessDenied, StorageError},
             {ExtendStatusCode::StorageTransientNetwork, StorageTransientError},
             {ExtendStatusCode::StorageTransientTimeout, StorageTransientError},
             {ExtendStatusCode::StorageTransientThrottling,
              StorageTransientError}}) {
        auto fs = std::make_shared<ProbeFileSystem>();
        fs->list_status =
            milvus_storage::MakeExtendError(detail, "backend probe failed");
        ExpectFailure(
            [&] { ArrowFileSystemChunkManager cm(fs, "bucket", "files"); },
            code,
            "backend probe failed");
    }
    auto fs = std::make_shared<ProbeFileSystem>();
    fs->list_status = arrow::Status::Invalid("invalid configured root");
    ExpectFailure(
        [&] { ArrowFileSystemChunkManager cm(fs, "bucket", "files"); },
        ConfigInvalid,
        "invalid configured root");
}

TEST(ArrowFileSystemChunkManager, SizeAndExistUseExactCompleteKeys) {
    auto fs = std::make_shared<ProbeFileSystem>();
    ArrowFileSystemChunkManager cm(fs, "bucket", "files");
    EXPECT_EQ(cm.Size("files/a"), 4);
    EXPECT_TRUE(cm.Exist("files/a"));
    EXPECT_EQ(fs->opened, (std::vector<std::string>{"files/a", "files/a"}));
    // A lazy HEAD miss must not be mistaken for an implicit directory.
    fs->size_status =
        arrow::internal::IOErrorFromErrno(ENOENT, "missing object");
    EXPECT_FALSE(cm.Exist("files/a"));
    ExpectFailure(
        [&] { cm.Size("files/a"); }, ObjectNotExist, "missing object");
    fs->size_status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::StorageTransientTimeout,
        "HEAD timeout");
    ExpectFailure(
        [&] { cm.Size("files/a"); }, StorageTransientError, "HEAD timeout");
    ExpectFailure(
        [&] { cm.Exist("files/a"); }, StorageTransientError, "HEAD timeout");
}

TEST(ArrowFileSystemChunkManager, ReadFailureRecheckPreservesOriginalError) {
    auto fs = std::make_shared<ProbeFileSystem>();
    ArrowFileSystemChunkManager cm(fs, "bucket", "files");
    char data[4];
    fs->read_status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::StorageTransientThrottling,
        "GET throttled");
    fs->size_status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::AwsErrorAccessDenied, "HEAD denied");
    ExpectFailure([&] { cm.Read("files/a", data, sizeof(data)); },
                  StorageTransientError,
                  "GET throttled");
    fs->size_status =
        arrow::internal::IOErrorFromErrno(ENOENT, "object deleted");
    ExpectFailure([&] { cm.Read("files/a", data, sizeof(data)); },
                  ObjectNotExist,
                  "GET throttled");
    fs->read_status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::AwsErrorNotFound, "GET missing");
    fs->opened.clear();
    ExpectFailure([&] { cm.Read("files/a", data, sizeof(data)); },
                  ObjectNotExist,
                  "GET missing");
    EXPECT_EQ(fs->opened.size(), 1);  // A typed miss needs no second HEAD.
}

TEST(ArrowFileSystemChunkManager, ListPreservesRawPrefixAndOmitsDirectories) {
    auto fs = std::make_shared<ProbeFileSystem>();
    ArrowFileSystemChunkManager cm(fs, "bucket", "files");
    for (const auto* path : {"a/3", "a/30", "a/4"}) {
        fs->entries.emplace_back(path, arrow::fs::FileType::File);
    }
    fs->entries.emplace_back("a/3/subdir", arrow::fs::FileType::Directory);
    EXPECT_EQ(cm.ListWithPrefix("a/3"),
              (std::vector<std::string>{"a/3", "a/30"}));
    EXPECT_EQ(fs->selectors.back().base_dir, "a");
    EXPECT_TRUE(fs->selectors.back().recursive);
    fs->list_status =
        arrow::internal::IOErrorFromErrno(ENOENT, "no such prefix");
    EXPECT_TRUE(cm.ListWithPrefix("a/").empty());
    fs->list_status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::AwsErrorNotFound, "bucket missing");
    ExpectFailure(
        [&] { cm.ListWithPrefix("a/"); }, ObjectNotExist, "bucket missing");
}

TEST(ArrowFileSystemChunkManager, RemoveIsIdempotentButKeepsOtherErrors) {
    auto fs = std::make_shared<ProbeFileSystem>();
    ArrowFileSystemChunkManager cm(fs, "bucket", "files");
    cm.Remove("files/a");
    fs->remove_status =
        arrow::internal::IOErrorFromErrno(ENOENT, "object missing");
    EXPECT_NO_THROW(cm.Remove("files/a"));
    EXPECT_EQ(fs->removed, (std::vector<std::string>{"files/a", "files/a"}));
    fs->remove_status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::AwsErrorAccessDenied,
        "DELETE denied");
    ExpectFailure([&] { cm.Remove("files/a"); }, StorageError, "DELETE denied");
}
