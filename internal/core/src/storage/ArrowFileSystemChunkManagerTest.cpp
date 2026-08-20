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

#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/ArrowFileSystemChunkManager.h"
#include "storage/Types.h"
#include "storage/Util.h"

using namespace milvus::storage;

namespace {

StorageConfig
LocalStorageConfig(const std::string& root_path) {
    StorageConfig config;
    config.storage_type = "local";
    config.root_path = root_path;
    config.bucket_name = "";
    return config;
}

class ArrowFileSystemChunkManagerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        root_path_ = (std::filesystem::temp_directory_path() /
                      ("arrow_fs_cm_test_" +
                       std::to_string(::getpid()) + "_" +
                       std::to_string(reinterpret_cast<uintptr_t>(this))))
                         .string();
        std::filesystem::create_directories(root_path_);
        cm_ = std::make_unique<ArrowFileSystemChunkManager>(
            LocalStorageConfig(root_path_));
    }

    void
    TearDown() override {
        cm_.reset();
        std::error_code ec;
        std::filesystem::remove_all(root_path_, ec);
    }

    std::string root_path_;
    std::unique_ptr<ArrowFileSystemChunkManager> cm_;
};

}  // namespace

TEST_F(ArrowFileSystemChunkManagerTest, BasicMeta) {
    EXPECT_EQ(cm_->GetName(), "ArrowFileSystemChunkManager");
    EXPECT_EQ(cm_->GetRootPath(), root_path_);
}

TEST_F(ArrowFileSystemChunkManagerTest, WriteReadExistSize) {
    std::string path = "insert_log/1/2/3";
    uint8_t data[5] = {0x17, 0x32, 0x00, 0x34, 0x23};

    EXPECT_FALSE(cm_->Exist(path));
    cm_->Write(path, data, sizeof(data));
    EXPECT_TRUE(cm_->Exist(path));
    EXPECT_EQ(cm_->Size(path), sizeof(data));

    uint8_t readdata[20] = {0};
    EXPECT_EQ(cm_->Read(path, readdata, sizeof(data)), sizeof(data));
    EXPECT_EQ(std::memcmp(data, readdata, sizeof(data)), 0);

    // overwrite
    uint8_t data2[3] = {0x1, 0x2, 0x3};
    cm_->Write(path, data2, sizeof(data2));
    EXPECT_EQ(cm_->Size(path), sizeof(data2));
}

TEST_F(ArrowFileSystemChunkManagerTest, NotFoundSemantics) {
    std::string path = "insert_log/not/exist";

    // Exist: not-found -> false, no throw
    EXPECT_FALSE(cm_->Exist(path));

    // Size: not-found -> throws ObjectNotExist
    try {
        cm_->Size(path);
        FAIL() << "Size on missing object should throw";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::ObjectNotExist);
    }

    // Read: not-found -> throws ObjectNotExist (classified via ENOENT /
    // AwsErrorNotFound detail, not message parsing)
    uint8_t buf[8];
    try {
        cm_->Read(path, buf, sizeof(buf));
        FAIL() << "Read on missing object should throw";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::ObjectNotExist);
    }

    // Remove: not-found is swallowed (idempotent), matching legacy
    EXPECT_NO_THROW(cm_->Remove(path));
}

TEST_F(ArrowFileSystemChunkManagerTest, RemoveAndList) {
    std::vector<std::string> paths = {
        "list_test/f1", "list_test/f2", "list_test/sub/f3", "list_other/f4"};
    uint8_t data[4] = {1, 2, 3, 4};
    for (const auto& p : paths) {
        cm_->Write(p, data, sizeof(data));
    }

    // raw prefix semantics: "list_test" also matches nothing outside it,
    // recursive within
    auto listed = cm_->ListWithPrefix("list_test");
    std::sort(listed.begin(), listed.end());
    ASSERT_EQ(listed.size(), 3);
    EXPECT_EQ(listed[0], "list_test/f1");
    EXPECT_EQ(listed[1], "list_test/f2");
    EXPECT_EQ(listed[2], "list_test/sub/f3");

    // prefix ending mid-segment still matches
    auto listed2 = cm_->ListWithPrefix("list_test/f");
    EXPECT_EQ(listed2.size(), 2);

    cm_->Remove("list_test/f1");
    EXPECT_FALSE(cm_->Exist("list_test/f1"));
    EXPECT_EQ(cm_->ListWithPrefix("list_test").size(), 2);
}

TEST_F(ArrowFileSystemChunkManagerTest, OffsetOpsNotImplemented) {
    uint8_t buf[4] = {0};
    EXPECT_THROW(cm_->Read("any", 0, buf, sizeof(buf)), milvus::SegcoreError);
    EXPECT_THROW(cm_->Write("any", 0, buf, sizeof(buf)), milvus::SegcoreError);
}

TEST(ArrowFileSystemChunkManagerSwitch, SupportedProviders) {
    EXPECT_TRUE(ArrowFileSystemChunkManager::SupportsCloudProvider("aws"));
    EXPECT_TRUE(ArrowFileSystemChunkManager::SupportsCloudProvider("gcp"));
    EXPECT_TRUE(ArrowFileSystemChunkManager::SupportsCloudProvider("aliyun"));
    EXPECT_TRUE(ArrowFileSystemChunkManager::SupportsCloudProvider("azure"));
    EXPECT_TRUE(ArrowFileSystemChunkManager::SupportsCloudProvider("tencent"));
    EXPECT_TRUE(ArrowFileSystemChunkManager::SupportsCloudProvider("huawei"));
    // gcpnative has no milvus-storage producer; must stay on legacy
    EXPECT_FALSE(
        ArrowFileSystemChunkManager::SupportsCloudProvider("gcpnative"));
    EXPECT_FALSE(ArrowFileSystemChunkManager::SupportsCloudProvider(""));
}

// NOTE: constructing the LEGACY remote chunk managers requires a live
// object-storage endpoint (their constructors run a network PreCheck), which
// is why RemoteChunkManagerTest.cpp is excluded from this binary. The
// routing test below therefore only constructs the arrow-fs backend (client
// build without network) and the local backend.
TEST(ArrowFileSystemChunkManagerSwitch, CreateChunkManagerRouting) {
    ASSERT_FALSE(UseArrowFileSystemChunkManager());

    StorageConfig remote_config;
    remote_config.storage_type = "remote";
    remote_config.cloud_provider = "aws";
    remote_config.address = "localhost:9000";
    remote_config.bucket_name = "a-bucket";

    SetUseArrowFileSystemChunkManager(true);
    // switch on: arrow filesystem backend
    {
        auto cm = CreateChunkManager(remote_config);
        EXPECT_EQ(cm->GetName(), "ArrowFileSystemChunkManager");
        EXPECT_EQ(cm->GetBucketName(), "a-bucket");
    }
    // local storage type is never rerouted
    {
        auto cm = CreateChunkManager(LocalStorageConfig("/tmp"));
        EXPECT_EQ(cm->GetName(), "LocalChunkManager");
    }
    SetUseArrowFileSystemChunkManager(false);
    ASSERT_FALSE(UseArrowFileSystemChunkManager());
}

// Real object-storage coverage: exercises the milvus-storage S3FileSystem
// backend (multipart upload finalize, HeadObject stat, AwsErrorNotFound
// classification) against a live MinIO. Skipped unless MINIO_ADDRESS is set
// (it is inside the CI/builder container).
TEST(ArrowFileSystemChunkManagerRemote, MinioCRUD) {
    const char* address = std::getenv("MINIO_ADDRESS");
    if (address == nullptr || std::string(address).empty()) {
        GTEST_SKIP() << "MINIO_ADDRESS not set, skip remote minio test";
    }

    StorageConfig config;
    config.storage_type = "remote";
    config.cloud_provider = "aws";
    config.address = address;
    config.bucket_name = "a-bucket";
    config.access_key_id = "minioadmin";
    config.access_key_value = "minioadmin";
    config.useSSL = false;
    config.useIAM = false;
    config.root_path = "files";

    ArrowFileSystemChunkManager cm(config);
    EXPECT_EQ(cm.GetBucketName(), "a-bucket");

    std::string prefix =
        "arrow_fs_cm_ut/" + std::to_string(::getpid()) + "/";
    std::string path = prefix + "insert_log/1/2/3";
    uint8_t data[5] = {0x17, 0x32, 0x00, 0x34, 0x23};

    EXPECT_FALSE(cm.Exist(path));
    cm.Write(path, data, sizeof(data));
    EXPECT_TRUE(cm.Exist(path));
    EXPECT_EQ(cm.Size(path), sizeof(data));

    uint8_t readdata[20] = {0};
    EXPECT_EQ(cm.Read(path, readdata, sizeof(data)), sizeof(data));
    EXPECT_EQ(std::memcmp(data, readdata, sizeof(data)), 0);

    auto listed = cm.ListWithPrefix(prefix);
    ASSERT_EQ(listed.size(), 1);
    EXPECT_EQ(listed[0], path);

    // not-found classification must survive the S3 error path
    // (ExtendStatusDetail AwsErrorNotFound -> ObjectNotExist)
    try {
        cm.Size(prefix + "not/exist");
        FAIL() << "Size on missing object should throw";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::ObjectNotExist);
    }
    try {
        cm.Read(prefix + "not/exist", readdata, sizeof(readdata));
        FAIL() << "Read on missing object should throw";
    } catch (const milvus::SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), milvus::ErrorCode::ObjectNotExist);
    }
    EXPECT_NO_THROW(cm.Remove(prefix + "not/exist"));

    cm.Remove(path);
    EXPECT_FALSE(cm.Exist(path));
    EXPECT_EQ(cm.ListWithPrefix(prefix).size(), 0);
}
