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
                      ("arrow_fs_cm_test_" + std::to_string(::getpid()) + "_" +
                       std::to_string(reinterpret_cast<uintptr_t>(this))))
                         .string();
        std::filesystem::create_directories(root_path_);
        cm_ = std::make_unique<ArrowFileSystemChunkManager>(
            LocalStorageConfig(root_path_));
    }

    // LocalChunkManager parity: callers address the local backend with OS
    // paths, not root-relative keys.
    std::string
    Abs(const std::string& rel) const {
        return root_path_ + "/" + rel;
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
    std::string path = Abs("insert_log/1/2/3");
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
    std::string path = Abs("insert_log/not/exist");

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
    std::vector<std::string> paths = {Abs("list_test/f1"),
                                      Abs("list_test/f2"),
                                      Abs("list_test/sub/f3"),
                                      Abs("list_other/f4")};
    uint8_t data[4] = {1, 2, 3, 4};
    for (const auto& p : paths) {
        cm_->Write(p, data, sizeof(data));
    }

    // raw prefix semantics: "list_test" also matches nothing outside it,
    // recursive within
    auto listed = cm_->ListWithPrefix(Abs("list_test"));
    std::sort(listed.begin(), listed.end());
    ASSERT_EQ(listed.size(), 3);
    EXPECT_EQ(listed[0], Abs("list_test/f1"));
    EXPECT_EQ(listed[1], Abs("list_test/f2"));
    EXPECT_EQ(listed[2], Abs("list_test/sub/f3"));

    // prefix ending mid-segment still matches
    auto listed2 = cm_->ListWithPrefix(Abs("list_test/f"));
    EXPECT_EQ(listed2.size(), 2);

    cm_->Remove(Abs("list_test/f1"));
    EXPECT_FALSE(cm_->Exist(Abs("list_test/f1")));
    EXPECT_EQ(cm_->ListWithPrefix(Abs("list_test")).size(), 2);
}

// LocalChunkManager parity: relative filepaths are plain OS paths resolved
// against the process CWD (the unittest suite relies on this via
// kOverrideRootPathForUT = "files"), and list results stay in the caller's
// relative namespace. The body runs inside a dedicated scratch CWD so that
// relative writes and the recursive ListWithPrefix walk (which lists the
// resolved prefix's parent directory) stay bounded to scratch space instead
// of walking / leaking into the repo/build tree that is the real CWD.
TEST_F(ArrowFileSystemChunkManagerTest, RelativePathsResolveAgainstCwd) {
    // RAII: chdir into a fresh scratch dir, then restore the original CWD and
    // reclaim the scratch dir on scope exit. The destructor runs on normal
    // return, on a fatal-ASSERT return, and on exception unwind, so a failure
    // below can neither leak a directory into the working tree nor leave the
    // process CWD changed for later tests in the binary.
    struct ScopedCwd {
        std::filesystem::path prev_;
        std::filesystem::path scratch_;
        explicit ScopedCwd(std::filesystem::path scratch)
            : prev_(std::filesystem::current_path()),
              scratch_(std::move(scratch)) {
            std::filesystem::create_directories(scratch_);
            std::filesystem::current_path(scratch_);
        }
        ~ScopedCwd() {
            std::error_code ec;
            std::filesystem::current_path(prev_, ec);
            std::filesystem::remove_all(scratch_, ec);
        }
    } scoped_cwd(std::filesystem::path(root_path_) / "cwd");

    std::string rel_path = "sub/f1";
    uint8_t data[3] = {7, 8, 9};
    cm_->Write(rel_path, data, sizeof(data));
    EXPECT_TRUE(cm_->Exist(rel_path));
    EXPECT_TRUE(
        std::filesystem::exists(std::filesystem::current_path() / rel_path));

    auto listed = cm_->ListWithPrefix("sub");
    ASSERT_EQ(listed.size(), 1);
    EXPECT_EQ(listed[0], rel_path);

    uint8_t buf[3] = {0};
    EXPECT_EQ(cm_->Read(rel_path, buf, sizeof(buf)), sizeof(buf));
    EXPECT_EQ(std::memcmp(data, buf, sizeof(buf)), 0);

    cm_->Remove(rel_path);
    EXPECT_FALSE(cm_->Exist(rel_path));
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
    // empty provider is supported: a minio/remote config with no explicit
    // provider still routes to the ArrowFileSystem backend
    EXPECT_TRUE(ArrowFileSystemChunkManager::SupportsCloudProvider(""));
}

// NOTE: constructing the LEGACY remote chunk managers requires a live
// object-storage endpoint (their constructors run a network PreCheck), which
// is why RemoteChunkManagerTest.cpp is excluded from this binary. The
// routing test below therefore only constructs the arrow-fs backend (client
// build without network) and the local backend.
TEST(ArrowFileSystemChunkManagerSwitch, CreateChunkManagerRouting) {
    // The suite-level master switch (MILVUS_USE_ARROW_FS_CHUNK_MANAGER in
    // init_gtest) may have flipped the flag already; restore whatever was
    // set when done.
    const bool prev = UseArrowFileSystemChunkManager();

    StorageConfig remote_config;
    remote_config.storage_type = "remote";
    remote_config.cloud_provider = "aws";
    remote_config.address = "localhost:9000";
    remote_config.bucket_name = "a-bucket";

    SetUseArrowFileSystemChunkManager(false);
    // switch off: legacy backends
    {
        auto cm = CreateChunkManager(LocalStorageConfig("/tmp"));
        EXPECT_EQ(cm->GetName(), "LocalChunkManager");
    }

    SetUseArrowFileSystemChunkManager(true);
    // switch on: arrow filesystem backend
    {
        auto cm = CreateChunkManager(remote_config);
        EXPECT_EQ(cm->GetName(), "ArrowFileSystemChunkManager");
        EXPECT_EQ(cm->GetBucketName(), "a-bucket");
    }
    // local storage type is rerouted too (OS-path passthrough semantics)
    {
        auto cm = CreateChunkManager(LocalStorageConfig("/tmp"));
        EXPECT_EQ(cm->GetName(), "ArrowFileSystemChunkManager");
        EXPECT_EQ(cm->GetRootPath(), "/tmp");
    }

    SetUseArrowFileSystemChunkManager(prev);
    ASSERT_EQ(UseArrowFileSystemChunkManager(), prev);
}
