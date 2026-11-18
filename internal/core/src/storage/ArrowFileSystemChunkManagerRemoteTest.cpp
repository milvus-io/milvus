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
#include <folly/ScopeGuard.h>

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/ArrowFileSystemChunkManager.h"
#include "storage/Types.h"
#include "storage/Util.h"

using namespace milvus::storage;

class ArrowFileSystemChunkManagerRemote : public testing::TestWithParam<bool> {
};

// Exercise the same object contract through both storage filesystem providers.
TEST_P(ArrowFileSystemChunkManagerRemote, CRUD) {
    const bool is_azure = GetParam();
    const char* enable_key =
        is_azure ? "MILVUS_TEST_AZURE_FS" : "MILVUS_TEST_REMOTE_FS";
    const auto* enabled = std::getenv(enable_key);
    if (enabled == nullptr || std::string(enabled) != "1") {
        GTEST_SKIP() << "set " << enable_key << "=1 to run remote FS tests";
    }
    const char* address =
        std::getenv(is_azure ? "AZURITE_ADDRESS" : "MINIO_ADDRESS");
    ASSERT_NE(address, nullptr);
    ASSERT_FALSE(std::string(address).empty());

    std::optional<std::string> previous_azurite;
    std::optional<std::string> previous_connection_string;
    if (const char* value = std::getenv("USE_AZURITE")) {
        previous_azurite = value;
    }
    if (const char* value = std::getenv("AZURE_STORAGE_CONNECTION_STRING")) {
        previous_connection_string = value;
    }
    const auto restore_env = folly::makeGuard([&] {
        if (!is_azure) {
            return;
        }
        if (previous_azurite) {
            setenv("USE_AZURITE", previous_azurite->c_str(), 1);
        } else {
            unsetenv("USE_AZURITE");
        }
        if (previous_connection_string) {
            setenv("AZURE_STORAGE_CONNECTION_STRING",
                   previous_connection_string->c_str(),
                   1);
        } else {
            unsetenv("AZURE_STORAGE_CONNECTION_STRING");
        }
    });
    if (is_azure) {
        setenv("USE_AZURITE", "true", 1);
        // All operations must use Properties, including metadata operations.
        // A surviving legacy Azure delegate would consume this invalid value.
        setenv(
            "AZURE_STORAGE_CONNECTION_STRING", "invalid-connection-string", 1);
    }

    const std::string bucket =
        is_azure ? "arrow-fs-azure-" + std::to_string(::getpid()) : "a-bucket";
    const std::string secret =
        is_azure
            ? "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
              "K1SZFPTOtr/KBHBeksoGMGw=="
            : "minioadmin";
    auto config = std::make_shared<milvus_storage::api::Properties>();
    (*config)[PROPERTY_FS_STORAGE_TYPE] = std::string("remote");
    (*config)[PROPERTY_FS_CLOUD_PROVIDER] =
        std::string(is_azure ? "azure" : "aws");
    (*config)[PROPERTY_FS_ADDRESS] = std::string(address);
    (*config)[PROPERTY_FS_BUCKET_NAME] = bucket;
    (*config)[PROPERTY_FS_ACCESS_KEY_ID] =
        std::string(is_azure ? "devstoreaccount1" : "minioadmin");
    (*config)[PROPERTY_FS_ACCESS_KEY_VALUE] = secret;
    (*config)[PROPERTY_FS_USE_SSL] = bool(false);
    (*config)[PROPERTY_FS_USE_IAM] = false;
    (*config)[PROPERTY_FS_USE_CUSTOM_PART_UPLOAD] = true;
    (*config)[PROPERTY_FS_ROOT_PATH] = std::string("files");

    milvus_storage::ArrowFileSystemPtr azure_fs;
    milvus_storage::ArrowFileSystemPtr azure_account_fs;
    const auto clean_bucket = folly::makeGuard([&] {
        if (azure_account_fs) {
            const auto status = azure_account_fs->DeleteDir(bucket);
            EXPECT_TRUE(status.ok()) << status.ToString();
        }
    });
    if (is_azure) {
        const auto result = GetArrowFileSystem(config);
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        azure_fs = *result;
        // The shared FS is rooted at the bucket; container setup/cleanup
        // belongs to its underlying account filesystem.
        const auto proxy =
            std::dynamic_pointer_cast<arrow::fs::SubTreeFileSystem>(azure_fs);
        ASSERT_NE(proxy, nullptr);
        const auto status = proxy->base_fs()->CreateDir(bucket);
        ASSERT_TRUE(status.ok()) << status.ToString();
        azure_account_fs = proxy->base_fs();
    }

    // S3 checks connectivity at construction; Azure remains lazy. Backend
    // failures must keep their classifications at the first actual request.
    auto invalid = std::make_shared<milvus_storage::api::Properties>(*config);
    (*invalid)[PROPERTY_FS_BUCKET_NAME] =
        std::string("missing-fs-chunk-" + std::to_string(::getpid()));
    ChunkManagerPtr invalid_azure_cm;
    if (is_azure) {
        ASSERT_NO_THROW(invalid_azure_cm = CreateChunkManager(invalid));
    }
    try {
        if (is_azure) {
            invalid_azure_cm->Size("missing");
        } else {
            CreateChunkManager(invalid);
        }
        FAIL() << "missing bucket must report ObjectNotExist";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::ObjectNotExist);
    }
    invalid = std::make_shared<milvus_storage::api::Properties>(*config);
    if (is_azure) {
        (*invalid)[PROPERTY_FS_ACCESS_KEY_VALUE] = "A" + secret.substr(1);
        ASSERT_NO_THROW(invalid_azure_cm = CreateChunkManager(invalid));
    } else {
        (*invalid)[PROPERTY_FS_ACCESS_KEY_ID] =
            std::string("invalid-fs-chunk-key");
    }
    try {
        if (is_azure) {
            invalid_azure_cm->Exist("missing");
        } else {
            CreateChunkManager(invalid);
        }
        FAIL() << "invalid credentials must report StorageError";
    } catch (const milvus::SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), milvus::ErrorCode::StorageError);
    }

    auto cm_ptr = CreateChunkManager(config);
    ASSERT_NE(cm_ptr, nullptr);
    auto& cm = *cm_ptr;
    EXPECT_EQ(cm.GetName(), "ArrowFileSystemChunkManager");
    EXPECT_EQ(cm.GetBucketName(), bucket);

    // pid-scoped prefix so concurrent/repeat runs don't collide; the Arrow
    // filesystem is rooted at the bucket, so this same key addresses the object
    // for every operation.
    std::string prefix = "arrow_fs_cm_ut/" + std::to_string(::getpid()) + "/";
    std::string path = prefix + "insert_log/1/2/3";
    uint8_t data[5] = {0x17, 0x32, 0x00, 0x34, 0x23};

    auto compatibility_config =
        std::make_shared<milvus_storage::api::Properties>(*config);
    (*compatibility_config)[PROPERTY_FS_STORAGE_TYPE] = std::string("minio");
    if (!is_azure) {
        (*compatibility_config)[PROPERTY_FS_CLOUD_PROVIDER] = std::string("");
    }
    const auto compatibility_cm = CreateChunkManager(compatibility_config);
    EXPECT_EQ(compatibility_cm->GetName(), "ArrowFileSystemChunkManager");

    const auto empty_path = prefix + "empty";
    cm.Write(empty_path, data, 0);
    EXPECT_TRUE(cm.Exist(empty_path));
    EXPECT_EQ(cm.Size(empty_path), 0);
    cm.Remove(empty_path);
    EXPECT_FALSE(cm.Exist(prefix.substr(0, prefix.size() - 1)));

    // clean any leftovers from a previous crashed run under this pid
    for (const auto& p : cm.ListWithPrefix(prefix)) {
        cm.Remove(p);
    }

    // Write, Exist, Size, and Read all use the same filesystem.
    EXPECT_FALSE(cm.Exist(path));
    cm.Write(path, data, sizeof(data));
    EXPECT_TRUE(cm.Exist(path));
    EXPECT_EQ(cm.Size(path), sizeof(data));

    // Read (Arrow data plane) round-trips the exact bytes
    uint8_t readdata[20] = {0};
    EXPECT_EQ(cm.Read(path, readdata, sizeof(data)), sizeof(data));
    EXPECT_EQ(std::memcmp(data, readdata, sizeof(data)), 0);
    EXPECT_EQ(cm.Read(path, readdata, sizeof(readdata)), sizeof(data));

    // overwrite: multipart finalize replaces the object, Size reflects the new
    // length (not appended)
    uint8_t data2[3] = {0x1, 0x2, 0x3};
    cm.Write(path, data2, sizeof(data2));
    EXPECT_EQ(cm.Size(path), sizeof(data2));

    // Raw-prefix listing returns complete keys. A second
    // object whose key extends the first (".../3" vs ".../30") pins the raw
    // byte-prefix semantics: a prefix ending mid-segment matches both, which a
    // directory-style FileSelector would miss (buqian review #1).
    std::string path2 = prefix + "insert_log/1/2/30";
    cm.Write(path2, data2, sizeof(data2));

    auto listed = cm.ListWithPrefix(prefix);
    std::sort(listed.begin(), listed.end());
    ASSERT_EQ(listed.size(), 2);
    EXPECT_EQ(listed[0], path);
    EXPECT_EQ(listed[1], path2);

    auto listed_mid = cm.ListWithPrefix(path);  // prefix ends mid-segment
    std::sort(listed_mid.begin(), listed_mid.end());
    ASSERT_EQ(listed_mid.size(), 2);
    EXPECT_EQ(listed_mid[0], path);
    EXPECT_EQ(listed_mid[1], path2);

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
    // Remove of a missing object is swallowed (idempotent), matching legacy
    EXPECT_NO_THROW(cm.Remove(prefix + "not/exist"));

    // Remove remains idempotent through the FS adapter.
    cm.Remove(path);
    EXPECT_FALSE(cm.Exist(path));
    cm.Remove(path2);
    EXPECT_EQ(cm.ListWithPrefix(prefix).size(), 0);
    EXPECT_NO_THROW(cm.Remove(path2));
    if (is_azure) {
        // Storage's flat-namespace DeleteFile preserves an empty parent
        // directory marker. It must not appear as a ChunkManager file.
        const auto parent = azure_fs->GetFileInfo(prefix + "insert_log/1/2");
        ASSERT_TRUE(parent.ok()) << parent.status().ToString();
        EXPECT_TRUE(parent->IsDirectory());
    }
}

INSTANTIATE_TEST_SUITE_P(ObjectStores,
                         ArrowFileSystemChunkManagerRemote,
                         testing::Values(false, true),
                         [](const testing::TestParamInfo<bool>& info) {
                             return info.param ? "Azurite" : "Minio";
                         });
