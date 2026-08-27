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

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/ArrowFileSystemChunkManager.h"
#include "storage/Types.h"

using namespace milvus::storage;

// Real object-storage coverage: exercises the milvus-storage S3FileSystem
// backend (multipart upload finalize, HeadObject stat, AwsErrorNotFound
// classification) against a live MinIO.
//
// Like RemoteChunkManagerTest.cpp and MinioChunkManagerTest.cpp, this file is
// EXCLUDED from the all_tests binary (see internal/core/unittest/CMakeLists.txt):
// the cpp-ut environment has no reachable object store (MINIO_ADDRESS points at
// the docker-compose `minio` service, which does not resolve there). Run it
// manually against a live MinIO, e.g. inside the docker-compose builder where
// the `minio` service is up.
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

    auto cm_ptr = ArrowFileSystemChunkManager::Make(config);
    ASSERT_NE(cm_ptr, nullptr);
    auto& cm = *cm_ptr;
    EXPECT_EQ(cm.GetBucketName(), "a-bucket");

    std::string prefix = "arrow_fs_cm_ut/" + std::to_string(::getpid()) + "/";
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
