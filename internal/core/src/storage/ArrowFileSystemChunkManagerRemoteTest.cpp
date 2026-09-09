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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "storage/ArrowFileSystemChunkManager.h"
#include "storage/Types.h"

using namespace milvus::storage;

// Real object-storage coverage for the remote hybrid against a live MinIO:
// Write/Read go through the milvus-storage S3FileSystem data plane (multipart
// upload finalize, arrow read); Exist/Size/ListWithPrefix/Remove go through the
// legacy MinioChunkManager control-plane delegate (HeadObject stat, raw-prefix
// ListObjects, idempotent DeleteObject); Read not-found is classified via the
// delegate's exact-object existence. Make() also PreChecks the endpoint when it
// builds the delegate.
//
// Like RemoteChunkManagerTest.cpp and MinioChunkManagerTest.cpp, this file is
// EXCLUDED from the all_tests binary (see internal/core/unittest/CMakeLists.txt)
// so the default cpp-ut run needs no object store. To run it, un-exclude the
// file and build/run all_tests against a live MinIO -- e.g. inside the
// docker-compose builder, where the `minio` service is reachable at
// MINIO_ADDRESS (minio:9000) with the bucket below created. The test skips
// (never fails) when MINIO_ADDRESS is unset or the endpoint is unreachable, so
// it is safe to leave compiled in an environment that may or may not have MinIO.
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

    // Make() PreChecks the endpoint when it builds the legacy delegate; treat a
    // connection failure (endpoint down, bucket absent) as "no environment" and
    // skip rather than fail -- a genuine hybrid bug surfaces in the CRUD ops
    // below, not in the connection PreCheck.
    ChunkManagerPtr cm_ptr;
    try {
        cm_ptr = ArrowFileSystemChunkManager::Make(config);
    } catch (const std::exception& e) {
        GTEST_SKIP() << "MinIO at " << address
                     << " unreachable, skip remote minio test: " << e.what();
    }
    ASSERT_NE(cm_ptr, nullptr);
    auto& cm = *cm_ptr;
    EXPECT_EQ(cm.GetName(), "ArrowFileSystemChunkManager");
    EXPECT_EQ(cm.GetBucketName(), "a-bucket");

    // pid-scoped prefix so concurrent/repeat runs don't collide; the Arrow
    // filesystem is rooted at the bucket, so this same key addresses the object
    // on both the data plane (Arrow) and the control-plane delegate (raw key).
    std::string prefix = "arrow_fs_cm_ut/" + std::to_string(::getpid()) + "/";
    std::string path = prefix + "insert_log/1/2/3";
    uint8_t data[5] = {0x17, 0x32, 0x00, 0x34, 0x23};

    // clean any leftovers from a previous crashed run under this pid
    for (const auto& p : cm.ListWithPrefix(prefix)) {
        cm.Remove(p);
    }

    // Write (Arrow data plane) -> Exist/Size (delegate control plane)
    EXPECT_FALSE(cm.Exist(path));
    cm.Write(path, data, sizeof(data));
    EXPECT_TRUE(cm.Exist(path));
    EXPECT_EQ(cm.Size(path), sizeof(data));

    // Read (Arrow data plane) round-trips the exact bytes
    uint8_t readdata[20] = {0};
    EXPECT_EQ(cm.Read(path, readdata, sizeof(data)), sizeof(data));
    EXPECT_EQ(std::memcmp(data, readdata, sizeof(data)), 0);

    // overwrite: multipart finalize replaces the object, Size reflects the new
    // length (not appended)
    uint8_t data2[3] = {0x1, 0x2, 0x3};
    cm.Write(path, data2, sizeof(data2));
    EXPECT_EQ(cm.Size(path), sizeof(data2));

    // raw-prefix ListObjects (delegate), returning full object keys. A second
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

    // Remove (delegate) is a single idempotent DeleteObject; the object is gone
    // on both planes afterwards
    cm.Remove(path);
    EXPECT_FALSE(cm.Exist(path));
    cm.Remove(path2);
    EXPECT_EQ(cm.ListWithPrefix(prefix).size(), 0);
}
