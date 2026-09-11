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

#include <filesystem>
#include <string>

#include "common/type_c.h"
#include "milvus-storage/properties.h"
#include "storage/StorageV2FSCache.h"
#include "storage/loon_ffi/util.h"

namespace {

std::string
MakeTempRoot(const std::string& name) {
    auto root =
        std::filesystem::temp_directory_path() /
        ("milvus_local_root_" + name + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    return root.string();
}

}  // namespace

// Milvus hands complete filesystem paths (already prefixed with
// localStorage.path) to the local Arrow / loon filesystem, so that filesystem
// must be rooted at "/". If it were rooted at localStorage.path again, every
// write would land under <root>/<root>/... (milvus-storage #351, #53051).
TEST(StorageV2FSCacheLocalRootTest, AbsolutePathsAreNotJoinedWithRootTwice) {
    const auto root = MakeTempRoot("fs");

    milvus::storage::StorageV2FSCache::Key key;
    key.root_path = root;
    key.storage_type = "local";
    auto fs = milvus::storage::StorageV2FSCache::Instance().Get(key);
    ASSERT_NE(fs, nullptr);

    const auto dir = root + "/insert_log/1/2/3/_data";
    const auto file = dir + "/0.parquet";
    ASSERT_TRUE(fs->CreateDir(dir, /*recursive=*/true).ok());
    auto out = fs->OpenOutputStream(file);
    ASSERT_TRUE(out.ok()) << out.status().ToString();
    ASSERT_TRUE(out.ValueOrDie()->Write("x", 1).ok());
    ASSERT_TRUE(out.ValueOrDie()->Close().ok());

    // Physical location is exactly the key.
    EXPECT_TRUE(std::filesystem::exists(file));
    // And not the double-joined location.
    EXPECT_FALSE(std::filesystem::exists(root + root + "/insert_log"));

    // Reading back through the same filesystem with the same key works.
    auto in = fs->OpenInputFile(file);
    ASSERT_TRUE(in.ok()) << in.status().ToString();
    auto size = in.ValueOrDie()->GetSize();
    ASSERT_TRUE(size.ok());
    EXPECT_EQ(size.ValueOrDie(), 1);

    std::filesystem::remove_all(root);
}

TEST(StorageV2FSCacheLocalRootTest, LoonFSRootPathRule) {
    EXPECT_EQ(LoonFSRootPath("local", "/var/lib/milvus/data"), "/");
    EXPECT_EQ(LoonFSRootPath("local", ""), "/");
    EXPECT_EQ(LoonFSRootPath("remote", "files"), "files");
    EXPECT_EQ(LoonFSRootPath("minio", "files"), "files");
    EXPECT_EQ(LoonFSRootPath("", "files"), "files");
}

TEST(StorageV2FSCacheLocalRootTest, InternalPropertiesUseSlashForLocal) {
    CStorageConfig c_config{};
    c_config.root_path = "/var/lib/milvus/data";
    c_config.storage_type = "local";
    auto props = MakeInternalPropertiesFromStorageConfig(c_config);
    auto root = milvus_storage::api::GetValue<std::string>(
        *props, PROPERTY_FS_ROOT_PATH);
    ASSERT_TRUE(root.ok());
    EXPECT_EQ(root.ValueOrDie(), "/");

    auto local_props = MakeInternalLocalProperies();
    auto local_root = milvus_storage::api::GetValue<std::string>(
        *local_props, PROPERTY_FS_ROOT_PATH);
    ASSERT_TRUE(local_root.ok());
    EXPECT_EQ(local_root.ValueOrDie(), "/");

    CStorageConfig remote{};
    remote.root_path = "files";
    remote.storage_type = "remote";
    auto remote_props = MakeInternalPropertiesFromStorageConfig(remote);
    auto remote_root = milvus_storage::api::GetValue<std::string>(
        *remote_props, PROPERTY_FS_ROOT_PATH);
    ASSERT_TRUE(remote_root.ok());
    EXPECT_EQ(remote_root.ValueOrDie(), "files");
}
