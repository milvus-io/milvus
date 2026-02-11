// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <stdint.h>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <memory>
#include <string>

#include "folly/init/Init.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "test_utils/Constants.h"
#include "test_utils/storage_test_utils.h"

std::string TestLocalPath;
std::string TestRemotePath;
std::string TestMmapPath;

int
main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::Init follyInit(&argc, &argv, false);

    // Determine the base directory for test output.
    // Priority: MILVUS_TEST_ROOT_DIR env var > compile-time MILVUS_CPPUT_OUTPUT_DIR
    std::string base_dir;
    if (auto* env = std::getenv("MILVUS_TEST_ROOT_DIR")) {
        base_dir = env;
    } else {
        base_dir = MILVUS_CPPUT_OUTPUT_DIR;
    }

    // Compute shard-aware test paths to avoid conflicts in parallel test runs
    int shard_index = 0;
    int total_shards = 1;
    if (auto* env = std::getenv("GTEST_SHARD_INDEX")) {
        shard_index = std::atoi(env);
    }
    if (auto* env = std::getenv("GTEST_TOTAL_SHARDS")) {
        total_shards = std::atoi(env);
    }
    std::srand(std::time(nullptr));
    int random = std::rand();
    // Ensure random % total_shards == shard_index so different shards use different paths
    random = random - (random % total_shards) + shard_index;
    TestLocalPath =
        base_dir + "/test_" + std::to_string(random) + "/local_data/";
    TestRemotePath =
        base_dir + "/test_" + std::to_string(random) + "/remote_data/";
    TestMmapPath = base_dir + "/test_" + std::to_string(random) + "/mmap_data/";

    // Ensure test directories exist before any Init calls
    std::filesystem::create_directories(TestLocalPath);
    std::filesystem::create_directories(TestRemotePath);
    std::filesystem::create_directories(TestMmapPath);

    milvus::storage::LocalChunkManagerSingleton::GetInstance().Init(
        TestLocalPath);
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(
        get_default_local_storage_config());
    milvus::storage::MmapManager::GetInstance().Init(get_default_mmap_config());

    milvus_storage::ArrowFileSystemConfig arrow_conf;
    arrow_conf.storage_type = "local";
    arrow_conf.root_path = TestLocalPath;
    milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(arrow_conf);

    static const int64_t mb = 1024 * 1024;

    milvus::cachinglayer::Manager::ConfigureTieredStorage(
        {CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable},
        {1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb},
        true,
        true,
        {10, true, 30},
        std::chrono::milliseconds(0));

    return RUN_ALL_TESTS();
}
