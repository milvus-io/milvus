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

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "cachinglayer/Manager.h"
#include "common/common_type_c.h"
#include "index/Meta.h"
#include "segcore/arrow_fs_c.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"
#include "test_utils/Constants.h"

std::string TestLocalPath;
std::string TestRemotePath;
std::string TestMmapPath;

int
main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    const auto root =
        std::filesystem::path(std::getenv("MILVUS_TEST_ROOT_DIR") != nullptr
                                  ? std::getenv("MILVUS_TEST_ROOT_DIR")
                                  : MILVUS_CPPUT_OUTPUT_DIR) /
        ("json_stats_" + std::to_string(::getpid()));
    TestLocalPath = (root / "local_data").string() + "/";
    TestRemotePath = (root / "remote_data").string() + "/";
    TestMmapPath = (root / "mmap_data").string() + "/";
    std::filesystem::create_directories(TestLocalPath);
    std::filesystem::create_directories(TestRemotePath);
    std::filesystem::create_directories(TestMmapPath);

    milvus::storage::LocalChunkManagerSingleton::GetInstance().Init(
        TestLocalPath);
    milvus::storage::MmapManager::GetInstance().Init(
        milvus::storage::MmapConfig{"willneed",
                                    TestMmapPath,
                                    uint64_t{2} * 1024 * 1024 * 1024,
                                    uint64_t{4} * 1024 * 1024,
                                    false});

    CStorageConfig arrow_fs_config = {};
    arrow_fs_config.root_path = TestLocalPath.c_str();
    arrow_fs_config.storage_type = "local";
    const auto status = InitArrowFileSystem(arrow_fs_config);
    if (status.error_code != 0) {
        throw std::runtime_error("failed to initialize Arrow filesystem");
    }

    constexpr int64_t mb = 1024 * 1024;
    milvus::cachinglayer::Manager::ConfigureTieredStorage(
        {CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable},
        {1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb},
        true,
        true,
        {10, true, 30},
        std::chrono::milliseconds(0),
        std::chrono::milliseconds(-1));
    milvus::index::kOverrideRootPathForUT = "files";

    return RUN_ALL_TESTS();
}
