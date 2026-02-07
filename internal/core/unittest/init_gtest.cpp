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
#include <memory>
#include <string>

#include "cachinglayer/Manager.h"
#include "common/common_type_c.h"
#include "exec/expression/function/init_c.h"
#include "folly/init/Init.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "segcore/arrow_fs_c.h"
#include "test_utils/Constants.h"
#include "test_utils/storage_test_utils.h"

// Get test local path, allowing override via environment variable for parallel test execution
std::string
GetTestLocalPath() {
    const char* env_path = std::getenv("MILVUS_TEST_LOCAL_PATH");
    if (env_path != nullptr && env_path[0] != '\0') {
        std::string path(env_path);
        // Ensure path ends with /
        if (path.back() != '/') {
            path += '/';
        }
        return path;
    }
    return TestLocalPath;
}

int
main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::Init follyInit(&argc, &argv, false);

    // Initialize expression function factory (registers aggregate functions like count, sum, min, max)
    InitExecExpressionFunctionFactory();

    std::string testLocalPath = GetTestLocalPath();
    milvus::storage::LocalChunkManagerSingleton::GetInstance().Init(
        testLocalPath);
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(
        get_default_local_storage_config());
    milvus::storage::MmapManager::GetInstance().Init(get_default_mmap_config());

    // Initialize ArrowFileSystemSingleton for tests that need it
    InitLocalArrowFileSystemSingleton(testLocalPath.c_str());

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
