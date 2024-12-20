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

#include "folly/init/Init.h"
#include "test_utils/Constants.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "test_utils/storage_test_utils.h"

int
main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::Init follyInit(&argc, &argv, false);

    boost::filesystem::path path = std::string(TestLocalPath);
    boost::filesystem::path indexnode = "indexnode";
    boost::filesystem::path querynode = "querynode";
    milvus::storage::LocalChunkManagerFactory::GetInstance().AddChunkManager(
        milvus::Role::QueryNode, (path / querynode).string());
    milvus::storage::LocalChunkManagerFactory::GetInstance().AddChunkManager(
        milvus::Role::IndexNode, (path / indexnode).string());
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(
        get_default_local_storage_config());
    milvus::storage::MmapManager::GetInstance().Init(get_default_mmap_config());

    return RUN_ALL_TESTS();
}
