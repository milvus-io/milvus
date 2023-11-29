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
#include <exception>

#include "config/ConfigKnowhere.h"
#include "gtest/gtest-death-test.h"
#include "segcore/segcore_init_c.h"
#include "test_utils/DataGen.h"

TEST(Init, Naive) {
    using namespace milvus;
    using namespace milvus::segcore;
    SegcoreInit(nullptr);
    SegcoreSetChunkRows(32768);
    auto simd_type = SegcoreSetSimdType("auto");
    free(simd_type);
}

TEST(Init, KnowhereThreadPoolInit) {
#ifdef BUILD_DISK_ANN
    try {
        milvus::config::KnowhereInitSearchThreadPool(0);
    } catch (std::exception& e) {
        ASSERT_TRUE(std::string(e.what()).find(
                        "Failed to set aio context pool") != std::string::npos);
    }
#endif
    milvus::config::KnowhereInitSearchThreadPool(8);
}

TEST(Init, KnowhereGPUMemoryPoolInit) {
#ifdef MILVUS_GPU_VERSION
    ASSERT_NO_THROW(milvus::config::KnowhereInitGPUMemoryPool(0, 0));
#endif
}