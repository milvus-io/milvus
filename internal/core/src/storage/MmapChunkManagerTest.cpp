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

#include <cstddef>
#include <cstdint>
#include <filesystem>

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "storage/MmapChunkManager.h"
#include "storage/MmapManager.h"

TEST(MmapChunkManager, UsesIndependentRootedFilesystems) {
    namespace fs = std::filesystem;
    auto base = fs::temp_directory_path() / "milvus_mmap_rooted_files";
    auto first_root = base / "first";
    auto second_root = base / "second";
    fs::remove_all(base);

    {
        milvus::storage::MmapChunkManager first(
            milvus::local::FileSystem::Open(first_root), 1 << 20, 4096);
        milvus::storage::MmapChunkManager second(
            milvus::local::FileSystem::Open(second_root), 1 << 20, 4096);
        auto first_descriptor = first.Register();
        auto second_descriptor = second.Register();
        ASSERT_NE(first.Allocate(first_descriptor, 128), nullptr);
        ASSERT_NE(second.Allocate(second_descriptor, 128), nullptr);
        EXPECT_FALSE(fs::is_empty(first_root));
        EXPECT_FALSE(fs::is_empty(second_root));
    }

    EXPECT_TRUE(fs::is_empty(first_root));
    EXPECT_TRUE(fs::is_empty(second_root));
    fs::remove_all(base);
}

/*
checking register function of mmap chunk manager
*/
TEST(MmapChunkManager, Register) {
    auto mcm =
        milvus::storage::MmapManager::GetInstance().GetMmapChunkManager();
    auto segment_descriptor = mcm->Register();
    ASSERT_TRUE(mcm->HasRegister(segment_descriptor));
    ASSERT_NO_THROW(mcm->UnRegister(segment_descriptor));
}

TEST(MmapChunkManager, AllocateKeepsTypedBuffersAligned) {
    auto mcm =
        milvus::storage::MmapManager::GetInstance().GetMmapChunkManager();
    auto segment_descriptor = mcm->Register();

    auto first = mcm->Allocate(segment_descriptor, 1);
    ASSERT_NE(first, nullptr);

    auto typed_buffer = mcm->Allocate(segment_descriptor, sizeof(double));
    ASSERT_NE(typed_buffer, nullptr);
    ASSERT_EQ(
        reinterpret_cast<uintptr_t>(typed_buffer) % alignof(std::max_align_t),
        0);

    ASSERT_NO_THROW(mcm->UnRegister(segment_descriptor));
}
