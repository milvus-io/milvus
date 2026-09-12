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

#include <gtest/gtest.h>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "storage/MmapChunkManager.h"
#include "storage/MmapManager.h"

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

TEST(MmapChunkManager, ConcurrentRegistrationsRemainIndependent) {
    auto mcm =
        milvus::storage::MmapManager::GetInstance().GetMmapChunkManager();
    using Descriptor = milvus::storage::MmapChunkDescriptorPtr;
    std::vector<std::future<std::vector<Descriptor>>> futures;
    for (int worker = 0; worker < 8; ++worker) {
        futures.push_back(std::async(std::launch::async, [mcm] {
            std::vector<Descriptor> descriptors;
            for (int i = 0; i < 16; ++i) {
                descriptors.push_back(mcm->Register());
            }
            return descriptors;
        }));
    }
    std::vector<Descriptor> descriptors;
    for (auto& future : futures) {
        auto registered = future.get();
        descriptors.insert(
            descriptors.end(), registered.begin(), registered.end());
    }

    for (size_t i = 0; i < descriptors.size(); ++i) {
        ASSERT_TRUE(mcm->HasRegister(descriptors[i]));
        mcm->UnRegister(descriptors[i]);
        ASSERT_FALSE(mcm->HasRegister(descriptors[i]));
        for (size_t j = i + 1; j < descriptors.size(); ++j) {
            ASSERT_TRUE(mcm->HasRegister(descriptors[j]));
        }
    }
}
