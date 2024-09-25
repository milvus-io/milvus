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
#include "storage/MmapManager.h"
/*
checking register function of mmap chunk manager
*/
TEST(MmapChunkManager, Register) {
    auto mcm =
        milvus::storage::MmapManager::GetInstance().GetMmapChunkManager();
    auto get_descriptor =
        [](int64_t seg_id,
           SegmentType seg_type) -> milvus::storage::MmapChunkDescriptorPtr {
        return std::shared_ptr<milvus::storage::MmapChunkDescriptor>(
            new milvus::storage::MmapChunkDescriptor({seg_id, seg_type}));
    };
    int64_t segment_id = 0x0000456789ABCDEF;
    int64_t flow_segment_id = 0x8000456789ABCDEF;
    mcm->Register(get_descriptor(segment_id, SegmentType::Growing));
    ASSERT_TRUE(
        mcm->HasRegister(get_descriptor(segment_id, SegmentType::Growing)));
    ASSERT_FALSE(
        mcm->HasRegister(get_descriptor(segment_id, SegmentType::Sealed)));
    mcm->Register(get_descriptor(segment_id, SegmentType::Sealed));
    ASSERT_FALSE(mcm->HasRegister(
        get_descriptor(flow_segment_id, SegmentType::Growing)));
    ASSERT_FALSE(
        mcm->HasRegister(get_descriptor(flow_segment_id, SegmentType::Sealed)));

    mcm->UnRegister(get_descriptor(segment_id, SegmentType::Sealed));
    ASSERT_TRUE(
        mcm->HasRegister(get_descriptor(segment_id, SegmentType::Growing)));
    ASSERT_FALSE(
        mcm->HasRegister(get_descriptor(segment_id, SegmentType::Sealed)));
    mcm->UnRegister(get_descriptor(segment_id, SegmentType::Growing));
}