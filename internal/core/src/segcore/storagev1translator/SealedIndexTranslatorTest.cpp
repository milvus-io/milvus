// Copyright (C) 2019-2026 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <string>

#include "common/Consts.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "segcore/Types.h"
#include "segcore/storagev1translator/SealedIndexTranslator.h"
#include "storage/ThreadPools.h"

namespace milvus::segcore::storagev1translator {
namespace {

LoadIndexInfo
MakeScalarLoadInfo(const IndexType& index_type,
                   int64_t index_size,
                   bool enable_mmap) {
    LoadIndexInfo info{};
    info.segment_id = 1;
    info.field_id = 2;
    info.index_id = 3;
    info.field_type = DataType::INT64;
    info.element_type = DataType::NONE;
    info.enable_mmap = enable_mmap;
    info.index_engine_version = 0;
    info.index_size = index_size;
    info.num_rows = 1'000'000;
    info.index_params = {{index::INDEX_TYPE, index_type},
                         {index::SCALAR_INDEX_ENGINE_VERSION, "3"}};
    info.schema.set_nullable(false);
    info.warmup_policy = "disable";
    return info;
}

index::CreateIndexInfo
MakeScalarIndexInfo(const IndexType& index_type) {
    index::CreateIndexInfo info{};
    info.field_type = DataType::INT64;
    info.index_type = index_type;
    info.scalar_index_engine_version = 3;
    return info;
}

TEST(SealedIndexTranslatorTest, UsesFactoryPeakDiskWithoutDoublingIt) {
    constexpr int64_t kIndexSize = 64 * 1024 * 1024;
    auto load_info =
        MakeScalarLoadInfo(index::RTREE_INDEX_TYPE, kIndexSize, true);
    SealedIndexTranslator translator(
        MakeScalarIndexInfo(index::RTREE_INDEX_TYPE),
        &load_info,
        tracer::TraceContext{},
        storage::FileManagerContext{},
        Config{});

    const auto [final_usage, peak_usage] =
        translator.estimated_loading_usage({0});

    EXPECT_EQ(final_usage.file_bytes, kIndexSize);
    EXPECT_EQ(peak_usage.file_bytes, kIndexSize);
}

TEST(SealedIndexTranslatorTest, CachesLoadEstimateAfterFirstUse) {
    constexpr int64_t kIndexSize = 256 * 1024 * 1024;
    auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::HIGH);
    const auto original_worker_count = pool.GetMaxThreadNum();
    struct PoolSizeGuard {
        ThreadPool& pool;
        size_t original_size;
        ~PoolSizeGuard() {
            pool.Resize(static_cast<int>(original_size));
        }
    } guard{pool, original_worker_count};

    pool.Resize(1);
    auto load_info =
        MakeScalarLoadInfo(index::ASCENDING_SORT, kIndexSize, true);
    SealedIndexTranslator translator(MakeScalarIndexInfo(index::ASCENDING_SORT),
                                     &load_info,
                                     tracer::TraceContext{},
                                     storage::FileManagerContext{},
                                     Config{});

    const auto first_peak =
        translator.estimated_loading_usage({0}).second.memory_bytes;
    pool.Resize(2);
    const auto second_peak =
        translator.estimated_loading_usage({0}).second.memory_bytes;

    const auto legacy_aux_bytes =
        load_info.num_rows * sizeof(int32_t) + (load_info.num_rows + 7) / 8;
    EXPECT_EQ(first_peak, legacy_aux_bytes + DEFAULT_INDEX_FILE_SLICE_SIZE);
    EXPECT_EQ(second_peak, first_peak);
}

}  // namespace
}  // namespace milvus::segcore::storagev1translator
