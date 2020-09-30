// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <fiu-control.h>
#include <fiu/fiu-local.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "cache/CachePlaceholder.h"
#include "cache/CpuCacheMgr.h"

namespace {
using CpuCacheMgr = milvus::cache::CpuCacheMgr;
using CahcePlaceholder = milvus::cache::CachePlaceholder;
using CahcePlaceholderPtr = milvus::cache::CachePlaceholderPtr;
}  // namespace

TEST(CacheTest, CachePlaceholderTest) {
    int64_t cache_size = 4 * 1024 * 1024;
    int64_t capacity = 256 * 1024 * 1024;
    auto& cache_mgr = CpuCacheMgr::GetInstance();
    cache_mgr.SetCapacity(capacity);

    {
        CahcePlaceholderPtr holder = std::make_shared<CahcePlaceholder>(cache_size);
        ASSERT_EQ(cache_mgr.ItemCount(), 1);
        ASSERT_EQ(cache_mgr.CacheUsage(), cache_size);
        auto key = holder->ItemKey();
        ASSERT_FALSE(key.empty());
    }

    ASSERT_EQ(cache_mgr.ItemCount(), 0);
    ASSERT_EQ(cache_mgr.CacheUsage(), 0);

    {
        CahcePlaceholderPtr holder = std::make_shared<CahcePlaceholder>(cache_size);
        holder->Erase();
        ASSERT_EQ(cache_mgr.ItemCount(), 0);
        ASSERT_EQ(cache_mgr.CacheUsage(), 0);
    }
}
