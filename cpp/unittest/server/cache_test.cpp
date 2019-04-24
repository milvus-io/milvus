////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"

using namespace zilliz::vecwise;

TEST(CacheTest, CACHE_TEST) {
    cache::CacheMgr* cpu_mgr = cache::CpuCacheMgr::GetInstance();
    cache::CacheMgr* gpu_mgr = cache::GpuCacheMgr::GetInstance();

    cpu_mgr->SetCapacity(15);
    ASSERT_EQ(cpu_mgr->CacheCapacity(), 15);

    gpu_mgr->SetCapacity(2);
    ASSERT_EQ(gpu_mgr->CacheCapacity(), 2);
}