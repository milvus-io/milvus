////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"

#include "wrapper/Index.h"

using namespace zilliz::vecwise;

TEST(CacheTest, CACHE_TEST) {
    cache::CacheMgr* cpu_mgr = cache::CpuCacheMgr::GetInstance();

    const int64_t gbyte = 1 << 30;
    int64_t g_num = 16;
    int64_t cap = g_num*gbyte;
    cpu_mgr->SetCapacity(cap);
    ASSERT_EQ(cpu_mgr->CacheCapacity(), cap);

    const int dim = 256;

    for(int i = 0; i < 20; i++) {
        std::shared_ptr<faiss::Index> raw_index(faiss::index_factory(dim, "IDMap,Flat"));
        engine::Index_ptr index = std::make_shared<engine::Index>(raw_index);
        index->ntotal = 1000000;//less 1G per index

        cpu_mgr->InsertItem("index_" + std::to_string(i), index);
    }
    ASSERT_LT(cpu_mgr->ItemCount(), g_num);

    {
        std::string item = "index_15";
        ASSERT_TRUE(cpu_mgr->ItemExists(item));
        cpu_mgr->EraseItem(item);
        ASSERT_FALSE(cpu_mgr->ItemExists(item));
    }

    {
        g_num = 5;
        cpu_mgr->SetCapacity(g_num * gbyte);

        std::shared_ptr<faiss::Index> raw_index(faiss::index_factory(dim, "IDMap,Flat"));
        engine::Index_ptr index = std::make_shared<engine::Index>(raw_index);
        index->ntotal = 6000000;//6G less

        cpu_mgr->InsertItem("index_6g", index);
        ASSERT_EQ(cpu_mgr->ItemCount(), 0);//data greater than capacity can not be inserted sucessfully
    }
}