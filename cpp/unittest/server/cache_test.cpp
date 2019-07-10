////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"

#include "wrapper/Index.h"
#include "wrapper/knowhere/vec_index.h"

using namespace zilliz::milvus;

namespace {

class InvalidCacheMgr : public cache::CacheMgr {
public:
    InvalidCacheMgr() {
    }
};

class LessItemCacheMgr : public cache::CacheMgr {
public:
    LessItemCacheMgr() {
        cache_ = std::make_shared<cache::Cache>(1UL << 12, 10);
    }
};

class MockVecIndex : public engine::VecIndex {
public:
    virtual void BuildAll(const long &nb,
                          const float *xb,
                          const long *ids,
                          const engine::Config &cfg,
                          const long &nt = 0,
                          const float *xt = nullptr) {

    }

    engine::IndexType GetType() override {
        return engine::IndexType::INVALID;
    }

    virtual void Add(const long &nb,
                     const float *xb,
                     const long *ids,
                     const engine::Config &cfg = engine::Config()) {

    }

    virtual void Search(const long &nq,
                        const float *xq,
                        float *dist,
                        long *ids,
                        const engine::Config &cfg = engine::Config()) {

    }

    virtual int64_t Dimension() {
        return dimension_;
    }

    virtual int64_t Count() {
        return ntotal_;
    }

    virtual zilliz::knowhere::BinarySet Serialize() {
        zilliz::knowhere::BinarySet binset;
        return binset;
    }

    virtual void Load(const zilliz::knowhere::BinarySet &index_binary) {

    }

public:
    int64_t dimension_ = 512;
    int64_t ntotal_ = 0;
};

}

TEST(CacheTest, CPU_CACHE_TEST) {
    cache::CacheMgr *cpu_mgr = cache::CpuCacheMgr::GetInstance();

    const int64_t gbyte = 1 << 30;
    int64_t g_num = 16;
    int64_t cap = g_num * gbyte;
    cpu_mgr->SetCapacity(cap);
    ASSERT_EQ(cpu_mgr->CacheCapacity(), cap);

    const int dim = 256;

    for (int i = 0; i < 20; i++) {
        MockVecIndex* mock_index = new MockVecIndex();
        mock_index->ntotal_ = 1000000;//less 1G per index
        engine::Index_ptr index(mock_index);

        cpu_mgr->InsertItem("index_" + std::to_string(i), index);
    }
    ASSERT_LT(cpu_mgr->ItemCount(), g_num);

    auto obj = cpu_mgr->GetIndex("index_0");
    ASSERT_TRUE(obj == nullptr);

    obj = cpu_mgr->GetIndex("index_19");
    ASSERT_TRUE(obj != nullptr);

    {
        std::string item = "index_15";
        ASSERT_TRUE(cpu_mgr->ItemExists(item));
        cpu_mgr->EraseItem(item);
        ASSERT_FALSE(cpu_mgr->ItemExists(item));
    }

    {
        g_num = 5;
        cpu_mgr->SetCapacity(g_num * gbyte);

        MockVecIndex* mock_index = new MockVecIndex();
        mock_index->ntotal_ = 6000000;//6G less
        engine::Index_ptr index(mock_index);

        cpu_mgr->InsertItem("index_6g", index);
        ASSERT_EQ(cpu_mgr->ItemCount(), 0);//data greater than capacity can not be inserted sucessfully
    }

    cpu_mgr->PrintInfo();
}

TEST(CacheTest, GPU_CACHE_TEST) {
    cache::CacheMgr* gpu_mgr = cache::GpuCacheMgr::GetInstance();

    const int dim = 256;

    for(int i = 0; i < 20; i++) {
        MockVecIndex* mock_index = new MockVecIndex();
        mock_index->ntotal_ = 1000;
        engine::Index_ptr index(mock_index);

        cache::DataObjPtr obj = std::make_shared<cache::DataObj>(index);

        gpu_mgr->InsertItem("index_" + std::to_string(i), obj);
    }

    auto obj = gpu_mgr->GetItem("index_0");

    gpu_mgr->ClearCache();
    ASSERT_EQ(gpu_mgr->ItemCount(), 0);
}

TEST(CacheTest, INVALID_TEST) {
    {
        InvalidCacheMgr mgr;
        ASSERT_EQ(mgr.ItemCount(), 0);
        ASSERT_FALSE(mgr.ItemExists("test"));
        ASSERT_EQ(mgr.GetItem("test"), nullptr);

        mgr.InsertItem("test", cache::DataObjPtr());
        mgr.InsertItem("test", engine::Index_ptr(nullptr));
        mgr.EraseItem("test");
        mgr.PrintInfo();
        mgr.ClearCache();
        mgr.SetCapacity(100);
        ASSERT_EQ(mgr.CacheCapacity(), 0);
        ASSERT_EQ(mgr.CacheUsage(), 0);
    }

    {
        LessItemCacheMgr mgr;
        for(int i = 0; i < 20; i++) {
            MockVecIndex* mock_index = new MockVecIndex();
            mock_index->ntotal_ = 2;
            engine::Index_ptr index(mock_index);

            cache::DataObjPtr obj = std::make_shared<cache::DataObj>(index);
            mgr.InsertItem("index_" + std::to_string(i), obj);
        }
        ASSERT_EQ(mgr.GetItem("index_0"), nullptr);
    }
}