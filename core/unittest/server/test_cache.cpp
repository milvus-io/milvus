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

#include <gtest/gtest.h>
#include <fiu-control.h>
#include <fiu-local.h>

#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "utils/Error.h"

namespace {

class InvalidCacheMgr : public milvus::cache::CacheMgr<milvus::cache::DataObjPtr> {
 public:
    InvalidCacheMgr() {
    }
};

class LessItemCacheMgr : public milvus::cache::CacheMgr<milvus::cache::DataObjPtr> {
 public:
    LessItemCacheMgr() {
        cache_ = std::make_shared<milvus::cache::Cache<milvus::cache::DataObjPtr>>(1UL << 12, 10);
    }
};

class MockVecIndex : public milvus::knowhere::VecIndex {
 public:
    MockVecIndex(int64_t dim, int64_t total) : dim_(dim), ntotal_(total) {
        int64_t data_size = Dim() * Count() * sizeof(float);
        SetIndexSize(data_size);
    }

    virtual void
    BuildAll(const milvus::knowhere::DatasetPtr& dataset_ptr, const milvus::knowhere::Config& cfg) {
    }

    virtual void
    Train(const milvus::knowhere::DatasetPtr& dataset_ptr, const milvus::knowhere::Config& cfg) {
    }

    virtual void
    Add(const milvus::knowhere::DatasetPtr& dataset, const milvus::knowhere::Config& cfg = milvus::knowhere::Config()) {
    }

    virtual void
    AddWithoutIds(const milvus::knowhere::DatasetPtr& dataset,
                  const milvus::knowhere::Config& cfg = milvus::knowhere::Config()) {
    }

    virtual milvus::knowhere::DatasetPtr
    Query(const milvus::knowhere::DatasetPtr& dataset,
          const milvus::knowhere::Config& cfg = milvus::knowhere::Config()) {
        return nullptr;
    }

    virtual int64_t
    Dim() {
        return dim_;
    }

    virtual int64_t
    Count() {
        return ntotal_;
    }

    milvus::knowhere::IndexType
    index_type() const override {
        return milvus::knowhere::IndexEnum::INVALID;
    }

    milvus::knowhere::BinarySet
    Serialize(const milvus::knowhere::Config& config = milvus::knowhere::Config()) override {
        milvus::knowhere::BinarySet binset;
        return binset;
    }

    virtual void
    Load(const milvus::knowhere::BinarySet& index_binary) {
    }

 public:
    int64_t dim_ = 256;
    int64_t ntotal_ = 0;
};

}  // namespace

TEST(CacheTest, DUMMY_TEST) {
    milvus::knowhere::Config cfg;
    milvus::knowhere::DatasetPtr dataset;
    MockVecIndex mock_index(256, 1000);
    mock_index.Dim();
    mock_index.Count();
    mock_index.Add(dataset, cfg);
    mock_index.BuildAll(dataset, cfg);
    mock_index.Query(dataset, cfg);
    mock_index.index_type();
    milvus::knowhere::BinarySet index_binary;
    mock_index.Load(index_binary);
    mock_index.Serialize(cfg);
}

TEST(CacheTest, CPU_CACHE_TEST) {
    auto cpu_mgr = milvus::cache::CpuCacheMgr::GetInstance();

    const int64_t gbyte = 1024 * 1024 * 1024;
    int64_t g_num = 16;
    int64_t cap = g_num * gbyte;
    cpu_mgr->SetCapacity(cap);
    ASSERT_EQ(cpu_mgr->CacheCapacity(), cap);

    uint64_t item_count = 20;
    for (uint64_t i = 0; i < item_count + 1; i++) {
        // each vector is 1k byte, total size less than 1G
        milvus::knowhere::VecIndexPtr mock_index = std::make_shared<MockVecIndex>(256, 1000000);
        milvus::cache::DataObjPtr data_obj = std::static_pointer_cast<milvus::cache::DataObj>(mock_index);
        cpu_mgr->InsertItem("index_" + std::to_string(i), data_obj);
    }
    ASSERT_LT(cpu_mgr->ItemCount(), g_num);

    //insert null data
    std::shared_ptr<milvus::cache::DataObj> null_data_obj = nullptr;
    cpu_mgr->InsertItem("index_null", null_data_obj);

    auto obj = cpu_mgr->GetIndex("index_0");
    ASSERT_TRUE(obj == nullptr);

    obj = cpu_mgr->GetIndex("index_" + std::to_string(item_count - 1));
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

        // each vector is 1k byte, total size less than 6G
        milvus::knowhere::VecIndexPtr mock_index = std::make_shared<MockVecIndex>(256, 6000000);
        milvus::cache::DataObjPtr data_obj = std::static_pointer_cast<milvus::cache::DataObj>(mock_index);
        cpu_mgr->InsertItem("index_6g", data_obj);
        ASSERT_TRUE(cpu_mgr->ItemExists("index_6g"));

        //insert aleady existed key
        cpu_mgr->InsertItem("index_6g", data_obj);
    }

    cpu_mgr->PrintInfo();

    cpu_mgr->ClearCache();
    ASSERT_EQ(cpu_mgr->ItemCount(), 0);

//    fiu_enable("CpuCacheMgr_CpuCacheMgr_ZeroCpucacheThreshold", 1, nullptr, 0);
//    auto* cpu_cache_mgr = new milvus::cache::CpuCacheMgr();
//    fiu_disable("CpuCacheMgr_CpuCacheMgr_ZeroCpucacheThreshold");
//    delete cpu_cache_mgr;
}

#ifdef MILVUS_GPU_VERSION
TEST(CacheTest, GPU_CACHE_TEST) {
    auto gpu_mgr = milvus::cache::GpuCacheMgr::GetInstance(0);

    for (int i = 0; i < 20; i++) {
        // each vector is 1k byte
        milvus::knowhere::VecIndexPtr mock_index = std::make_shared<MockVecIndex>(256, 1000);
        milvus::cache::DataObjPtr data_obj = std::static_pointer_cast<milvus::cache::DataObj>(mock_index);
        gpu_mgr->InsertItem("index_" + std::to_string(i), data_obj);
    }

    auto obj = gpu_mgr->GetItem("index_0");

    gpu_mgr->ClearCache();
    ASSERT_EQ(gpu_mgr->ItemCount(), 0);

    for (auto i = 0; i < 3; i++) {
        // TODO(myh): use gpu index to mock
        // each vector is 1k byte, total size less than 2G
        milvus::knowhere::VecIndexPtr mock_index = std::make_shared<MockVecIndex>(256, 2000000);
        milvus::cache::DataObjPtr data_obj = std::static_pointer_cast<milvus::cache::DataObj>(mock_index);
        std::cout << data_obj->Size() << std::endl;
        gpu_mgr->InsertItem("index_" + std::to_string(i), data_obj);
    }

    gpu_mgr->ClearCache();
    ASSERT_EQ(gpu_mgr->ItemCount(), 0);

//    fiu_enable("GpuCacheMgr_GpuCacheMgr_ZeroGpucacheThreshold", 1, nullptr, 0);
//    auto* gpu_cache_mgr = new milvus::cache::GpuCacheMgr();
//    fiu_disable("GpuCacheMgr_GpuCacheMgr_ZeroGpucacheThreshold");
//    delete gpu_cache_mgr;
}
#endif

TEST(CacheTest, INVALID_TEST) {
    {
        InvalidCacheMgr mgr;
        ASSERT_EQ(mgr.ItemCount(), 0);
        ASSERT_FALSE(mgr.ItemExists("test"));
        ASSERT_EQ(mgr.GetItem("test"), nullptr);

        mgr.InsertItem("test", milvus::cache::DataObjPtr());
        mgr.InsertItem("test", nullptr);
        mgr.EraseItem("test");
        mgr.PrintInfo();
        mgr.ClearCache();
        mgr.SetCapacity(100);
        ASSERT_EQ(mgr.CacheCapacity(), 0);
        ASSERT_EQ(mgr.CacheUsage(), 0);
    }

    {
        LessItemCacheMgr mgr;
        for (int i = 0; i < 20; i++) {
            // each vector is 1k byte
            milvus::knowhere::VecIndexPtr mock_index = std::make_shared<MockVecIndex>(256, 2);
            milvus::cache::DataObjPtr data_obj = std::static_pointer_cast<milvus::cache::DataObj>(mock_index);
            mgr.InsertItem("index_" + std::to_string(i), data_obj);
        }
        ASSERT_EQ(mgr.GetItem("index_0"), nullptr);
    }
}

TEST(CacheTest, PARTIAL_LRU_TEST) {
    constexpr int MAX_SIZE = 5;
    milvus::cache::LRU<int, int> lru(MAX_SIZE);

    lru.put(0, 2);
    lru.put(0, 3);
    ASSERT_EQ(lru.size(), 1);

    for (int i = 1; i < MAX_SIZE; ++i) {
        lru.put(i, 0);
    }
    ASSERT_EQ(lru.size(), MAX_SIZE);

    lru.put(99, 0);
    ASSERT_EQ(lru.size(), MAX_SIZE);
    ASSERT_TRUE(lru.exists(99));

    ASSERT_ANY_THROW(lru.get(-1));
}
