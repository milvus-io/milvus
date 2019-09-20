// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "scheduler/Scheduler.h"
#include <gtest/gtest.h>
#include "src/scheduler/tasklabel/DefaultLabel.h"
#include "cache/DataObj.h"
#include "cache/GpuCacheMgr.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/resource/Resource.h"
#include "utils/Error.h"
#include "src/wrapper/vec_index.h"
#include "scheduler/tasklabel/SpecResLabel.h"


namespace zilliz {
namespace milvus {
namespace engine {

class MockVecIndex : public engine::VecIndex {
public:
    virtual Status BuildAll(const long &nb,
                               const float *xb,
                               const long *ids,
                               const engine::Config &cfg,
                               const long &nt = 0,
                               const float *xt = nullptr) {
    }

    engine::VecIndexPtr Clone() override {
        return zilliz::milvus::engine::VecIndexPtr();
    }

    int64_t GetDeviceId() override {
        return 0;
    }

    engine::IndexType GetType() override {
        return engine::IndexType::INVALID;
    }

    virtual Status Add(const long &nb,
                          const float *xb,
                          const long *ids,
                          const engine::Config &cfg = engine::Config()) {

    }

    virtual Status Search(const long &nq,
                             const float *xq,
                             float *dist,
                             long *ids,
                             const engine::Config &cfg = engine::Config()) {

    }

    engine::VecIndexPtr CopyToGpu(const int64_t &device_id, const engine::Config &cfg) override {

    }

    engine::VecIndexPtr CopyToCpu(const engine::Config &cfg) override {

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

    virtual Status Load(const zilliz::knowhere::BinarySet &index_binary) {

    }

public:
    int64_t dimension_ = 512;
    int64_t ntotal_ = 0;
};


class SchedulerTest : public testing::Test {
protected:
    void
    SetUp() override {
        constexpr int64_t cache_cap = 1024*1024*1024;
        cache::GpuCacheMgr::GetInstance(0)->SetCapacity(cache_cap);
        cache::GpuCacheMgr::GetInstance(1)->SetCapacity(cache_cap);

        ResourcePtr cpu = ResourceFactory::Create("cpu", "CPU", 0, true, false);
        ResourcePtr gpu_0 = ResourceFactory::Create("gpu0", "GPU", 0);
        ResourcePtr gpu_1 = ResourceFactory::Create("gpu1", "GPU", 1);

        res_mgr_ = std::make_shared<ResourceMgr>();
        cpu_resource_ = res_mgr_->Add(std::move(cpu));
        gpu_resource_0_ = res_mgr_->Add(std::move(gpu_0));
        gpu_resource_1_ = res_mgr_->Add(std::move(gpu_1));

        auto PCIE = Connection("IO", 11000.0);
        res_mgr_->Connect("cpu", "gpu0", PCIE);
        res_mgr_->Connect("cpu", "gpu1", PCIE);

        scheduler_ = std::make_shared<Scheduler>(res_mgr_);

        res_mgr_->Start();
        scheduler_->Start();
    }

    void
    TearDown() override {
        scheduler_->Stop();
        res_mgr_->Stop();
    }

    ResourceWPtr cpu_resource_;
    ResourceWPtr gpu_resource_0_;
    ResourceWPtr gpu_resource_1_;

    ResourceMgrPtr res_mgr_;
    std::shared_ptr<Scheduler> scheduler_;
};

void
insert_dummy_index_into_gpu_cache(uint64_t device_id) {
    MockVecIndex *mock_index = new MockVecIndex();
    mock_index->ntotal_ = 1000;
    engine::VecIndexPtr index(mock_index);

    cache::DataObjPtr obj = std::make_shared<cache::DataObj>(index);

    cache::GpuCacheMgr::GetInstance(device_id)->InsertItem("location", obj);
}

TEST_F(SchedulerTest, ON_LOAD_COMPLETED) {
    const uint64_t NUM = 10;
    std::vector<std::shared_ptr<TestTask>> tasks;
    meta::TableFileSchemaPtr dummy = std::make_shared<meta::TableFileSchema>();
    dummy->location_ = "location";

    insert_dummy_index_into_gpu_cache(1);

    for (uint64_t i = 0; i < NUM; ++i) {
        auto task = std::make_shared<TestTask>(dummy);
        task->label() = std::make_shared<DefaultLabel>();
        tasks.push_back(task);
        cpu_resource_.lock()->task_table().Put(task);
    }

    sleep(3);
    ASSERT_EQ(res_mgr_->GetResource(ResourceType::GPU, 1)->task_table().Size(), NUM);

}

TEST_F(SchedulerTest, PUSH_TASK_TO_NEIGHBOUR_RANDOMLY_TEST) {
    const uint64_t NUM = 10;
    std::vector<std::shared_ptr<TestTask>> tasks;
    meta::TableFileSchemaPtr dummy1 = std::make_shared<meta::TableFileSchema>();
    dummy1->location_ = "location";

    tasks.clear();

    for (uint64_t i = 0; i < NUM; ++i) {
        auto task = std::make_shared<TestTask>(dummy1);
        task->label() = std::make_shared<DefaultLabel>();
        tasks.push_back(task);
        cpu_resource_.lock()->task_table().Put(task);
    }

    sleep(3);
//    ASSERT_EQ(res_mgr_->GetResource(ResourceType::GPU, 1)->task_table().Size(), NUM);
}

class SchedulerTest2 : public testing::Test {
protected:
    void
    SetUp() override {
        ResourcePtr disk = ResourceFactory::Create("disk", "DISK", 0, true, false);
        ResourcePtr cpu0 = ResourceFactory::Create("cpu0", "CPU", 0, true, false);
        ResourcePtr cpu1 = ResourceFactory::Create("cpu1", "CPU", 1, true, false);
        ResourcePtr cpu2 = ResourceFactory::Create("cpu2", "CPU", 2, true, false);
        ResourcePtr gpu0 = ResourceFactory::Create("gpu0", "GPU", 0, true, true);
        ResourcePtr gpu1 = ResourceFactory::Create("gpu1", "GPU", 1, true, true);

        res_mgr_ = std::make_shared<ResourceMgr>();
        disk_ = res_mgr_->Add(std::move(disk));
        cpu_0_ = res_mgr_->Add(std::move(cpu0));
        cpu_1_ = res_mgr_->Add(std::move(cpu1));
        cpu_2_ = res_mgr_->Add(std::move(cpu2));
        gpu_0_ = res_mgr_->Add(std::move(gpu0));
        gpu_1_ = res_mgr_->Add(std::move(gpu1));
        auto IO = Connection("IO", 5.0);
        auto PCIE1 = Connection("PCIE", 11.0);
        auto PCIE2 = Connection("PCIE", 20.0);
        res_mgr_->Connect("disk", "cpu0", IO);
        res_mgr_->Connect("cpu0", "cpu1", IO);
        res_mgr_->Connect("cpu1", "cpu2", IO);
        res_mgr_->Connect("cpu0", "cpu2", IO);
        res_mgr_->Connect("cpu1", "gpu0", PCIE1);
        res_mgr_->Connect("cpu2", "gpu1", PCIE2);

        scheduler_ = std::make_shared<Scheduler>(res_mgr_);

        res_mgr_->Start();
        scheduler_->Start();
    }

    void
    TearDown() override {
        scheduler_->Stop();
        res_mgr_->Stop();
    }

    ResourceWPtr disk_;
    ResourceWPtr cpu_0_;
    ResourceWPtr cpu_1_;
    ResourceWPtr cpu_2_;
    ResourceWPtr gpu_0_;
    ResourceWPtr gpu_1_;
    ResourceMgrPtr res_mgr_;

    std::shared_ptr<Scheduler> scheduler_;
};


TEST_F(SchedulerTest2, SPECIFIED_RESOURCE_TEST) {
    const uint64_t NUM = 10;
    std::vector<std::shared_ptr<TestTask>> tasks;
    meta::TableFileSchemaPtr dummy = std::make_shared<meta::TableFileSchema>();
    dummy->location_ = "location";

    for (uint64_t i = 0; i < NUM; ++i) {
        std::shared_ptr<TestTask> task = std::make_shared<TestTask>(dummy);
        task->label() = std::make_shared<SpecResLabel>(disk_);
        tasks.push_back(task);
        disk_.lock()->task_table().Put(task);
    }

//    ASSERT_EQ(res_mgr_->GetResource(ResourceType::GPU, 1)->task_table().Size(), NUM);
}

}
}
}
