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
#include <fiu-local.h>
#include <fiu-control.h>

#include "src/scheduler/SchedInst.h"
#include "cache/DataObj.h"
#include "cache/GpuCacheMgr.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/Scheduler.h"
#include "scheduler/resource/Resource.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/tasklabel/SpecResLabel.h"
#include "utils/Error.h"

namespace milvus {
namespace scheduler {

//class MockVecIndex : public engine::VecIndex {
// public:
//    virtual Status
//    BuildAll(const int64_t& nb, const float* xb, const int64_t* ids, const engine::Config& cfg, const int64_t& nt = 0,
//             const float* xt = nullptr) {
//    }
//
//    //    engine::VecIndexPtr
//    //    Clone() override {
//    //        return milvus::engine::VecIndexPtr();
//    //    }
//
//    int64_t
//    GetDeviceId() override {
//        return 0;
//    }
//
//    engine::IndexType
//    GetType() const override {
//        return engine::IndexType::INVALID;
//    }
//
//    virtual Status
//    Add(const int64_t& nb, const float* xb, const int64_t* ids, const engine::Config& cfg = engine::Config()) {
//    }
//
//    virtual Status
//    Search(const int64_t& nq, const float* xq, float* dist, int64_t* ids,
//           const engine::Config& cfg = engine::Config()) {
//    }
//
//    engine::VecIndexPtr
//    CopyToGpu(const int64_t& device_id, const engine::Config& cfg) override {
//    }
//
//    engine::VecIndexPtr
//    CopyToCpu(const engine::Config& cfg) override {
//    }
//
//    virtual int64_t
//    Dimension() {
//        return dimension_;
//    }
//
//    virtual int64_t
//    Count() {
//        return ntotal_;
//    }
//
//    virtual knowhere::BinarySet
//    Serialize() {
//        knowhere::BinarySet binset;
//        return binset;
//    }
//
//    virtual Status
//    Load(const knowhere::BinarySet& index_binary) {
//    }
//
// public:
//    int64_t dimension_ = 512;
//    int64_t ntotal_ = 0;
//};

class SchedulerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        res_mgr_ = std::make_shared<ResourceMgr>();
        ResourcePtr disk = ResourceFactory::Create("disk", "DISK", 0, false);
        ResourcePtr cpu = ResourceFactory::Create("cpu", "CPU", 0, true);
        disk_resource_ = res_mgr_->Add(std::move(disk));
        cpu_resource_ = res_mgr_->Add(std::move(cpu));

#ifdef MILVUS_GPU_VERSION
        constexpr int64_t cache_cap = 1024 * 1024 * 1024;
        cache::GpuCacheMgr::GetInstance(0)->SetCapacity(cache_cap);
        cache::GpuCacheMgr::GetInstance(1)->SetCapacity(cache_cap);
        ResourcePtr gpu_0 = ResourceFactory::Create("gpu0", "GPU", 0);
        ResourcePtr gpu_1 = ResourceFactory::Create("gpu1", "GPU", 1);
        gpu_resource_0_ = res_mgr_->Add(std::move(gpu_0));
        gpu_resource_1_ = res_mgr_->Add(std::move(gpu_1));

        auto PCIE = Connection("IO", 11000.0);
        res_mgr_->Connect("cpu", "gpu0", PCIE);
        res_mgr_->Connect("cpu", "gpu1", PCIE);
#endif

        scheduler_ = std::make_shared<Scheduler>(res_mgr_);

        res_mgr_->Start();
        scheduler_->Start();
    }

    void
    TearDown() override {
        scheduler_->Stop();
        res_mgr_->Stop();
    }

    ResourceWPtr disk_resource_;
    ResourceWPtr cpu_resource_;
    ResourceWPtr gpu_resource_0_;
    ResourceWPtr gpu_resource_1_;

    ResourceMgrPtr res_mgr_;
    std::shared_ptr<Scheduler> scheduler_;
};

class SchedulerTest2 : public testing::Test {
 protected:
    void
    SetUp() override {
        ResourcePtr disk = ResourceFactory::Create("disk", "DISK", 0, false);
        ResourcePtr cpu0 = ResourceFactory::Create("cpu0", "CPU", 0, false);
        ResourcePtr cpu1 = ResourceFactory::Create("cpu1", "CPU", 1, false);
        ResourcePtr cpu2 = ResourceFactory::Create("cpu2", "CPU", 2, false);

        res_mgr_ = std::make_shared<ResourceMgr>();
        disk_ = res_mgr_->Add(std::move(disk));
        cpu_0_ = res_mgr_->Add(std::move(cpu0));
        cpu_1_ = res_mgr_->Add(std::move(cpu1));
        cpu_2_ = res_mgr_->Add(std::move(cpu2));

        auto IO = Connection("IO", 5.0);
        auto PCIE1 = Connection("PCIE", 11.0);
        auto PCIE2 = Connection("PCIE", 20.0);
        res_mgr_->Connect("disk", "cpu0", IO);
        res_mgr_->Connect("cpu0", "cpu1", IO);
        res_mgr_->Connect("cpu1", "cpu2", IO);
        res_mgr_->Connect("cpu0", "cpu2", IO);

#ifdef MILVUS_GPU_VERSION
        ResourcePtr gpu0 = ResourceFactory::Create("gpu0", "GPU", 0);
        ResourcePtr gpu1 = ResourceFactory::Create("gpu1", "GPU", 1);
        gpu_0_ = res_mgr_->Add(std::move(gpu0));
        gpu_1_ = res_mgr_->Add(std::move(gpu1));
        res_mgr_->Connect("cpu1", "gpu0", PCIE1);
        res_mgr_->Connect("cpu2", "gpu1", PCIE2);
#endif

        scheduler_ = std::make_shared<Scheduler>(res_mgr_);

        res_mgr_->Start();
        scheduler_->Start();
    }

    void
    TearDown() override {
        scheduler_->Stop();
        res_mgr_->Stop();
        res_mgr_->Clear();
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

TEST_F(SchedulerTest, schedule) {
    scheduler_->Dump();
}

TEST(SchedulerService, service) {
    fiu_enable("load_simple_config_mock", 1, nullptr, 0);
    StartSchedulerService();
    StopSchedulerService();
    fiu_disable("load_simple_config_mock");
}

}  // namespace scheduler
}  // namespace milvus
