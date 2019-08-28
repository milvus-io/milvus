/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "scheduler/Scheduler.h"
#include <gtest/gtest.h>
#include <src/scheduler/tasklabel/DefaultLabel.h>
#include "cache/DataObj.h"
#include "cache/GpuCacheMgr.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/resource/Resource.h"
#include "utils/Error.h"
#include "wrapper/knowhere/vec_index.h"

namespace zilliz {
namespace milvus {
namespace engine {

class MockVecIndex : public engine::VecIndex {
public:
    virtual server::KnowhereError BuildAll(const long &nb,
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

    virtual server::KnowhereError Add(const long &nb,
                                      const float *xb,
                                      const long *ids,
                                      const engine::Config &cfg = engine::Config()) {

    }

    virtual server::KnowhereError Search(const long &nq,
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

    virtual server::KnowhereError Load(const zilliz::knowhere::BinarySet &index_binary) {

    }

public:
    int64_t dimension_ = 512;
    int64_t ntotal_ = 0;
};


class SchedulerTest : public testing::Test {
protected:
    void
    SetUp() override {
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
    uint64_t load_count_ = 0;
    std::mutex load_mutex_;
    std::condition_variable cv_;
};

void
insert_dummy_index_into_gpu_cache(uint64_t device_id) {
    MockVecIndex* mock_index = new MockVecIndex();
    mock_index->ntotal_ = 1000;
    engine::VecIndexPtr index(mock_index);

    cache::DataObjPtr obj = std::make_shared<cache::DataObj>(index);

    cache::GpuCacheMgr::GetInstance(device_id)->InsertItem("location",obj);
}

TEST_F(SchedulerTest, OnCopyCompleted) {
    const uint64_t NUM = 10;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = std::make_shared<meta::TableFileSchema>();
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

}
}
}
