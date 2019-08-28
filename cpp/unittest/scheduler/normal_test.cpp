#include "scheduler/ResourceFactory.h"
#include "scheduler/ResourceMgr.h"
#include "scheduler/Scheduler.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/SchedInst.h"
#include "utils/Log.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

TEST(normal_test, test1) {
    // ResourceMgr only compose resources, provide unified event
//    auto res_mgr = std::make_shared<ResourceMgr>();
    auto res_mgr = ResMgrInst::GetInstance();
    auto disk = res_mgr->Add(ResourceFactory::Create("disk", "ssd", true, false));
    auto cpu = res_mgr->Add(ResourceFactory::Create("cpu", "CPU", 0));
    auto gpu1 = res_mgr->Add(ResourceFactory::Create("gpu", "gpu0", false, false));
    auto gpu2 = res_mgr->Add(ResourceFactory::Create("gpu", "gpu2", false, false));

    auto IO = Connection("IO", 500.0);
    auto PCIE = Connection("IO", 11000.0);
    res_mgr->Connect(disk, cpu, IO);
    res_mgr->Connect(cpu, gpu1, PCIE);
    res_mgr->Connect(cpu, gpu2, PCIE);

    res_mgr->Start();

//    auto scheduler = new Scheduler(res_mgr);
    auto scheduler = SchedInst::GetInstance();
    scheduler->Start();

    const uint64_t NUM_TASK = 1000;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;

    for (uint64_t i = 0; i < NUM_TASK; ++i) {
        if (auto observe = disk.lock()) {
            auto task = std::make_shared<TestTask>(dummy);
            tasks.push_back(task);
            observe->task_table().Put(task);
        }
    }

    sleep(1);

    scheduler->Stop();
    res_mgr->Stop();

    auto pcpu = cpu.lock();
    for (uint64_t i = 0; i < NUM_TASK; ++i) {
        auto task = std::static_pointer_cast<TestTask>(pcpu->task_table()[i]->task);
        ASSERT_EQ(task->load_count_, 1);
        ASSERT_EQ(task->exec_count_, 1);
    }
}
