#include "scheduler/ResourceFactory.h"
#include "scheduler/ResourceMgr.h"
#include "scheduler/Scheduler.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

TEST(normal_test, DISABLED_test1) {

    // ResourceMgr only compose resources, provide unified event
    auto res_mgr = std::make_shared<ResourceMgr>();
    auto disk = res_mgr->Add(ResourceFactory::Create("disk", "ssd"));
    auto cpu = res_mgr->Add(ResourceFactory::Create("cpu"));
    auto gpu1 = res_mgr->Add(ResourceFactory::Create("gpu"));
    auto gpu2 = res_mgr->Add(ResourceFactory::Create("gpu"));

    auto IO = Connection("IO", 500.0);
    auto PCIE = Connection("IO", 11000.0);
    res_mgr->Connect(disk, cpu, IO);
    res_mgr->Connect(cpu, gpu1, PCIE);
    res_mgr->Connect(cpu, gpu2, PCIE);

    res_mgr->Start();

    auto task1 = std::make_shared<XSearchTask>();
    auto task2 = std::make_shared<XSearchTask>();
    if (auto observe = disk.lock()) {
        observe->task_table().Put(task1);
        observe->task_table().Put(task2);
        observe->task_table().Put(task1);
        observe->task_table().Put(task1);
    }

    auto scheduler = new Scheduler(res_mgr);
    scheduler->Start();

    while (true) sleep(1);
}
