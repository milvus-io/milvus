#include "scheduler/ResourceFactory.h"
#include "scheduler/ResourceMgr.h"
#include "scheduler/Scheduler.h"
#include "scheduler/task/TestTask.h"
#include "utils/Log.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

TEST(normal_test, test1) {
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

    auto scheduler = new Scheduler(res_mgr);
    scheduler->Start();

    const uint64_t NUM_TASK = 10;
    std::vector<std::shared_ptr<TestTask>> tasks;
    for (uint64_t i = 0; i < NUM_TASK; ++i) {
        if (auto observe = disk.lock()) {
            auto task = std::make_shared<TestTask>();
            tasks.push_back(task);
            observe->task_table().Put(task);
        }
    }

    if (auto disk_r = disk.lock()) {
        if (auto cpu_r = cpu.lock()) {
            if (auto gpu1_r = gpu1.lock()) {
                if (auto gpu2_r = gpu2.lock()) {
                    std::cout << "<<<<<<<<<<before<<<<<<<<<<" << std::endl;
                    std::cout << "disk:" << std::endl;
                    std::cout << disk_r->task_table().Dump() << std::endl;
                    std::cout << "cpu:" << std::endl;
                    std::cout << cpu_r->task_table().Dump() << std::endl;
                    std::cout << "gpu1:" << std::endl;
                    std::cout << gpu1_r->task_table().Dump() << std::endl;
                    std::cout << "gpu2:" << std::endl;
                    std::cout << gpu2_r->task_table().Dump() << std::endl;
                    std::cout << ">>>>>>>>>>before>>>>>>>>>>" << std::endl;
                }
            }
        }
    }

    sleep(1);

    if (auto disk_r = disk.lock()) {
        if (auto cpu_r = cpu.lock()) {
            if (auto gpu1_r = gpu1.lock()) {
                if (auto gpu2_r = gpu2.lock()) {
                    std::cout << "<<<<<<<<<<after<<<<<<<<<<" << std::endl;
                    std::cout << "disk:" << std::endl;
                    std::cout << disk_r->task_table().Dump() << std::endl;
                    std::cout << "cpu:" << std::endl;
                    std::cout << cpu_r->task_table().Dump() << std::endl;
                    std::cout << "gpu1:" << std::endl;
                    std::cout << gpu1_r->task_table().Dump() << std::endl;
                    std::cout << "gpu2:" << std::endl;
                    std::cout << gpu2_r->task_table().Dump() << std::endl;
                    std::cout << ">>>>>>>>>>after>>>>>>>>>>" << std::endl;
                }
            }
        }
    }
    scheduler->Stop();
    res_mgr->Stop();

    for (uint64_t i = 0 ; i < NUM_TASK; ++i) {
        ASSERT_EQ(tasks[i]->load_count_, 1);
        ASSERT_EQ(tasks[i]->exec_count_, 1);
    }
}
