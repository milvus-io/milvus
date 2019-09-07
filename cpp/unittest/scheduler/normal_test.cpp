#include "scheduler/ResourceFactory.h"
#include "scheduler/ResourceMgr.h"
#include "scheduler/Scheduler.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/tasklabel/DefaultLabel.h"
#include "scheduler/SchedInst.h"
#include "utils/Log.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;


TEST(normal_test, inst_test) {
    // ResourceMgr only compose resources, provide unified event
    auto res_mgr = ResMgrInst::GetInstance();

    res_mgr->Add(ResourceFactory::Create("disk", "DISK", 0, true, false));
    res_mgr->Add(ResourceFactory::Create("cpu", "CPU", 0, true, true));

    auto IO = Connection("IO", 500.0);
    res_mgr->Connect("disk", "cpu", IO);

    auto scheduler = SchedInst::GetInstance();

    res_mgr->Start();
    scheduler->Start();

    const uint64_t NUM_TASK = 1000;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;

    auto disks = res_mgr->GetDiskResources();
    ASSERT_FALSE(disks.empty());
    if (auto observe = disks[0].lock()) {
        for (uint64_t i = 0; i < NUM_TASK; ++i) {
            auto task = std::make_shared<TestTask>(dummy);
            task->label() = std::make_shared<DefaultLabel>();
            tasks.push_back(task);
            observe->task_table().Put(task);
        }
    }

    for (auto &task : tasks) {
        task->Wait();
        ASSERT_EQ(task->load_count_, 1);
        ASSERT_EQ(task->exec_count_, 1);
    }

    scheduler->Stop();
    res_mgr->Stop();

}
