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

#include <gtest/gtest.h>
#include "scheduler/ResourceFactory.h"
#include "scheduler/ResourceMgr.h"
#include "scheduler/SchedInst.h"
#include "scheduler/Scheduler.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/tasklabel/DefaultLabel.h"
#include "utils/Log.h"

namespace {

namespace ms = milvus::scheduler;

}  // namespace

TEST(NormalTest, INST_TEST) {
    // ResourceMgr only compose resources, provide unified event
    auto res_mgr = ms::ResMgrInst::GetInstance();

    res_mgr->Add(ms::ResourceFactory::Create("disk", "DISK", 0, true, false));
    res_mgr->Add(ms::ResourceFactory::Create("cpu", "CPU", 0, true, true));

    auto IO = ms::Connection("IO", 500.0);
    res_mgr->Connect("disk", "cpu", IO);

    auto scheduler = ms::SchedInst::GetInstance();

    res_mgr->Start();
    scheduler->Start();

    const uint64_t NUM_TASK = 2;
    std::vector<std::shared_ptr<ms::TestTask>> tasks;
    ms::TableFileSchemaPtr dummy = nullptr;

    auto disks = res_mgr->GetDiskResources();
    ASSERT_FALSE(disks.empty());
    if (auto observe = disks[0].lock()) {
        for (uint64_t i = 0; i < NUM_TASK; ++i) {
            auto label = std::make_shared<ms::DefaultLabel>();
            auto task = std::make_shared<ms::TestTask>(dummy, label);
            task->label() = std::make_shared<ms::DefaultLabel>();
            tasks.push_back(task);
            observe->task_table().Put(task);
        }
    }

    for (auto& task : tasks) {
        task->Wait();
        ASSERT_EQ(task->load_count_, 1);
        ASSERT_EQ(task->exec_count_, 1);
    }

    scheduler->Stop();
    res_mgr->Stop();
}
