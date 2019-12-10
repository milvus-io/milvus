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
#include "scheduler/resource/CpuResource.h"
#include "scheduler/resource/DiskResource.h"
#include "scheduler/resource/GpuResource.h"
#include "scheduler/resource/Resource.h"
#include "scheduler/resource/TestResource.h"
#include "scheduler/task/Task.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/tasklabel/SpecResLabel.h"

namespace milvus {
namespace scheduler {

constexpr uint64_t max_once_load = 2;

/************ ResourceBaseTest ************/
class ResourceBaseTest : public testing::Test {
 protected:
    void
    SetUp() override {
        enable_executor_ = std::make_shared<CpuResource>(name1, id1, true);
        disable_executor_ = std::make_shared<GpuResource>(name2, id2, false);
    }

    const std::string name1 = "enable_executor_";
    const std::string name2 = "disable_executor_";

    const uint64_t id1 = 1;
    const uint64_t id2 = 2;

    ResourcePtr enable_executor_ = nullptr;
    ResourcePtr disable_executor_ = nullptr;
};

TEST_F(ResourceBaseTest, NAME) {
    ASSERT_EQ(enable_executor_->name(), name1);
    ASSERT_EQ(disable_executor_->name(), name2);
}

TEST_F(ResourceBaseTest, TYPE) {
    ASSERT_EQ(enable_executor_->type(), ResourceType::CPU);
    ASSERT_EQ(disable_executor_->type(), ResourceType::GPU);
}

TEST_F(ResourceBaseTest, DEVICE_ID) {
    ASSERT_EQ(enable_executor_->device_id(), id1);
    ASSERT_EQ(disable_executor_->device_id(), id2);
}

TEST_F(ResourceBaseTest, HAS_EXECUTOR) {
    ASSERT_TRUE(enable_executor_->HasExecutor());
    ASSERT_FALSE(disable_executor_->HasExecutor());
}

TEST_F(ResourceBaseTest, DUMP) {
    ASSERT_FALSE(enable_executor_->Dump().empty());
    ASSERT_FALSE(disable_executor_->Dump().empty());
    std::cout << *enable_executor_ << *disable_executor_;
}

/************ ResourceAdvanceTest ************/

class ResourceAdvanceTest : public testing::Test {
 protected:
    void
    SetUp() override {
        disk_resource_ = ResourceFactory::Create("ssd", "DISK", 0);
        cpu_resource_ = ResourceFactory::Create("cpu", "CPU", 0);
        gpu_resource_ = ResourceFactory::Create("gpu", "GPU", 0);
        test_resource_ = std::make_shared<TestResource>("test", 0, true);
        resources_.push_back(disk_resource_);
        resources_.push_back(cpu_resource_);
        resources_.push_back(gpu_resource_);
        resources_.push_back(test_resource_);

        auto subscriber = [&](EventPtr event) {
            if (event->Type() == EventType::LOAD_COMPLETED) {
                {
                    std::lock_guard<std::mutex> lock(load_mutex_);
                    ++load_count_;
                }
                cv_.notify_one();
            }

            if (event->Type() == EventType::FINISH_TASK) {
                {
                    std::lock_guard<std::mutex> lock(load_mutex_);
                    ++exec_count_;
                }
                cv_.notify_one();
            }
        };

        disk_resource_->RegisterSubscriber(subscriber);
        cpu_resource_->RegisterSubscriber(subscriber);
        gpu_resource_->RegisterSubscriber(subscriber);
        test_resource_->RegisterSubscriber(subscriber);

        disk_resource_->Start();
        cpu_resource_->Start();
        gpu_resource_->Start();
        test_resource_->Start();
    }

    void
    TearDown() override {
        disk_resource_->Stop();
        cpu_resource_->Stop();
        gpu_resource_->Stop();
        test_resource_->Stop();
    }

    void
    WaitLoader(uint64_t count) {
        std::unique_lock<std::mutex> lock(load_mutex_);
        cv_.wait(lock, [&] { return load_count_ == count; });
    }

    void
    WaitExecutor(uint64_t count) {
        std::unique_lock<std::mutex> lock(exec_mutex_);
        cv_.wait(lock, [&] { return exec_count_ == count; });
    }

    ResourcePtr disk_resource_;
    ResourcePtr cpu_resource_;
    ResourcePtr gpu_resource_;
    ResourcePtr test_resource_;
    std::vector<ResourcePtr> resources_;
    uint64_t load_count_ = 0;
    uint64_t exec_count_ = 0;
    std::mutex load_mutex_;
    std::mutex exec_mutex_;
    std::condition_variable cv_;
};

TEST_F(ResourceAdvanceTest, DISK_RESOURCE_TEST) {
    const uint64_t NUM = max_once_load;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;
    for (uint64_t i = 0; i < NUM; ++i) {
        auto label = std::make_shared<SpecResLabel>(disk_resource_);
        auto task = std::make_shared<TestTask>(std::make_shared<server::Context>("dummy_request_id"), dummy, label);
        std::vector<std::string> path{disk_resource_->name()};
        task->path() = Path(path, 0);
        tasks.push_back(task);
        disk_resource_->task_table().Put(task);
    }

    disk_resource_->WakeupLoader();
    WaitLoader(NUM);

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->load_count_, 0);
    }

    disk_resource_->WakeupExecutor();
    WaitExecutor(NUM);

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->exec_count_, 0);
    }
}

TEST_F(ResourceAdvanceTest, CPU_RESOURCE_TEST) {
    const uint64_t NUM = max_once_load;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;
    for (uint64_t i = 0; i < NUM; ++i) {
        auto label = std::make_shared<SpecResLabel>(cpu_resource_);
        auto task = std::make_shared<TestTask>(std::make_shared<server::Context>("dummy_request_id"), dummy, label);
        std::vector<std::string> path{cpu_resource_->name()};
        task->path() = Path(path, 0);
        tasks.push_back(task);
        cpu_resource_->task_table().Put(task);
    }

    cpu_resource_->WakeupLoader();
    WaitLoader(NUM);

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->load_count_, 1);
    }

    cpu_resource_->WakeupExecutor();
    WaitExecutor(NUM);

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->exec_count_, 1);
    }
}

TEST_F(ResourceAdvanceTest, GPU_RESOURCE_TEST) {
    const uint64_t NUM = max_once_load;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;
    for (uint64_t i = 0; i < NUM; ++i) {
        auto label = std::make_shared<SpecResLabel>(gpu_resource_);
        auto task = std::make_shared<TestTask>(std::make_shared<server::Context>("dummy_request_id"), dummy, label);
        std::vector<std::string> path{gpu_resource_->name()};
        task->path() = Path(path, 0);
        tasks.push_back(task);
        gpu_resource_->task_table().Put(task);
    }

    gpu_resource_->WakeupLoader();
    WaitLoader(NUM);

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->load_count_, 1);
    }

    gpu_resource_->WakeupExecutor();
    WaitExecutor(NUM);

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->exec_count_, 1);
    }
}

TEST_F(ResourceAdvanceTest, TEST_RESOURCE_TEST) {
    const uint64_t NUM = max_once_load;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;
    for (uint64_t i = 0; i < NUM; ++i) {
        auto label = std::make_shared<SpecResLabel>(test_resource_);
        auto task = std::make_shared<TestTask>(std::make_shared<server::Context>("dummy_request_id"), dummy, label);
        std::vector<std::string> path{test_resource_->name()};
        task->path() = Path(path, 0);
        tasks.push_back(task);
        test_resource_->task_table().Put(task);
    }

    test_resource_->WakeupLoader();
    WaitLoader(NUM);

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->load_count_, 1);
    }

    test_resource_->WakeupExecutor();
    WaitExecutor(NUM);

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->exec_count_, 1);
    }
}

}  // namespace scheduler
}  // namespace milvus
