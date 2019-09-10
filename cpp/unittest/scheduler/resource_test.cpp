/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "scheduler/resource/Resource.h"
#include "scheduler/resource/DiskResource.h"
#include "scheduler/resource/CpuResource.h"
#include "scheduler/resource/GpuResource.h"
#include "scheduler/resource/TestResource.h"
#include "scheduler/task/Task.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/ResourceFactory.h"
#include <gtest/gtest.h>


namespace zilliz {
namespace milvus {
namespace engine {

/************ ResourceBaseTest ************/
class ResourceBaseTest : public testing::Test {
protected:
    void
    SetUp() override {
        only_loader_  = std::make_shared<DiskResource>(name1, id1, true, false);
        only_executor_  = std::make_shared<CpuResource>(name2, id2, false, true);
        both_enable_  = std::make_shared<GpuResource>(name3, id3, true, true);
        both_disable_  = std::make_shared<TestResource>(name4, id4, false, false);
    }

    const std::string name1 = "only_loader_";
    const std::string name2 = "only_executor_";
    const std::string name3 = "both_enable_";
    const std::string name4 = "both_disable_";

    const uint64_t id1 = 1;
    const uint64_t id2 = 2;
    const uint64_t id3 = 3;
    const uint64_t id4 = 4;

    ResourcePtr only_loader_ = nullptr;
    ResourcePtr only_executor_ = nullptr;
    ResourcePtr both_enable_ = nullptr;
    ResourcePtr both_disable_ = nullptr;
};

TEST_F(ResourceBaseTest, name) {
    ASSERT_EQ(only_loader_->name(), name1);
    ASSERT_EQ(only_executor_->name(), name2);
    ASSERT_EQ(both_enable_->name(), name3);
    ASSERT_EQ(both_disable_->name(), name4);
}

TEST_F(ResourceBaseTest, type) {
    ASSERT_EQ(only_loader_->type(), ResourceType::DISK);
    ASSERT_EQ(only_executor_->type(), ResourceType::CPU);
    ASSERT_EQ(both_enable_->type(), ResourceType::GPU);
    ASSERT_EQ(both_disable_->type(), ResourceType::TEST);
}

TEST_F(ResourceBaseTest, device_id) {
    ASSERT_EQ(only_loader_->device_id(), id1);
    ASSERT_EQ(only_executor_->device_id(), id2);
    ASSERT_EQ(both_enable_->device_id(), id3);
    ASSERT_EQ(both_disable_->device_id(), id4);
}

TEST_F(ResourceBaseTest, has_loader) {
    ASSERT_TRUE(only_loader_->HasLoader());
    ASSERT_FALSE(only_executor_->HasLoader());
    ASSERT_TRUE(both_enable_->HasLoader());
    ASSERT_FALSE(both_disable_->HasLoader());
}

TEST_F(ResourceBaseTest, has_executor) {
    ASSERT_FALSE(only_loader_->HasExecutor());
    ASSERT_TRUE(only_executor_->HasExecutor());
    ASSERT_TRUE(both_enable_->HasExecutor());
    ASSERT_FALSE(both_disable_->HasExecutor());
}

TEST_F(ResourceBaseTest, dump) {
    ASSERT_FALSE(only_loader_->Dump().empty());
    ASSERT_FALSE(only_executor_->Dump().empty());
    ASSERT_FALSE(both_enable_->Dump().empty());
    ASSERT_FALSE(both_disable_->Dump().empty());
    std::cout << *only_loader_ << *only_executor_ << *both_enable_ << *both_disable_;
}

/************ ResourceAdvanceTest ************/

class ResourceAdvanceTest : public testing::Test {
protected:
    void
    SetUp() override {
        disk_resource_ = ResourceFactory::Create("ssd", "DISK", 0);
        cpu_resource_ = ResourceFactory::Create("cpu", "CPU", 0);
        gpu_resource_ = ResourceFactory::Create("gpu", "GPU", 0);
        test_resource_ = std::make_shared<TestResource>("test", 0, true, true);
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

TEST_F(ResourceAdvanceTest, disk_resource_test) {
    const uint64_t NUM = 100;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;
    for (uint64_t i = 0; i < NUM; ++i) {
        auto task = std::make_shared<TestTask>(dummy);
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

TEST_F(ResourceAdvanceTest, cpu_resource_test) {
    const uint64_t NUM = 100;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;
    for (uint64_t i = 0; i < NUM; ++i) {
        auto task = std::make_shared<TestTask>(dummy);
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

TEST_F(ResourceAdvanceTest, gpu_resource_test) {
    const uint64_t NUM = 100;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;
    for (uint64_t i = 0; i < NUM; ++i) {
        auto task = std::make_shared<TestTask>(dummy);
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

TEST_F(ResourceAdvanceTest, test_resource_test) {
    const uint64_t NUM = 100;
    std::vector<std::shared_ptr<TestTask>> tasks;
    TableFileSchemaPtr dummy = nullptr;
    for (uint64_t i = 0; i < NUM; ++i) {
        auto task = std::make_shared<TestTask>(dummy);
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

}
}
}
