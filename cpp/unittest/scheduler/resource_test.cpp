/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "scheduler/resource/Resource.h"
#include "scheduler/resource/DiskResource.h"
#include "scheduler/resource/CpuResource.h"
#include "scheduler/resource/GpuResource.h"
#include "scheduler/task/Task.h"
#include "scheduler/task/TestTask.h"
#include "scheduler/ResourceFactory.h"
#include <gtest/gtest.h>


namespace zilliz {
namespace milvus {
namespace engine {

class ResourceTest : public testing::Test {
protected:
    void
    SetUp() override {
        disk_resource_ = ResourceFactory::Create("ssd", "DISK", 0);
        cpu_resource_ = ResourceFactory::Create("cpu", "CPU", 0);
        gpu_resource_ = ResourceFactory::Create("gpu", "GPU", 0);
        resources_.push_back(disk_resource_);
        resources_.push_back(cpu_resource_);
        resources_.push_back(gpu_resource_);

        auto subscriber = [&](EventPtr event) {
            if (event->Type() == EventType::COPY_COMPLETED) {
                std::lock_guard<std::mutex> lock(load_mutex_);
                ++load_count_;
                cv_.notify_one();
            }

            if (event->Type() == EventType::FINISH_TASK) {
                std::lock_guard<std::mutex> lock(load_mutex_);
                ++exec_count_;
                cv_.notify_one();
            }
        };

        disk_resource_->RegisterSubscriber(subscriber);
        cpu_resource_->RegisterSubscriber(subscriber);
        gpu_resource_->RegisterSubscriber(subscriber);

        disk_resource_->Start();
        cpu_resource_->Start();
        gpu_resource_->Start();
    }

    void
    TearDown() override {
        disk_resource_->Stop();
        cpu_resource_->Stop();
        gpu_resource_->Stop();
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
    std::vector<ResourcePtr> resources_;
    uint64_t load_count_ = 0;
    uint64_t exec_count_ = 0;
    std::mutex load_mutex_;
    std::mutex exec_mutex_;
    std::condition_variable cv_;
};

TEST_F(ResourceTest, cpu_resource_test) {
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
//    std::cout << "after WakeupLoader" << std::endl;
//    std::cout << cpu_resource_->task_table().Dump();

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->load_count_, 1);
    }

    cpu_resource_->WakeupExecutor();
    WaitExecutor(NUM);
//    std::cout << "after WakeupExecutor" << std::endl;
//    std::cout << cpu_resource_->task_table().Dump();

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->exec_count_, 1);
    }
}

TEST_F(ResourceTest, gpu_resource_test) {
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
//    std::cout << "after WakeupLoader" << std::endl;
//    std::cout << cpu_resource_->task_table().Dump();

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->load_count_, 1);
    }

    gpu_resource_->WakeupExecutor();
    WaitExecutor(NUM);
//    std::cout << "after WakeupExecutor" << std::endl;
//    std::cout << cpu_resource_->task_table().Dump();

    for (uint64_t i = 0; i < NUM; ++i) {
        ASSERT_EQ(tasks[i]->exec_count_, 1);
    }
}


}
}
}
