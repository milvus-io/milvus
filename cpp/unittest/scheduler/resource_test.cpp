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
        disk_resource_ = ResourceFactory::Create("disk");
        cpu_resource_ = ResourceFactory::Create("cpu");
        gpu_resource_ = ResourceFactory::Create("gpu");
        flag_ = false;

        auto subscriber = [&](EventPtr  event) {
            std::unique_lock<std::mutex> lock(mutex_);
            if (event->Type() == EventType::COPY_COMPLETED || event->Type() == EventType::FINISH_TASK) {
                flag_ = true;
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
    Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&] { return flag_; });
    }

    ResourcePtr disk_resource_;
    ResourcePtr cpu_resource_;
    ResourcePtr gpu_resource_;
    bool flag_;
    std::mutex mutex_;
    std::condition_variable cv_;
};


TEST_F(ResourceTest, cpu_resource_test) {
    auto task = std::make_shared<TestTask>();
    cpu_resource_->task_table().Put(task);
    cpu_resource_->WakeupLoader();
    Wait();
    ASSERT_EQ(task->load_count_, 1);
    flag_ = false;
    cpu_resource_->WakeupExecutor();
    Wait();
    ASSERT_EQ(task->exec_count_, 1);
}

TEST_F(ResourceTest, gpu_resource_test) {
    auto task = std::make_shared<TestTask>();
    gpu_resource_->task_table().Put(task);
    gpu_resource_->WakeupLoader();
    Wait();
    ASSERT_EQ(task->load_count_, 1);
    flag_ = false;
    gpu_resource_->WakeupExecutor();
    Wait();
    ASSERT_EQ(task->exec_count_, 1);
}


}
}
}
