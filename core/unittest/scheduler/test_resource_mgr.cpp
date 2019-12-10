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

#include "scheduler/ResourceMgr.h"
#include "scheduler/resource/CpuResource.h"
#include "scheduler/resource/DiskResource.h"
#include "scheduler/resource/GpuResource.h"
#include "scheduler/resource/TestResource.h"
#include "scheduler/task/TestTask.h"

namespace milvus {
namespace scheduler {

/************ ResourceMgrBaseTest ************/
class ResourceMgrBaseTest : public testing::Test {
 protected:
    void
    SetUp() override {
        empty_mgr_ = std::make_shared<ResourceMgr>();
        mgr1_ = std::make_shared<ResourceMgr>();
        disk_res = std::make_shared<DiskResource>("disk", 0, false);
        cpu_res = std::make_shared<CpuResource>("cpu", 1, false);
        gpu_res = std::make_shared<GpuResource>("gpu", 2, true);
        mgr1_->Add(ResourcePtr(disk_res));
        mgr1_->Add(ResourcePtr(cpu_res));
        mgr1_->Add(ResourcePtr(gpu_res));
    }

    void
    TearDown() override {
    }

    ResourceMgrPtr empty_mgr_;
    ResourceMgrPtr mgr1_;
    ResourcePtr disk_res;
    ResourcePtr cpu_res;
    ResourcePtr gpu_res;
};

TEST_F(ResourceMgrBaseTest, ADD) {
    auto resource = std::make_shared<TestResource>("test", 0, true);
    auto ret = empty_mgr_->Add(ResourcePtr(resource));
    ASSERT_EQ(ret.lock(), resource);
}

TEST_F(ResourceMgrBaseTest, ADD_DISK) {
    auto resource = std::make_shared<DiskResource>("disk", 0, true);
    auto ret = empty_mgr_->Add(ResourcePtr(resource));
    ASSERT_EQ(ret.lock(), resource);
}

TEST_F(ResourceMgrBaseTest, CONNECT) {
    auto resource1 = std::make_shared<TestResource>("resource1", 0, true);
    auto resource2 = std::make_shared<TestResource>("resource2", 2, true);
    empty_mgr_->Add(resource1);
    empty_mgr_->Add(resource2);
    Connection io("io", 500.0);
    ASSERT_TRUE(empty_mgr_->Connect("resource1", "resource2", io));
}

TEST_F(ResourceMgrBaseTest, INVALID_CONNECT) {
    auto resource1 = std::make_shared<TestResource>("resource1", 0, true);
    auto resource2 = std::make_shared<TestResource>("resource2", 2, true);
    empty_mgr_->Add(resource1);
    empty_mgr_->Add(resource2);
    Connection io("io", 500.0);
    ASSERT_FALSE(empty_mgr_->Connect("xx", "yy", io));
}

TEST_F(ResourceMgrBaseTest, CLEAR) {
    ASSERT_EQ(mgr1_->GetNumOfResource(), 3);
    mgr1_->Clear();
    ASSERT_EQ(mgr1_->GetNumOfResource(), 0);
}

TEST_F(ResourceMgrBaseTest, GET_DISK_RESOURCES) {
    auto disks = mgr1_->GetDiskResources();
    ASSERT_EQ(disks.size(), 1);
    ASSERT_EQ(disks[0].lock(), disk_res);
}

TEST_F(ResourceMgrBaseTest, GET_ALL_RESOURCES) {
    bool disk = false, cpu = false, gpu = false;
    auto resources = mgr1_->GetAllResources();
    ASSERT_EQ(resources.size(), 3);
    for (auto& res : resources) {
        if (res->type() == ResourceType::DISK)
            disk = true;
        if (res->type() == ResourceType::CPU)
            cpu = true;
        if (res->type() == ResourceType::GPU)
            gpu = true;
    }

    ASSERT_TRUE(disk);
    ASSERT_TRUE(cpu);
    ASSERT_TRUE(gpu);
}

TEST_F(ResourceMgrBaseTest, GET_COMPUTE_RESOURCES) {
    auto compute_resources = mgr1_->GetComputeResources();
    ASSERT_EQ(compute_resources.size(), 1);
    ASSERT_EQ(compute_resources[0], gpu_res);
}

TEST_F(ResourceMgrBaseTest, GET_RESOURCE_BY_TYPE_AND_DEVICEID) {
    auto cpu = mgr1_->GetResource(ResourceType::CPU, 1);
    ASSERT_EQ(cpu, cpu_res);

    auto invalid = mgr1_->GetResource(ResourceType::GPU, 1);
    ASSERT_EQ(invalid, nullptr);
}

TEST_F(ResourceMgrBaseTest, GET_RESOURCE_BY_NAME) {
    auto disk = mgr1_->GetResource("disk");
    ASSERT_EQ(disk, disk_res);

    auto invalid = mgr1_->GetResource("invalid");
    ASSERT_EQ(invalid, nullptr);
}

TEST_F(ResourceMgrBaseTest, GET_NUM_OF_RESOURCE) {
    ASSERT_EQ(empty_mgr_->GetNumOfResource(), 0);
    ASSERT_EQ(mgr1_->GetNumOfResource(), 3);
}

TEST_F(ResourceMgrBaseTest, GET_NUM_OF_COMPUTE_RESOURCE) {
    ASSERT_EQ(empty_mgr_->GetNumOfComputeResource(), 0);
    ASSERT_EQ(mgr1_->GetNumOfComputeResource(), 1);
}

TEST_F(ResourceMgrBaseTest, GET_NUM_OF_GPU_RESOURCE) {
    ASSERT_EQ(empty_mgr_->GetNumGpuResource(), 0);
    ASSERT_EQ(mgr1_->GetNumGpuResource(), 1);
}

TEST_F(ResourceMgrBaseTest, DUMP) {
    ASSERT_FALSE(mgr1_->Dump().empty());
}

TEST_F(ResourceMgrBaseTest, DUMP_TASKTABLES) {
    ASSERT_FALSE(mgr1_->DumpTaskTables().empty());
}

/************ ResourceMgrAdvanceTest ************/

class ResourceMgrAdvanceTest : public testing::Test {
 protected:
    void
    SetUp() override {
        mgr1_ = std::make_shared<ResourceMgr>();
        disk_res = std::make_shared<DiskResource>("disk", 0, false);
        cpu_res = std::make_shared<CpuResource>("cpu", 0, true);
        mgr1_->Add(ResourcePtr(disk_res));
        mgr1_->Add(ResourcePtr(cpu_res));
        mgr1_->Start();
    }

    void
    TearDown() override {
        mgr1_->Stop();
    }

    ResourceMgrPtr mgr1_;
    ResourcePtr disk_res;
    ResourcePtr cpu_res;
};

TEST_F(ResourceMgrAdvanceTest, REGISTER_SUBSCRIBER) {
    bool flag = false;
    auto callback = [&](EventPtr event) { flag = true; };
    mgr1_->RegisterSubscriber(callback);
    TableFileSchemaPtr dummy = nullptr;
    disk_res->task_table().Put(
        std::make_shared<TestTask>(std::make_shared<server::Context>("dummy_request_id"), dummy, nullptr));
    sleep(1);
    ASSERT_TRUE(flag);
}

}  // namespace scheduler
}  // namespace milvus
