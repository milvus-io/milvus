// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <iostream>
#include <ostream>

#include "scheduler/Algorithm.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/ResourceMgr.h"
#include "scheduler/resource/CpuResource.h"
#include "scheduler/resource/Resource.h"

namespace milvus {
namespace scheduler {

class AlgorithmTest : public testing::Test {
 protected:
    void
    SetUp() override {
        ResourcePtr disk = ResourceFactory::Create("disk", "DISK", 0, false);
        ResourcePtr cpu0 = ResourceFactory::Create("cpu0", "CPU", 0);
        ResourcePtr cpu1 = ResourceFactory::Create("cpu1", "CPU", 1);
        ResourcePtr cpu2 = ResourceFactory::Create("cpu2", "CPU", 2);
        ResourcePtr gpu0 = ResourceFactory::Create("gpu0", "GPU", 0);
        ResourcePtr gpu1 = ResourceFactory::Create("gpu1", "GPU", 1);

        res_mgr_ = std::make_shared<ResourceMgr>();
        disk_ = res_mgr_->Add(std::move(disk));
        cpu_0_ = res_mgr_->Add(std::move(cpu0));
        cpu_1_ = res_mgr_->Add(std::move(cpu1));
        cpu_2_ = res_mgr_->Add(std::move(cpu2));
        gpu_0_ = res_mgr_->Add(std::move(gpu0));
        gpu_1_ = res_mgr_->Add(std::move(gpu1));
        auto IO = Connection("IO", 5.0);
        auto PCIE = Connection("PCIE", 11.0);
        res_mgr_->Connect("disk", "cpu0", IO);
        res_mgr_->Connect("cpu0", "cpu1", IO);
        res_mgr_->Connect("cpu1", "cpu2", IO);
        res_mgr_->Connect("cpu0", "cpu2", IO);
        res_mgr_->Connect("cpu1", "gpu0", PCIE);
        res_mgr_->Connect("cpu2", "gpu1", PCIE);
    }

    ResourceWPtr disk_;
    ResourceWPtr cpu_0_;
    ResourceWPtr cpu_1_;
    ResourceWPtr cpu_2_;
    ResourceWPtr gpu_0_;
    ResourceWPtr gpu_1_;
    ResourceMgrPtr res_mgr_;
};

TEST_F(AlgorithmTest, SHORTESTPATH_INVALID_PATH_TEST) {
    std::vector<std::string> sp;
    uint64_t cost;
    // disk to disk is invalid
    cost = ShortestPath(disk_.lock(), disk_.lock(), res_mgr_, sp);
    ASSERT_TRUE(sp.empty());

    // cpu_0 to disk is invalid
    cost = ShortestPath(cpu_0_.lock(), disk_.lock(), res_mgr_, sp);
    ASSERT_TRUE(sp.empty());

    // cpu2 to gpu0 is invalid
    cost = ShortestPath(cpu_2_.lock(), gpu_0_.lock(), res_mgr_, sp);
    ASSERT_TRUE(sp.empty());


    // gpu0 to gpu1 is invalid
    cost = ShortestPath(gpu_0_.lock(), gpu_1_.lock(), res_mgr_, sp);
    ASSERT_TRUE(sp.empty());
}

TEST_F(AlgorithmTest, SHORTESTPATH_TEST) {
    std::vector<std::string> sp;
    uint64_t cost;

    //disk to gpu0
    //disk -> cpu0 -> cpu1 -> gpu0
    std::cout << "************************************\n";
    cost = ShortestPath(disk_.lock(), gpu_0_.lock(), res_mgr_, sp);
    ASSERT_EQ(sp.size(), 4);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;

    //disk to gpu1
    //disk -> cpu0 -> cpu2 -> gpu1
    std::cout << "************************************\n";
    cost = ShortestPath(disk_.lock(), gpu_1_.lock(), res_mgr_, sp);
    ASSERT_EQ(sp.size(), 4);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;

    // disk to cpu0
    // disk -> cpu0
    std::cout << "************************************\n";
    cost = ShortestPath(disk_.lock(), cpu_0_.lock(), res_mgr_, sp);
    ASSERT_EQ(sp.size(), 2);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;

    // disk to cpu1
    // disk -> cpu0 -> cpu1
    std::cout << "************************************\n";
    cost = ShortestPath(disk_.lock(), cpu_1_.lock(), res_mgr_, sp);
    ASSERT_EQ(sp.size(), 3);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;

    // disk to cpu2
    // disk -> cpu0 -> cpu2
    std::cout << "************************************\n";
    cost = ShortestPath(disk_.lock(), cpu_2_.lock(), res_mgr_, sp);
    ASSERT_EQ(sp.size(), 3);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;

    // cpu0 to gpu0
    // cpu0 -> cpu1 -> gpu0
    std::cout << "************************************\n";
    cost = ShortestPath(cpu_0_.lock(), gpu_0_.lock(), res_mgr_, sp);
    ASSERT_EQ(sp.size(), 3);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;

    // cpu0 to cpu1
    // cpu0 -> cpu1
    std::cout << "************************************\n";
    cost = ShortestPath(cpu_0_.lock(), cpu_1_.lock(), res_mgr_, sp);
    ASSERT_EQ(sp.size(), 2);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;

    // cpu0 to cpu2
    // cpu0 -> cpu2
    std::cout << "************************************\n";
    cost = ShortestPath(cpu_0_.lock(), cpu_2_.lock(), res_mgr_, sp);
    // ASSERT_EQ(sp.size(), 2);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;

    // cpu0 to gpu1
    // cpu0 -> cpu2 -> gpu1
    std::cout << "************************************\n";
    cost = ShortestPath(cpu_0_.lock(), gpu_1_.lock(), res_mgr_, sp);
    // ASSERT_EQ(sp.size(), 3);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << " ";
        sp.pop_back();
    }
    std::cout << std::endl;
}

}  // namespace scheduler
}  // namespace milvus
