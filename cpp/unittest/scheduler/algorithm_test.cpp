/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include <gtest/gtest.h>

#include "scheduler/resource/Resource.h"
#include "scheduler/ResourceMgr.h"
#include "scheduler/resource/CpuResource.h"
#include "scheduler/ResourceFactory.h"
#include "scheduler/Algorithm.h"

namespace zilliz {
namespace milvus {
namespace engine {

class AlgorithmTest : public testing::Test {
 protected:
    void
    SetUp() override {
        ResourcePtr disk = ResourceFactory::Create("disk", "DISK", 0, true, false);
        ResourcePtr cpu0 = ResourceFactory::Create("cpu0", "CPU", 0, true, true);
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

TEST_F(AlgorithmTest, SHORTESTPATH_TEST) {
    std::vector<std::string> sp;
    uint64_t cost;
    cost = ShortestPath(disk_.lock(), gpu_0_.lock(), res_mgr_, sp);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << std::endl;
        sp.pop_back();
    }

    std::cout << "************************************\n";
    cost = ShortestPath(cpu_0_.lock(), gpu_0_.lock(), res_mgr_, sp);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << std::endl;
        sp.pop_back();
    }

    std::cout << "************************************\n";
    cost = ShortestPath(disk_.lock(), disk_.lock(), res_mgr_, sp);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << std::endl;
        sp.pop_back();
    }

    std::cout << "************************************\n";
    cost = ShortestPath(cpu_0_.lock(), disk_.lock(), res_mgr_, sp);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << std::endl;
        sp.pop_back();
    }

    std::cout << "************************************\n";
    cost = ShortestPath(cpu_2_.lock(), gpu_0_.lock(), res_mgr_, sp);
    while (!sp.empty()) {
        std::cout << sp[sp.size() - 1] << std::endl;
        sp.pop_back();
    }


}


}
}
}