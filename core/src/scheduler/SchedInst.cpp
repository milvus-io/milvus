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

#include "scheduler/SchedInst.h"
#include "ResourceFactory.h"
#include "Utils.h"
#include "config/Config.h"

#include <fiu-local.h>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace milvus {
namespace scheduler {

ResourceMgrPtr ResMgrInst::instance = nullptr;
std::mutex ResMgrInst::mutex_;

SchedulerPtr SchedInst::instance = nullptr;
std::mutex SchedInst::mutex_;

scheduler::JobMgrPtr JobMgrInst::instance = nullptr;
std::mutex JobMgrInst::mutex_;

OptimizerPtr OptimizerInst::instance = nullptr;
std::mutex OptimizerInst::mutex_;

BuildMgrPtr BuildMgrInst::instance = nullptr;
std::mutex BuildMgrInst::mutex_;

CPUBuilderPtr CPUBuilderInst::instance = nullptr;
std::mutex CPUBuilderInst::mutex_;

void
load_simple_config() {
    // create and connect
    ResMgrInst::GetInstance()->Add(ResourceFactory::Create("disk", "DISK", 0, false));

    auto io = Connection("io", 500);
    ResMgrInst::GetInstance()->Add(ResourceFactory::Create("cpu", "CPU", 0));
    ResMgrInst::GetInstance()->Connect("disk", "cpu", io);

#if defined(MILVUS_GPU_VERSION) || defined(MILVUS_FPGA_VERSION)
    server::Config& config = server::Config::GetInstance();
#endif

#ifdef MILVUS_FPGA_VERSION
    bool enable_fpga = false;
    config.GetFpgaResourceConfigEnable(enable_fpga);
    if (enable_fpga) {
        std::vector<int64_t> fpga_ids;
        config.GetFpgaResourceConfigSearchResources(fpga_ids);
        auto pcie = Connection("pcie", 12000);

        for (auto& fpga_id : fpga_ids) {
            LOG_SERVER_DEBUG_ << LogOut("[%ld]", fpga_id);
            std::string fpga_name = "fpga" + std::to_string(fpga_id);
            ResMgrInst::GetInstance()->Add(ResourceFactory::Create(fpga_name, "FPGA", fpga_id));
            ResMgrInst::GetInstance()->Connect("cpu", fpga_name, pcie);
        }
    }

#endif
// get resources
#ifdef MILVUS_GPU_VERSION
    bool enable_gpu = false;
    config.GetGpuResourceConfigEnable(enable_gpu);
    if (enable_gpu) {
        std::vector<int64_t> gpu_ids;
        config.GetGpuResourceConfigSearchResources(gpu_ids);
        std::vector<int64_t> build_gpu_ids;
        config.GetGpuResourceConfigBuildIndexResources(build_gpu_ids);
        auto pcie = Connection("pcie", 12000);
        fiu_do_on("load_simple_config_mock", build_gpu_ids.push_back(1));

        std::vector<int64_t> not_find_build_ids;
        for (auto& build_id : build_gpu_ids) {
            bool find_gpu_id = false;
            for (auto& gpu_id : gpu_ids) {
                if (gpu_id == build_id) {
                    find_gpu_id = true;
                    break;
                }
            }
            if (not find_gpu_id) {
                not_find_build_ids.emplace_back(build_id);
            }
        }

        for (auto& gpu_id : gpu_ids) {
            std::string gpu_name = "gpu" + std::to_string(gpu_id);
            ResMgrInst::GetInstance()->Add(ResourceFactory::Create(gpu_name, "GPU", gpu_id));
            ResMgrInst::GetInstance()->Connect("cpu", gpu_name, pcie);
        }

        for (auto& not_find_id : not_find_build_ids) {
            std::string gpu_name = "gpu" + std::to_string(not_find_id);
            ResMgrInst::GetInstance()->Add(ResourceFactory::Create(gpu_name, "GPU", not_find_id));
            ResMgrInst::GetInstance()->Connect("cpu", gpu_name, pcie);
        }
    }
#endif
}

void
StartSchedulerService() {
    load_simple_config();
    OptimizerInst::GetInstance()->Init();
    ResMgrInst::GetInstance()->Start(); // resevt_thread
    SchedInst::GetInstance()->Start(); // schedevt_thread
    JobMgrInst::GetInstance()->Start(); // jobmgr_thread
    CPUBuilderInst::GetInstance()->Start();
}

void
StopSchedulerService() {
    CPUBuilderInst::GetInstance()->Stop();
    JobMgrInst::GetInstance()->Stop();
    SchedInst::GetInstance()->Stop();
    ResMgrInst::GetInstance()->Stop();
}

}  // namespace scheduler
}  // namespace milvus
