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

// get resources
#ifdef MILVUS_GPU_VERSION
    bool enable_gpu = false;
    server::Config& config = server::Config::GetInstance();
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
            ResMgrInst::GetInstance()->Add(ResourceFactory::Create(std::to_string(gpu_id), "GPU", gpu_id));
            ResMgrInst::GetInstance()->Connect("cpu", std::to_string(gpu_id), pcie);
        }

        for (auto& not_find_id : not_find_build_ids) {
            ResMgrInst::GetInstance()->Add(ResourceFactory::Create(std::to_string(not_find_id), "GPU", not_find_id));
            ResMgrInst::GetInstance()->Connect("cpu", std::to_string(not_find_id), pcie);
        }
    }
#endif
}

void
StartSchedulerService() {
    load_simple_config();
    OptimizerInst::GetInstance()->Init();
    ResMgrInst::GetInstance()->Start();
    SchedInst::GetInstance()->Start();
    JobMgrInst::GetInstance()->Start();
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
